package storage

import (
	"compress/gzip"
	"compress/zlib"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"

	"github.com/pkg/errors"
)

type compressMethod int

const (
	compressMethodNone compressMethod = 0
	compressMethodGzip                = 1
	compressMethodZlib                = 2
)

type treeStorageDriverInstance struct {
	dirname        string
	compressmethod compressMethod
}

func newTreeStoreDriver(dir, comp string) (*treeStorageDriverInstance, error) {
	inst := &treeStorageDriverInstance{dirname: dir}
	if comp == "none" {
		inst.compressmethod = compressMethodNone
	} else if comp == "gzip" {
		inst.compressmethod = compressMethodGzip
	} else if comp == "zlib" {
		inst.compressmethod = compressMethodZlib
	} else {
		return nil, errors.Errorf("Invalid compressiong method %s", comp)
	}
	return inst, nil
}

type nopWriter struct{ w io.Writer }

func (w *nopWriter) Write(buf []byte) (int, error) {
	return w.w.Write(buf)
}
func (w *nopWriter) Close() error {
	return nil
}

func (d *treeStorageDriverInstance) compressExtension() string {
	if d.compressmethod == compressMethodGzip {
		return ".gz"
	} else if d.compressmethod == compressMethodZlib {
		return ".xz"
	}
	return ""
}

func (d *treeStorageDriverInstance) compressReader(r io.Reader) (io.ReadCloser, error) {
	if d.compressmethod == compressMethodGzip {
		return gzip.NewReader(r)
	} else if d.compressmethod == compressMethodZlib {
		return zlib.NewReader(r)
	}
	return ioutil.NopCloser(r), nil
}

func (d *treeStorageDriverInstance) compressWriter(w io.Writer) io.WriteCloser {
	if d.compressmethod == compressMethodGzip {
		return gzip.NewWriter(w)
	} else if d.compressmethod == compressMethodZlib {
		return zlib.NewWriter(w)
	}
	return &nopWriter{w: w}
}

func (d *treeStorageDriverInstance) GetProjectStorage(project string) ProjectStorageDriver {
	return &treeStorageProjectDriverInstance{t: d, p: project}
}

type treeStorageProjectDriverInstance struct {
	t *treeStorageDriverInstance
	p string
}

type treeStorageProjectPushDriverInstance struct {
	t *treeStorageProjectDriverInstance
	c chan error
}

type treeStorageProjectDriverStagedObject struct {
	p         *treeStorageProjectDriverInstance
	f         *os.File
	w         *oidWriter
	finalized bool
}

func (t *treeStorageProjectDriverInstance) getObjPath(objectid ObjectID) string {
	sobjectid := string(objectid)
	objdir := path.Join(t.t.dirname, t.p, sobjectid[:2])
	return path.Join(objdir, sobjectid[2:]) + t.t.compressExtension()
}

type innerReadCloser struct {
	file *os.File
	read io.ReadCloser
}

func (i *innerReadCloser) Read(p []byte) (int, error) {
	return i.read.Read(p)
}

func (i *innerReadCloser) Close() error {
	err := i.read.Close()
	if err != nil {
		return err
	}
	return i.file.Close()
}

func (t *treeStorageProjectDriverInstance) ReadObject(objectid ObjectID) (ObjectType, uint, io.ReadCloser, error) {
	f, err := os.Open(t.getObjPath(objectid))
	if err != nil {
		if os.IsNotExist(err) {
			return ObjectTypeBad, 0, nil, ErrObjectNotFound
		}
		return ObjectTypeBad, 0, nil, err
	}

	r, err := t.t.compressReader(f)
	if err != nil {
		return ObjectTypeBad, 0, nil, err
	}
	read := &innerReadCloser{file: f, read: r}

	var hdr string
	var len uint
	fmt.Fscanf(read, "%s %d", &hdr, &len)
	objtype := ObjectTypeFromHdrName(hdr)

	return objtype, len, read, nil
}

func (t *treeStorageProjectDriverInstance) GetPusher(_ string) ProjectStoragePushDriver {
	return &treeStorageProjectPushDriverInstance{
		t: t,
		c: make(chan error),
	}
}

func (t *treeStorageProjectPushDriverInstance) Done() {
	close(t.c)
}

func (t *treeStorageProjectPushDriverInstance) GetPushResultChannel() <-chan error {
	return t.c
}

func (t *treeStorageProjectPushDriverInstance) StageObject(objtype ObjectType, objsize uint) (StagedObject, error) {
	f, err := ioutil.TempFile(t.t.t.dirname, t.t.p+"_stage_")
	if os.IsNotExist(err) {
		// Seems the project folder didn't exist yet, create it
		err = os.MkdirAll(path.Join(t.t.t.dirname, t.t.p), 0755)
		if err != nil {
			return nil, err
		}
		f, err = ioutil.TempFile(t.t.t.dirname, t.t.p+"_stage_")
	}

	if err != nil {
		return nil, err
	}

	w := t.t.t.compressWriter(f)
	fmt.Fprintf(w, "%s %d\x00", objtype.HdrName(), objsize)

	return &treeStorageProjectDriverStagedObject{
		p:         t.t,
		f:         f,
		w:         createOidWriter(objtype, objsize, w),
		finalized: false,
	}, nil
}

func (t *treeStorageProjectDriverStagedObject) Write(buf []byte) (int, error) {
	return t.w.Write(buf)
}

func (t *treeStorageProjectDriverStagedObject) Finalize(objid ObjectID) (ObjectID, error) {
	err := t.w.Close()
	if err != nil {
		return ZeroID, err
	}
	err = t.f.Close()
	if err != nil {
		return ZeroID, err
	}

	calced := t.w.getObjectID()

	if objid != ZeroID && calced != objid {
		return ZeroID, errors.Errorf("Calculated object does not match provided: %s != %s",
			calced,
			objid,
		)
	}

	destpath := t.p.getObjPath(calced)

	if _, err := os.Stat(destpath); os.IsNotExist(err) {
		// File did not yet exist, write it
		err := os.Rename(t.f.Name(), t.p.getObjPath(calced))
		if os.IsNotExist(err) {
			err = os.MkdirAll(path.Dir(destpath), 0755)
			if err != nil {
				return ZeroID, err
			}
			err = os.Rename(t.f.Name(), t.p.getObjPath(calced))
		}
		if err != nil {
			return ZeroID, err
		}
		t.finalized = true
		return calced, nil
	}

	// The object already existed, call our Close to just remove the stage
	return calced, t.Close()
}

func (t *treeStorageProjectDriverStagedObject) Close() error {
	if t.finalized {
		return nil
	}
	// If we got here, we were closed without finalizing. Toss
	t.f.Close()
	t.finalized = true
	return os.Remove(t.f.Name())
}
