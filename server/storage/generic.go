package storage

import (
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
)

type Object []byte
type ObjectID string

const ZeroID ObjectID = "0000000000000000000000000000000000000000"

func ObjectIDFromRaw(rawid []byte) ObjectID {
	return ObjectID(hex.EncodeToString(rawid))
}

type Reference string

type ObjectType int

const (
	ObjectTypeBad    ObjectType = -1
	ObjectTypeNone   ObjectType = 0
	ObjectTypeCommit ObjectType = 1
	ObjectTypeTree   ObjectType = 2
	ObjectTypeBlob   ObjectType = 3
	ObjectTypeTag    ObjectType = 4
	// 5 is "for future expansion"
	ObjectTypeOfsDelta ObjectType = 6
	ObjectTypeRefDelta ObjectType = 7
	ObjectTypeAny      ObjectType = 8
	ObjectTypeMax      ObjectType = 9
)

func (o ObjectType) String() string {
	return o.HdrName()
}

func (o ObjectType) HdrName() string {
	switch o {
	case ObjectTypeCommit:
		return "commit"
	case ObjectTypeTree:
		return "tree"
	case ObjectTypeBlob:
		return "blob"
	case ObjectTypeTag:
		return "tag"
	case ObjectTypeRefDelta:
		return "refdelta"
	default:
		panic(fmt.Errorf("Invalid object type %s requested", o))
	}
}

func ObjectTypeFromHdrName(hdrname string) ObjectType {
	switch hdrname {
	case "commit":
		return ObjectTypeCommit
	case "tree":
		return ObjectTypeTree
	case "blob":
		return ObjectTypeBlob
	case "tag":
		return ObjectTypeTag
	case "refdelta":
		return ObjectTypeRefDelta
	default:
		panic(fmt.Errorf("Invalid object header %s requested", hdrname))
	}
}

type StorageDriver interface {
	GetProjectStorage(project string) ProjectStorageDriver
}

var ErrObjectNotFound = errors.New("Object not found")

type ProjectStorageDriver interface {
	ReadObject(objectid ObjectID) (ObjectType, uint, io.ReadCloser, error)

	GetPusher(pushuuid string) ProjectStoragePushDriver
}

type ProjectStoragePushDriver interface {
	StageObject(objtype ObjectType, objsize uint) (StagedObject, error)
	GetPushResultChannel() <-chan error
	Done()
}

type StagedObject interface {
	io.Writer
	io.Closer
	Finalize(objid ObjectID) (ObjectID, error)
}

func InitializeStorageDriver(config map[string]string) (StorageDriver, error) {
	clustered, ok := config["clustered"]
	if !ok || clustered != "false" {
		// Unless "clustered" is set to "false", we actually return a clusteredStorageDriver
		// with the real backend
	}
	storagetype, ok := config["type"]
	if !ok {
		return nil, errors.New("No storage driver type provided")
	}

	switch storagetype {
	case "tree":
		dirname, ok := config["directory"]
		if !ok {
			return nil, errors.New("No directory provided")
		}
		return &treeStorageDriverInstance{dirname: dirname}, nil
	default:
		return nil, errors.New("Unknown storage driver " + storagetype)
	}
}

type oidWriter struct {
	hasher hash.Hash
	writer io.Writer
}

func (w *oidWriter) Write(buf []byte) (int, error) {
	return w.writer.Write(buf)
}

func (w *oidWriter) getObjectID() ObjectID {
	return ObjectIDFromRaw(w.hasher.Sum(nil))
}

func createOidWriter(objtype ObjectType, objsize uint, inner io.Writer) *oidWriter {
	hasher := sha1.New()
	fmt.Fprintf(hasher, "%s %d\x00", objtype.HdrName(), objsize)

	w := &oidWriter{
		hasher: hasher,
		writer: io.MultiWriter(hasher, inner),
	}

	return w
}
