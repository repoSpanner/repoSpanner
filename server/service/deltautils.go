package service

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"os"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"repospanner.org/repospanner/server/storage"
)

type resolveInfo struct {
	deltaobj storage.ObjectID
	baseobj  storage.ObjectID
}

func processDeltas(reqlogger *logrus.Entry, deltaqueuesize int, deltasqueue *os.File, pushresultc <-chan error, rw *wrappedResponseWriter, sbstatus sideBandStatus, projectstore storage.ProjectStorageDriver, pusher storage.ProjectStoragePushDriver) error {
	// Resolve deltas
	reqlogger.Debugf("Resolving %d deltas", deltaqueuesize)
	var deltasresolved int
	totaldeltas := deltaqueuesize

	for {
		if deltaqueuesize == 0 {
			return nil
		}

		var newdeltaqueuesize int
		newdeltasqueue, err := ioutil.TempFile("", "repospanner_deltaqueue_ng_")
		if err != nil {
			return errors.Wrap(err, "Error creating new delta queue")
		}
		defer newdeltasqueue.Close()
		defer os.Remove(newdeltasqueue.Name())

		select {
		case syncerr := <-pushresultc:
			return errors.Wrap(syncerr, "Error syncing object to enough peers")

		default:
			if rw.IsClosed() {
				return errors.New("Connection closed")
			}

			reqlogger.Debugf("Next deneration of delta solving: %d", deltaqueuesize)
			madeProgress := false

			n, err := deltasqueue.Seek(0, 0)
			if err != nil {
				// Seeking failed? Weird...
				return errors.Wrap(err, "Seeking in deltaqueue failed")
			}
			if n != 0 {
				return errors.New("Seek did not go to start of file?")
			}

			for {
				if deltasresolved%100 == 0 {
					sendSideBandPacket(
						rw,
						sbstatus,
						sideBandProgress,
						[]byte(fmt.Sprintf(
							"Resolving deltas (%d/%d)...\n",
							deltasresolved,
							totaldeltas,
						)),
					)
				}

				var deltaobj string
				var baseobj string

				_, err := fmt.Fscanf(deltasqueue, "%s %s\n", &deltaobj, &baseobj)
				if err == io.EOF {
					break
				}
				if err != nil {
					return errors.Wrap(err, "Error resolving delta")
				}
				deltaqueuesize--
				toresolve := resolveInfo{
					deltaobj: storage.ObjectID(deltaobj),
					baseobj:  storage.ObjectID(baseobj),
				}

				_, _, err = resolveDelta(projectstore, pusher, toresolve)
				if err == nil {
					// We managed to fully resolve this delta
					deltasresolved++
					madeProgress = true
				} else if err == errNotResolvableDelta {
					// We did not manage to make progress, but this might be fixed the next run
					reqlogger.Debug("Base object was not found, keeping in list")
					newdeltaqueuesize++
					fmt.Fprintf(newdeltasqueue, "%s %s\n", toresolve.deltaobj, toresolve.baseobj)
				} else {
					return errors.Wrap(err, "Error resolving delta")
				}
			}
			if !madeProgress {
				// We did not make any progress, give up
				reqlogger.Debug("Did not make any progress resolving deltas")
				return errors.New("No delta progress was made")
			}

			deltaqueuesize = newdeltaqueuesize
			deltasqueue.Close()
			deltasqueue = newdeltasqueue

			if deltaqueuesize == 0 {
				reqlogger.Debug("Done with all deltas")
				return nil
			}
		}
	}
}

var errNotResolvableDelta = errors.New("Delta base object unresolvable")

func resolveDelta(r storage.ProjectStorageDriver, p storage.ProjectStoragePushDriver, toresolve resolveInfo) (storage.ObjectID, storage.ObjectType, error) {
	baseobjtype, baseobjsize, basereader, err := r.ReadObject(toresolve.baseobj)
	if err == storage.ErrObjectNotFound {
		return storage.ZeroID, storage.ObjectTypeBad, errNotResolvableDelta
	} else if err != nil {
		return storage.ZeroID, storage.ObjectTypeBad, err
	}
	defer basereader.Close()

	deltaobjtype, deltasize, deltareader, err := r.ReadObject(toresolve.deltaobj)
	if err != nil {
		return storage.ZeroID, storage.ObjectTypeBad, err
	}
	defer deltareader.Close()
	if deltaobjtype != storage.ObjectTypeRefDelta {
		return storage.ZeroID, storage.ObjectTypeBad, fmt.Errorf("Invalid object type found for delta: %s", deltaobjtype)
	}

	if deltasize < 2 {
		return storage.ZeroID, storage.ObjectTypeBad, errors.New("Delta object too small")
	}

	objsize, bytesread, err := getDeltaDestSize(deltareader, baseobjsize)
	if err != nil {
		return storage.ZeroID, storage.ObjectTypeBad, err
	}
	deltasize -= bytesread // This is read by getDeltaDestSize

	stager, err := p.StageObject(baseobjtype, objsize)
	if err != nil {
		return storage.ZeroID, 0, err
	}
	defer stager.Close()
	oidwriter := getObjectIDStart(baseobjtype, objsize)
	combwriter := io.MultiWriter(oidwriter, stager)

	err = rebuildDelta(combwriter, deltareader, deltasize, objsize, basereader, baseobjsize)
	if err != nil {
		return storage.ZeroID, 0, err
	}

	objectid := getObjectIDFinish(oidwriter)
	toresolve.deltaobj = objectid

	_, err = stager.Finalize(objectid)
	if err != nil {
		return storage.ZeroID, 0, err
	}

	return objectid, baseobjtype, nil
}

func getDeltaDestSize(delta io.Reader, expectedBaseSize uint) (objsize uint, bytesread uint, err error) {
	var readBaseSize uint
	var read uint
	readBaseSize, read, err = getSizeFromDeltaHeader(delta)
	if err != nil {
		return
	}
	bytesread += read
	if readBaseSize != expectedBaseSize {
		err = fmt.Errorf("Read base size (%d) does not match provided (%d)",
			readBaseSize, expectedBaseSize)
		return
	}
	objsize, read, err = getSizeFromDeltaHeader(delta)
	if err != nil {
		return
	}
	bytesread += read
	return
}

func getSizeFromDeltaHeader(r io.Reader) (objsize uint, bytesread uint, err error) {
	var shift uint
	for {
		buf := make([]byte, 1)
		if _, err = r.Read(buf); err != nil {
			return
		}
		bytesread++
		val := uint(buf[0])

		objsize += (val & 0x7F) << shift
		shift += 7

		if val&0x80 == 00 {
			// No more size bytes
			break
		}
	}
	return
}

func memcpy(dst io.Writer, src []byte, srcbegin, len uint) error {
	var read uint
	for read < len {
		start := srcbegin + read
		end := srcbegin + read + 1024
		if end > srcbegin+len {
			end = srcbegin + len
		}
		buf := src[start:end]
		n, err := dst.Write(buf)
		if err != nil {
			return err
		}
		if n != int(end-start) {
			return errors.Errorf("Not exactly %d bytes copied?", end-start)
		}
		read++
	}
	return nil
}

func rebuildDelta(w io.Writer, delta io.Reader, deltasize uint, destobjsize uint, base io.Reader, basesize uint) error {
	// We need random access in srcbuf.
	srcbuf := make([]byte, basesize)
	n, err := io.ReadFull(base, srcbuf)
	if err != nil {
		return err
	}
	if uint(n) != basesize {
		return errors.New("Not full basebuf was read")
	}

	// We want to ReadByte
	data := bufio.NewReaderSize(delta, 1)

	var written uint
	var pos uint
	for pos < deltasize {
		cmd, err := data.ReadByte()
		if err == io.EOF {
			// Assume that we finished rebuilding the delta
			return nil
		}
		if err != nil {
			return err
		}
		pos++

		if (cmd & 0x80) != 0x00 {
			// Patch opcode
			var cpOff, cpSize uint

			if (cmd & 0x01) != 0x00 {
				bt, err := data.ReadByte()
				if err != nil {
					return err
				}
				pos++
				cpOff = uint(bt)
			}
			if (cmd & 0x02) != 0x00 {
				bt, err := data.ReadByte()
				if err != nil {
					return err
				}
				pos++
				cpOff |= uint(bt) << 8
			}
			if (cmd & 0x04) != 0x00 {
				bt, err := data.ReadByte()
				if err != nil {
					return err
				}
				pos++
				cpOff |= uint(bt) << 16
			}
			if (cmd & 0x08) != 0x00 {
				bt, err := data.ReadByte()
				if err != nil {
					return err
				}
				pos++
				cpOff |= uint(bt) << 24
			}
			if (cmd & 0x10) != 0x00 {
				bt, err := data.ReadByte()
				if err != nil {
					return err
				}
				pos++
				cpSize = uint(bt)
			}
			if (cmd & 0x20) != 0x00 {
				bt, err := data.ReadByte()
				if err != nil {
					return err
				}
				pos++
				cpSize |= uint(bt) << 8
			}
			if (cmd & 0x40) != 0x00 {
				bt, err := data.ReadByte()
				if err != nil {
					return err
				}
				pos++
				cpSize |= uint(bt) << 16
			}
			if cpSize == 0 {
				cpSize = 0x10000
			}

			if ((cpOff + cpSize) > basesize) || (cpSize > (destobjsize - written)) {
				return errors.New("Invalid cpOff/cpSize")
			}

			if err := memcpy(w, srcbuf, cpOff, cpSize); err != nil {
				return err
			}
			written += cpSize
		} else if cmd != 0x00 {
			// Grab <cmd> bytes from base to delta
			ncmd := int(cmd)
			buf := make([]byte, ncmd)
			if _, err := io.ReadFull(data, buf); err != nil {
				return err
			}
			n, err := w.Write(buf)
			if err != nil {
				return errors.Wrap(err, "Error writing into delta dest")
			}
			if n != ncmd {
				return errors.New("Incorrect number of bytes written")
			}
			pos += uint(ncmd)
			written += uint(ncmd)
		} else {
			return errors.New("Unexpected delta opcode 0")
		}
	}

	if written != destobjsize {
		return errors.New("Not full destination object was written")
	}
	return nil
}
