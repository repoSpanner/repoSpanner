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

type resolveFailure struct {
	obj resolveInfo
	err error
}

func deltaProcessor(projectstore storage.ProjectStorageDriver, pusher storage.ProjectStoragePushDriver, input <-chan resolveInfo, results chan<- *resolveFailure) {
	for msg := range input {
		_, _, err := resolveDelta(projectstore, pusher, msg)
		if err == nil {
			// We managed to fully resolve this delta
			results <- nil
		} else if err == errNotResolvableDelta {
			// We did not manage to make progress, but this might be fixed the next run
			results <- &resolveFailure{obj: msg}
		} else {
			results <- &resolveFailure{err: err}
		}
	}
}

func getNextDelta(deltasqueue *os.File) (*resolveInfo, error) {
	var deltaobj string
	var baseobj string

	_, err := fmt.Fscanf(deltasqueue, "%s %s\n", &deltaobj, &baseobj)
	if err == io.EOF {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &resolveInfo{
		deltaobj: storage.ObjectID(deltaobj),
		baseobj:  storage.ObjectID(baseobj),
	}, nil

}

func processDeltas(reqlogger *logrus.Entry, deltaqueuesize int, deltasqueue *os.File, pushresultc <-chan error, rw *wrappedResponseWriter, sbstatus sideBandStatus, projectstore storage.ProjectStorageDriver, pusher storage.ProjectStoragePushDriver) error {
	// Resolve deltas
	reqlogger.Debugf("Resolving %d deltas", deltaqueuesize)
	var deltasresolved int
	totaldeltas := deltaqueuesize

	for deltaqueuesize != 0 {
		// This is the generation loop
		reqlogger.Debugf("Next generation of delta solving: %d", deltaqueuesize)

		// Prepare and start the workers
		input := make(chan resolveInfo)
		defer func() {
			if input != nil {
				close(input)
			}
		}()
		results := make(chan *resolveFailure)
		// TODO: Figure out how to decide how many workers to start
		for i := 0; i < 4; i++ {
			go deltaProcessor(projectstore, pusher, input, results)
		}

		var newdeltaqueuesize int
		newdeltasqueue, err := ioutil.TempFile("", "repospanner_deltaqueue_ng_")
		if err != nil {
			return errors.Wrap(err, "Error creating new delta queue")
		}
		defer newdeltasqueue.Close()
		defer os.Remove(newdeltasqueue.Name())

		n, err := deltasqueue.Seek(0, 0)
		if err != nil {
			// Seeking failed? Weird...
			return err
		}
		if n != 0 {
			return errors.New("Delta queue seeking did not go to start of file")
		}

		madeProgress := false
		nextDelta, err := getNextDelta(deltasqueue)
		if err != nil {
			return err
		}
		if nextDelta == nil {
			// Nothing to queue in this generation, we must be done
			if deltaqueuesize == 0 {
				return nil
			}
			return errors.New("No further entries but non-empty queue")
		}
		deltaqueuesize--

		inflight := 0
		nextToSend := *nextDelta
		lastprint := -1
		for {
			if deltasresolved%100 == 0 && deltasresolved != lastprint {
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
				lastprint = deltasresolved
			}

			select {
			case syncerr := <-pushresultc:
				// Make sure that the workers terminate
				if input != nil {
					close(input)
				}
				return errors.Wrap(syncerr, "Error syncing object to enough peers")

			case resolveres := <-results:
				inflight--
				if resolveres == nil {
					deltasresolved++
					madeProgress = true
					// wg.Done()
				} else if resolveres.err == nil {
					// This is a requeued object: just didn't have a base
					reqlogger.Debug("Base object was not found, keeping in list")
					newdeltaqueuesize++
					fmt.Fprintf(newdeltasqueue, "%s %s\n", resolveres.obj.deltaobj, resolveres.obj.baseobj)
				} else {
					// This was a resolve error
					return errors.Wrap(err, "Error resolving delta")
				}

			case input <- nextToSend:
				// Get next to send
				inflight++
				if rw.IsClosed() {
					return errors.New("Connection closed")
				}
				nextDelta, err = getNextDelta(deltasqueue)
				if err != nil {
					return err
				}
				if nextDelta == nil {
					reqlogger.Debug("Generation entries all submitted")
					close(input)
					input = nil
				} else {
					nextToSend = *nextDelta
				}
			}

			if inflight == 0 && input == nil {
				// Nothing more in flight for this generation: move on to the next
				break
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
	}

	return nil
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
		read += (end - start)
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
