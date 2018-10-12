package service

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"

	"github.com/pkg/errors"

	"github.com/sirupsen/logrus"
	"repospanner.org/repospanner/server/storage"
)

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
				panic(err)
			}
			if n != 0 {
				panic("Seek did not go to start of file?")
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
