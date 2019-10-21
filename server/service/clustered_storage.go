package service

import (
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"path"
	"strconv"
	"sync"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"repospanner.org/repospanner/server/storage"
)

const maxAttempts = 3

var errDupeObject = errors.New("clustered_storage: Duplicated object ignored")

type clusterStorageDriverInstance struct {
	inner storage.StorageDriver
	cfg   *Service
}

func (d *clusterStorageDriverInstance) GetProjectStorage(project string) storage.ProjectStorageDriver {
	return &clusterStorageProjectDriverInstance{
		d:       d,
		project: project,
		inner:   d.inner.GetProjectStorage(project),
	}
}

type clusterStorageProjectDriverInstance struct {
	d       *clusterStorageDriverInstance
	project string
	inner   storage.ProjectStorageDriver
}

type objectPusherInfo struct {
	objid   storage.ObjectID
	reschan chan bool
}

type clusterStorageProjectPushDriverInstance struct {
	d *clusterStorageProjectDriverInstance

	pushuuid string

	errchan chan error
	// This lets us wait until all the nodes are done syncing
	outstandingNodes *sync.WaitGroup

	objectSyncMux *sync.Mutex
	// This maps peer node ids to a bool that indicates whether there was a failure on syncing any
	// objects to the node or not. This is used to determine at the end of a push whether the push
	// objects successfully made it to a majority of nodes. It is also used to count the
	// number of nodes we have to push to. A true means that no errors have been detected, and
	// false means that at least one object has failed to sync to the given node.
	objectSyncPeerSuccesses map[uint64]bool
	// These are queues that we should push ObjectIDs into when we want to tell nodes to sync them
	objectSyncPeerQueues []chan<- storage.ObjectID
	syncersWg            *sync.WaitGroup
	hasFailed            bool

	innerPusher storage.ProjectStoragePushDriver
}

type clusterStorageDriverObject struct {
	driver     *clusterStorageProjectDriverInstance
	objectid   storage.ObjectID
	sourcenode uint64
	staged     storage.StagedObject
	body       io.ReadCloser
}

type clusterStorageDriverStagedObject struct {
	driver  *clusterStorageProjectPushDriverInstance
	inner   storage.StagedObject
	syncout bool
}

func (o *clusterStorageDriverObject) Read(p []byte) (in int, err error) {
	in, err = o.body.Read(p)
	if err != nil && err != io.EOF {
		return
	}

	if in > 0 {
		var out int
		var outerr error
		out, outerr = o.staged.Write(p[:in])
		if outerr != nil {
			return in, outerr
		}
		if in != out {
			return in, errors.Errorf("Invalid number of bytes staged while streaming: %d != %d",
				in,
				out,
			)
		}
	}

	if err == io.EOF {
		// End of object, finalize
		_, err = o.staged.Finalize(o.objectid)
		if err != nil {
			return in, err
		}
		return in, io.EOF
	}
	return
}

func (o *clusterStorageDriverObject) Close() error {
	return o.staged.Close()
}

func (d *clusterStorageProjectDriverInstance) tryObjectFromNode(objectid storage.ObjectID, nodeid uint64) (storage.ObjectType, uint, io.ReadCloser, error) {
	if nodeid == d.d.cfg.nodeid || nodeid == 0 {
		return storage.ObjectTypeBad, 0, nil, storage.ErrObjectNotFound
	}
	objecturl := d.d.cfg.GetPeerURL(nodeid, "/rpc/object/single/"+d.project+".git/"+string(objectid))

	logger := d.d.cfg.log.WithFields(logrus.Fields{
		"project":  d.project,
		"objectid": objectid,
		"peer":     nodeid,
		"url":      objecturl,
	})
	logger.Debug("Trying to get object from peer")

	var resp *http.Response
	attempt := 1
	for resp == nil {
		var err error
		resp, err = d.d.cfg.rpcClient.Get(objecturl)
		if err != nil {
			if attempt < maxAttempts {
				// Retry
				attempt++
				if attempt >= maxAttempts {
					return storage.ObjectTypeBad, 0, nil, err
				}
				logger.WithError(err).Debugf("Failed, retrying %d/%d", attempt, maxAttempts)
				continue
			}
			return storage.ObjectTypeBad, 0, nil, err
		}
		if resp.StatusCode == http.StatusNotFound {
			resp.Body.Close()
			return storage.ObjectTypeBad, 0, nil, storage.ErrObjectNotFound
		}
		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			return storage.ObjectTypeBad, 0, nil, errors.Errorf("Status code: %d, status message: %s",
				resp.StatusCode,
				resp.Status,
			)
		}
	}

	// We got an object! Stream and store
	logger.Debug("Peer returned object")

	objtypes, hasobjtype := resp.Header["X-Objecttype"]
	if !hasobjtype || len(objtypes) != 1 {
		resp.Body.Close()
		return storage.ObjectTypeBad, 0, nil, errors.New("No ObjectType header returned")
	}
	objtype := storage.ObjectTypeFromHdrName(objtypes[0])

	objsizes, hasobjsize := resp.Header["X-Objectsize"]
	if !hasobjsize || len(objsizes) != 1 {
		resp.Body.Close()
		return storage.ObjectTypeBad, 0, nil, errors.New("No ObjectSize header returned")
	}
	objsize := objsizes[0]
	objsizei, err := strconv.ParseUint(objsize, 10, 64)
	if err != nil {
		resp.Body.Close()
		return storage.ObjectTypeBad, 0, nil, err
	}

	// TODO: On Close, handle the pusher results for non-tree drivers
	pusher := d.inner.GetPusher("")
	staged, err := pusher.StageObject(objtype, uint(objsizei))
	if err != nil {
		resp.Body.Close()
		return storage.ObjectTypeBad, 0, nil, err
	}
	logger.Debug("Streaming object from peer")
	return objtype, uint(objsizei), &clusterStorageDriverObject{
		driver:     d,
		objectid:   objectid,
		sourcenode: nodeid,
		staged:     staged,
		body:       resp.Body,
	}, nil
}

type clusteredStorageNoLister int

func (d *clusterStorageProjectDriverInstance) ListObjects() storage.ProjectStorageObjectLister {
	return clusteredStorageNoLister(42)
}

func (l clusteredStorageNoLister) Objects() <-chan storage.ObjectID {
	c := make(chan storage.ObjectID)
	close(c)
	return c
}

func (l clusteredStorageNoLister) Err() error {
	return errors.New("Clustered storage driver cannot list objects")
}

func (d *clusterStorageProjectDriverInstance) ReadObject(objectid storage.ObjectID) (storage.ObjectType, uint, io.ReadCloser, error) {
	logger := d.d.cfg.log.WithFields(logrus.Fields{
		"objectid": objectid,
		"project":  d.project,
	})

	objtype, objsize, reader, err := d.inner.ReadObject(objectid)
	if err == storage.ErrObjectNotFound {
		// Okay, now we get to perform our clustered magic

		// First try at the node that got the last push for this project.
		// We are hopeful that it will be the most up-to-date
		primarypeer := d.d.cfg.statestore.GetLastPushNode(d.project)
		objtype, objsize, reader, err := d.tryObjectFromNode(objectid, primarypeer)
		if err == nil {
			return objtype, objsize, reader, err
		} else if err != storage.ErrObjectNotFound {
			logger.WithError(err).Info("Error retrieving object from primary peer")
		}

		// If that didn't have the object (or crashed), fall back
		for k := range d.d.cfg.statestore.Peers {
			objtype, objsize, reader, err := d.tryObjectFromNode(objectid, k)
			if err == nil {
				return objtype, objsize, reader, err
			} else if err != storage.ErrObjectNotFound {
				logger.WithError(err).Info("Error retrieving object from peer")
			}
		}

		logger.Info("Requested object could not be found at any peer")

		return storage.ObjectTypeBad, 0, nil, storage.ErrObjectNotFound
	}

	// The underlying driver gave something that's not NoObject, return
	return objtype, objsize, reader, err
}

func (d *clusterStorageProjectPushDriverInstance) pushSingleObject(peerid uint64, objid storage.ObjectID) error {
	return d.d.d.cfg.syncSingleObject(
		peerid,
		d.d.inner,
		d.d.project,
		objid,
	)
}

// Sync all objects queued in the database for the given peer. peerid is used to specify which peer
// to sync. taskQueue is a chan of ObjectIDs that need to be synced with this peer.
func (d *clusterStorageProjectPushDriverInstance) runPeerSyncer(peerid uint64, taskQueue <-chan storage.ObjectID) {
	d.syncersWg.Add(1)
	defer d.syncersWg.Done()

	mylog := d.d.d.cfg.log.WithFields(logrus.Fields{
		"pushuuid": d.pushuuid,
		"syncpeer": peerid,
	})

	var retryfile *os.File
	retrydir := path.Join(
		d.d.d.cfg.statestore.directory,
		"async-outqueues",
		"peer-"+strconv.FormatUint(peerid, 16),
	)
	retryfilename := path.Join(
		retrydir,
		d.pushuuid,
	)

	mylog.Debug("Starting peer syncer")

	for nextentry := range taskQueue {
		mylog.Debug("Attempted to get an entry to sync")

		mylog := mylog.WithField("objectid", nextentry)
		mylog.Debug("Pushing single object to peer")

		err := d.pushSingleObject(peerid, nextentry)
		mylog.Debug("Results are in", "result", err)
		if err == nil {
			mylog.Debug("Object synced")
		} else {
			mylog.WithError(err).Info("Error during object sync")

			if retryfile == nil {
				if _, err := os.Stat(retrydir); os.IsNotExist(err) {
					err := os.MkdirAll(retrydir, 0775)
					if err != nil {
						mylog.WithError(err).Info("Error creating retry dir")
						break
					}
				}
				var err error
				retryfile, err = os.Create(retryfilename + ".inprogress")
				if err != nil {
					mylog.WithError(err).Info("Error creating retry file")
					retryfile = nil
				} else {
					fmt.Fprintf(retryfile, "project:%s\n", d.d.project)
				}
			}
			if retryfile != nil {
				fmt.Fprintln(retryfile, nextentry)
			}

			mylog.WithError(err).Info("Error syncing object to peer, queued for async")
		}

		if err != nil {
			// Since there was an error syncing this object, we need to mark the node as failed.
			d.objectSyncPeerSuccesses[peerid] = false
		}
	}

	// We are done with the queue
	mylog.Debug("We have synced everything out")
	d.outstandingNodes.Done()

	// We have been told we are all done.

	if retryfile != nil {
		// If we still had any entries we need to retry, rename the retryfile
		err := retryfile.Close()
		if err != nil {
			mylog.WithError(err).Info("Error storing async retry queue")
			return
		}
		err = os.Rename(retryfilename+".inprogress", retryfilename)
		if err != nil {
			mylog.WithError(err).Info("Error renaming async retry queue")
		}
	}
}

func (d *clusterStorageProjectPushDriverInstance) addPeer(peerid uint64) <-chan storage.ObjectID {
	// We will set all nodes to success by default, and they will get marked as failing if any
	// object fails to sync to them.
	d.objectSyncPeerSuccesses[peerid] = true
	peerQueuePath := path.Join(d.d.d.cfg.statestore.directory, "objectsyncs", fmt.Sprintf("%s_%d.queue", d.pushuuid, peerid))
	send, recv := NewObjectIDBufferedChan(peerQueuePath, d.errchan)
	d.objectSyncPeerQueues = append(d.objectSyncPeerQueues, send)
	d.outstandingNodes.Add(1)
	return recv
}

func (d *clusterStorageProjectDriverInstance) GetPusher(pushuuid string) storage.ProjectStoragePushDriver {
	inst := &clusterStorageProjectPushDriverInstance{
		d:                d,
		errchan:          make(chan error, 5),
		outstandingNodes: new(sync.WaitGroup),
		pushuuid:         pushuuid,

		objectSyncPeerSuccesses: make(map[uint64]bool, 0),
		objectSyncMux:           new(sync.Mutex),

		syncersWg: new(sync.WaitGroup),

		innerPusher: d.inner.GetPusher(pushuuid),
	}

	syncwg := new(sync.WaitGroup)

	for peerid := range d.d.cfg.statestore.Peers {
		if peerid != d.d.cfg.statestore.NodeID {
			syncwg.Add(1)
			peerTaskQueue := inst.addPeer(peerid)

			go func(peerid uint64) {
				syncwg.Done()
				inst.runPeerSyncer(peerid, peerTaskQueue)
			}(peerid)
		}
	}

	syncwg.Wait()
	return inst
}

func (d *clusterStorageProjectPushDriverInstance) StageObject(objtype storage.ObjectType, objsize uint) (storage.StagedObject, error) {
	if d.hasFailed {
		return nil, errors.New("Pusher has already failed prior")
	}
	// For writing, just write directly to underlying storage
	inner, err := d.innerPusher.StageObject(objtype, objsize)
	isdelta := false
	if objtype == storage.ObjectTypeOfsDelta || objtype == storage.ObjectTypeRefDelta {
		isdelta = true
	}
	return &clusterStorageDriverStagedObject{
		driver:  d,
		inner:   inner,
		syncout: !isdelta,
	}, err
}

func (d *clusterStorageProjectPushDriverInstance) GetPushResultChannel() <-chan error {
	return d.errchan
}

func (d *clusterStorageProjectPushDriverInstance) Done() {
	go func() {
		d.d.d.cfg.log.Debug("Marking pusher as done")

		for _, taskQueue := range d.objectSyncPeerQueues {
			// If we don't do this, then the worker goroutines will wait forever for new tasks.
			// Let's not be so mean to them and do the right thing and let them know we are done.
			close(taskQueue)
		}

		// Wait until all the nodes are done syncing.
		d.outstandingNodes.Wait()
		numpeers := len(d.objectSyncPeerSuccesses)
		allowedFailures := numpeers - int(math.Floor(float64(numpeers+1)/2.0))
		// Count how many failures we've seen.
		numFailures := 0
		for _, value := range d.objectSyncPeerSuccesses {
			if value == false {
				numFailures++
			}
		}
		if numFailures > allowedFailures {
			// Too many nodes have failed to sync objects. We need to indicate that the push has
			// failed by sending an error to errchan.
			d.errchan <- errors.New("Not enough nodes succeeded")
		}
		d.d.d.cfg.log.Debug("Required set of nodes have objects")

		close(d.errchan)
		d.d.d.cfg.log.Debug("Syncer is done")

		go func(waiter *sync.WaitGroup) {
			// Make sure we only close the database after all syncers are done with it
			waiter.Wait()
		}(d.syncersWg)
	}()
}

func (d *clusterStorageProjectPushDriverInstance) startObjectSync(objid storage.ObjectID) {
	if d.hasFailed {
		return
	}
	if len(d.objectSyncPeerSuccesses) != 0 {
		// We need to add objid to each node's tasks queue for syncing.
		for _, taskQueue := range d.objectSyncPeerQueues {
			taskQueue <- objid
		}
	}
}

func (o *clusterStorageDriverStagedObject) Write(p []byte) (int, error) {
	return o.inner.Write(p)
}

func (o *clusterStorageDriverStagedObject) Close() error {
	return o.inner.Close()
}

func (o *clusterStorageDriverStagedObject) Finalize(objid storage.ObjectID) (storage.ObjectID, error) {
	calced, err := o.inner.Finalize(objid)
	if err != nil {
		return storage.ZeroID, err
	}

	if o.syncout {
		o.driver.startObjectSync(calced)
	}
	return calced, nil
}
