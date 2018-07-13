package service

import (
	"database/sql"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"path"
	"strconv"
	"sync"

	_ "github.com/mattn/go-sqlite3"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"repospanner.org/repospanner/server/storage"
)

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

	errchan            chan error
	outstandingobjects *sync.WaitGroup

	objectSyncMux          *sync.Mutex
	objectSyncPeers        []uint64
	objectSyncDBPath       string
	objectSyncDB           *sql.DB
	objectSyncAllSubmitted bool
	objectSyncNewObjects   *sync.Cond
	syncersWg              *sync.WaitGroup
}

type clusterStorageDriverObject struct {
	driver     *clusterStorageProjectDriverInstance
	objectid   storage.ObjectID
	sourcenode uint64
	staged     storage.StagedObject
	body       io.ReadCloser
}

type clusterStorageDriverStagedObject struct {
	driver *clusterStorageProjectPushDriverInstance
	inner  storage.StagedObject
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
	objecturl := d.d.cfg.GetPeerURL(nodeid, "/rpc/object/single/"+d.project+".git/"+string(objectid))

	logger := d.d.cfg.log.WithFields(logrus.Fields{
		"project":  d.project,
		"objectid": objectid,
		"peer":     nodeid,
		"url":      objecturl,
	})
	logger.Debug("Trying to get object from peer")

	resp, err := d.d.cfg.rpcClient.Get(objecturl)
	if err != nil {
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

func (d *clusterStorageProjectPushDriverInstance) runPeerSyncer(peerid uint64) {
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

	for {
		nextentry, err := d.dbGetNextObject(peerid)
		if err != nil {
			d.errchan <- err
			return
		}

		mylog.Debug("Attempted to get an entry to sync")

		if nextentry == storage.ZeroID {
			// Nothing in the queue for us at this moment
			d.objectSyncNewObjects.L.Lock()
			if d.objectSyncAllSubmitted {
				d.objectSyncNewObjects.L.Unlock()
				mylog.Debug("We have synced everything out")

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
						return
					}
				}

				return
			} else {
				// We have no object right now, but more might be inbound.
				// Wait until we get notified the queues have changed, and then retry
				mylog.Debug("Nothing to sync out, but not done. Wait")
				d.objectSyncNewObjects.Wait()
				d.objectSyncNewObjects.L.Unlock()
				mylog.Debug("We were kicked")
				continue
			}
		}

		mylog := mylog.WithField("objectid", nextentry)
		mylog.Debug("Pushing single object to peer")

		res := d.pushSingleObject(peerid, nextentry)
		mylog.Debug("Results are in", "result", res)
		if res == nil {
			mylog.Debug("Object synced")
		} else {
			mylog.WithError(err).Info("Error during object sync")

			if retryfile == nil {
				if _, err := os.Stat(retrydir); os.IsNotExist(err) {
					err := os.MkdirAll(retrydir, 0755)
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

			mylog.WithError(res).Info("Error syncing object to peer, queued for async")
		}

		err = d.dbReportObject(nextentry, peerid, res == nil)
		if err != nil {
			d.errchan <- err
		}
	}
}

func (d *clusterStorageProjectPushDriverInstance) dbPrepare() error {
	_, err := d.objectSyncDB.Exec(
		`CREATE TABLE syncstatus (
			objectid TEXT PRIMARY KEY NOT NULL,
			alerted INTEGER NOT NULL,
			neededpeers INTEGER NOT NULL,
			outstanding INTEGER NOT NULL
		)`,
	)
	if err != nil {
		return err
	}
	_, err = d.objectSyncDB.Exec(
		`CREATE UNIQUE INDEX objididx ON syncstatus (objectid)`,
	)
	if err != nil {
		return err
	}
	return nil
}

func (d *clusterStorageProjectPushDriverInstance) dbPeerColumn(peerid uint64) string {
	return "node_" + strconv.Itoa(int(peerid)) + "_queue"
}

func (d *clusterStorageProjectPushDriverInstance) dbAddPeer(peerid uint64) error {
	_, err := d.objectSyncDB.Exec(
		`CREATE TABLE ` + d.dbPeerColumn(peerid) + ` (objectid TEXT PRIMARY KEY NOT NULL)`,
	)
	d.objectSyncPeers = append(d.objectSyncPeers, peerid)
	return err
}

func (d *clusterStorageProjectPushDriverInstance) dbAddObject(objid storage.ObjectID) error {
	d.objectSyncMux.Lock()
	defer d.objectSyncMux.Unlock()

	numpeers := len(d.objectSyncPeers)
	neededpeers := int(math.Floor(float64(numpeers+1) / 2.0))

	tx, err := d.objectSyncDB.Begin()
	if err != nil {
		return err
	}
	_, err = tx.Exec(
		`INSERT INTO syncstatus (objectid, alerted, neededpeers, outstanding) VALUES ($1, 0, $2, $3)`,
		objid,
		neededpeers,
		numpeers,
	)
	if err != nil {
		tx.Rollback()
		return err
	}
	d.d.d.cfg.log.WithFields(logrus.Fields{
		"objid":       objid,
		"neededpeers": neededpeers,
		"numpeers":    numpeers,
	}).Debug("Inserted object into syncstatus")
	for _, peerid := range d.objectSyncPeers {
		_, err := tx.Exec(
			`INSERT INTO `+d.dbPeerColumn(peerid)+` (objectid) VALUES ($1)`,
			objid,
		)
		if err != nil {
			tx.Rollback()
			return err
		}
	}
	err = tx.Commit()
	d.d.d.cfg.log.Debug("Kicking syncers")
	d.objectSyncNewObjects.Broadcast()
	d.d.d.cfg.log.Debug("Syncers kicked")
	return err
}

func (d *clusterStorageProjectPushDriverInstance) dbGetNextObject(nodeid uint64) (storage.ObjectID, error) {
	d.objectSyncMux.Lock()
	defer d.objectSyncMux.Unlock()

	tx, err := d.objectSyncDB.Begin()
	if err != nil {
		return storage.ZeroID, err
	}
	row := tx.QueryRow("SELECT objectid FROM " + d.dbPeerColumn(nodeid) + " LIMIT 1")
	var objidS string
	err = row.Scan(&objidS)
	if err == sql.ErrNoRows {
		tx.Rollback()
		return storage.ZeroID, nil
	}
	if err != nil {
		tx.Rollback()
		return storage.ZeroID, err
	}
	res, err := tx.Exec(
		`DELETE FROM `+d.dbPeerColumn(nodeid)+` WHERE objectid=$1`, objidS,
	)
	if err != nil {
		tx.Rollback()
		return storage.ZeroID, err
	}
	aff, err := res.RowsAffected()
	if err != nil {
		tx.Rollback()
		return storage.ZeroID, err
	}
	if aff != 1 {
		tx.Rollback()
		return storage.ZeroID, errors.Errorf("Invalid number of rows affected in delete: %d != 1", aff)
	}
	return storage.ObjectID(objidS), tx.Commit()
}

func (d *clusterStorageProjectPushDriverInstance) dbReportObject(objid storage.ObjectID, nodeid uint64, success bool) error {
	d.d.d.cfg.log.Debug("Starting report")
	d.objectSyncMux.Lock()
	defer d.objectSyncMux.Unlock()
	d.d.d.cfg.log.Debug("Got locks report")

	tx, err := d.objectSyncDB.Begin()
	if err != nil {
		return err
	}
	row := tx.QueryRow(
		"SELECT alerted,neededpeers,outstanding FROM syncstatus WHERE objectid=$1",
		objid,
	)
	var alerted, neededpeers, outstanding int
	err = row.Scan(&alerted, &neededpeers, &outstanding)
	if err != nil {
		tx.Rollback()
		return err
	}
	outstanding--
	if success {
		neededpeers--
	}
	if alerted == 1 {
		// We have already notified people of the result of this object push
		tx.Rollback()
		return nil
	}
	if neededpeers <= 0 {
		// We have reached low enough
		d.d.d.cfg.log.Debug("Required number of peers synced")
		d.outstandingobjects.Done()
		alerted = 1
	} else if outstanding < neededpeers {
		// We still need more successes than we will ever get (happens if too many peers fail sync)
		d.d.d.cfg.log.Info("Impossible to get to needed number of peers now")
		d.errchan <- errors.Errorf(
			"Too many nodes fail. Needed: %d, outstanding: %d",
			neededpeers,
			outstanding,
		)
		d.outstandingobjects.Done()
		alerted = 1
	}
	res, err := tx.Exec(
		`UPDATE syncstatus SET alerted=$1,neededpeers=$2,outstanding=$3 WHERE objectid=$4`,
		alerted,
		neededpeers,
		outstanding,
		objid,
	)
	if err != nil {
		tx.Rollback()
		return err
	}
	aff, err := res.RowsAffected()
	if err != nil {
		tx.Rollback()
		return err
	}
	if aff != 1 {
		tx.Rollback()
		return errors.Errorf("Invalid number of rows affected in update: %d != 1", aff)
	}
	err = tx.Commit()
	if err != nil {
		return err
	}

	return nil
}

func (d *clusterStorageProjectDriverInstance) GetPusher(pushuuid string) storage.ProjectStoragePushDriver {
	dbpath := path.Join(d.d.cfg.statestore.directory, "objectsyncs", pushuuid) + ".db"

	db, err := sql.Open(
		"sqlite3",
		"file:"+dbpath+"?cache=shared&mode=memory",
	)
	if err != nil {
		panic(err)
	}
	if err := db.Ping(); err != nil {
		panic(err)
	}

	inst := &clusterStorageProjectPushDriverInstance{
		d:                  d,
		errchan:            make(chan error),
		outstandingobjects: new(sync.WaitGroup),
		pushuuid:           pushuuid,

		objectSyncAllSubmitted: false,
		objectSyncPeers:        make([]uint64, 0),
		objectSyncMux:          new(sync.Mutex),
		objectSyncDBPath:       dbpath,
		objectSyncDB:           db,
		objectSyncNewObjects:   sync.NewCond(new(sync.Mutex)),

		syncersWg: new(sync.WaitGroup),
	}

	if err := inst.dbPrepare(); err != nil {
		panic(err)
	}

	syncwg := new(sync.WaitGroup)

	for peerid := range d.d.cfg.statestore.Peers {
		if peerid != d.d.cfg.statestore.NodeID {
			syncwg.Add(1)
			inst.dbAddPeer(peerid)

			go func(peerid uint64) {
				syncwg.Done()
				inst.runPeerSyncer(peerid)
			}(peerid)
		}
	}

	syncwg.Wait()
	return inst
}

func (d *clusterStorageProjectPushDriverInstance) StageObject(objtype storage.ObjectType, objsize uint) (storage.StagedObject, error) {
	// For writing, just write directly to underlying storage
	// TODO: Handle recursive non-tree pusher
	pusher := d.d.inner.GetPusher(d.pushuuid)

	inner, err := pusher.StageObject(objtype, objsize)
	return &clusterStorageDriverStagedObject{
		driver: d,
		inner:  inner,
	}, err
}

func (d *clusterStorageProjectPushDriverInstance) GetPushResultChannel() <-chan error {
	return d.errchan
}

func (d *clusterStorageProjectPushDriverInstance) Done() {
	go func() {
		d.d.d.cfg.log.Debug("Marking pusher as done")

		d.objectSyncAllSubmitted = true
		d.objectSyncNewObjects.Broadcast()
		d.d.d.cfg.log.Debug("Told syncers we submitted everything")

		d.outstandingobjects.Wait()
		d.d.d.cfg.log.Debug("Required set of nodes have objects")

		close(d.errchan)
		d.d.d.cfg.log.Debug("Syncer is done")

		go func(waiter *sync.WaitGroup) {
			// Make sure we only close the database after all syncers are done with it
			waiter.Wait()

			d.objectSyncDB.Close()
			os.Remove(d.objectSyncDBPath)
			d.d.d.cfg.log.Debug("Syncer removed DB")
		}(d.syncersWg)
	}()
}

func (d *clusterStorageProjectPushDriverInstance) startObjectSync(objid storage.ObjectID) {
	if len(d.objectSyncPeers) != 0 {
		d.outstandingobjects.Add(1)
		err := d.dbAddObject(objid)
		if err != nil {
			panic(err)
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

	o.driver.startObjectSync(calced)
	return calced, nil
}
