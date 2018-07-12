package service

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"

	"repospanner.org/repospanner/server/storage"
)

type syncer struct {
	cfg *Service

	errchan chan error
	wg      *sync.WaitGroup

	syncerStopC chan struct{}
}

func (cfg *Service) syncSingleObject(peer uint64, d storage.ProjectStorageDriver, reponame string, objid storage.ObjectID) error {
	clustered, isclustered := d.(*clusterStorageProjectDriverInstance)
	if isclustered {
		d = clustered.inner
	}
	objtype, objsize, reader, err := d.ReadObject(objid)
	if err != nil {
		return err
	}
	defer reader.Close()

	req, err := http.NewRequest(
		"PUT",
		cfg.GetPeerURL(peer, "/rpc/object/write/"+reponame+".git/"+string(objid)),
		reader,
	)
	if err != nil {
		return err
	}
	req.Header.Add("X-Objecttype", objtype.HdrName())
	req.Header.Add("X-Objectsize", strconv.FormatUint(uint64(objsize), 10))

	resp, err := cfg.rpcClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return errors.Errorf(
			"Error returned by peer: %d: %s",
			resp.StatusCode,
			resp.Status)
	}
	return nil
}

func (s *syncer) Run(errchan chan error) {
	s.errchan = errchan

	for peerid := range s.cfg.statestore.Peers {
		s.AddPeer(peerid)
	}
}

func (s *syncer) AddPeer(peerid uint64) {
	if peerid == s.cfg.nodeid {
		// We don't have to sync with ourselves
		return
	}
	go s.runPeer(peerid)
}

func (s *syncer) syncSingleFile(peerid uint64, queue *os.File) error {
	defer queue.Close()

	var projectname string
	_, err := fmt.Fscanf(queue, "project:%s\n", &projectname)
	if err != nil {
		return err
	}

	s.cfg.log.Debugw(
		"Syncing outqueue to peer",
		"peer", peerid,
		"projectname", projectname,
	)
	driver := s.cfg.gitstore.GetProjectStorage(projectname)

	for {
		var objectidS string
		_, err := fmt.Fscanln(queue, &objectidS)
		if err != nil {
			return err
		}
		if !isValidRef(objectidS) {
			return errors.Errorf("Invalid object ID: %s", objectidS)
		}
		objectid := storage.ObjectID(objectidS)

		s.cfg.log.Debugw("Syncing object",
			"peer", peerid,
			"projectname", projectname,
			"objectid", objectid,
		)

		return s.cfg.syncSingleObject(
			peerid,
			driver,
			projectname,
			objectid,
		)
	}
}

func (s *syncer) runSingleSync(peerid uint64) {
	s.cfg.log.Debugw(
		"Running single sync",
		"peer", peerid,
	)
	peerdir := path.Join(
		s.cfg.statestore.directory,
		"async-outqueues",
		"peer-"+strconv.FormatUint(peerid, 16),
	)
	dirent, err := ioutil.ReadDir(peerdir)
	if os.IsNotExist(err) {
		// If there is no outgoing queue, there's nothing to sync
		return
	}
	if err != nil {
		s.cfg.log.Infow(
			"Error getting outgoing queue",
			"peer", peerid,
			"error", err,
		)
		return
	}

	for _, file := range dirent {
		if strings.HasSuffix(file.Name(), ".inprogress") {
			continue
		}

		if file.IsDir() {
			s.cfg.log.Errorw(
				"Directory found in outqueue",
				"peer", peerid,
			)
			continue
		}

		s.cfg.log.Debugw(
			"Entry to sync out",
			"peer", peerid,
			"pushuuid", file.Name(),
		)

		queuefilepath := path.Join(peerdir, file.Name())
		queuefile, err := os.Open(queuefilepath)
		if err != nil {
			s.cfg.log.Infow(
				"Error opening outgoing queue",
				"peer", peerid,
				"pushuuid", file.Name(),
				"error", err,
			)
			continue
		}
		err = s.syncSingleFile(peerid, queuefile)
		if err != nil {
			s.cfg.log.Debugw(
				"Error syncing queue",
				"peer", peerid,
				"pushuuid", file.Name(),
				"error", err,
			)
			continue
		}

		// We finished syncing this out to the peer, delete the queue
		err = os.Remove(queuefilepath)
		if err != nil {
			s.cfg.log.Infow(
				"Unable to delete queue file after syncing",
				"peer", peerid,
				"pushuuid", file.Name(),
				"error", err,
			)
			continue
		}

		s.cfg.log.Debugw(
			"Queue synced out",
			"peer", peerid,
			"pushuuid", file.Name(),
		)
	}
}

func (s *syncer) runPeer(peerid uint64) {
	s.cfg.log.Debugw("Starting peer syncer", "peer", peerid)
	s.wg.Add(1)
	defer s.wg.Done()

	peerticker := time.NewTicker(5 * time.Minute)

	for {
		select {
		case <-s.syncerStopC:
			// Stop syncer
			return
		case <-peerticker.C:
			// Perform a sync
			s.runSingleSync(peerid)
		}
	}
}

func (cfg *Service) createSyncer() (*syncer, error) {
	return &syncer{
		cfg:         cfg,
		syncerStopC: make(chan struct{}),
		wg:          new(sync.WaitGroup),
	}, nil
}

func (s *syncer) Stop() {
	close(s.syncerStopC)

	go func() {
		// Wait for all peer syncers to finish, and then tell the controller we are done
		s.wg.Wait()
		s.errchan <- nil
	}()
}
