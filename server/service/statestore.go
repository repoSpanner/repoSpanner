package service

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/coreos/etcd/raft/raftpb"
	"github.com/coreos/etcd/snap"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
	"repospanner.org/repospanner/server/datastructures"
	pb "repospanner.org/repospanner/server/protobuf"
	"repospanner.org/repospanner/server/storage"
)

type PushResult struct {
	success       bool
	branchresults map[string]string
	clienterror   error
	logerror      error
}

type stateStore struct {
	cfg       *Service
	mux       sync.RWMutex
	directory string

	Peers       map[uint64]string
	peerPings   map[uint64]pb.PingMessage
	ClusterName string
	RegionName  string
	NodeName    string
	NodeID      uint64

	repoinfos map[string]datastructures.RepoInfo

	fakerefs map[string]map[string]string

	repoChangeListeners    map[string][]chan *pb.ChangeRequest
	repoChangeListenersMux sync.RWMutex

	confChangeListeners    []chan raftpb.ConfChange
	confChangeListenersMux sync.RWMutex

	raftnode     *stateRaftNode
	started      bool
	stopOnFinish bool
	joining      bool

	commitC     <-chan *[]byte
	errorC      <-chan error
	proposeC    chan<- []byte
	confChangeC chan<- raftpb.ConfChange
	snapshotter *snap.Snapshotter

	snapShotterReady <-chan *snap.Snapshotter
}

func (store *stateStore) attemptJoin(joinnode string) (err error) {
	req := rpcJoinNodeRequest{
		NodeID: store.cfg.nodeid,
		RPCURL: store.cfg.findRPCURL(),
	}
	var cts []byte
	cts, err = json.Marshal(req)
	if err != nil {
		return
	}
	buf := bytes.NewBuffer(cts)

	resp, err := store.cfg.rpcClient.Post(fmt.Sprintf("%s/rpc/join", joinnode), "application/json", buf)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	cts, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		return
	}
	if resp.StatusCode != 200 {
		return errors.Errorf("Error on join request: %d: %s",
			resp.StatusCode,
			string(cts),
		)
	}

	var reply rpcJoinNodeReply
	if err = json.Unmarshal(cts, &reply); err != nil {
		return
	}
	if !reply.Success {
		return errors.Errorf("Join failed: %s", reply.ErrorMessage)
	}

	// We were accepted into the region
	store.Peers = reply.NodeInfo.Peers
	return
}

func (cfg *Service) loadStateStore(spawning bool, joinnode string, directory string) (store *stateStore, err error) {
	store = &stateStore{
		directory:           directory,
		cfg:                 cfg,
		repoinfos:           make(map[string]datastructures.RepoInfo),
		repoChangeListeners: make(map[string][]chan *pb.ChangeRequest),
		confChangeListeners: []chan raftpb.ConfChange{},
		fakerefs:            make(map[string]map[string]string),
		peerPings:           make(map[uint64]pb.PingMessage),
	}

	cts, err := ioutil.ReadFile(path.Join(directory, "state.json"))
	if err == nil {
		if spawning || joinnode != "" {
			err = errors.New("Spawning or joining an already initiated node?")
			return
		}
		err = json.Unmarshal(cts, &store)
		cfg.log.Info("State loaded")
	} else if os.IsNotExist(err) {
		// No state yet, initialize
		if !spawning && joinnode == "" {
			err = errors.New("No state found and not spawning or joining")
			return
		}
		cfg.log.Info("No state existed, initializing")
		if err = os.MkdirAll(store.directory, 0775); err != nil {
			return
		}
		if err = os.MkdirAll(path.Join(store.directory, "async-outqueues"), 0775); err != nil {
			return
		}
		if err = os.MkdirAll(path.Join(store.directory, "repoinfos"), 0775); err != nil {
			return
		}
		if err = os.MkdirAll(path.Join(store.directory, "objectsyncs"), 0775); err != nil {
			return
		}

		store.stopOnFinish = true
		store.ClusterName = cfg.cluster
		store.RegionName = cfg.region
		store.NodeName = cfg.nodename
		store.NodeID = cfg.nodeid
		store.Peers = make(map[uint64]string)
		if joinnode != "" {
			err = store.attemptJoin(joinnode)
			if err != nil {
				return
			}
			store.joining = true
		}
		store.Peers[cfg.nodeid] = cfg.findRPCURL()
		err = store.Save()
	}
	if err != nil {
		return
	}

	store.createStateRaftNode()

	return
}

func (store *stateStore) AddFakeRefs(repo string, req *pb.PushRequest) {
	store.mux.Lock()
	defer store.mux.Unlock()

	_, inthere := store.fakerefs[repo]
	if !inthere {
		store.fakerefs[repo] = make(map[string]string)
	}

	for _, creq := range req.GetRequests() {
		if creq.ToObject() != storage.ZeroID {
			refname := fmt.Sprintf("refs/heads/fake/%s/%s", req.UUID(), creq.GetRef())
			store.fakerefs[repo][refname] = creq.GetTo()
		}
	}
}

func (store *stateStore) RemoveFakeRefs(repo string, req *pb.PushRequest) {
	store.mux.Lock()
	defer store.mux.Unlock()

	_, inthere := store.fakerefs[repo]
	if !inthere {
		// We clearly didn't run AddFakeRefs yet.... weird, but ok
		return
	}

	for _, creq := range req.GetRequests() {
		if creq.ToObject() != storage.ZeroID {
			refname := fmt.Sprintf("refs/heads/fake/%s/%s", req.UUID(), creq.GetRef())
			delete(store.fakerefs[repo], refname)
		}
	}
}

func (store *stateStore) GetLastPushNode(project string) uint64 {
	store.mux.RLock()
	defer store.mux.RUnlock()

	return store.repoinfos[project].LastPushNode
}

func (store *stateStore) GetRepoHooks(project string) datastructures.RepoHookInfo {
	store.mux.RLock()
	defer store.mux.RUnlock()

	return store.repoinfos[project].Hooks
}

func (store *stateStore) IsRepoPublic(project string) bool {
	store.mux.RLock()
	defer store.mux.RUnlock()

	return store.repoinfos[project].Public
}

func (store *stateStore) getSnapshot() ([]byte, error) {
	store.cfg.log.Debug("Getting snapshot")
	store.mux.RLock()
	defer store.mux.RUnlock()

	return json.Marshal(store.repoinfos)
}

func (store *stateStore) recoverFromSnapshot(snapshot []byte) error {
	store.cfg.log.Debug("Recovering from snapshot")
	store.mux.Lock()
	defer store.mux.Unlock()

	var info map[string]datastructures.RepoInfo
	if err := json.Unmarshal(snapshot, &info); err != nil {
		return errors.Wrap(err, "Unable to recover from snapshot")
	}
	store.repoinfos = info
	store.cfg.log.Debugf("Restored snapshot with %d repos", len(store.repoinfos))
	return nil
}

func (store *stateStore) Save() error {
	store.mux.Lock()
	defer store.mux.Unlock()

	cts, err := json.Marshal(store)
	if err != nil {
		return err
	}
	err = ioutil.WriteFile(path.Join(store.directory, "state.json"), cts, 0660)
	if err != nil {
		return err
	}
	return nil
}

func (store *stateStore) RunStateStore(errchan chan<- error, startedC chan<- struct{}) {
	// First start the raft node
	go store.raftnode.startRaft(startedC)
	store.cfg.log.Debug("Started raft")
	// Wait for snapshotter ready
	snapshotter := <-store.snapShotterReady
	store.cfg.log.Debug("Got snapshotter")
	store.snapshotter = snapshotter
	// Now replay the logs
	store.readCommits(true)
	store.cfg.log.Debug("WAL Replayed")
	go store.readCommits(false)
	store.cfg.log.Debug("stateStore ready")
	var pingchan <-chan time.Time
	if store.stopOnFinish {
		// We have finished initialization, terminate
		go func() {
			<-store.cfg.logwrapper.HasInitialized
			store.cfg.Shutdown()
		}()
		time.AfterFunc(5*time.Second, func() {
			errchan <- errors.New("Not started within the expected time")
		})
	} else {
		if err := store.sendPing(); err != nil {
			errchan <- err
			return
		}
		ticker := time.NewTicker(5 * time.Second)
		pingchan = ticker.C
	}
	for {
		select {
		case <-pingchan:
			// Do a regular ping
			if err := store.sendPing(); err != nil {
				errchan <- err
			}

		case err := <-store.errorC:
			errchan <- errors.Wrap(err, "State error")
			return
		case <-store.raftnode.stoppedc:
			store.cfg.log.Info("Stopped state")
			errchan <- nil
			return
		}
	}
}

func (store *stateStore) sendPing() error {
	timestamp := time.Now().UTC().UnixNano()
	features := pb.SupportedFeatures()
	creq := &pb.ChangeRequest{
		Ctype: pb.ChangeRequest_PING.Enum(),
		Pingmsg: &pb.PingMessage{
			Pingnode:     &store.NodeID,
			Timestamp:    &timestamp,
			AppliedIndex: &store.raftnode.appliedIndex,
			FeatureBits:  &features,
		},
	}
	out, err := proto.Marshal(creq)
	if err != nil {
		return errors.Wrap(err, "Error marshalling ping")
	}
	store.proposeC <- out
	return nil
}

func (store *stateStore) announceConfChange(req raftpb.ConfChange) {
	store.confChangeListenersMux.RLock()
	defer store.confChangeListenersMux.RUnlock()

	for _, listener := range store.confChangeListeners {
		listener <- req
	}
}

func (store *stateStore) subscribeConfChange() chan raftpb.ConfChange {
	store.confChangeListenersMux.Lock()
	defer store.confChangeListenersMux.Unlock()

	ccC := make(chan raftpb.ConfChange)
	store.confChangeListeners = append(store.confChangeListeners, ccC)
	return ccC
}

func (store *stateStore) unsubscribeConfChange(ccC chan raftpb.ConfChange) {
	// Make sure that any further messages sent until the channel is closed gets
	// tossed. Otherwise, we can block the announce
	go func(c chan raftpb.ConfChange) {
		for range c {
		}
	}(ccC)

	store.confChangeListenersMux.Lock()
	defer store.confChangeListenersMux.Unlock()

	index := -1
	for i, listener := range store.confChangeListeners {
		if listener == ccC {
			close(ccC)
			index = i
		}
	}
	if index != -1 {
		// This was a known listener
		store.confChangeListeners = append(
			store.confChangeListeners[:index],
			store.confChangeListeners[index+1:]...,
		)
	} else {
		store.cfg.log.Error("Invalid confchangelistener removal")
	}
}

func (store *stateStore) announceRepoChanges(repo string, req *pb.ChangeRequest) {
	store.repoChangeListenersMux.RLock()
	defer store.repoChangeListenersMux.RUnlock()

	listeners, anylisteners := store.repoChangeListeners[repo]
	if anylisteners {
		for _, listener := range listeners {
			listener <- req
		}
	}
}

func (store *stateStore) subscribeRepoChangeRequest(repo string) chan *pb.ChangeRequest {
	store.repoChangeListenersMux.Lock()
	defer store.repoChangeListenersMux.Unlock()

	crC := make(chan *pb.ChangeRequest)

	_, anylisteners := store.repoChangeListeners[repo]
	if anylisteners {
		store.repoChangeListeners[repo] = append(store.repoChangeListeners[repo], crC)
	} else {
		store.repoChangeListeners[repo] = []chan *pb.ChangeRequest{crC}
	}

	return crC
}

func (store *stateStore) unsubscribeRepoChangeRequest(repo string, crC chan *pb.ChangeRequest) {
	// Make sure that any further messages sent until the channel is closed gets
	// tossed. Otherwise, we can block the announce
	go func(c chan *pb.ChangeRequest) {
		for range c {
		}
	}(crC)

	store.repoChangeListenersMux.Lock()
	defer store.repoChangeListenersMux.Unlock()

	listeners, anylisteners := store.repoChangeListeners[repo]
	if anylisteners {
		index := -1
		for i, listener := range listeners {
			if listener == crC {
				close(crC)
				index = i
			}
		}
		if index != -1 {
			// This was a known listener
			store.repoChangeListeners[repo] = append(
				listeners[:index],
				listeners[index+1:]...,
			)
		} else {
			store.cfg.log.Error("Invalid repochangelistener removal")
		}
	} else {
		store.cfg.log.Error("repochangelistener removal while no listeners")
	}
}

func (store *stateStore) applySymrefUpdate(repo datastructures.RepoInfo, symrefreq string) {
	store.cfg.log.Errorf("Got symrefreq: %s", symrefreq)
	updates := strings.Split(symrefreq, " ")

	for _, update := range updates {
		req := strings.SplitN(update, "=", 2)
		store.cfg.log.Errorf("Req: %s", req)
		symref := req[0]
		target := req[1]

		if target == "" {
			delete(repo.Symrefs, symref)
		} else {
			repo.Symrefs[symref] = target
		}
	}
}

func (store *stateStore) applyUpdateRequest(reponame string, request datastructures.RepoUpdateRequest) {
	repo := store.repoinfos[reponame]
	for field, val := range request.UpdateRequest {
		switch field {
		case datastructures.RepoUpdatePublic:
			valB := val == "true"
			repo.Public = valB
		case datastructures.RepoUpdateHookPreReceive:
			repo.Hooks.PreReceive = val
		case datastructures.RepoUpdateHookUpdate:
			repo.Hooks.Update = val
		case datastructures.RepoUpdateHookPostReceive:
			repo.Hooks.PostReceive = val
		case datastructures.RepoUpdateSymref:
			store.applySymrefUpdate(repo, val)
		}
	}
	store.repoinfos[reponame] = repo
}

var lastSnapshotApplied uint64

func (store *stateStore) readCommits(initial bool) {
	for data := range store.commitC {
		if data == nil {
			// done replaying log; now data incoming
			// OR signaled to load snapshot
			snapshot, err := store.snapshotter.Load()
			if err == snap.ErrNoSnapshot {
				store.cfg.log.Debug("Nil data with no snapshot")
				return
			}
			if err != nil && err != snap.ErrNoSnapshot {
				store.cfg.log.WithError(err).Fatal("Error replaying log")
				return
			}
			if snapshot.Metadata.Index <= lastSnapshotApplied {
				store.cfg.log.Debugf("Skipping snapshot with index %d", snapshot.Metadata.Index)
				if initial {
					return
				}
				continue
			}
			store.cfg.log.Debugf("Loading snapshot with index %d", snapshot.Metadata.Index)
			lastSnapshotApplied = snapshot.Metadata.Index
			if err := store.recoverFromSnapshot(snapshot.Data); err != nil {
				store.cfg.log.WithError(err).Fatal("Error loading snapshot")
				return
			}
			continue
		}

		req := &pb.ChangeRequest{}
		if err := proto.Unmarshal(*data, req); err != nil {
			store.cfg.log.WithError(err).Fatal("Unable to unmarshal")
		}

		switch req.GetCtype() {
		case pb.ChangeRequest_NEWREPO:
			r := req.GetNewreporeq()

			// This is more of a joke, and not documented since nobody
			// in their sane mind should probably use this.
			defhead := viper.GetString("silly.defaulthead")
			if defhead == "" {
				// We still want to default to a sane value
				defhead = "master"
			}

			store.cfg.log.Debug("New repo request received")
			store.mux.Lock()
			store.repoinfos[r.GetReponame()] = datastructures.RepoInfo{
				Public:  r.GetPublic(),
				Refs:    make(map[string]string),
				Symrefs: map[string]string{"HEAD": "refs/heads/" + defhead},
				Hooks: datastructures.RepoHookInfo{
					PreReceive:  string(storage.ZeroID),
					Update:      string(storage.ZeroID),
					PostReceive: string(storage.ZeroID),
				},
			}
			store.mux.Unlock()
			store.announceRepoChanges(r.GetReponame(), req)

		case pb.ChangeRequest_EDITREPO:
			r := req.GetEditreporeq()

			var updaterequest datastructures.RepoUpdateRequest
			err := json.Unmarshal(r.GetUpdaterequest(), &updaterequest)
			if err != nil {
				store.cfg.log.WithError(err).Error("Unable to apply update request")
				continue
			}

			store.cfg.log.Debug("Edit repo request received")
			store.mux.Lock()
			store.applyUpdateRequest(r.GetReponame(), updaterequest)
			store.mux.Unlock()
			store.announceRepoChanges(r.GetReponame(), req)

		case pb.ChangeRequest_DELETEREPO:
			r := req.GetDeletereporeq()

			store.cfg.log.Debug("Delete repo request received")
			store.mux.Lock()
			delete(store.repoinfos, r.GetReponame())
			store.mux.Unlock()
			store.announceRepoChanges(r.GetReponame(), req)

		case pb.ChangeRequest_PUSHREQUEST:
			r := req.GetPushreq()

			store.cfg.log.Debugf("Push request received to %s", r.GetReponame())
			store.processPush(r)
			store.announceRepoChanges(r.GetReponame(), req)

		case pb.ChangeRequest_PING:
			r := req.GetPingmsg()

			store.peerPings[r.GetPingnode()] = *r

		default:
			store.cfg.log.Error("Unknown ChangeRequest request received")
		}
	}
	if err, ok := <-store.errorC; ok {
		store.cfg.log.WithError(err).Fatal("Error received")
	}
}

func (store *stateStore) createRepo(repo string, public bool) error {
	_, exists := store.repoinfos[repo]
	if exists {
		return errors.Errorf("Repo %s already exists", repo)
	}
	creq := &pb.ChangeRequest{
		Ctype: pb.ChangeRequest_NEWREPO.Enum(),
		Newreporeq: &pb.NewRepoRequest{
			Reponame: &repo,
			Public:   &public,
		},
	}
	out, err := proto.Marshal(creq)
	if err != nil {
		return errors.Wrap(err, "Error marshalling newrepo request")
	}
	store.cfg.log.Infof("Repo creation requested of %s", repo)
	crC := store.subscribeRepoChangeRequest(repo)
	defer store.unsubscribeRepoChangeRequest(repo, crC)
	store.proposeC <- out
	rcreq := <-crC
	if rcreq.GetNewreporeq() == nil {
		return errors.New("Received a non-new-repo-req response")
	}
	return nil
}

func (store *stateStore) deleteRepo(repo string) error {
	_, exists := store.repoinfos[repo]
	if !exists {
		return errors.Errorf("Repo %s does not exists", repo)
	}
	creq := &pb.ChangeRequest{
		Ctype: pb.ChangeRequest_DELETEREPO.Enum(),
		Deletereporeq: &pb.DeleteRepoRequest{
			Reponame: &repo,
		},
	}
	out, err := proto.Marshal(creq)
	if err != nil {
		return errors.Wrap(err, "Error marshalling deleterepo request")
	}
	store.cfg.log.Infof("Repo deletion requested of %s", repo)
	crC := store.subscribeRepoChangeRequest(repo)
	defer store.unsubscribeRepoChangeRequest(repo, crC)
	store.proposeC <- out
	rcreq := <-crC
	if rcreq.GetDeletereporeq() == nil {
		return errors.New("Received a non-delete-repo-req response")
	}
	return nil
}

func (store *stateStore) editRepo(repo string, request []byte) error {
	_, exists := store.repoinfos[repo]
	if !exists {
		return errors.Errorf("Repo %s does not exists", repo)
	}
	creq := &pb.ChangeRequest{
		Ctype: pb.ChangeRequest_EDITREPO.Enum(),
		Editreporeq: &pb.EditRepoRequest{
			Reponame:      &repo,
			Updaterequest: request,
		},
	}
	out, err := proto.Marshal(creq)
	if err != nil {
		return errors.Wrap(err, "Error marshalling editrepo request")
	}
	store.cfg.log.Infof("Repo editing requested on %s", repo)
	crC := store.subscribeRepoChangeRequest(repo)
	defer store.unsubscribeRepoChangeRequest(repo, crC)
	store.proposeC <- out
	rcreq := <-crC
	if rcreq.GetEditreporeq() == nil {
		return errors.New("Received a non-edit-repo-req response")
	}
	return nil
}

func (store *stateStore) hasRepo(repo string) (exists bool) {
	store.mux.RLock()
	defer store.mux.RUnlock()

	_, exists = store.repoinfos[repo]
	return
}

func (store *stateStore) getSymRefs(repo string) map[string]string {
	return store.repoinfos[repo].Symrefs
}

func (store *stateStore) getGitRefs(repo string) map[string]string {
	return store.repoinfos[repo].Refs
}

func (store *stateStore) getPushResult(req *pb.PushRequest) (result PushResult) {
	refs := store.repoinfos[req.GetReponame()].Refs

	result.success = true
	result.branchresults = make(map[string]string)

	for _, request := range req.Requests {
		refname := request.GetRef()

		if !result.success {
			result.branchresults[refname] = "previous-failure"
			continue
		}

		curstate, exists := refs[refname]

		if request.FromObject() != storage.ZeroID && !exists {
			// Failure: ref isn't being created and doesn't exist yet
			result.success = false
			result.branchresults[refname] = "does-not-exist"
			result.clienterror = fmt.Errorf("Ref %s is not created but does not exist", refname)
			result.logerror = fmt.Errorf("Ref %s is not created but does not exist", refname)
			continue
		}
		if request.FromObject() == storage.ZeroID && exists {
			// Failure: ref is being created but already exists
			result.success = false
			result.branchresults[refname] = "already-exists"
			result.clienterror = fmt.Errorf("Ref %s already exists", refname)
			result.logerror = fmt.Errorf("Ref %s already exists", refname)
			continue
		}
		if exists && string(request.FromObject()) != curstate {
			// Failure: from is not the same
			result.success = false
			result.branchresults[refname] = "outdated"
			result.clienterror = fmt.Errorf("Ref %s already updated", refname)
			result.logerror = fmt.Errorf("Ref %s already updated", refname)
			continue
		}

		// We passed all checks
		result.branchresults[refname] = "OK"
	}

	return
}

const maxRetries = 3

func (store *stateStore) performPush(ctx context.Context, req *pb.PushRequest) PushResult {
	reqlogger := loggerFromCtx(ctx)
	result := store.getPushResult(req)

	if result.success {
		reqlogger.Debug("Initial check success, sending to raft")
		creq := &pb.ChangeRequest{
			Ctype:   pb.ChangeRequest_PUSHREQUEST.Enum(),
			Pushreq: req,
		}
		cts, err := proto.Marshal(creq)
		if err != nil {
			store.cfg.log.WithError(err).Warn("Error marshalling pushrequest")
			result.success = false
			result.clienterror = errors.New("Error sending request")
			result.logerror = errors.New("Error sending request")
			return result
		}
		retryTimer := time.NewTicker(2 * time.Second)
		var retryCount int
		defer retryTimer.Stop()

		crC := store.subscribeRepoChangeRequest(req.GetReponame())
		defer store.unsubscribeRepoChangeRequest(req.GetReponame(), crC)
		store.proposeC <- cts

		for {
			select {
			case msg := <-crC:
				reqlogger.Debug("Got announced message", msg)
				pushresp := msg.GetPushreq()
				if pushresp == nil {
					// Something changed other than a push.....
					reqlogger.Debug("Non-push occured")
					result.logerror = errors.New("Non-push response received")
					result.clienterror = errors.New("Something strange happened to this repo")
					result.success = false
					return result
				}

				reqlogger.Debug("Push went through for ", pushresp.UUID())
				if req.UUID() == pushresp.UUID() || req.Equals(pushresp) {
					// Done!
					return result
				} else if req.Conflicts(pushresp) {
					result.logerror = errors.New("Conflicting push occured")
					result.clienterror = errors.New("Conflicting push occured")
					result.success = false
					return result
				}
			case <-retryTimer.C:
				if retryCount >= maxRetries {
					result.success = false
					result.clienterror = errors.New("Timeout while rolling out")
					result.logerror = errors.New("Timeout occured while syncing to cluster")
					return result
				}

				retryCount++
			}
		}
	}

	return result
}

func (store *stateStore) processPush(req *pb.PushRequest) {
	store.mux.Lock()
	defer store.mux.Unlock()

	info, ok := store.repoinfos[req.GetReponame()]

	if !ok {
		panic("Push on invalid repo detected")
	}

	store.cfg.log.Debug("Attempting to apply", req)
	result := store.getPushResult(req)
	if !result.success {
		store.cfg.log.Error("Applied PushRequest was impossible....")
		return
	}

	info.LastPushNode = req.GetPushnode()

	for _, request := range req.Requests {
		refname := request.GetRef()

		if request.ToObject() == storage.ZeroID {
			delete(info.Refs, refname)
			continue
		}
		info.Refs[refname] = string(request.ToObject())
	}

	for symref, target := range info.Symrefs {
		_, hasref := info.Refs[target]
		if !hasref {
			delete(info.Symrefs, symref)
		}
	}

	store.cfg.log.Debug("New info:", info)
	store.repoinfos[req.GetReponame()] = info
}
