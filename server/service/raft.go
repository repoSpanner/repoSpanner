package service

import (
	"context"
	"fmt"
	"hash/fnv"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/coreos/etcd/etcdserver/stats"
	"github.com/coreos/etcd/pkg/fileutil"
	"github.com/coreos/etcd/pkg/types"
	"github.com/coreos/etcd/wal/walpb"
	"github.com/sirupsen/logrus"

	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/coreos/etcd/rafthttp"
	"github.com/coreos/etcd/snap"
	"github.com/coreos/etcd/wal"
)

type stateRaftNode struct {
	store *stateStore

	proposeC    <-chan []byte
	confChangeC <-chan raftpb.ConfChange
	commitC     chan<- *[]byte
	errorC      chan<- error

	waldir      string
	snapdir     string
	getSnapshot func() ([]byte, error)
	lastIndex   uint64

	confState     raftpb.ConfState
	snapshotIndex uint64
	appliedIndex  uint64

	node        raft.Node
	raftStorage *raft.MemoryStorage
	wal         *wal.WAL

	snapshotter      *snap.Snapshotter
	snapshotterReady chan *snap.Snapshotter

	snapCount uint64
	transport *rafthttp.Transport
	stoppedc  chan struct{}
	stopc     chan struct{}
	httpstopc chan struct{}
	httpdonec chan struct{}
}

func (cfg *Service) getRegionHash() uint64 {
	h := fnv.New64a()
	h.Write([]byte(cfg.region))
	return h.Sum64()
}

const defaultSnapCount uint64 = 10000

func (store *stateStore) createStateRaftNode() {
	commitC := make(chan *[]byte)
	errorC := make(chan error)
	proposeC := make(chan []byte)
	confChangeC := make(chan raftpb.ConfChange)
	snapshotterReady := make(chan *snap.Snapshotter, 1)

	store.raftnode = &stateRaftNode{
		store:       store,
		proposeC:    proposeC,
		confChangeC: confChangeC,
		commitC:     commitC,
		errorC:      errorC,
		waldir:      fmt.Sprintf("%s/wal", store.directory),
		snapdir:     fmt.Sprintf("%s/snap", store.directory),
		getSnapshot: store.getSnapshot,
		snapCount:   defaultSnapCount,
		stopc:       make(chan struct{}),
		stoppedc:    make(chan struct{}),
		httpstopc:   make(chan struct{}),
		httpdonec:   make(chan struct{}),

		snapshotterReady: snapshotterReady,
	}
	// Give other sides of channels to statestore
	store.commitC = commitC
	store.errorC = errorC
	store.proposeC = proposeC
	store.confChangeC = confChangeC
	store.snapShotterReady = snapshotterReady
	return
}

func (rc *stateRaftNode) saveSnap(snap raftpb.Snapshot) error {
	walSnap := walpb.Snapshot{
		Index: snap.Metadata.Index,
		Term:  snap.Metadata.Term,
	}
	if err := rc.wal.SaveSnapshot(walSnap); err != nil {
		return err
	}
	if err := rc.snapshotter.SaveSnap(snap); err != nil {
		return err
	}
	return rc.wal.ReleaseLockTo(snap.Metadata.Index)
}

func (rc *stateRaftNode) entriesToApply(ents []raftpb.Entry) (nents []raftpb.Entry) {
	if len(ents) == 0 {
		return
	}
	firstIdx := ents[0].Index
	if firstIdx > rc.appliedIndex+1 {
		rc.store.cfg.log.Fatalf("First index of committed entry[%d] should be <= progress.appliedInde[%d]", firstIdx, rc.appliedIndex)
	}
	if rc.appliedIndex-firstIdx+1 < uint64(len(ents)) {
		nents = ents[rc.appliedIndex-firstIdx+1:]
	}
	return nents
}

func (rc *stateRaftNode) publishEntries(ents []raftpb.Entry) bool {
	for i := range ents {
		switch ents[i].Type {
		case raftpb.EntryNormal:
			if len(ents[i].Data) == 0 {
				// ignore empty messages
				break
			}
			s := ents[i].Data
			select {
			case rc.commitC <- &s:
			case <-rc.stopc:
				return false
			}

		case raftpb.EntryConfChange:
			var cc raftpb.ConfChange
			cc.Unmarshal(ents[i].Data)
			rc.confState = *rc.node.ApplyConfChange(cc)
			switch cc.Type {
			case raftpb.ConfChangeAddNode:
				if len(cc.Context) > 0 {
					rc.store.Peers[cc.NodeID] = string(cc.Context)
					rc.store.Save()
					rc.transport.AddPeer(types.ID(cc.NodeID), []string{string(cc.Context)})
					rc.store.cfg.sync.AddPeer(cc.NodeID)
				}
			case raftpb.ConfChangeRemoveNode:
				if cc.NodeID == rc.store.cfg.nodeid {
					rc.store.cfg.log.Info("Removed from cluster, shutting down")
					return false
				}
				delete(rc.store.Peers, cc.NodeID)
				rc.store.Save()
				rc.transport.RemovePeer(types.ID(cc.NodeID))
			}

			rc.store.announceConfChange(cc)
		}

		// after commit, update appliedIndex
		rc.appliedIndex = ents[i].Index

		// special nil commit to signal replay has finished
		if ents[i].Index == rc.lastIndex {
			select {
			case rc.commitC <- nil:
			case <-rc.stopc:
				return false
			}
		}
	}
	return true
}

func (rc *stateRaftNode) loadSnapshot() *raftpb.Snapshot {
	snapshot, err := rc.snapshotter.Load()
	if err != nil && err != snap.ErrNoSnapshot {
		log.Fatalf("raftexample: error loading snapshot (%v)", err)
	}
	return snapshot
}

func (rc *stateRaftNode) openWAL(snapshot *raftpb.Snapshot) *wal.WAL {
	if !wal.Exist(rc.waldir) {
		if err := os.Mkdir(rc.waldir, 0750); err != nil {
			log.Fatalf("raftexample: cannot create dir for wal (%v)", err)
		}

		w, err := wal.Create(rc.waldir, nil)
		if err != nil {
			log.Fatalf("raftexample: create wal error (%v)", err)
		}
		w.Close()
	}

	walsnap := walpb.Snapshot{}
	if snapshot != nil {
		walsnap.Index, walsnap.Term = snapshot.Metadata.Index, snapshot.Metadata.Term
	}
	rc.store.cfg.log.WithFields(logrus.Fields{
		"term":  walsnap.Term,
		"index": walsnap.Index,
	}).Debug("Loading wal")
	w, err := wal.Open(rc.waldir, walsnap)
	if err != nil {
		log.Fatalf("raftexample: error loading wal (%v)", err)
	}

	return w
}

// replayWAL replays WAL entries into the raft instance.
func (rc *stateRaftNode) replayWAL() *wal.WAL {
	rc.store.cfg.log.Debug("Replaying WAL")
	snapshot := rc.loadSnapshot()
	w := rc.openWAL(snapshot)
	_, st, ents, err := w.ReadAll()
	if err != nil {
		log.Fatalf("raftexample: failed to read WAL (%v)", err)
	}
	rc.raftStorage = raft.NewMemoryStorage()
	if snapshot != nil {
		rc.raftStorage.ApplySnapshot(*snapshot)
	}
	rc.raftStorage.SetHardState(st)

	// append to storage so raft starts at the right place in log
	rc.raftStorage.Append(ents)
	// send nil once lastIndex is published so client knows commit channel is current
	if len(ents) > 0 {
		rc.lastIndex = ents[len(ents)-1].Index
	} else {
		rc.commitC <- nil
	}
	return w
}

func (rc *stateRaftNode) writeError(err error) {
	rc.stopHTTP()
	close(rc.commitC)
	rc.errorC <- err
	close(rc.errorC)
	rc.node.Stop()
}

func (rc *stateRaftNode) startRaft(startedC chan<- struct{}) {
	if !fileutil.Exist(rc.snapdir) {
		if err := os.Mkdir(rc.snapdir, 0750); err != nil {
			log.Fatalf("raftexample: cannot create dir for snapshot (%v)", err)
		}
	}
	rc.snapshotter = snap.New(rc.snapdir)
	rc.snapshotterReady <- rc.snapshotter

	oldwal := wal.Exist(rc.waldir)
	rc.wal = rc.replayWAL()

	rpeers := make([]raft.Peer, len(rc.store.Peers))
	var i int
	for nid := range rc.store.Peers {
		rpeers[i] = raft.Peer{ID: nid}
		i++
	}
	c := &raft.Config{
		ID:              rc.store.cfg.nodeid,
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         rc.raftStorage,
		MaxSizePerMsg:   1024 * 1024,
		MaxInflightMsgs: 256,
		Logger:          rc.store.cfg.logwrapper,
	}

	if oldwal {
		rc.node = raft.RestartNode(c)
	} else {
		startPeers := rpeers
		if rc.store.joining {
			startPeers = nil
		}
		rc.node = raft.StartNode(c, startPeers)
	}

	rc.transport = &rafthttp.Transport{
		ID:          types.ID(rc.store.cfg.nodeid),
		ClusterID:   types.ID(rc.store.cfg.getRegionHash()),
		Raft:        rc,
		ServerStats: stats.NewServerStats("", ""),
		LeaderStats: stats.NewLeaderStats(strconv.Itoa(int(rc.store.cfg.nodeid))),
		ErrorC:      make(chan error),
		TLSInfo:     rc.store.cfg.raftClientTLSInfo,
	}

	close(startedC)

	rc.transport.Start()
	for pid, purl := range rc.store.Peers {
		rc.transport.AddPeer(types.ID(pid), []string{purl})
	}

	go rc.serveChannels()

	rc.store.cfg.log.Debug("Raft started")
}

// stop closes http, closes all channels, and stops raft.
func (rc *stateRaftNode) stop() {
	rc.stopHTTP()
	close(rc.commitC)
	close(rc.errorC)
	rc.node.Stop()
	close(rc.stoppedc)
}

func (rc *stateRaftNode) stopHTTP() {
	rc.transport.Stop()
	close(rc.httpstopc)
}

func (rc *stateRaftNode) publishSnapshot(snapshotToSave raftpb.Snapshot) {
	if raft.IsEmptySnap(snapshotToSave) {
		return
	}

	rc.store.cfg.log.Debugf("Publishing snapshot at index %d",
		rc.snapshotIndex,
	)
	defer rc.store.cfg.log.Debug("Finished publishing snapshot")

	if snapshotToSave.Metadata.Index <= rc.appliedIndex {
		log.Fatalf("snapshot index [%d] should > progress.appliedIndex [%d] + 1", snapshotToSave.Metadata.Index, rc.appliedIndex)
	}
	rc.commitC <- nil // trigger kvstore to load snapshot

	rc.confState = snapshotToSave.Metadata.ConfState
	rc.snapshotIndex = snapshotToSave.Metadata.Index
	rc.appliedIndex = snapshotToSave.Metadata.Index
}

var snapshotCatchUpEntriesN uint64 = 10000

func (rc *stateRaftNode) maybeTriggerSnapshot() {
	if rc.appliedIndex-rc.snapshotIndex <= rc.snapCount {
		return
	}

	rc.store.cfg.log.Debugf("Starting snapshot applied %d, last %d",
		rc.appliedIndex,
		rc.snapshotIndex,
	)
	data, err := rc.getSnapshot()
	if err != nil {
		log.Panic(err)
	}
	snap, err := rc.raftStorage.CreateSnapshot(rc.appliedIndex, &rc.confState, data)
	if err != nil {
		panic(err)
	}
	if err := rc.saveSnap(snap); err != nil {
		panic(err)
	}

	compactIndex := uint64(1)
	if rc.appliedIndex > snapshotCatchUpEntriesN {
		compactIndex = rc.appliedIndex - snapshotCatchUpEntriesN
	}
	if err := rc.raftStorage.Compact(compactIndex); err != nil {
		panic(err)
	}

	rc.store.cfg.log.Debugf("Compacted log at index %d",
		compactIndex,
	)
	rc.snapshotIndex = rc.appliedIndex
}

func (rc *stateRaftNode) serveChannels() {
	snap, err := rc.raftStorage.Snapshot()
	if err != nil {
		panic(err)
	}
	rc.confState = snap.Metadata.ConfState
	rc.snapshotIndex = snap.Metadata.Index
	rc.appliedIndex = snap.Metadata.Index

	defer rc.wal.Close()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	// send proposals over raft
	go func() {
		var confChangeCount uint64

		for rc.proposeC != nil && rc.confChangeC != nil {
			select {
			case prop, ok := <-rc.proposeC:
				if !ok {
					rc.proposeC = nil
				} else {
					// blocks until accepted by raft state machine
					rc.node.Propose(context.TODO(), prop)
				}

			case cc, ok := <-rc.confChangeC:
				if !ok {
					rc.confChangeC = nil
				} else {
					confChangeCount++
					cc.ID = confChangeCount
					rc.node.ProposeConfChange(context.TODO(), cc)
				}
			}
		}
		// client closed channel; shutdown raft if not already
		close(rc.stopc)
	}()

	// event loop on raft state machine updates
	for {
		select {
		case <-ticker.C:
			rc.node.Tick()

		// store raft entries to wal, then publish over commit channel
		case rd := <-rc.node.Ready():
			rc.wal.Save(rd.HardState, rd.Entries)
			if !raft.IsEmptySnap(rd.Snapshot) {
				rc.saveSnap(rd.Snapshot)
				rc.raftStorage.ApplySnapshot(rd.Snapshot)
				rc.publishSnapshot(rd.Snapshot)
			}
			rc.raftStorage.Append(rd.Entries)
			rc.transport.Send(rd.Messages)
			if ok := rc.publishEntries(rc.entriesToApply(rd.CommittedEntries)); !ok {
				rc.stop()
				return
			}
			rc.maybeTriggerSnapshot()
			rc.node.Advance()

		case err := <-rc.transport.ErrorC:
			rc.writeError(err)
			return

		case <-rc.stopc:
			rc.stop()
			return
		}
	}
}

func (rc *stateRaftNode) Process(ctx context.Context, m raftpb.Message) error {
	return rc.node.Step(ctx, m)
}

func (rc *stateRaftNode) IsIDRemoved(id uint64) bool {
	return false
}

func (rc *stateRaftNode) ReportUnreachable(id uint64) {
	/*rc.store.cfg.log.Debugw("Node found unreachable",
		"nodeid", id,
	)*/
}

func (rc *stateRaftNode) ReportSnapshot(id uint64, status raft.SnapshotStatus) {
	rc.store.cfg.log.Debugf("Snapshot reported by node %d, status %d",
		id,
		status,
	)
}
