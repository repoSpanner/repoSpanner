package service

import (
	"crypto/tls"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/coreos/etcd/raft/raftpb"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"repospanner.org/repospanner/server/datastructures"
	"repospanner.org/repospanner/server/storage"
)

func (cfg *Service) runRPC(errchan chan<- error) {
	lis, err := tls.Listen("tcp", cfg.ListenRPC, cfg.rpcTLSConfig)
	if err != nil {
		errchan <- err
		return
	}
	muxer, ok := cfg.statestore.raftnode.transport.Handler().(*http.ServeMux)
	if !ok {
		panic("Raftnode did not return muxer")
	}

	// Add our RPC handlers here
	muxer.HandleFunc("/rpc/join", cfg.rpcJoinNode)
	muxer.HandleFunc("/rpc/object/single/", cfg.rpcGetSingleObject)
	muxer.HandleFunc("/rpc/object/write/", cfg.rpcWriteSingleObject)
	muxer.HandleFunc("/rpc/repo/", cfg.rpcRepoHandler)

	// Start serving
	cfg.rpcServer = &http.Server{Handler: muxer}
	cfg.log.Debug("RPC server listening")
	err = cfg.rpcServer.Serve(lis)
	cfg.log.Debug("RPC HTTP server shut down")

	if err != http.ErrServerClosed {
		errchan <- errors.Wrap(err, "RPC HTTP error")
	} else {
		errchan <- nil
	}
	return
}

func (cfg *Service) rpcRepoHandler(w http.ResponseWriter, r *http.Request) {
	reqlogger, perminfo := cfg.prereq(w, r, "rpc")

	pathparts := strings.Split(r.URL.Path, "/")[3:]
	reponame, command := findProjectAndOp(pathparts)
	if reponame == "" || command == "" {
		reqlogger.Info("RPC Repo URL requested without repo or command")
		http.NotFound(w, r)
		return
	}
	reqlogger = reqlogger.WithFields(logrus.Fields{
		"reponame": reponame,
		"command":  command,
	})
	if !cfg.statestore.hasRepo(reponame) {
		reqlogger.Debug("Non-existing repo requested")
		http.NotFound(w, r)
		return
	}

	if command == "info/refs" {
		cfg.serveGitDiscovery(w, r, perminfo, reqlogger, reponame, true)
	} else if command == "git-upload-pack" {
		cfg.serveGitUploadPack(w, r, reqlogger, reponame)
	} else {
		reqlogger.Info("Invalid action requested")
		http.NotFound(w, r)
	}
}

func (cfg *Service) rpcWriteSingleObject(w http.ResponseWriter, r *http.Request) {
	cfg.prereq(w, r, "rpc")

	split := strings.Split(r.URL.Path, "/")
	if len(split) < 6 {
		http.Error(w, "Invalid write request", 400)
		return
	}

	projectname, objectidS := findProjectAndOp(split[4:])

	if projectname == "" || objectidS == "" {
		http.Error(w, "Invalid write request", 400)
		return
	}

	if !isValidRef(objectidS) {
		http.Error(w, "Invalid write request", 400)
		return
	}

	objectid := storage.ObjectID(objectidS)

	d := cfg.gitstore.GetProjectStorage(projectname)
	clustered, isclustered := d.(*clusterStorageProjectDriverInstance)
	if isclustered {
		d = clustered.inner
	}

	objtypehdr, ok := r.Header["X-Objecttype"]
	if !ok || len(objtypehdr) != 1 {
		cfg.log.Info("Invalid write attempted: no objecttype")
		http.Error(w, "Invalid write request", 400)
		return
	}
	objtype := storage.ObjectTypeFromHdrName(objtypehdr[0])

	objsizehdr, ok := r.Header["X-Objectsize"]
	if !ok || len(objsizehdr) != 1 {
		cfg.log.Info("Invalid write attempted: no objectsize")
		http.Error(w, "Invalid write request", 400)
		return
	}
	objsizel, err := strconv.ParseUint(objsizehdr[0], 10, 64)
	if err != nil {
		http.Error(w, "Invalid write request", 400)
		return
	}
	objsize := uint(objsizel)

	storedtype, storedsize, storedr, err := d.ReadObject(objectid)
	if err == nil {
		storedr.Close()

		if objsize == storedsize && objtype == storedtype {
			http.Error(w, "Object existed", 200)
			return
		}

		http.Error(w, "Invalid object existed", 409)
		return
	}

	pusher := d.GetPusher("rpc")
	s, err := pusher.StageObject(objtype, objsize)
	if err != nil {
		cfg.log.WithError(err).Info("Invalid write attempted: error while staging")
		http.Error(w, "Invalid write request", 400)
		return
	}

	written, err := io.Copy(s, r.Body)
	if err != nil {
		cfg.log.WithError(err).Info("Invalid write attempted: error while writing")
		http.Error(w, "Error while writing", 500)
		return
	}
	if written != int64(objsizel) {
		cfg.log.Info("Invalid write attempted: not everything written")
		http.Error(w, "Invalid write request", 400)
		return
	}

	_, err = s.Finalize(objectid)
	if err != nil {
		cfg.log.WithError(err).Info("Invalid write attempted: error while finalizing")
		http.Error(w, "Error while writing", 500)
		return
	}

	pusher.Done()
	syncerr := <-pusher.GetPushResultChannel()
	if syncerr != nil {
		cfg.log.WithError(err).Info("Invalid write attempted: error while syncing")
		http.Error(w, "Error while syncing", 500)
		return
	}

	w.WriteHeader(200)
	w.Write([]byte("OK"))
}

func (cfg *Service) rpcGetSingleObject(w http.ResponseWriter, r *http.Request) {
	cfg.prereq(w, r, "rpc")

	split := strings.Split(r.URL.Path, "/")
	if len(split) < 6 {
		http.NotFound(w, r)
		return
	}

	projectname, objid := findProjectAndOp(split[4:])
	objectid := storage.ObjectID(objid)

	if projectname == "" || objectid == "" {
		http.NotFound(w, r)
		return
	}

	projdriver := cfg.gitstore.GetProjectStorage(projectname)
	// Make sure we don't go on endlessly... For the internal request, don't use clustered
	clustered, isclustered := projdriver.(*clusterStorageProjectDriverInstance)
	if isclustered {
		projdriver = clustered.inner
	}

	objtype, objsize, reader, err := projdriver.ReadObject(objectid)

	if err == storage.ErrObjectNotFound {
		http.NotFound(w, r)
		return
	} else if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	w.Header()["X-ObjectType"] = []string{objtype.HdrName()}
	w.Header()["X-ObjectSize"] = []string{strconv.FormatUint(uint64(objsize), 10)}
	w.WriteHeader(200)

	buf := make([]byte, 1024)
	for {
		n, err := reader.Read(buf)
		if err == io.EOF {
			return
		}
		if err != nil {
			cfg.log.WithError(err).Info("Error while reading for streaming object to client")
			return
		}

		out, err := w.Write(buf[:n])
		if err != nil {
			cfg.log.WithError(err).Info("Error while streaming object to client")
			return
		}
		if n != out {
			cfg.log.Info("Unexpected amount of data written")
			return
		}
	}
}

type rpcJoinNodeRequest struct {
	NodeID uint64
	RPCURL string
}

type rpcJoinNodeReply struct {
	Success      bool
	ErrorMessage string
	NodeInfo     datastructures.NodeInfo
}

func (cfg *Service) rpcJoinNode(w http.ResponseWriter, r *http.Request) {
	cfg.prereq(w, r, "rpc")

	var joinrequest rpcJoinNodeRequest
	if cont := cfg.parseJSONRequest(w, r, &joinrequest); !cont {
		return
	}

	reply := rpcJoinNodeReply{
		NodeInfo: cfg.getNodeInfo(),
	}

	confchange := raftpb.ConfChange{
		Type:    raftpb.ConfChangeAddNode,
		NodeID:  joinrequest.NodeID,
		Context: []byte(joinrequest.RPCURL),
	}
	ccC := cfg.statestore.subscribeConfChange()
	defer cfg.statestore.unsubscribeConfChange(ccC)
	cfg.statestore.confChangeC <- confchange
	// TODO: Add a resend timer
	for msg := range ccC {
		if msg.NodeID == joinrequest.NodeID {
			// It's about the current node!
			if msg.Type == raftpb.ConfChangeAddNode {
				// We were added!
				reply.Success = true
			} else if msg.Type == raftpb.ConfChangeRemoveNode {
				reply.Success = false
				reply.ErrorMessage = "Node was removed?"
			} else {
				cfg.log.Info("Unexpected config change for node arrived")
			}
			break
		}
	}

	if reply.Success {
		cfg.log.Info("Node join request succesful")
	} else {
		cfg.log.Info("Node join failed")
	}

	cfg.respondJSONResponse(w, reply)
}
