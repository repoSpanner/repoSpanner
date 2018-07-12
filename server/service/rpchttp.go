package service

import (
	"crypto/tls"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/coreos/etcd/raft/raftpb"
	"github.com/pkg/errors"
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
	cfg.log.Debugw("RPC server listening",
		"bind", cfg.ListenRPC,
	)
	err = cfg.rpcServer.Serve(lis)
	cfg.log.Infow("RPC HTTP server shut down",
		"err", err,
	)

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
	reqlogger = reqlogger.With(
		"reponame", reponame,
		"command", command,
	)
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
		cfg.log.Infow("Invalid write attempted",
			"pathinfo", r.URL.Path,
		)

		http.Error(w, "Invalid write request", 400)
		return
	}

	projectname, objectidS := findProjectAndOp(split[4:])

	if projectname == "" || objectidS == "" {
		cfg.log.Infow("Invalid write attempted",
			"pathinfo", r.URL.Path,
			"projectname", projectname,
			"objectid", objectidS,
		)

		http.Error(w, "Invalid write request", 400)
		return
	}

	if !isValidRef(objectidS) {
		cfg.log.Infow("Invalid objectid write attempted",
			"pathinfo", r.URL.Path,
			"projectname", projectname,
			"objectid", objectidS,
		)

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
		cfg.log.Infow("Invalid write attempted: no objecttype")
		http.Error(w, "Invalid write request", 400)
		return
	}
	objtype := storage.ObjectTypeFromHdrName(objtypehdr[0])

	objsizehdr, ok := r.Header["X-Objectsize"]
	if !ok || len(objsizehdr) != 1 {
		cfg.log.Infow("Invalid write attempted: no objectsize")
		http.Error(w, "Invalid write request", 400)
		return
	}
	objsizel, err := strconv.ParseUint(objsizehdr[0], 10, 64)
	if err != nil {
		cfg.log.Infow("Invalid write attempted: invalid objectsize",
			"error", err,
		)
		http.Error(w, "Invalid write request", 400)
		return
	}
	objsize := uint(objsizel)

	storedtype, storedsize, storedr, err := d.ReadObject(objectid)
	if err == nil {
		storedr.Close()

		if objsize == storedsize && objtype == storedtype {
			cfg.log.Debugw(
				"Object write attempted that we already have, pre-emtively returning",
				"pathinfo", r.URL.Path,
				"projectname", projectname,
				"objectid", objectid,
			)

			http.Error(w, "Object existed", 200)
			return
		}

		cfg.log.Infow(
			"Object attempted to be written with different size and/or type",
			"pathinfo", r.URL.Path,
			"projectname", projectname,
			"objectid", objectid,
			"storedsize", storedsize,
			"objsize", objsize,
			"storedtype", storedtype,
			"objtype", objtype,
		)
		http.Error(w, "Invalid object existed", 409)
		return
	}

	pusher := d.GetPusher("rpc")
	s, err := pusher.StageObject(objtype, objsize)
	if err != nil {
		cfg.log.Infow("Invalid write attempted: error while staging",
			"error", err,
		)
		http.Error(w, "Invalid write request", 400)
		return
	}

	written, err := io.Copy(s, r.Body)
	if err != nil {
		cfg.log.Infow("Invalid write attempted: error while writing",
			"error", err,
		)
		http.Error(w, "Error while writing", 500)
		return
	}
	if written != int64(objsizel) {
		cfg.log.Infow("Invalid write attempted: not everything written",
			"objsize", objsize,
			"written", written,
		)
		http.Error(w, "Invalid write request", 400)
		return
	}

	_, err = s.Finalize(objectid)
	if err != nil {
		cfg.log.Infow("Invalid write attempted: error while finalizing",
			"error", err,
		)
		http.Error(w, "Error while writing", 500)
		return
	}

	pusher.Done()
	syncerr := <-pusher.GetPushResultChannel()
	if syncerr != nil {
		cfg.log.Infow("Invalid write attempted: error while syncing",
			"error", err,
		)
		http.Error(w, "Error while syncing", 500)
		return
	}

	cfg.log.Debugw("Received pushed object",
		"project", projectname,
		"objectid", objectid,
		"objecttype", objtype,
		"objectsize", objsize)

	w.WriteHeader(200)
	w.Write([]byte("OK"))
}

func (cfg *Service) rpcGetSingleObject(w http.ResponseWriter, r *http.Request) {
	cfg.prereq(w, r, "rpc")

	split := strings.Split(r.URL.Path, "/")
	if len(split) < 6 {
		cfg.log.Debugw("Invalid Single Object request",
			"pathinfo", r.URL.Path,
		)

		http.NotFound(w, r)
		return
	}

	projectname, objid := findProjectAndOp(split[4:])
	objectid := storage.ObjectID(objid)

	if projectname == "" || objectid == "" {
		cfg.log.Debugw("Invalid Single Object request",
			"pathinfo", r.URL.Path,
			"projectname", projectname,
			"objectid", objectid,
		)

		http.NotFound(w, r)
		return
	}

	cfg.log.Debugw("Single object requested by peer",
		"project", projectname,
		"objectid", objectid,
	)

	projdriver := cfg.gitstore.GetProjectStorage(projectname)
	// Make sure we don't go on endlessly... For the internal request, don't use clustered
	clustered, isclustered := projdriver.(*clusterStorageProjectDriverInstance)
	if isclustered {
		projdriver = clustered.inner
	}

	objtype, objsize, reader, err := projdriver.ReadObject(objectid)

	if err == storage.ErrObjectNotFound {
		cfg.log.Debugw("Non-existing object requested",
			"project", projectname,
			"objectid", objectid,
		)

		http.NotFound(w, r)
		return
	} else if err != nil {
		cfg.log.Infow("Error retrieving requested object",
			"project", projectname,
			"objectid", objectid,
			"error", err,
		)
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
			cfg.log.Infow("Error while reading for streaming object to client",
				"error", err,
			)
			return
		}

		out, err := w.Write(buf[:n])
		if err != nil {
			cfg.log.Infow("Error while streaming object to client",
				"error", err,
			)
			return
		}
		if n != out {
			cfg.log.Infow("Unexpected amount of data written",
				"in", n,
				"out", out,
			)
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
	reqlogger, _ := cfg.prereq(w, r, "rpc")

	var joinrequest rpcJoinNodeRequest
	if cont := cfg.parseJSONRequest(w, r, &joinrequest); !cont {
		return
	}

	reqlogger.Infow("Join request received",
		"nodeid", joinrequest.NodeID,
		"rpcurl", joinrequest.RPCURL,
	)

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
				cfg.log.Infow("Unexpected config change for node arrived",
					"change", msg,
				)
			}
			break
		}
	}

	if reply.Success {
		cfg.log.Infow("Node join request succesful",
			"nodeid", joinrequest.NodeID,
		)
	} else {
		cfg.log.Infow("Node join failed",
			"nodeid", joinrequest.NodeID,
			"error", reply.ErrorMessage,
		)
	}

	cfg.respondJSONResponse(w, reply)
}
