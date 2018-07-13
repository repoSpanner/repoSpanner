package service

import (
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"time"

	"repospanner.org/repospanner/server/constants"
	"repospanner.org/repospanner/server/datastructures"
	"repospanner.org/repospanner/server/storage"
)

func (cfg *Service) getNodeInfo() datastructures.NodeInfo {
	return datastructures.NodeInfo{
		NodeID:      cfg.nodeid,
		NodeName:    cfg.nodename,
		RegionName:  cfg.region,
		ClusterName: cfg.cluster,
		Version:     constants.VersionString(),
		Peers:       cfg.statestore.Peers,
	}
}

func (cfg *Service) parseJSONRequest(w http.ResponseWriter, r *http.Request, out interface{}) (cont bool) {
	if r.Method != "POST" {
		w.WriteHeader(405)
		w.Write([]byte("POST required"))
		return false
	}
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(500)
		w.Write([]byte("Error parsing request"))
		return false
	}

	err = json.Unmarshal(body, out)
	if err != nil {
		w.WriteHeader(500)
		w.Write([]byte("Error parsing request"))
		return false
	}
	return true
}

func (cfg *Service) respondJSONResponse(w http.ResponseWriter, in interface{}) {
	cts, err := json.Marshal(in)
	if err != nil {
		w.WriteHeader(500)
		w.Write([]byte("Error formatting nodeinfo"))
		return
	}

	w.WriteHeader(200)
	w.Write(cts)
}

func (cfg *Service) serveAdminNodeInfo(w http.ResponseWriter, r *http.Request) {
	info := cfg.getNodeInfo()
	cfg.respondJSONResponse(w, info)
}

func (cfg *Service) serveAdminCreateRepo(w http.ResponseWriter, r *http.Request) {
	var createreporequest datastructures.RepoRequestInfo
	if cont := cfg.parseJSONRequest(w, r, &createreporequest); !cont {
		return
	}

	err := cfg.statestore.createRepo(
		createreporequest.Reponame,
		createreporequest.Public,
	)
	if err != nil {
		w.WriteHeader(200)
		cfg.respondJSONResponse(w, datastructures.CommandResponse{
			Success: false,
			Error:   err.Error(),
		})
		return
	}

	w.WriteHeader(200)
	cfg.respondJSONResponse(w, datastructures.CommandResponse{
		Success: true,
	})
	return
}

func (cfg *Service) serveAdminEditRepo(w http.ResponseWriter, r *http.Request) {
	var editreporequest datastructures.RepoUpdateRequest
	if cont := cfg.parseJSONRequest(w, r, &editreporequest); !cont {
		return
	}

	remarshal, err := json.Marshal(editreporequest)
	if err != nil {
		w.WriteHeader(200)
		cfg.respondJSONResponse(w, datastructures.CommandResponse{
			Success: false,
			Error:   err.Error(),
		})
		return
	}

	err = cfg.statestore.editRepo(
		editreporequest.Reponame,
		remarshal,
	)
	if err != nil {
		w.WriteHeader(200)
		cfg.respondJSONResponse(w, datastructures.CommandResponse{
			Success: false,
			Error:   err.Error(),
		})
		return
	}

	w.WriteHeader(200)
	cfg.respondJSONResponse(w, datastructures.CommandResponse{
		Success: true,
	})
	return
}

func (cfg *Service) serveAdminListRepos(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(200)
	cfg.respondJSONResponse(w, datastructures.RepoList{
		Repos: cfg.statestore.GetRepos(),
	})
	return
}

func (cfg *Service) serveAdminHooksMgmt(w http.ResponseWriter, r *http.Request) {
	pathparts := strings.Split(r.URL.Path, "/")[1:]
	pathparts = pathparts[2:]
	reponame, command := findProjectAndOp(pathparts)
	if reponame == "" || command == "" {
		cfg.log.Debug("Repo URL requested without repo or command")
		http.NotFound(w, r)
		return
	}
	if command != "upload" {
		cfg.log.Info("Command not understood")
		http.NotFound(w, r)
		return
	}
	w.WriteHeader(200)

	sizes := r.Header["X-Object-Size"]
	if len(sizes) != 1 {
		cfg.log.Info("Missing object size")
		cfg.respondJSONResponse(w, datastructures.CommandResponse{
			Success: false,
			Error:   "Missing object size",
		})
		return
	}
	size, err := strconv.Atoi(sizes[0])
	if err != nil {
		cfg.respondJSONResponse(w, datastructures.CommandResponse{
			Success: false,
			Error:   err.Error(),
		})
		return
	}

	drv := cfg.gitstore.GetProjectStorage(constants.HooksRepoName)
	psh := drv.GetPusher("admin-hook-" + strconv.Itoa(int(time.Now().UTC().UnixNano())))
	stg, err := psh.StageObject(
		storage.ObjectTypeBlob,
		uint(size),
	)
	if err != nil {
		cfg.respondJSONResponse(w, datastructures.CommandResponse{
			Success: false,
			Error:   err.Error(),
		})
		return
	}
	written, err := io.Copy(stg, r.Body)
	if err != nil {
		cfg.respondJSONResponse(w, datastructures.CommandResponse{
			Success: false,
			Error:   err.Error(),
		})
		return
	}
	if int(written) != size {
		cfg.respondJSONResponse(w, datastructures.CommandResponse{
			Success: false,
			Error:   "Not full object written",
		})
		return
	}
	objid, err := stg.Finalize(storage.ZeroID)
	if err != nil {
		cfg.respondJSONResponse(w, datastructures.CommandResponse{
			Success: false,
			Error:   err.Error(),
		})
		return
	}
	cfg.log.Info("Wrote hook file")
	psh.Done()
	err = <-psh.GetPushResultChannel()
	if err != nil {
		cfg.respondJSONResponse(w, datastructures.CommandResponse{
			Success: false,
			Error:   err.Error(),
		})
		return
	}
	cfg.respondJSONResponse(w, datastructures.CommandResponse{
		Success: true,
		Info:    string(objid),
	})
}
