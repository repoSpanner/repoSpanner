package service

import (
	"bytes"
	"encoding/json"
	"io"
	"io/ioutil"
	"os/exec"

	"github.com/pkg/errors"
	"github.com/spf13/viper"

	"repospanner.org/repospanner/server/datastructures"
	pb "repospanner.org/repospanner/server/protobuf"
	"repospanner.org/repospanner/server/storage"
)

type hookType string

const (
	hookTypePreReceive  hookType = "pre-receieve"
	hookTypeUpdate      hookType = "update"
	hookTypePostReceive hookType = "post-receive"
)

func (cfg *Service) runHook(hook hookType, errout, infoout io.Writer, projectname string, request *pb.PushRequest) error {
	hooks := cfg.statestore.GetRepoHooks(projectname)
	var hookid storage.ObjectID
	switch hook {
	case hookTypePreReceive:
		hookid = storage.ObjectID(hooks.PreReceive)
	case hookTypeUpdate:
		hookid = storage.ObjectID(hooks.Update)
	case hookTypePostReceive:
		hookid = storage.ObjectID(hooks.PostReceive)
	}
	if hookid == "" {
		return errors.New("Hook not configured??")
	}
	if hookid == storage.ZeroID {
		// No hook configured, nothing to run
		return nil
	}

	// We have a hook to run! Let's prepare the request
	var err error
	var clientcacert = []byte{}
	if cfg.ClientCaCertFile != "" {
		clientcacert, err = ioutil.ReadFile(cfg.ClientCaCertFile)
		if err != nil {
			return errors.Wrap(err, "Error preparing hook calling")
		}
	}
	clientcert, err := ioutil.ReadFile(cfg.ClientCertificate)
	if err != nil {
		return errors.Wrap(err, "Error preparing hook calling")
	}
	clientkey, err := ioutil.ReadFile(cfg.ClientKey)
	if err != nil {
		return errors.Wrap(err, "Error preparing hook calling")
	}

	hookreq := datastructures.HookRunRequest{
		RPCURL:       cfg.findRPCURL(),
		PushUUID:     request.UUID(),
		BwrapConfig:  viper.GetStringMap("hooks.bubblewrap"),
		User:         viper.GetInt("hooks.user"),
		ProjectName:  projectname,
		Hook:         string(hook),
		HookObject:   string(hookid),
		Requests:     make(map[string][2]string),
		ClientCaCert: string(clientcacert),
		ClientCert:   string(clientcert),
		ClientKey:    string(clientkey),
	}
	for _, req := range request.Requests {
		reqt := [2]string{req.GetFrom(), req.GetTo()}
		hookreq.Requests[req.GetRef()] = reqt
	}
	req, err := json.Marshal(hookreq)
	if err != nil {
		return errors.Wrap(err, "Error preparing hook calling")
	}
	reqbuf := bytes.NewBuffer(req)

	// Okay, we are all set. Let's now run this hook runner
	return cfg.runHookBinary(errout, infoout, reqbuf)
}

func (cfg *Service) runHookBinary(errout, infoout io.Writer, req io.Reader) error {
	binary := viper.GetString("hooks.runner")
	if binary == "" {
		return errors.New("No hook runner configured")
	}
	cmd := exec.Command(
		binary,
	)
	cmd.Stdin = req
	cmd.Stdout = infoout
	cmd.Stderr = errout

	err := cmd.Run()
	if err != nil {
		return errors.Wrap(err, "Error executing hook")
	}
	return nil
}
