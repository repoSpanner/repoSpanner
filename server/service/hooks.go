package service

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/rpc"
	"os"
	"os/exec"
	"syscall"

	"github.com/pkg/errors"
	"github.com/spf13/viper"

	"repospanner.org/repospanner/server/datastructures"
	pb "repospanner.org/repospanner/server/protobuf"
	"repospanner.org/repospanner/server/storage"
)

type hookType string

const (
	hookTypePreReceive  hookType = "pre-receive"
	hookTypeUpdate      hookType = "update"
	hookTypePostReceive hookType = "post-receive"
)

type hookRunning interface {
	finishPreparing() error
	fetchFakeRefs() error
	runHook(hook hookType) error
	close()
}

type nullHookRunning struct{}

func (n nullHookRunning) finishPreparing() error {
	return nil
}

func (n nullHookRunning) fetchFakeRefs() error {
	return nil
}

func (n nullHookRunning) runHook(hook hookType) error {
	return nil
}

func (n nullHookRunning) close() {}

type localHookRunning struct {
	cmdnum   int
	proc     *exec.Cmd
	procdone <-chan interface{}
	control  *os.File
	client   *rpc.Client

	cancel context.CancelFunc

	prepareCall *rpc.Call
}

func (r *localHookRunning) finishPreparing() error {
	if r.prepareCall == nil {
		return errors.New("Finish preparing called without outstanding call")
	}
	call := r.prepareCall
	r.prepareCall = nil
	reply := <-call.Done
	return reply.Error
}

func (r *localHookRunning) fetchFakeRefs() error {
	if r.prepareCall != nil {
		return errors.New("Call done with outstanding prepare")
	}

	return r.client.Call("Hook.FetchFakeRefs", 0, nil)
}

func (r *localHookRunning) runHook(hook hookType) error {
	if r.prepareCall != nil {
		return errors.New("Call done with outstanding prepare")
	}

	return r.client.Call("Hook.RunHook", string(hook), nil)
}

func (r *localHookRunning) close() {
	r.cancel()
}

func (r *localHookRunning) runCtxHandler(ctx context.Context) {
	<-ctx.Done()

	r.control.Close()

	r.proc.Process.Signal(syscall.SIGTERM)

	select {
	// TODO: Add timeout
	case <-r.procdone:
		// Process exited normally
	}

	r.control.Close()
}

func (cfg *Service) prepareHookRunning(ctx context.Context, errout, infoout io.Writer, projectname string, request *pb.PushRequest, extraEnv map[string]string) (hookRunning, error) {
	hooks := cfg.statestore.GetRepoHooks(projectname)
	if hooks.PreReceive == "" || hooks.Update == "" || hooks.PostReceive == "" {
		return nil, fmt.Errorf("Hook not configured??")
	}
	if storage.ObjectID(hooks.PreReceive) == storage.ZeroID && storage.ObjectID(hooks.Update) == storage.ZeroID && storage.ObjectID(hooks.PostReceive) == storage.ZeroID {
		return nullHookRunning{}, nil
	}

	// We have a hook to run! Let's prepare the request
	var err error
	var clientcacert = []byte{}
	if cfg.ClientCaCertFile != "" {
		clientcacert, err = ioutil.ReadFile(cfg.ClientCaCertFile)
		if err != nil {
			return nil, errors.Wrap(err, "Error preparing hook calling")
		}
	}
	clientcert, err := ioutil.ReadFile(cfg.ClientCertificate)
	if err != nil {
		return nil, errors.Wrap(err, "Error preparing hook calling")
	}
	clientkey, err := ioutil.ReadFile(cfg.ClientKey)
	if err != nil {
		return nil, errors.Wrap(err, "Error preparing hook calling")
	}

	hookreq := datastructures.HookRunRequest{
		Debug:        viper.GetBool("hooks.debug"),
		RPCURL:       cfg.findRPCURL(),
		PushUUID:     request.UUID(),
		BwrapConfig:  viper.GetStringMap("hooks.bubblewrap"),
		User:         viper.GetInt("hooks.user"),
		ProjectName:  projectname,
		HookObjects:  make(map[string]string),
		Requests:     make(map[string][2]string),
		ClientCaCert: string(clientcacert),
		ClientCert:   string(clientcert),
		ClientKey:    string(clientkey),
		ExtraEnv:     extraEnv,
	}
	for _, req := range request.Requests {
		reqt := [2]string{req.GetFrom(), req.GetTo()}
		hookreq.Requests[req.GetRef()] = reqt
	}

	// Add hook objects
	hookreq.HookObjects[string(hookTypePreReceive)] = hooks.PreReceive
	hookreq.HookObjects[string(hookTypeUpdate)] = hooks.Update
	hookreq.HookObjects[string(hookTypePostReceive)] = hooks.PostReceive

	// Marshal the request
	req, err := json.Marshal(hookreq)
	if err != nil {
		return nil, errors.Wrap(err, "Error preparing hook calling")
	}
	reqbuf := bytes.NewBuffer(req)

	// Start the local hook runner
	return cfg.runLocalHookBinary(ctx, errout, infoout, reqbuf)
}

func getSocketPair() (*os.File, *os.File, error) {
	socks, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_STREAM, 0)
	if err != nil {
		return nil, nil, err
	}

	sockParent := os.NewFile(uintptr(socks[0]), "control_parent")
	sockChild := os.NewFile(uintptr(socks[1]), "control_child")
	if sockParent == nil || sockChild == nil {
		syscall.Close(socks[0])
		syscall.Close(socks[1])
		return nil, nil, errors.New("Error creating files for control channel")
	}

	return sockParent, sockChild, nil
}

func (cfg *Service) runLocalHookBinary(ctx context.Context, errout, infoout io.Writer, req io.Reader) (hookRunning, error) {
	binary := viper.GetString("hooks.runner")
	if binary == "" {
		return nil, errors.New("No hook runner configured")
	}
	cmd := exec.Command(
		binary,
	)
	cmd.Stdin = req
	cmd.Stdout = infoout
	cmd.Stderr = errout

	// Add control channels
	sockParent, sockChild, err := getSocketPair()
	if err != nil {
		return nil, errors.Wrap(err, "Error creating hook control channel")
	}
	cmd.ExtraFiles = []*os.File{sockChild}

	// Start the binary
	err = cmd.Start()
	if err != nil {
		return nil, errors.Wrap(err, "Error executing hook")
	}
	procdone := make(chan interface{})
	go func() {
		cmd.Wait()
		close(procdone)
	}()

	client := rpc.NewClient(sockParent)

	// Start the prepare call
	prepareCall := client.Go("Hook.Prepare", 0, nil, nil)

	// Create a child context for killing the process
	hookctx, cancel := context.WithCancel(ctx)

	// And now, just return all that goodness
	runner := &localHookRunning{
		proc:     cmd,
		procdone: procdone,
		control:  sockParent,
		client:   client,

		prepareCall: prepareCall,

		cancel: cancel,
	}

	go runner.runCtxHandler(hookctx)

	return runner, nil
}
