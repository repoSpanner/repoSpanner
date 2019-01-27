package hookrun

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path"

	"github.com/pkg/errors"
	"repospanner.org/repospanner/server/constants"
	"repospanner.org/repospanner/server/datastructures"
	"repospanner.org/repospanner/server/storage"
)

func prepareRequest() (*datastructures.HookRunRequest, error) {
	request, err := getRequest()
	if err != nil {
		return nil, errors.Wrap(err, "Error getting request")
	}

	if debug {
		fmt.Println("repoSpanner Hook Runner " + constants.PublicVersionString())
	}

	// Before doing anything else, lower privileges
	if request.User != 0 {
		err := dropToUser(request.User)
		if err != nil {
			return nil, errors.Wrap(err, "Error dropping privileges")
		}
	}

	return &request, nil
}

func getRequest() (hrr datastructures.HookRunRequest, err error) {
	if !constants.VersionBuiltIn() {
		fmt.Fprintf(os.Stderr, "Invalid build")
		err = errors.New("Invalid build")
		return
	}
	if len(os.Args) == 2 && os.Args[1] == "--version" {
		fmt.Println("repoSpanner Hook Runner " + constants.PublicVersionString())
		os.Exit(0)
	}

	breq, err := ioutil.ReadAll(os.Stdin)
	if err != nil {
		return
	}

	err = json.Unmarshal(breq, &hrr)
	if err != nil {
		return
	}

	debug = hrr.Debug
	return
}

func cloneRepository(request datastructures.HookRunRequest) (string, error) {
	workdir, err := ioutil.TempDir("", "repospanner_hook_runner_")
	if err != nil {
		return "", errors.Wrap(err, "Error creating runner work directory")
	}

	armShutdownHandler(workdir)

	err = os.Mkdir(path.Join(workdir, "hookrun"), 0755)
	if err != nil {
		return "", errors.Wrap(err, "Error creating runner work directory")
	}
	err = os.Mkdir(path.Join(workdir, "keys"), 0700)
	if err != nil {
		return "", errors.Wrap(err, "Error creating runner keys directory")
	}
	err = ioutil.WriteFile(
		path.Join(workdir, "keys", "ca.crt"),
		[]byte(request.ClientCaCert),
		0600,
	)
	if err != nil {
		return "", errors.Wrap(err, "Error writing CA cert")
	}
	err = ioutil.WriteFile(
		path.Join(workdir, "keys", "client.crt"),
		[]byte(request.ClientCert),
		0600,
	)
	if err != nil {
		return "", errors.Wrap(err, "Error writing client cert")
	}
	err = ioutil.WriteFile(
		path.Join(workdir, "keys", "client.key"),
		[]byte(request.ClientKey),
		0600,
	)
	if err != nil {
		return "", errors.Wrap(err, "Error writing client key")
	}

	cmd := exec.Command(
		"git",
		"clone",
		"--bare",
		"--config", "http.sslcainfo="+path.Join(workdir, "keys", "ca.crt"),
		"--config", "http.sslcert="+path.Join(workdir, "keys", "client.crt"),
		"--config", "http.sslkey="+path.Join(workdir, "keys", "client.key"),
		request.RPCURL+"/rpc/repo/"+request.ProjectName+".git",
		path.Join(workdir, "hookrun", "clone"),
	)
	if debug {
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
	}
	err = cmd.Run()
	if err != nil {
		return "", errors.Wrap(err, "Error cloning the repository")
	}

	// Delete Git's .sample hook files
	hookfiles, err := ioutil.ReadDir(path.Join(workdir, "hookrun", "clone", "hooks"))
	if err != nil {
		return "", errors.Wrap(err, "Error getting hookfiles")
	}
	for _, hookfile := range hookfiles {
		err := os.Remove(path.Join(workdir, "hookrun", "clone", "hooks", hookfile.Name()))
		if err != nil {
			return "", errors.Wrap(err, "Error removing hookfile")
		}
	}

	return workdir, nil
}

func fetchFakeRefs(request datastructures.HookRunRequest, workdir string) error {
	// At this point, we have started preparations.
	tos := make([]string, 0)

	for refname, req := range request.Requests {
		if req[1] == string(storage.ZeroID) {
			// This is a deletion. Nothing to fetch for that
			continue
		}
		tos = append(tos, fmt.Sprintf("refs/heads/fake/%s/%s", request.PushUUID, refname))
	}

	if len(tos) == 0 {
		return nil
	}

	cmdstr := []string{
		"fetch",
		"origin",
	}
	cmdstr = append(cmdstr, tos...)
	cmd := exec.Command(
		"git",
		cmdstr...,
	)
	cmd.Dir = path.Join(workdir, "hookrun", "clone")
	if debug {
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
	}
	err := cmd.Run()
	return errors.Wrap(err, "Error grabbing the To objects")
}

func deleteKeys(request *datastructures.HookRunRequest, workdir string) error {
	// We are done cloning, let's remove the keys from file system and memory
	// This is under the mantra "What's not there, can't be stolen"
	err := os.RemoveAll(path.Join(workdir, "keys"))
	if err != nil {
		return errors.Wrap(err, "Error removing keys from disk")
	}
	request.ClientCert = ""
	request.ClientKey = ""
	request.ClientCaCert = ""

	keysDeleted = true

	return nil
}
