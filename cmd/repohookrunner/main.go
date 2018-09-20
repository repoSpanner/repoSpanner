package main

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"os/exec"
	"path"
	"strconv"
	"strings"
	"syscall"

	"golang.org/x/net/http2"

	"repospanner.org/repospanner/server/datastructures"
	"repospanner.org/repospanner/server/storage"

	"repospanner.org/repospanner/server/constants"
)

func failIfError(err error, msg string) {
	if err != nil {
		failNow(msg)
	}
}

func failNow(msg string) {
	fmt.Fprintln(os.Stderr, msg)
	panic("Erroring")
}

func main() {
	defer func() {
		/*if r := recover(); r != nil {
			fmt.Fprintln(os.Stderr, "Error occured")
			os.Exit(1)
		}*/
	}()

	if !constants.VersionBuiltIn() {
		fmt.Fprintf(os.Stderr, "Invalid build")
		os.Exit(1)
	}
	if len(os.Args) == 2 && os.Args[1] == "--version" {
		fmt.Println("repoSpanner Hook Runner " + constants.PublicVersionString())
		os.Exit(0)
	}

	breq, err := ioutil.ReadAll(os.Stdin)
	failIfError(err, "Error reading request")

	var request datastructures.HookRunRequest
	err = json.Unmarshal(breq, &request)
	failIfError(err, "Error parsing request")

	// Before doing anything else, lower privileges
	if request.User != 0 {
		err = syscall.Setresuid(request.User, request.User, request.User)
		failIfError(err, "Error dropping privileges")
	}

	// At this moment, we have lowered privileges. Clone the repo
	workdir, err := ioutil.TempDir("", "repospanner_hook_runner_")
	failIfError(err, "Error creating runner work directory")
	defer os.RemoveAll(workdir)
	err = os.Mkdir(path.Join(workdir, "hookrun"), 0755)
	failIfError(err, "Error creating runner work directory")
	err = os.Mkdir(path.Join(workdir, "keys"), 0700)
	failIfError(err, "Error creating runner keys directory")
	err = ioutil.WriteFile(
		path.Join(workdir, "keys", "ca.crt"),
		[]byte(request.ClientCaCert),
		0600,
	)
	failIfError(err, "Error writing keys")
	err = ioutil.WriteFile(
		path.Join(workdir, "keys", "client.crt"),
		[]byte(request.ClientCert),
		0600,
	)
	failIfError(err, "Error writing keys")
	err = ioutil.WriteFile(
		path.Join(workdir, "keys", "client.key"),
		[]byte(request.ClientKey),
		0600,
	)
	failIfError(err, "Error writing keys")

	cmd := exec.Command(
		"git",
		"clone",
		"--config", "http.sslcainfo="+path.Join(workdir, "keys", "ca.crt"),
		"--config", "http.sslcert="+path.Join(workdir, "keys", "client.crt"),
		"--config", "http.sslkey="+path.Join(workdir, "keys", "client.key"),
		request.RPCURL+"/rpc/repo/"+request.ProjectName+".git",
		path.Join(workdir, "hookrun", "clone"),
	)
	err = cmd.Run()
	failIfError(err, "Error cloning the repository")

	for refname, req := range request.Requests {
		if req[1] == string(storage.ZeroID) {
			// This is a deletion. Nothing to fetch for that
			continue
		}
		cmd := exec.Command(
			"git",
			"fetch",
			"origin",
			fmt.Sprintf("refs/heads/fake/%s/%s", request.PushUUID, refname),
		)
		cmd.Dir = path.Join(workdir, "hookrun", "clone")
		err = cmd.Run()
		failIfError(err, "Error grabbing the To objects")
	}
	getScript(request, workdir)

	// We are done cloning, let's remove the keys from file system and memory
	// This is under the phrasing "What's not there, can't be stolen"
	err = os.RemoveAll(path.Join(workdir, "keys"))
	failIfError(err, "Error removing keys")
	request.ClientCert = ""
	request.ClientKey = ""
	request.ClientCaCert = ""

	// We have now downloaded the repo and script, and removed our keys.
	// Let's actually get to running this hook!
	// But first.... a message from our sponsors!
	if request.Hook == "update" {
		for branch, req := range request.Requests {
			runHook(request, workdir, branch, req)
		}
	} else {
		runHook(request, workdir, "", [2]string{"", ""})
	}
}

func getHookArgs(request datastructures.HookRunRequest, branch string, req [2]string) ([]string, io.Reader) {
	buf := bytes.NewBuffer(nil)
	if request.Hook == "update" {
		return []string{branch, req[0], req[1]}, buf
	} else {
		for branch, req := range request.Requests {
			fmt.Fprintf(buf, "%s %s %s\n", req[0], req[1], branch)
		}
		return []string{}, buf
	}
}

func runHook(request datastructures.HookRunRequest, workdir string, branch string, req [2]string) {
	hookArgs, hookbuf := getHookArgs(request, branch, req)
	usebwrap, bwrapcmd := getBwrapConfig(request.BwrapConfig)

	var cmd *exec.Cmd
	if usebwrap {
		bwrapcmd = append(
			bwrapcmd,
			"--bind", path.Join(workdir, "hookrun"), "/hookrun",
			"--chdir", "/hookrun/clone",
			"--setenv", "GIT_DIR", "/hookrun/clone",
			"--setenv", "HOOKTYPE", request.Hook,
		)

		for key, val := range request.ExtraEnv {
			bwrapcmd = append(
				bwrapcmd,
				"--setenv", "extra_"+key, val,
			)
		}

		bwrapcmd = append(
			bwrapcmd,
			path.Join("/hookrun", request.Hook),
		)
		bwrapcmd = append(
			bwrapcmd,
			hookArgs...,
		)
		cmd = exec.Command(
			"bwrap",
			bwrapcmd...,
		)
	} else {
		cmd = exec.Command(
			path.Join(workdir, "hookrun", request.Hook),
			hookArgs...,
		)
		cmd.Dir = path.Join(workdir, "hookrun", "clone")
		cmd.Env = []string{
			"GIT_DIR=" + cmd.Path,
		}

		for key, val := range request.ExtraEnv {
			cmd.Env = append(
				cmd.Env,
				"extra_"+key+"="+val,
			)
		}
	}

	cmd.Stdin = hookbuf
	cmd.Stderr = os.Stderr
	cmd.Stdout = os.Stdout
	err := cmd.Run()
	if err == nil {
		return
	}

	_, iseerr := err.(*exec.ExitError)
	if iseerr {
		fmt.Fprintln(os.Stderr, "Hook returned error")
		panic("Aborting")
	}
	failIfError(err, "Error running hook")
}

func getScript(request datastructures.HookRunRequest, workdir string) {
	cert, err := tls.LoadX509KeyPair(
		path.Join(workdir, "keys", "client.crt"),
		path.Join(workdir, "keys", "client.key"),
	)
	failIfError(err, "Unable to load client keys")

	certpool := x509.NewCertPool()
	certpool.AppendCertsFromPEM([]byte(request.ClientCaCert))

	clientconf := &tls.Config{
		Certificates: []tls.Certificate{cert},
		NextProtos:   []string{"h2"},
		RootCAs:      certpool,
	}
	transport := &http.Transport{
		TLSClientConfig: clientconf,
	}
	err = http2.ConfigureTransport(transport)
	failIfError(err, "Error reconfiguring transport")
	clnt := &http.Client{
		Transport: transport,
	}

	// Grab the hook script itself
	script, err := os.Create(path.Join(workdir, "hookrun", request.Hook))
	failIfError(err, "Error opening script file")
	resp, err := clnt.Get(
		request.RPCURL + "/rpc/object/single/admin/hooks.git/" + request.HookObject,
	)
	failIfError(err, "Error retrieving hook script")
	if resp.StatusCode != 200 {
		fmt.Fprintln(os.Stderr, "Unable to retrieve hook script")
		panic("Error")
	}

	_, err = io.Copy(script, resp.Body)
	resp.Body.Close()
	failIfError(err, "Error writing script file")
	err = script.Close()
	failIfError(err, "Error flushing script file")

	err = os.Chmod(script.Name(), 0755)
	failIfError(err, "Error changing script permissions")
}

func bwrapGetBool(bwrap map[string]interface{}, opt string, required bool) bool {
	vali, exists := bwrap[opt]
	if !exists {
		if required {
			failNow("BWrap configuration invalid: " + opt + " not configured")
		}
		return false
	}
	val, ok := vali.(bool)
	if !ok {
		failNow("BWrap configuration invalid: " + opt + " not bool")
	}
	return val
}

func bwrapAddStringMap(bwrap map[string]interface{}, opt string, cmd []string) []string {
	argname := "--" + strings.Replace(opt, "_", "-", -1)
	vali, exists := bwrap[opt]
	if !exists || vali == nil {
		return cmd
	}
	val, ok := vali.(map[string]interface{})
	if !ok {
		failNow("BWrap configuration invalid: " + opt + " not string map")
	}
	for key, valuei := range val {
		value, ok := valuei.(string)
		if !ok {
			failNow("BWrap configuration invalid: " + opt + " not string map")
		}
		cmd = append(
			cmd,
			argname,
			key,
			value,
		)
	}
	return cmd
}

func bwrapAddString(bwrap map[string]interface{}, opt string, cmd []string) []string {
	argname := "--" + opt
	vali, exists := bwrap[opt]
	if !exists || vali == nil {
		return cmd
	}
	val, ok := vali.(string)
	if !ok {
		failNow("BWrap configuration invalid: " + opt + " not string")
	}
	return append(
		cmd,
		argname,
		val,
	)
}

func bwrapAddInt(bwrap map[string]interface{}, opt string, cmd []string) []string {
	argname := "--" + opt
	vali, exists := bwrap[opt]
	if !exists || vali == nil {
		return cmd
	}
	val, ok := vali.(int)
	if !ok {
		failNow("BWrap configuration invalid: " + opt + " not integer")
	}
	return append(
		cmd,
		argname,
		strconv.Itoa(val),
	)
}

func getBwrapConfig(bwrap map[string]interface{}) (usebwrap bool, cmd []string) {
	cmd = []string{}

	if !bwrapGetBool(bwrap, "enabled", true) {
		// BubbleWrap not enabled, we are done
		return
	}
	usebwrap = true

	unshare, has := bwrap["unshare"]
	if has {
		val, ok := unshare.([]interface{})
		if !ok {
			failNow("BWrap configuration invalid: unshare not list of strings")
		}
		for _, iopt := range val {
			opt, ok := iopt.(string)
			if !ok {
				failNow("BWrap configuration invalid: unshare not list of strings")
			}
			cmd = append(cmd, "--unshare-"+opt)
		}
	}

	if bwrapGetBool(bwrap, "share_net", false) {
		cmd = append(cmd, "--share-net")
	}

	if bwrapGetBool(bwrap, "mount_proc", false) {
		cmd = append(cmd, "--proc", "/proc")
	}

	if bwrapGetBool(bwrap, "mount_dev", false) {
		cmd = append(cmd, "--dev", "/dev")
	}

	cmd = bwrapAddStringMap(bwrap, "bind", cmd)
	cmd = bwrapAddStringMap(bwrap, "ro_bind", cmd)
	cmd = bwrapAddStringMap(bwrap, "symlink", cmd)
	cmd = bwrapAddString(bwrap, "hostname", cmd)
	cmd = bwrapAddInt(bwrap, "uid", cmd)
	cmd = bwrapAddInt(bwrap, "gid", cmd)

	return
}
