package hookrun

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path"
	"strconv"
	"strings"

	"repospanner.org/repospanner/server/storage"

	"github.com/pkg/errors"
	"golang.org/x/net/http2"
	"repospanner.org/repospanner/server/datastructures"
)

func getHookArgs(request datastructures.HookRunRequest, hookname, branch string, req [2]string) ([]string, io.Reader) {
	buf := bytes.NewBuffer(nil)
	if hookname == "update" {
		return []string{branch, req[0], req[1]}, buf
	}

	for branch, req := range request.Requests {
		fmt.Fprintf(buf, "%s %s %s\n", req[0], req[1], branch)
	}
	return []string{}, buf
}

func runHook(request datastructures.HookRunRequest, workdir, hookname string, branch string, req [2]string) error {
	if !keysDeleted {
		return errors.New("Keys are not deleted")
	}

	if storage.ObjectID(request.HookObjects[hookname]) == storage.ZeroID {
		return nil
	}

	hookArgs, hookbuf := getHookArgs(request, hookname, branch, req)
	usebwrap, bwrapcmd, err := getBwrapConfig(request.BwrapConfig)
	if err != nil {
		return errors.Wrap(err, "Error getting bubblewrap configuration")
	}

	var cmd *exec.Cmd
	if usebwrap {
		bwrapcmd = append(
			bwrapcmd,
			"--bind", path.Join(workdir, "hookrun"), "/hookrun",
			"--chdir", "/hookrun/clone",
			"--setenv", "GIT_DIR", "/hookrun/clone",
			"--setenv", "HOOKTYPE", hookname,
		)

		for key, val := range request.ExtraEnv {
			bwrapcmd = append(
				bwrapcmd,
				"--setenv", "extra_"+key, val,
			)
		}

		bwrapcmd = append(
			bwrapcmd,
			path.Join("/hookrun", hookname),
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
			path.Join(workdir, "hookrun", hookname),
			hookArgs...,
		)
		cmd.Dir = path.Join(workdir, "hookrun", "clone")
		cmd.Env = []string{
			"GIT_DIR=" + cmd.Dir,
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
	err = cmd.Run()
	if err == nil {
		return nil
	}

	_, iseerr := err.(*exec.ExitError)
	if iseerr {
		fmt.Fprintln(os.Stderr, "Hook returned error")
	} else {
		fmt.Fprintln(os.Stderr, "Error running hook")
	}
	return err
}

func getScripts(request datastructures.HookRunRequest, workdir string) error {
	cert, err := tls.LoadX509KeyPair(
		path.Join(workdir, "keys", "client.crt"),
		path.Join(workdir, "keys", "client.key"),
	)
	if err != nil {
		return errors.Wrap(err, "Unable to load client keys")
	}

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
	if err = http2.ConfigureTransport(transport); err != nil {
		return errors.Wrap(err, "Error reconfiguring transport")
	}
	clnt := &http.Client{
		Transport: transport,
	}

	// Grab the hook scripts
	for hook, object := range request.HookObjects {
		if storage.ObjectID(object) == storage.ZeroID {
			continue
		}

		script, err := os.Create(path.Join(workdir, "hookrun", hook))
		if err != nil {
			return errors.Wrap(err, "Error opening script file")
		}
		resp, err := clnt.Get(
			request.RPCURL + "/rpc/object/single/admin/hooks.git/" + object,
		)
		if err != nil {
			return errors.Wrap(err, "Error retrieving hook script")
		}
		if resp.StatusCode != 200 {
			msg := fmt.Sprintf(
				"Unable to retrieve hook script, received status code %d",
				resp.StatusCode)
			fmt.Fprintln(os.Stderr, msg)
			return errors.New(msg)
		}

		_, err = io.Copy(script, resp.Body)
		resp.Body.Close()
		if err != nil {
			return errors.Wrap(err, "Error writing script file")
		}
		err = script.Close()
		if err != nil {
			return errors.Wrap(err, "Error flushing script file")
		}

		err = os.Chmod(script.Name(), 0755)
		if err != nil {
			return errors.Wrap(err, "Error changing script permissions")
		}
	}

	return nil
}

func bwrapGetBool(bwrap map[string]interface{}, opt string, required bool) (bool, error) {
	vali, exists := bwrap[opt]
	if !exists {
		if required {
			return false, errors.Errorf("BWrap configuration invalid: %s not configured", opt)
		}
		return false, nil
	}
	val, ok := vali.(bool)
	if !ok {
		return false, errors.Errorf("BWrap configuration invalid: %s not bool", opt)
	}
	return val, nil
}

func bwrapAddFakeStringMap(bwrap map[string]interface{}, opt string, cmd []string) ([]string, error) {
	argname := "--" + strings.Replace(opt, "_", "-", -1)
	vali, exists := bwrap[opt]
	if !exists || vali == nil {
		return cmd, nil
	}
	val, ok := vali.([]interface{})
	if !ok {
		return nil, errors.Errorf("BWrap configuration invalid: %s not fake string map", opt)
	}
	for _, valuei := range val {
		value, ok := valuei.([]interface{})
		if !ok {
			return nil, errors.Errorf("BWrap configuration invalid: %s not string map", opt)
		}
		if len(value) != 2 {
			return nil, errors.Errorf("BWrap configuiration invalid: %s has entry with invalid length", opt)
		}
		key, ok := value[0].(string)
		if !ok {
			return nil, errors.Errorf("BWrap configuration invalid: %s has invalid entry", opt)
		}
		keyval, ok := value[1].(string)
		if !ok {
			return nil, errors.Errorf("BWRap configuration invalid: %s has invalid entry", opt)
		}
		cmd = append(
			cmd,
			argname,
			key,
			keyval,
		)
	}
	return cmd, nil
}

func bwrapAddString(bwrap map[string]interface{}, opt string, cmd []string) ([]string, error) {
	argname := "--" + opt
	vali, exists := bwrap[opt]
	if !exists || vali == nil {
		return cmd, nil
	}
	val, ok := vali.(string)
	if !ok {
		return nil, errors.Errorf("BWrap configuration invalid: %s not string", opt)
	}
	return append(
		cmd,
		argname,
		val,
	), nil
}

func bwrapAddInt(bwrap map[string]interface{}, opt string, cmd []string) ([]string, error) {
	argname := "--" + opt
	vali, exists := bwrap[opt]
	if !exists || vali == nil {
		return cmd, nil
	}
	val, ok := vali.(int)
	if !ok {
		return nil, errors.Errorf("BWrap configuration invalid: %s not integer", opt)
	}
	return append(
		cmd,
		argname,
		strconv.Itoa(val),
	), nil
}

func getBwrapBools(bwrap map[string]interface{}) ([]string, error) {
	cmd := []string{}
	shareNet, err := bwrapGetBool(bwrap, "share_net", false)
	if err != nil {
		return nil, errors.Wrap(err, "Unable to determine bubblewrap share net state")
	}
	if shareNet {
		cmd = append(cmd, "--share-net")
	}

	mountProc, err := bwrapGetBool(bwrap, "mount_proc", false)
	if err != nil {
		return nil, errors.Wrap(err, "Unable to determine bubblewrap mount proc state")
	}
	if mountProc {
		cmd = append(cmd, "--proc", "/proc")
	}

	mountDev, err := bwrapGetBool(bwrap, "mount_dev", false)
	if err != nil {
		return nil, errors.Wrap(err, "Unable to determine bubblewrap mount dev state")
	}
	if mountDev {
		cmd = append(cmd, "--dev", "/dev")
	}
	return cmd, nil
}

func getBwrapFakeStrings(bwrap map[string]interface{}) ([]string, error) {
	cmd := []string{}
	var err error
	cmd, err = bwrapAddFakeStringMap(bwrap, "bind", cmd)
	if err != nil {
		return nil, errors.Wrap(err, "Unable to determine bubblewrap bind map")
	}
	cmd, err = bwrapAddFakeStringMap(bwrap, "ro_bind", cmd)
	if err != nil {
		return nil, errors.Wrap(err, "Unable to determine bubblewrap ro_bind map")
	}
	cmd, err = bwrapAddFakeStringMap(bwrap, "symlink", cmd)
	if err != nil {
		return nil, errors.Wrap(err, "Unable to determine bubblewrap symlink map")
	}
	return cmd, nil
}

func getBwrapConfig(bwrap map[string]interface{}) (usebwrap bool, cmd []string, err error) {
	cmd = []string{}

	bwrapEnabled, err := bwrapGetBool(bwrap, "enabled", true)
	if err != nil {
		return false, nil, errors.Wrap(err, "Unable to determine bwrap enabled state")
	}
	if !bwrapEnabled {
		// BubbleWrap not enabled, we are done
		return
	}
	usebwrap = true

	unshare, has := bwrap["unshare"]
	if has {
		val, ok := unshare.([]interface{})
		if !ok {
			return false, nil, errors.New("BWrap configuration invalid: unshare not list of strings")
		}
		for _, iopt := range val {
			opt, ok := iopt.(string)
			if !ok {
				return false, nil, errors.New("BWrap configuration invalid: unshare not list of strings")
			}
			cmd = append(cmd, "--unshare-"+opt)
		}
	}

	cmdAdd, err := getBwrapBools(bwrap)
	if err != nil {
		return false, nil, errors.Wrap(err, "Unable to determine buublewrap bools")
	}
	cmd = append(cmd, cmdAdd...)
	cmdAdd, err = getBwrapFakeStrings(bwrap)
	if err != nil {
		return false, nil, errors.Wrap(err, "Unable to determine bubblewrap fakestrings")
	}
	cmd = append(cmd, cmdAdd...)

	cmd, err = bwrapAddString(bwrap, "hostname", cmd)
	if err != nil {
		return false, nil, errors.Wrap(err, "Unable to determine bubblewrap hostname")
	}
	cmd, err = bwrapAddInt(bwrap, "uid", cmd)
	if err != nil {
		return false, nil, errors.Wrap(err, "Unable to determine bubblewrap uid")
	}
	cmd, err = bwrapAddInt(bwrap, "gid", cmd)

	return
}
