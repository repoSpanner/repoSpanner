package functional_tests

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"repospanner.org/repospanner/server/service"
)

var (
	binary           string
	bridgebinary     string
	hookrunnerbinary string
)

func checkFileExist(t *testing.T, path string) {
	_, err := os.Stat(path)
	if os.IsNotExist(err) {
		t.Fatalf("Binary %s did not exist", path)
	}
	failIfErr(t, err, "determining binary paths")
}

func setBinaryPaths(t *testing.T) {
	if binary != "" && bridgebinary != "" {
		return
	}
	codedir, err := os.Getwd()
	failIfErr(t, err, "determining binary paths")
	codedir = filepath.Join(codedir, "..")
	possiblebinary := filepath.Join(codedir, "repospanner")
	possiblebridgebinary := filepath.Join(codedir, "repobridge")
	possiblehookrunnerbinary := filepath.Join(codedir, "repohookrunner")

	checkFileExist(t, possiblebinary)
	checkFileExist(t, possiblebridgebinary)
	checkFileExist(t, possiblehookrunnerbinary)

	binary = possiblebinary
	bridgebinary = possiblebridgebinary
	hookrunnerbinary = possiblehookrunnerbinary

	atleast110, sure := service.IsAtLeastGo110(runtime.Version())
	if sure && !atleast110 {
		skipNameConstraints = true
	}
}

const (
	testCluster = "repospanner.local"
	testRegion  = "regiona"

	insecureKeysFlag       = "--very-insecure-weak-keys"
	skipNameConstraintFlag = "--no-name-constraint"
)

var (
	skipRemovingTestDir = os.Getenv("REPOSPANNER_FUNCTIONAL_NO_REMOVE") != ""
	skipNameConstraints bool
)

type nodeState struct {
	process *exec.Cmd
	ctx     *context.Context
	procout *os.File
	readout *os.File
	killed  bool
}

var (
	testDir       string
	cloneDir      string
	builtCa       bool
	nodes         = make(map[nodeNrType]*nodeState)
	doneC         = make(chan struct{})
	useBubbleWrap bool
)

func killNode(t *testing.T, nodenr nodeNrType) {
	state := nodes[nodenr]

	if state.killed {
		t.Log("Node pre-killed")
		return
	}
	state.killed = true

	t.Log("Killing node ", nodenr.Name())
	state.process.Process.Signal(os.Interrupt)
	state.process.Wait()
	err := state.procout.Close()
	if err != nil {
		t.Errorf("Error closing process output: %s", err)
	}

	state.readout.Seek(0, 0)

	output, err := ioutil.ReadAll(state.readout)
	if err != nil {
		t.Errorf("Error reading output: %s", err)
	}
	t.Log("Stderr: ", string(output))

	state.readout.Close()

	if !strings.Contains(string(output), "Shutdown complete") {
		t.Error("Node did not shut down cleanly")
	}
}

func testCleanup(t *testing.T) {
	for nodenr := range nodes {
		killNode(t, nodenr)
	}

	if testDir != "" && !skipRemovingTestDir {
		os.RemoveAll(testDir)
	}

	// Reset test environ
	close(doneC)
	doneC = make(chan struct{})

	testDir = ""
	cloneDir = ""
	builtCa = false
	nodes = make(map[nodeNrType]*nodeState)
	useBubbleWrap = false
}

func _runRawCommand(t *testing.T, binname, pwd string, envupdates []string, args ...string) (string, error) {
	if envupdates == nil {
		envupdates = make([]string, 0)
	}
	envupdates = append(
		envupdates,
		"USER=admin",
		"REPOBRIDGE_CONFIG="+pwd+".json",
	)
	cmd := exec.Command(
		binname,
		args...,
	)
	cmd.Dir = pwd
	cmd.Env = append(os.Environ(), envupdates...)
	out, err := cmd.CombinedOutput()
	t.Log("Output to command: ", cmd, " was: ", string(out))
	return string(out), err
}

func runRawCommand(t *testing.T, binname, pwd string, envupdates []string, args ...string) string {
	out, err := _runRawCommand(t, binname, pwd, envupdates, args...)
	if err != nil {
		t.Fatal("Error running command")
	}
	return out
}

func runFailingRawCommand(t *testing.T, binname, pwd string, envupdates []string, args ...string) string {
	out, err := _runRawCommand(t, binname, pwd, envupdates, args...)
	if err == nil {
		t.Fatal("No error in expecting failing command")
	}
	_, isexiterr := err.(*exec.ExitError)
	if !isexiterr {
		t.Fatal("Not exit error occured")
	}
	return out
}

type cloneMethod int

const (
	cloneMethodHTTPS cloneMethod = iota
	cloneMethodSSH
)

var (
	testedCloneMethods = []cloneMethod{
		//cloneMethodHTTPS,
		cloneMethodSSH,
	}
)

func runForTestedCloneMethods(t *testing.T, m func(*testing.T, cloneMethod)) {
	for _, method := range testedCloneMethods {
		m(t, method)
	}
}

func createSSHBridgeConfig(t *testing.T, node nodeNrType, confpath string) {
	examplecfgB, err := ioutil.ReadFile("../bridge_config.json.example")
	failIfErr(t, err, "reading example config")
	examplecfg := string(examplecfgB)

	// Perform replacements
	examplecfg = strings.Replace(
		examplecfg,
		"/etc/pki/repospanner",
		path.Join(testDir, "ca"),
		-1,
	)
	examplecfg = strings.Replace(
		examplecfg,
		"debug: false",
		"debug: true",
		-1,
	)
	examplecfg = strings.Replace(
		examplecfg,
		"usera",
		"admin",
		-1,
	)
	examplecfg = strings.Replace(
		examplecfg,
		"https://nodea.regiona.repospanner.local",
		node.HTTPBase(),
		-1,
	)

	// Write generated config file
	examplecfgB = []byte(examplecfg)
	err = ioutil.WriteFile(confpath, examplecfgB, 0644)
	failIfErr(t, err, "writing bridge config file")

	t.Log("Bridge config for", node, confpath, examplecfg)
}

func cloneCmdSSH(t *testing.T, node nodeNrType, reponame, username string) (cmd []string, envupdates []string) {
	cmd = []string{
		"clone",
		"ext::" + bridgebinary + " " + reponame,
	}

	return
}

func cloneCmdHTTPS(t *testing.T, node nodeNrType, reponame, username string) (cmd []string, envupdates []string) {
	cmd = []string{
		"clone",
		fmt.Sprintf("%s/repo/%s.git",
			node.HTTPBase(),
			reponame,
		),
		"--config",
		fmt.Sprintf("http.SslCAInfo=%s",
			path.Join(testDir, "ca", "ca.crt"),
		),
	}
	if username != "" {
		cmd = append(
			cmd,
			"--config",
			fmt.Sprintf("http.sslCert=%s",
				path.Join(testDir, "ca", username+".crt"),
			),
		)
		cmd = append(
			cmd,
			"--config",
			fmt.Sprintf("http.sslKey=%s",
				path.Join(testDir, "ca", username+".key"),
			),
		)
	}

	return
}

func clone(t *testing.T, method cloneMethod, node nodeNrType, reponame, username string, expectSuccess bool) string {
	ourdir, err := ioutil.TempDir(cloneDir, fmt.Sprintf("clone_%s_%s_", reponame, username))
	failIfErr(t, err, "creating clone directory")

	createSSHBridgeConfig(t, node, ourdir+".json")

	var cmd []string
	var envupdates []string
	if method == cloneMethodHTTPS {
		cmd, envupdates = cloneCmdHTTPS(t, node, reponame, username)
	} else if method == cloneMethodSSH {
		cmd, envupdates = cloneCmdSSH(t, node, reponame, username)
	} else {
		t.Fatal("Unknown clone method", method)
	}
	cmd = append(cmd, ourdir)

	if !expectSuccess {
		runFailingRawCommand(t, "git", "", envupdates, cmd...)
		return ""
	}

	runRawCommand(t, "git", ourdir, envupdates, cmd...)
	t.Logf("Clone repo %s to %s", reponame, ourdir)

	runRawCommand(t, "git", ourdir, envupdates, "config", "user.name", "testuser "+username)
	runRawCommand(t, "git", ourdir, envupdates, "config", "user.email", username+"@"+testCluster)

	return ourdir
}

func _runCommand(t *testing.T, config string, args ...string) (string, error) {
	cargs := make([]string, len(args)+2)
	cargs[0] = "--config"
	cargs[1] = path.Join(testDir, config+"-config.yml")
	for i, val := range args {
		cargs[i+2] = val
	}
	return _runRawCommand(t, binary, "", nil, cargs...)
}

func runCommand(t *testing.T, config string, args ...string) string {
	out, err := _runCommand(t, config, args...)
	if err != nil {
		t.Fatal("Error running command")
	}
	return out
}

func runFailingCommand(t *testing.T, config string, args ...string) string {
	out, err := _runCommand(t, config, args...)
	if err == nil {
		t.Fatal("No error in expecting failing command")
	}
	_, isexiterr := err.(*exec.ExitError)
	if !isexiterr {
		t.Fatal("Not exit error occured")
	}
	return out
}

func waitForNodeStart(t *testing.T, node nodeNrType, readout io.Reader) {
	started := make(chan struct{})
	ticker := time.NewTicker(5 * time.Second)

	go func() {
		buffer := make([]byte, 0)
		for {
			// Reading 1 byte at a time is really inefficient, but it makes sure that
			// we do not overread
			tmpbuf := make([]byte, 1)
			n, err := readout.Read(tmpbuf)
			if err != nil {
				if err == io.EOF {
					time.Sleep(50 * time.Millisecond)
					continue
				}
				t.Fatalf("Error reading from node out: %s", err)
			}
			buffer = append(buffer, tmpbuf[:n]...)

			if strings.Contains(string(buffer), "became leader at term") {
				close(started)
				return
			}
			if strings.Contains(string(buffer), "elected leader") {
				close(started)
				return
			}
		}
	}()

	for {
		select {
		case _, open := <-started:
			if !open {
				return
			}
		case <-ticker.C:
			t.Fatalf("Node %s did not start after 5 seconds", node.Name())
		}
	}
}

type nodeNrType int

func (n nodeNrType) Name() string {
	return "node" + strconv.Itoa(int(n))
}

func (n nodeNrType) HTTPPort() int {
	return int(n)*1000 + 443
}

func (n nodeNrType) RPCPort() int {
	return int(n)*1000 + 444
}

func (n nodeNrType) HTTPBase() string {
	return fmt.Sprintf(
		"https://%s.%s.%s:%d",
		n.Name(),
		testRegion,
		testCluster,
		n.HTTPPort(),
	)
}

func (n nodeNrType) RPCBase() string {
	return fmt.Sprintf(
		"https://%s.%s.%s:%d",
		n.Name(),
		testRegion,
		testCluster,
		n.RPCPort(),
	)
}

func createRepo(t *testing.T, node nodeNrType, reponame string, public bool) {
	if public {
		runCommand(t, node.Name(),
			"admin", "repo", "create", reponame, "--public")
	} else {
		runCommand(t, node.Name(),
			"admin", "repo", "create", reponame)
	}
}

func createNodes(t *testing.T, nodes ...nodeNrType) {
	spawned := false
	var lastnode nodeNrType
	for _, node := range nodes {
		if !spawned {
			spawnNode(t, node)
			spawned = true
		} else {
			joinNode(t, node, lastnode)
		}
		lastnode = node
	}
}

func joinNode(t *testing.T, newnodenr nodeNrType, joiningnode nodeNrType) {
	createNodeCert(t, newnodenr)
	runCommand(t, newnodenr.Name(), "serve", "--joinnode", joiningnode.RPCBase())
	startNode(t, newnodenr)
}

func spawnNode(t *testing.T, nodenr nodeNrType) {
	createNodeCert(t, nodenr)
	runCommand(t, nodenr.Name(), "serve", "--spawn")
	startNode(t, nodenr)
}

func startNode(t *testing.T, node nodeNrType) {
	t.Log("Starting node", node.Name())

	procout, err := os.Create(path.Join(testDir, node.Name()+"-output"))
	if err != nil {
		t.Fatalf("Error creating node output: %s", err)
	}
	readout, err := os.Open(procout.Name())
	if err != nil {
		t.Fatalf("Error opening node output: %s", err)
	}
	state := &nodeState{}
	state.procout = procout
	state.readout = readout

	process := exec.Command(
		binary,
		"--config",
		path.Join(testDir, node.Name()+"-config.yml"),
		"serve",
	)
	// These are different from the state objects so we can seek without messing up
	// the process channels
	process.Stdout = procout
	process.Stderr = procout

	state.process = process

	nodes[node] = state

	err = state.process.Start()
	if err != nil {
		t.Fatalf("Error starting node: %s", err)
	}

	waitForNodeStart(t, node, readout)
}

func createNodeCert(t *testing.T, node nodeNrType) {
	createTestCA(t)
	createTestConfig(t, node.Name(), node)
	runCommand(t, "ca",
		"ca", "node", testRegion, node.Name(),
		insecureKeysFlag,
	)
}

func createTestCA(t *testing.T) {
	if builtCa {
		return
	}

	createTestConfig(t, "ca", 0)

	cmd := []string{"ca", "init", testCluster, insecureKeysFlag}
	if skipNameConstraints {
		cmd = append(cmd, skipNameConstraintFlag)
	}
	out := runCommand(t, "ca", cmd...)

	if !strings.Contains(out, "WEAK KEY GENERATION USED") {
		t.Fatal("Weak key usage warning not printed")
	}

	runCommand(t, "ca", "ca", "leaf",
		"admin", "--admin", "--read", "--write",
		"--repo", "*", "--region", "*",
		insecureKeysFlag,
	)
	runCommand(t, "ca", "ca", "leaf",
		"client", "--region", "*", "--repo", "*", "--read", "--write",
		insecureKeysFlag,
	)
	runCommand(t, "ca", "ca", "leaf",
		"usera", "--region", "*", "--repo", "*", "--read", "--write",
		insecureKeysFlag,
	)
	runCommand(t, "ca", "ca", "leaf",
		"userb", "--region", "*", "--repo", "testuserb", "--read", "--write",
		insecureKeysFlag,
	)

	builtCa = true
}

func createTestConfig(t *testing.T, node string, nodenr nodeNrType, extras ...string) {
	if testDir == "" {
		createTestDirectory(t)
	}
	if _, err := os.Stat(path.Join(testDir, node+"-config.yml")); !os.IsNotExist(err) {
		// Don't recreate if another test specifically created this config
		t.Log("Config for", node, "left in place")
		return
	}

	// Create base config by modifying the example config
	examplecfgB, err := ioutil.ReadFile("../config.yml.example")
	failIfErr(t, err, "reading example config")
	examplecfg := string(examplecfgB)
	examplecfg = strings.Replace(
		examplecfg,
		"url:  https://nodea.regiona.repospanner.local/",
		fmt.Sprintf("url:  %s",
			nodenr.HTTPBase(),
		),
		-1,
	)
	examplecfg = strings.Replace(
		examplecfg,
		"/usr/bin/repohookrunner",
		hookrunnerbinary,
		-1,
	)
	examplecfg = strings.Replace(
		examplecfg,
		"/etc/pki/repospanner",
		path.Join(testDir, "ca"),
		-1,
	)
	examplecfg = strings.Replace(
		examplecfg,
		"/var/lib/repospanner",
		path.Join(testDir, "states", nodenr.Name()),
		-1,
	)
	examplecfg = strings.Replace(
		examplecfg,
		"/ca/public.",
		"/ca/nodea.regiona.",
		-1,
	)
	examplecfg = strings.Replace(
		examplecfg,
		"0.0.0.0:8443",
		fmt.Sprintf("0.0.0.0:%d", nodenr.RPCPort()),
		-1,
	)
	examplecfg = strings.Replace(
		examplecfg,
		"0.0.0.0:443",
		fmt.Sprintf("0.0.0.0:%d", nodenr.HTTPPort()),
		-1,
	)
	examplecfg = strings.Replace(
		examplecfg,
		"nodea",
		node,
		-1,
	)
	if !useBubbleWrap {
		examplecfg = strings.Replace(
			examplecfg,
			"enabled: true",
			"enabled: false",
			-1,
		)
	}

	var key string
	for _, arg := range extras {
		if key == "" {
			key = arg
			continue
		}
		examplecfg = strings.Replace(
			examplecfg,
			key,
			arg,
			-1,
		)
	}

	err = ioutil.WriteFile(
		path.Join(testDir, node+"-config.yml"),
		[]byte(examplecfg),
		0755,
	)
	failIfErr(t, err, "writing test config")

	t.Log("Config for", node, examplecfg)
}

func killTestIfTooLong(t *testing.T) {
	timer := time.NewTicker(2 * time.Minute)

	select {
	case <-doneC:
		timer.Stop()

	case <-timer.C:
		// Took too long, let's cancel test
		testCleanup(t)
		t.Fatal(t.Name() + " test aborted after running for two minutes")
	}
}

func createTestDirectory(t *testing.T) {
	setBinaryPaths(t)

	go killTestIfTooLong(t)

	// Create testdir
	var err error
	testDir, err = ioutil.TempDir("", "repospanner_functional_test_"+t.Name()+"_")
	failIfErr(t, err, "creating testDir")
	cloneDir = path.Join(testDir, "clones")
	err = os.Mkdir(cloneDir, 0755)
	failIfErr(t, err, "creating clonedir")
	err = ioutil.WriteFile(path.Join(testDir, "testname"), []byte(t.Name()), 0644)
	failIfErr(t, err, "writing testname")
}

func failIfErr(t *testing.T, err error, doing string) {
	if err != nil {
		t.Fatalf("Error while %s: %s", doing, err)
	}
}

func getBlobRepo(t *testing.T, workdir, issuenr string) {
	issuedir, err := filepath.Abs(path.Join("blobs", "issue-"+issuenr))
	failIfErr(t, err, "getting absolute issuepath")
	files, err := ioutil.ReadDir(issuedir)
	failIfErr(t, err, "reading issue folder")
	for _, file := range files {
		dest := ""

		if file.Name() == "packed-refs" {
			dest = path.Join(".git")
		} else if strings.HasPrefix(file.Name(), "pack-") {
			dest = path.Join(".git", "objects", "pack")
		}

		if dest != "" {
			destdir := path.Join(workdir, dest)
			err := os.MkdirAll(destdir, 0755)
			failIfErr(t, err, "creating test folder")
			err = os.Symlink(
				path.Join(issuedir, file.Name()),
				path.Join(destdir, file.Name()),
			)
			failIfErr(t, err, "linking file from test folder")
		}
	}
}
