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
	"sync"
	"testing"
	"time"

	"repospanner.org/repospanner/server/service"
)

var (
	binary           string
	bridgebinary     string
	hookrunnerbinary string
)

func checkFileExist(t tester, path string) {
	_, err := os.Stat(path)
	if os.IsNotExist(err) {
		t.Fatalf("Binary %s did not exist", path)
	}
	failIfErr(t, err, "determining binary paths")
}

func setBinaryPaths(t tester) {
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
	testCluster = "localdomain"
	testRegion  = "localhost"

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

type tester interface {
	Name() string
	Errorf(format string, args ...interface{})
	Fatalf(format string, args ...interface{})
	Logf(format string, args ...interface{})
}

var (
	testDir       string
	cloneDir      string
	builtCa       bool
	nodes         = make(map[nodeNrType]*nodeState)
	doneC         = make(chan struct{})
	useBubbleWrap bool
	ticker        *time.Ticker
	cleanlock     sync.Mutex
	// This is used to prevent more than one certificate from being signed at once, because doing
	// that would cause two processes to try to write the same serial file.
	caLock        sync.Mutex
)

func killNode(t tester, nodenr nodeNrType) {
	state := nodes[nodenr]

	if state.killed {
		t.Logf("Node pre-killed")
		return
	}
	state.killed = true

	t.Logf("Killing node %s", nodenr.Name())
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
	t.Logf("Stderr: %s", string(output))

	state.readout.Close()

	if !strings.Contains(string(output), "Shutdown complete") {
		t.Errorf("Node did not shut down cleanly")
	}
}

func testCleanup(t tester) {
	cleanlock.Lock()
	defer cleanlock.Unlock()

	for nodenr := range nodes {
		killNode(t, nodenr)
	}

	if testDir != "" && !skipRemovingTestDir {
		os.RemoveAll(testDir)
	}

	// Reset test environ
	if doneC != nil {
		close(doneC)
		doneC = nil
	}
	doneC = make(chan struct{})

	testDir = ""
	cloneDir = ""
	builtCa = false
	nodes = make(map[nodeNrType]*nodeState)
	useBubbleWrap = false
}

func _runRawCommand(t tester, binname, pwd string, envupdates []string, args ...string) (string, error) {
	if envupdates == nil {
		envupdates = make([]string, 0)
	}
	envupdates = append(
		envupdates,
		"USER=admin",
		"REPOBRIDGE_CONFIG="+pwd+".json",
	)
	prefix := []string{}
	if binname == "git" {
		prefix = []string{"-c", "protocol.ext.allow=always"}
	}
	args = append(prefix, args...)
	cmd := exec.Command(
		binname,
		args...,
	)
	cmd.Dir = pwd
	cmd.Env = append(os.Environ(), envupdates...)
	out, err := cmd.CombinedOutput()
	t.Logf("Output to command: %s %s was: %s", cmd.Path, cmd.Args, string(out))
	return string(out), err
}

func runRawCommand(t tester, binname, pwd string, envupdates []string, args ...string) string {
	out, err := _runRawCommand(t, binname, pwd, envupdates, args...)
	if err != nil {
		t.Fatalf("Error running command: %s", err)
	}
	return out
}

func runFailingRawCommand(t tester, binname, pwd string, envupdates []string, args ...string) string {
	out, err := _runRawCommand(t, binname, pwd, envupdates, args...)
	if err == nil {
		t.Fatalf("No error in expecting failing command")
	}
	_, isexiterr := err.(*exec.ExitError)
	if !isexiterr {
		t.Fatalf("Not exit error occured")
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
		cloneMethodHTTPS,
		cloneMethodSSH,
	}
)

func runCloneMethodIndependentTest(t *testing.T) bool {
	testedMethod := os.Getenv("REPOSPANNER_FUNCTIONAL_CLONE_METHOD")
	if testedMethod == "" || testedMethod == "indep" {
		return true
	}
	t.Log("Skipping clonemethod independent test")
	return false
}

func runForTestedCloneMethods(t *testing.T, m func(*testing.T, cloneMethod)) {
	torun := testedCloneMethods
	testedMethod := os.Getenv("REPOSPANNER_FUNCTIONAL_CLONE_METHOD")
	if testedMethod == "ssh" {
		torun = []cloneMethod{cloneMethodSSH}
	} else if testedMethod == "https" {
		torun = []cloneMethod{cloneMethodHTTPS}
	} else if testedMethod == "indep" {
		t.Log("Skipping clonemethod dependent test")
		return
	}
	for _, method := range torun {
		createTestDirectory(t)
		m(t, method)
		testCleanup(t)
	}
}

func createSSHBridgeConfig(t tester, node nodeNrType, confpath string) {
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

	t.Logf("Bridge config for %d: %s: %s", node, confpath, examplecfg)
}

func cloneCmdSSH(t tester, node nodeNrType, reponame, username string) (cmd []string, envupdates []string) {
	cmd = []string{
		"clone",
		"ext::" + bridgebinary + " " + reponame,
	}

	return
}

func cloneCmdHTTPS(t tester, node nodeNrType, reponame, username string) (cmd []string, envupdates []string) {
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

func clone(t tester, method cloneMethod, node nodeNrType, reponame, username string, expectSuccess bool) string {
	return _clone(t, method, node, reponame, username, "", expectSuccess, false)
}

func cloneBare(t tester, method cloneMethod, node nodeNrType, reponame, username string, expectSuccess bool) string {
	return _clone(t, method, node, reponame, username, "", expectSuccess, true)
}

func _clone(t tester, method cloneMethod, node nodeNrType, reponame, username, ourdir string, expectSuccess bool, bare bool) string {
	if ourdir == "" {
		tempdir, err := ioutil.TempDir(cloneDir, fmt.Sprintf("clone_%s_%s_", reponame, username))
		failIfErr(t, err, "creating clone directory")
		ourdir = tempdir
	} else {
		os.Mkdir(ourdir, 0750)
	}

	createSSHBridgeConfig(t, node, ourdir+".json")

	var cmd []string
	var envupdates []string
	if method == cloneMethodHTTPS {
		cmd, envupdates = cloneCmdHTTPS(t, node, reponame, username)
	} else if method == cloneMethodSSH {
		cmd, envupdates = cloneCmdSSH(t, node, reponame, username)
	} else {
		t.Fatalf("Unknown clone method: %d", method)
	}
	cmd = append(cmd, ourdir)
	if bare {
		cmd = append(cmd, "--bare")
	}

	if !expectSuccess {
		runFailingRawCommand(t, "git", "", envupdates, cmd...)
		return ""
	}

	runRawCommand(t, "git", ourdir, envupdates, cmd...)
	t.Logf("Clone repo %s to %s", reponame, ourdir)

	runRawCommand(t, "git", ourdir, envupdates, "config", "user.name", "testuser "+username)
	runRawCommand(t, "git", ourdir, envupdates, "config", "user.email", username+"@"+testCluster)
	runRawCommand(t, "git", ourdir, envupdates, "config", "push.default", "simple")

	return ourdir
}

func _runCommand(t tester, config string, args ...string) (string, error) {
	cargs := make([]string, len(args)+2)
	cargs[0] = "--config"
	cargs[1] = path.Join(testDir, config+"-config.yml")
	for i, val := range args {
		cargs[i+2] = val
	}
	return _runRawCommand(t, binary, "", nil, cargs...)
}

func runCommand(t tester, config string, args ...string) string {
	out, err := _runCommand(t, config, args...)
	if err != nil {
		t.Fatalf("Error running command")
	}
	return out
}

func runFailingCommand(t tester, config string, args ...string) string {
	out, err := _runCommand(t, config, args...)
	if err == nil {
		t.Fatalf("No error in expecting failing command")
	}
	_, isexiterr := err.(*exec.ExitError)
	if !isexiterr {
		t.Fatalf("Not exit error occured")
	}
	return out
}

// Wait for 32 seconds for the given node to print out messages indicating it has joined the
// cluster, or call t.Fatalf if this doesn't happen in time. readout is used to read the console
// output from the node.
func waitForNodeStart(t tester, node nodeNrType, readout io.Reader) {
	started := make(chan struct{})
	timeout := 32
	startTimer := time.NewTimer(time.Duration(timeout) * time.Second)

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
		case <-startTimer.C:
			t.Fatalf("Node %s did not start after %d seconds", node.Name(), timeout)
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

func createRepo(t tester, node nodeNrType, reponame string, public bool) {
	if public {
		runCommand(t, node.Name(),
			"admin", "repo", "create", reponame, "--public")
	} else {
		runCommand(t, node.Name(),
			"admin", "repo", "create", reponame)
	}
	runCommand(
		t, node.Name(),
		"admin", "repo", "edit", reponame, "--hook-pre-receive", "blobs/test.sh",
	)
}

// Start the given list of nodes. The first node will become the spawn node, and the rest of the
// nodes will join it.
func createNodes(t tester, nodes ...nodeNrType) {
	spawned := false
	var firstnode nodeNrType
	var wg sync.WaitGroup
	// This allows us to start the remaining nodes in parallel to ensure that repospanner
	// correctly handles simultaneous join requests. This tests the fix for
	// https://github.com/repoSpanner/repoSpanner/issues/78
	asyncJoin := func(t tester, newnodenr nodeNrType, joiningnode nodeNrType) {
		defer wg.Done()
		joinNode(t, newnodenr, joiningnode)
	}
	for _, node := range nodes {
		if !spawned {
			spawnNode(t, node)
			spawned = true
			firstnode = node
		} else {
			wg.Add(1)
			go asyncJoin(t, node, firstnode)
		}
	}
	wg.Wait()
}

func joinNode(t tester, newnodenr nodeNrType, joiningnode nodeNrType) {
	createNodeCert(t, newnodenr)
	runCommand(t, newnodenr.Name(), "serve", "--debug", "--joinnode", joiningnode.RPCBase())
	startNode(t, newnodenr)
}

func spawnNode(t tester, nodenr nodeNrType) {
	createNodeCert(t, nodenr)
	runCommand(t, nodenr.Name(), "serve", "--debug", "--spawn")
	startNode(t, nodenr)
}

func startNode(t tester, node nodeNrType) {
	t.Logf("Starting node %s", node.Name())

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
		"--debug",
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

// Create a node certificate for the given node.
func createNodeCert(t tester, node nodeNrType) {
	createTestCA(t)
	createTestConfig(t, node.Name(), node)
	// We need to lock the CA because if two run at once, one of them will get a blank serial
	caLock.Lock()
	defer caLock.Unlock()
	runCommand(t, "ca",
		"ca", "node", testRegion, node.Name(),
		insecureKeysFlag,
	)
}

func createTestCA(t tester) {
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
		t.Fatalf("Weak key usage warning not printed")
	}

	runCommand(t, "ca", "ca", "leaf",
		"admin", "--admin", "--read", "--write",
		"--repo", "*", "--region", "*",
		insecureKeysFlag,
	)
	runCommand(t, "ca", "ca", "leaf",
		"monitor", "--monitor",
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

	for i := 0; i < 10; i++ {
		// Create some test certificates, so that all node IDs will be > 10.
		// This is done to make sure that all synchronization remains working with a high node ID.
		runCommand(t, "ca", "ca", "leaf",
			"user"+strconv.Itoa(i), "--region", "*", "--repo", "*", "--read",
			insecureKeysFlag,
		)
	}

	builtCa = true
}

func createTestConfig(t tester, node string, nodenr nodeNrType, extras ...string) {
	if testDir == "" {
		createTestDirectory(t)
	}
	if _, err := os.Stat(path.Join(testDir, node+"-config.yml")); !os.IsNotExist(err) {
		// Don't recreate if another test specifically created this config
		t.Logf("Config for %s left in place", node)
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
		"/ca/nodea.localhost.",
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
	examplecfg = strings.Replace(
		examplecfg,
		"regiona",
		testRegion,
		-1,
	)
	examplecfg = strings.Replace(
		examplecfg,
		"paranoid: false",
		"paranoid: true",
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
	examplecfg = strings.Replace(
		examplecfg,
		"debug: false",
		"debug: true",
		-1,
	)

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

	t.Logf("Config for %s: %s", node, examplecfg)
}

func killTestIfTooLong(t tester) {
	ticker = time.NewTicker(2 * time.Minute)

	select {
	case <-doneC:
		ticker.Stop()

	case <-ticker.C:
		// Took too long, let's cancel test
		testCleanup(t)
		t.Fatalf("%s test aborted after running for two minutes", t.Name())
	}
}

func createTestDirectory(t tester) {
	cleanlock.Lock()
	defer cleanlock.Unlock()

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

func failIfErr(t tester, err error, doing string) {
	if err != nil {
		t.Fatalf("Error while %s: %s", doing, err)
	}
}

func getBlobRepo(t tester, workdir, issuenr string) {
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
