// +build stress

package functional_tests

import (
	"fmt"
	"path"
	"sync"
	"testing"
)

const numRaceRepos = 5
const numRaceBranches = 10
const numRaceCommits = 50

type fail struct {
	format string
	args   []interface{}
}

type goRoutineTester struct {
	inner tester
	mux   sync.Mutex
	fails []fail
}

func createGoroutineTester(inner *testing.T) *goRoutineTester {
	return &goRoutineTester{
		inner: inner,
		fails: make([]fail, 0),
	}
}

func (t *goRoutineTester) Name() string { return t.inner.Name() }
func (t *goRoutineTester) Errorf(format string, args ...interface{}) {
	t.inner.Errorf(format, args...)
}
func (t *goRoutineTester) Logf(format string, args ...interface{}) {
	t.inner.Logf(format, args...)
}
func (t *goRoutineTester) Fatalf(format string, args ...interface{}) {
	t.mux.Lock()
	defer t.mux.Unlock()

	t.inner.Errorf(format, args...)

	t.fails = append(t.fails, fail{format: format, args: args})
}
func (t *goRoutineTester) resolveFails() {
	for _, fail := range t.fails {
		t.inner.Fatalf(fail.format, fail.args...)
	}
}

func TestStress(t *testing.T) {
	defer testCleanup(t)
	nodea := nodeNrType(1)
	nodeb := nodeNrType(2)
	nodec := nodeNrType(3)
	createNodes(t, nodea, nodeb, nodec)
	nodes := []nodeNrType{nodea, nodeb, nodec}

	// In this test, we depend on the Go timeout rather than our own
	ticker.Stop()

	tester := createGoroutineTester(t)

	// Preparation done, start stress tests

	// 1. Create repositories
	runRace(tester, nodes, false, "Creating repositories", raceCreateRepo)
	// 2. First (empty) pull
	racePullRepos(tester, nodes, "master", false)
	// 3. Create master branches
	runRace(tester, nodes, false, "Preparing master", racePrepareMaster)
	// 4. Push master branches
	racePushRepos(tester, nodes, "master", false)
	// 5. Second pull set
	racePullRepos(tester, nodes, "prepare", true)
	// 6. Fill branches with test contents
	runRace(tester, nodes, true, "Preparing branch contents", racePrepareBranchContents)
	// 7. Push branches
	racePushRepos(tester, nodes, "prepare", true)
	// 8. Third pull set
	racePullRepos(tester, nodes, "confirm", true)
	// 9. Verify branch contents
	runRace(tester, nodes, true, "Verifying contents", raceVerifyRepo)
}

func nthNode(nodes []nodeNrType, nr, shift int) nodeNrType {
	return nodes[(shift+nr)%len(nodes)]
}

func raceCreateRepo(t tester, nodes []nodeNrType, repo, branch int) {
	reponame := fmt.Sprintf("race%d", repo)
	node := nthNode(nodes, repo, 0)
	createRepo(t, node, reponame, false)
}

func raceVerifyRepo(t tester, nodes []nodeNrType, repo, branch int) {
	repodir := raceCloneDir("confirm", true, repo, branch)

	branchname := fmt.Sprintf("branch_%d_%d", repo, branch)
	runRawCommand(t, "git", repodir, nil, "checkout", branchname)
	testFiles(t, repodir, 0, numRaceCommits+10-1)
}

func racePrepareMaster(t tester, nodes []nodeNrType, repo, branch int) {
	repodir := raceCloneDir("master", false, repo, branch)
	writeTestFiles(t, repodir, 0, 3)
	runRawCommand(t, "git", repodir, nil, "commit", "-sm", "Writing our tests")
}

func racePrepareBranchContents(t tester, nodes []nodeNrType, repo, branch int) {
	repodir := raceCloneDir("prepare", true, repo, branch)
	branchname := fmt.Sprintf("branch_%d_%d", repo, branch)
	runRawCommand(t, "git", repodir, nil, "branch", branchname, "master")
	runRawCommand(t, "git", repodir, nil, "checkout", branchname)
	for commit := 0; commit < numRaceCommits; commit++ {
		writeTestFiles(t, repodir, 1, commit+10)
		runRawCommand(
			t, "git", repodir, nil, "commit", "-sm",
			fmt.Sprintf("Test repo %d, branch %d, commit %d", repo, branch, commit),
		)
	}
}

func raceCloneDir(suffix string, forBranches bool, repo, branch int) string {
	reldir := fmt.Sprintf("race_%d_%s", repo, suffix)
	if forBranches {
		reldir += fmt.Sprintf("_%d", branch)
	}
	return path.Join(cloneDir, reldir)
}

func racePushRepos(t *goRoutineTester, nodes []nodeNrType, suffix string, forBranches bool) {
	runRace(
		t,
		nodes,
		forBranches,
		"Pushing: "+suffix,
		func(t tester, nodes []nodeNrType, repo, branch int) {
			repodir := raceCloneDir(suffix, forBranches, repo, branch)
			if forBranches {
				branchname := fmt.Sprintf("branch_%d_%d", repo, branch)
				runRawCommand(t, "git", repodir, nil, "push", "origin", branchname)
			} else {
				runRawCommand(t, "git", repodir, nil, "push", "origin", "master")
			}
		},
	)
}

func racePullRepos(t *goRoutineTester, nodes []nodeNrType, suffix string, forBranches bool) {
	runRace(
		t,
		nodes,
		forBranches,
		"Pulling: "+suffix,
		func(t tester, nodes []nodeNrType, repo, branch int) {
			reponame := fmt.Sprintf("race%d", repo)
			dir := raceCloneDir(suffix, forBranches, repo, branch)
			node := nthNode(nodes, repo, branch)
			_clone(t, cloneMethodHTTPS, node, reponame, "admin", dir, true, false)
		},
	)
}

func runRace(t *goRoutineTester, nodes []nodeNrType, forBranches bool, descrip string, f func(t tester, nodes []nodeNrType, repo, branch int)) {
	t.Logf(descrip)

	var wg sync.WaitGroup
	syncchan := make(chan interface{})

	for i := 0; i < numRaceRepos; i++ {
		for j := 0; j < numRaceBranches; j++ {
			if forBranches || j == 0 {
				wg.Add(1)
				go func(repo, branch int) {
					defer wg.Done()
					// Wait until all routines are created
					<-syncchan

					// TODO: Run
					f(t, nodes, repo, branch)
				}(i, j)
			}
		}
	}
	t.Logf("Kicking of goroutines")
	close(syncchan)
	t.Logf("Waiting for finish")
	wg.Wait()
	t.Logf("Resolving fails")
	t.resolveFails()
	t.Logf("Done")
}
