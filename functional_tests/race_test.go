package functional_tests

import (
	"fmt"
	"path"
	"sync"
	"testing"
)

const numRaceRepos = 100
const numRaceBranches = 100
const numRaceCommits = 100

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

func TestRace(t *testing.T) {
	defer testCleanup(t)
	nodea := nodeNrType(1)
	nodeb := nodeNrType(2)
	nodec := nodeNrType(3)
	createNodes(t, nodea, nodeb, nodec)
	nodes := []nodeNrType{nodea, nodeb, nodec}

	tester := createGoroutineTester(t)

	raceCreateRepos(tester, nodes)
	tester.resolveFails()
	racePull(tester, nodes, "pre")
	tester.resolveFails()
	raceCreateRepoContents(tester)
	tester.resolveFails()
	racePush(tester, nodes)
	tester.resolveFails()
	racePull(tester, nodes, "post")
	tester.resolveFails()
	raceVerifyRepos(tester)
	tester.resolveFails()
}

func nthNode(nodes []nodeNrType, nr, shift int) nodeNrType {
	return nodes[(shift+nr)%len(nodes)]
}

func raceCreateRepos(t tester, nodes []nodeNrType) {
	var createWg sync.WaitGroup
	syncchan := make(chan interface{})
	t.Logf("Creating repos")
	for i := 0; i <= numRaceRepos; i++ {
		reponame := fmt.Sprintf("race%d", i)
		createWg.Add(1)
		go func(i int, rname string) {
			defer createWg.Done()
			node := nthNode(nodes, i, 0)

			// This channel is used to make all goroutines schedulable at the same time
			<-syncchan

			t.Logf("Creating repo %s at node %d", reponame, node)
			createRepo(t, node, rname, false)
		}(i, reponame)
	}
	t.Logf("Starting repo creation")
	close(syncchan)
	t.Logf("Waiting for repo creations to finish")
	createWg.Wait()
	t.Logf("Repos created")
}

func raceVerifyRepos(t tester) {
	var checkWg sync.WaitGroup

	for repo := 0; repo < numRaceRepos; repo++ {
		checkWg.Add(1)

		go func(repo int) {
			defer checkWg.Done()

			reldir := fmt.Sprintf("race_%d_post", repo)
			repodir := path.Join(cloneDir, reldir)

			for branch := 0; branch < numRaceBranches; branch++ {
				branchname := fmt.Sprintf("branch_%d_%d", repo, branch)
				runRawCommand(t, "git", repodir, nil, "checkout", branchname)
				testFiles(t, repodir, 0, numRaceCommits+10-1)
			}
		}(repo)
	}
	t.Logf("Waiting for checks")
	checkWg.Wait()
	t.Logf("Repos all checked")
}

func raceCreateRepoContents(t tester) {
	var contentsWg sync.WaitGroup

	t.Logf("Creating repo contents")
	for repo := 0; repo < numRaceRepos; repo++ {
		contentsWg.Add(1)

		go func(repo int) {
			defer contentsWg.Done()

			reldir := fmt.Sprintf("race_%d_pre", repo)
			repodir := path.Join(cloneDir, reldir)

			// Make sure "master" exists
			writeTestFiles(t, repodir, 0, 3)
			runRawCommand(t, "git", repodir, nil, "commit", "-sm", "Writing our tests")

			for branch := 0; branch < numRaceBranches; branch++ {
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
		}(repo)
	}
	t.Logf("Waiting for content generation")
	contentsWg.Wait()
	t.Logf("Contents all generated")
}

func racePush(t tester, nodes []nodeNrType) {
	var pushWg sync.WaitGroup
	syncchan := make(chan interface{})
	t.Logf("Initializing push")
	for repo := 0; repo < numRaceRepos; repo++ {
		for branch := 0; branch < numRaceBranches; branch++ {
			pushWg.Add(1)
			go func(repo, branch int) {
				defer pushWg.Done()

				reldir := fmt.Sprintf("race_%d_pre", repo)
				repodir := path.Join(cloneDir, reldir)
				branchname := fmt.Sprintf("branch_%d_%d", repo, branch)

				<-syncchan

				t.Logf("Pushing branch %s for repo %d", branchname, repo)

				if branch == 0 {
					runRawCommand(t, "git", repodir, nil, "push", "origin", "master")
				}
				runRawCommand(t, "git", repodir, nil, "push", "origin", branchname)
			}(repo, branch)
		}
	}
	t.Logf("Starting pushes")
	close(syncchan)
	t.Logf("Waiting for pushes to finish")
	pushWg.Wait()
	t.Logf("Pushes finished")
}

func racePull(t tester, nodes []nodeNrType, postfix string) {
	var pullWg sync.WaitGroup
	syncchan := make(chan interface{})
	t.Logf("Initializing pull %s", postfix)
	for repo := 0; repo < numRaceRepos; repo++ {
		reldir := fmt.Sprintf("race_%d_%s", repo, postfix)
		dir := path.Join(cloneDir, reldir)
		reponame := fmt.Sprintf("race%d", repo)
		pullWg.Add(1)
		go func(nr int, reponame, dir string) {
			defer pullWg.Done()
			node := nthNode(nodes, nr, 1)

			<-syncchan

			t.Logf("Pulling repo %s from node %d to %s", reponame, node, dir)
			_clone(t, cloneMethodHTTPS, node, reponame, "admin", dir, true, false)
		}(repo, reponame, dir)
	}
	t.Logf("Starting repo pulls")
	close(syncchan)
	t.Logf("Waiting for repo pulls to finish")
	pullWg.Wait()
	t.Logf("Repos pulled")
}
