package functional_tests

import (
	"io/ioutil"
	"os"
	"path"
	"strconv"
	"strings"
	"testing"
)

func TestEmptyClone(t *testing.T) {
	runForTestedCloneMethods(t, performEmptyCloneTest)
}

func performEmptyCloneTest(t *testing.T, method cloneMethod) {
	defer testCleanup(t)
	nodea := nodeNrType(1)
	nodeb := nodeNrType(2)
	nodec := nodeNrType(3)

	createNodes(t, nodea, nodeb, nodec)

	clone(t, method, nodea, "test1", "admin", false)
	createRepo(t, nodeb, "test1", false)
	clone(t, method, nodea, "test1", "admin", true)
}

func TestAnonymousClones(t *testing.T) {
	defer testCleanup(t)
	nodea := nodeNrType(1)
	nodeb := nodeNrType(2)
	nodec := nodeNrType(3)

	createNodes(t, nodea, nodeb, nodec)

	// There are no anonymous repos on SSH
	method := cloneMethodHTTPS

	clone(t, method, nodea, "test1", "", false)
	clone(t, method, nodea, "test1", "admin", false)
	createRepo(t, nodeb, "test1", false)
	clone(t, method, nodea, "test1", "", false)
	clone(t, method, nodea, "test1", "admin", true)

	runCommand(t, nodec.Name(), "admin", "repo", "edit", "test1", "--public=true")
	clone(t, method, nodea, "test1", "", true)

	clone(t, method, nodea, "test2", "", false)
	clone(t, method, nodea, "test2", "admin", false)
	createRepo(t, nodec, "test2", true)
	clone(t, method, nodea, "test2", "", true)
	clone(t, method, nodea, "test2", "admin", true)

	runCommand(t, nodeb.Name(), "admin", "repo", "edit", "test2", "--public=false")
	clone(t, method, nodea, "test2", "", false)
}

const (
	body1 = "Testing the planet"
	body2 = "Testing all the things"
	body3 = "Testing the code"
	body4 = "Testing even more"
)

func inRange(start, stop, n int) bool {
	return n >= start && n <= stop
}

func writeTestFile(t tester, wdir, name, body string, intestdir bool) {
	var fname string
	if intestdir {
		fname = path.Join(wdir, "testdir", name)
	} else {

		fname = path.Join(wdir, name)
	}

	err := ioutil.WriteFile(
		fname,
		[]byte(body),
		0644,
	)
	failIfErr(t, err, "writing "+name)
}

func writeTestFiles(t tester, wdir string, start, stop int) {
	if start == 0 {
		err := os.Mkdir(path.Join(wdir, "testdir"), 0755)
		failIfErr(t, err, "creating test directory")
	}
	if inRange(start, stop, 1) {
		writeTestFile(t, wdir, "testfile1", body1, true)
	}
	if inRange(start, stop, 2) {
		writeTestFile(t, wdir, "testfile2", body2, true)
	}
	if inRange(start, stop, 3) {
		writeTestFile(t, wdir, "testfile3", body3, false)
	}
	if inRange(start, stop, 4) {
		writeTestFile(t, wdir, "testfile4", body4, false)
	}
	if stop >= 5 {
		for i := start; i <= stop; i++ {
			j := strconv.Itoa(i)
			writeTestFile(t, wdir, "testfile-auto"+j, "testcontent"+j, false)
		}
	}

	runRawCommand(t, "git", wdir, nil, "add", ".")
}

func testFile(t tester, wdir, name, body string, intestdir bool) {
	var fname string
	if intestdir {
		fname = path.Join(wdir, "testdir", name)
	} else {

		fname = path.Join(wdir, name)
	}

	cts, err := ioutil.ReadFile(fname)
	failIfErr(t, err, "reading "+name)

	if string(cts) != body {
		t.Errorf("%s contents were wrong: %s != %s", name, string(cts), body)
	}
}

func testFiles(t tester, wdir string, start, stop int) {
	if inRange(start, stop, 1) {
		testFile(t, wdir, "testfile1", body1, true)
	}
	if inRange(start, stop, 2) {
		testFile(t, wdir, "testfile2", body2, true)
	}
	if inRange(start, stop, 3) {
		testFile(t, wdir, "testfile3", body3, false)
	}
	if inRange(start, stop, 4) {
		testFile(t, wdir, "testfile4", body4, false)
	}
	if start >= 5 {
		for i := start; i <= stop; i++ {
			j := strconv.Itoa(i)
			testFile(t, wdir, "testfile-auto"+j, "testcontent"+j, false)
		}
	}
}

func TestCloneEditPushReclone(t *testing.T) {
	runForTestedCloneMethods(t, performCloneEditPushRecloneTest)
}

func performCloneEditPushRecloneTest(t *testing.T, method cloneMethod) {
	defer testCleanup(t)
	nodea := nodeNrType(1)
	nodeb := nodeNrType(2)
	nodec := nodeNrType(3)

	createNodes(t, nodea, nodeb, nodec)

	createRepo(t, nodeb, "test1", true)

	wdir1 := clone(t, method, nodea, "test1", "admin", true)
	writeTestFiles(t, wdir1, 0, 3)
	runRawCommand(t, "git", wdir1, nil, "commit", "-sm", "Writing our tests")

	// Push
	pushout := runRawCommand(t, "git", wdir1, nil, "push")
	if !strings.Contains(pushout, "* [new branch]      master -> master") {
		t.Fatal("Something went wrong in pushing")
	}

	// And reclone
	wdir2 := clone(t, method, nodec, "test1", "admin", true)
	testFiles(t, wdir2, 0, 3)

	// Add a new file
	writeTestFiles(t, wdir2, 4, 4)
	runRawCommand(t, "git", wdir2, nil, "commit", "-sm", "Testing the push again")

	// Push again
	pushout = runRawCommand(t, "git", wdir2, nil, "push")
	if !strings.Contains(pushout, "  master -> master") {
		t.Fatal("Something went wrong in pushing")
	}

	// And clone once more
	wdir3 := clone(t, method, nodeb, "test1", "", true)
	testFiles(t, wdir3, 0, 4)
}

func TestCloneEditPushRecloneSingleNode(t *testing.T) {
	runForTestedCloneMethods(t, performCloneEditPushRecloneSingleNodeTest)
}

func performCloneEditPushRecloneSingleNodeTest(t *testing.T, method cloneMethod) {
	defer testCleanup(t)
	nodea := nodeNrType(1)

	spawnNode(t, nodea)

	createRepo(t, nodea, "test1", true)

	wdir1 := clone(t, method, nodea, "test1", "admin", true)
	writeTestFiles(t, wdir1, 0, 3)
	runRawCommand(t, "git", wdir1, nil, "commit", "-sm", "Writing our tests")

	// Push
	pushout := runRawCommand(t, "git", wdir1, nil, "push")
	if !strings.Contains(pushout, "* [new branch]      master -> master") {
		t.Fatal("Something went wrong in pushing")
	}

	// And reclone
	wdir2 := clone(t, method, nodea, "test1", "admin", true)
	testFiles(t, wdir2, 0, 3)

	// Add a new file
	writeTestFiles(t, wdir2, 4, 4)
	runRawCommand(t, "git", wdir2, nil, "commit", "-sm", "Testing the push again")

	// Push again
	pushout = runRawCommand(t, "git", wdir2, nil, "push")
	if !strings.Contains(pushout, "  master -> master") {
		t.Fatal("Something went wrong in pushing")
	}

	// And clone once more
	wdir3 := clone(t, method, nodea, "test1", "", true)
	testFiles(t, wdir3, 0, 4)

	// And make sure we can bring wdir1 up to date
	runRawCommand(t, "git", wdir1, nil, "pull")
}

func TestCloneEditPushRecloneWithKill(t *testing.T) {
	runForTestedCloneMethods(t, performCloneEditPushRecloneWithKillTest)
}

func performCloneEditPushRecloneWithKillTest(t *testing.T, method cloneMethod) {
	defer testCleanup(t)
	nodea := nodeNrType(1)
	nodeb := nodeNrType(2)
	nodec := nodeNrType(3)

	createNodes(t, nodea, nodeb, nodec)

	createRepo(t, nodeb, "test1", true)

	wdir1 := clone(t, method, nodea, "test1", "admin", true)
	writeTestFiles(t, wdir1, 0, 3)
	runRawCommand(t, "git", wdir1, nil, "commit", "-sm", "Writing our tests")

	// Kill nodec
	killNode(t, nodec)

	// Push
	pushout := runRawCommand(t, "git", wdir1, nil, "push")
	if !strings.Contains(pushout, "* [new branch]      master -> master") {
		t.Fatal("Something went wrong in pushing")
	}

	// And reclone
	wdir2 := clone(t, method, nodeb, "test1", "admin", true)
	testFiles(t, wdir2, 0, 3)

	// Add a new file
	writeTestFiles(t, wdir2, 4, 4)
	runRawCommand(t, "git", wdir2, nil, "commit", "-sm", "Testing the push again")

	// Push again
	pushout = runRawCommand(t, "git", wdir2, nil, "push")
	if !strings.Contains(pushout, "  master -> master") {
		t.Fatal("Something went wrong in pushing")
	}

	// Start node C back up
	startNode(t, nodec)

	// And clone once more
	wdir3 := clone(t, method, nodec, "test1", "", true)
	testFiles(t, wdir3, 0, 4)
}

func TestCloneEditPushRecloneWithMajorityOffline(t *testing.T) {
	runForTestedCloneMethods(t, performCloneEditPushRecloneWithMajorityOfflineTest)
}

func performCloneEditPushRecloneWithMajorityOfflineTest(t *testing.T, method cloneMethod) {
	defer testCleanup(t)
	nodea := nodeNrType(1)
	nodeb := nodeNrType(2)
	nodec := nodeNrType(3)

	createNodes(t, nodea, nodeb, nodec)

	createRepo(t, nodeb, "test1", true)

	wdir1 := clone(t, method, nodea, "test1", "admin", true)
	writeTestFiles(t, wdir1, 0, 3)
	runRawCommand(t, "git", wdir1, nil, "commit", "-sm", "Writing our tests")

	// Push
	pushout := runRawCommand(t, "git", wdir1, nil, "push")
	if !strings.Contains(pushout, "* [new branch]      master -> master") {
		t.Fatal("Something went wrong in pushing")
	}

	// Kill nodeb and nodec
	// This brings us down to a minority. Cloning should still work, pushing not.
	killNode(t, nodeb)
	killNode(t, nodec)

	// And reclone
	wdir2 := clone(t, method, nodea, "test1", "admin", true)
	testFiles(t, wdir2, 0, 3)

	// Add a new file
	writeTestFiles(t, wdir2, 4, 4)
	runRawCommand(t, "git", wdir2, nil, "commit", "-sm", "Testing the push again")

	// Push again. This should fail, since a majority is offline.
	pushout = runFailingRawCommand(t, "git", wdir2, nil, "push")
	if !strings.Contains(pushout, "remote: ERR Object sync failed") {
		t.Fatal("Pushing failed for different reason")
	}

	// Start node C back up
	startNode(t, nodec)

	// And retry that push
	pushout = runRawCommand(t, "git", wdir2, nil, "push")
	if !strings.Contains(pushout, "  master -> master") {
		t.Fatal("Something went wrong in pushing")
	}
}

func TestDuplicateObjects(t *testing.T) {
	// This tests to make sure that we don't panic if git sends the same
	// object multiple times in a single push
	// Issue #20

	runForTestedCloneMethods(t, performDuplicateObjectsTest)
}

func performDuplicateObjectsTest(t *testing.T, method cloneMethod) {
	// This test catches the case where the same object is pushed twice.
	// This happens if multiple branches are pushed at the same time.
	performBrokenRepoTest(t, method, "20")
}

func TestEmptyTree(t *testing.T) {
	// This tests to make sure we don't crash when empty trees are found.
	runForTestedCloneMethods(t, performEmptyTreeTest)
}

func performEmptyTreeTest(t *testing.T, method cloneMethod) {
	performBrokenRepoTest(t, method, "26")
}

func TestSubmodules(t *testing.T) {
	// This tests to make sure we don't verify submodule contents
	runForTestedCloneMethods(t, performSubmoduleTest)
}

func performSubmoduleTest(t *testing.T, method cloneMethod) {
	performBrokenRepoTest(t, method, "27")
}

// performBrokenRepoTest tries pushing and pulling a blob repository from
// the test suite. This is useful to reproduce bugs from weird repos.
func performBrokenRepoTest(t *testing.T, method cloneMethod, issue string) {
	defer testCleanup(t)
	nodea := nodeNrType(1)
	nodeb := nodeNrType(2)
	nodec := nodeNrType(3)

	createNodes(t, nodea, nodeb, nodec)

	createRepo(t, nodea, "test1", true)

	wdir1 := clone(t, method, nodea, "test1", "admin", true)
	getBlobRepo(t, wdir1, issue)

	// Push
	pushout := runRawCommand(t, "git", wdir1, nil, "push", "origin", "--mirror")
	if !strings.Contains(pushout, "* [new branch]      ") {
		t.Fatal("Something went wrong in pushing")
	}

	// Pull it back
	clone(t, method, nodeb, "test1", "admin", true)
}

func TestCloneEditPushRecloneSingleNodeEmojiBranch(t *testing.T) {
	runForTestedCloneMethods(t, performCloneEditPushRecloneSingleNodeEmojiBranchTest)
}

func performCloneEditPushRecloneSingleNodeEmojiBranchTest(t *testing.T, method cloneMethod) {
	defer testCleanup(t)
	nodea := nodeNrType(1)

	createTestConfig(t, nodea.Name(), nodea, "hooks:", "silly:\n  defaulthead: 😃\nhooks:")
	spawnNode(t, nodea)

	createRepo(t, nodea, "test1", true)

	wdir1 := clone(t, method, nodea, "test1", "admin", true)
	runRawCommand(t, "git", wdir1, nil, "checkout", "--orphan", "😃")
	writeTestFiles(t, wdir1, 0, 3)
	runRawCommand(t, "git", wdir1, nil, "commit", "-sm", "Writing our tests")

	// Push
	pushout := runRawCommand(t, "git", wdir1, nil, "push", "--set-upstream", "origin", "😃")
	if !strings.Contains(pushout, "* [new branch]      😃 -> 😃") {
		t.Fatal("Something went wrong in pushing")
	}

	// And reclone
	wdir2 := clone(t, method, nodea, "test1", "admin", true)
	testFiles(t, wdir2, 0, 3)

	// Add a new file
	writeTestFiles(t, wdir2, 4, 4)
	runRawCommand(t, "git", wdir2, nil, "commit", "-sm", "Testing the push again")

	// Push again
	pushout = runRawCommand(t, "git", wdir2, nil, "push")
	if !strings.Contains(pushout, "  😃 -> 😃") {
		t.Fatal("Something went wrong in pushing")
	}

	// And clone once more
	wdir3 := clone(t, method, nodea, "test1", "", true)
	testFiles(t, wdir3, 0, 4)

	// And make sure we can bring wdir1 up to date
	runRawCommand(t, "git", wdir1, nil, "pull")
}

func TestCompressedContent(t *testing.T) {
	runForTestedCloneMethods(t, performCompressedContentTest)
}

func performCompressedContentTest(t *testing.T, method cloneMethod) {
	defer testCleanup(t)
	nodea := nodeNrType(1)

	createNodes(t, nodea)

	createRepo(t, nodea, "test1", true)

	wdir1 := clone(t, method, nodea, "test1", "admin", true)
	writeTestFiles(t, wdir1, 0, 3)
	runRawCommand(t, "git", wdir1, nil, "commit", "-sm", "Writing our tests")

	// Create 100 branches
	for i := 0; i < 100; i++ {
		branch := "test-" + strconv.Itoa(i)
		runRawCommand(t, "git", wdir1, nil, "checkout", "-b", branch)
		writeTestFiles(t, wdir1, 1, i+10)
		runRawCommand(t, "git", wdir1, nil, "add", ".")
		runRawCommand(t, "git", wdir1, nil, "commit", "-sm", "Testing branch "+branch)
		runRawCommand(t, "git", wdir1, nil, "checkout", "master")
	}

	// Push
	pushout := runRawCommand(t, "git", wdir1, nil, "push", "--all")
	if !strings.Contains(pushout, "* [new branch]      master -> master") {
		t.Fatal("Something went wrong in pushing")
	}

	// And reclone
	wdir2 := clone(t, method, nodea, "test1", "admin", true)
	testFiles(t, wdir2, 0, 3)
}
