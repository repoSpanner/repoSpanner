package functional_tests

import (
	"strings"
	"testing"
)

// The most Meta test ever: pushing ourselves to a repoSpanner cluster!
func TestMeta(t *testing.T) {
	runForTestedCloneMethods(t, performMetaTest)
}

func performMetaTest(t *testing.T, method cloneMethod) {
	defer testCleanup(t)
	nodea := nodeNrType(1)
	nodeb := nodeNrType(2)
	nodec := nodeNrType(3)

	createNodes(t, nodea, nodeb, nodec)

	createRepo(t, nodea, "repoSpanner", true)

	// Determine the repoSpanner repo main directory
	repodir := runRawCommand(t, "git", "", nil, "rev-parse", "--show-toplevel")
	repodir = strings.Trim(repodir, "\n")

	// Get the master ref
	masterref := runRawCommand(t, "git", repodir, nil, "rev-parse", "master")

	// Create a repo that's cloned so it's setup
	wdir1 := cloneBare(t, method, nodea, "repoSpanner", "admin", true)

	// Push the main repoSpanner repo to the cloned repo
	pushout := runRawCommand(t, "git", repodir, nil, "push", wdir1, "--mirror")
	if !strings.Contains(pushout, "* [new branch]      master -> master") {
		t.Fatal("Something went wrong in pushing to local")
	}

	// Push
	pushout = runRawCommand(t, "git", wdir1, nil, "push", "--all")
	if !strings.Contains(pushout, "* [new branch]      master -> master") {
		t.Fatal("Something went wrong in pushing to cluster")
	}

	// And reclone
	wdir2 := clone(t, method, nodea, "repoSpanner", "admin", true)
	clonedmasterref := runRawCommand(t, "git", wdir2, nil, "rev-parse", "master")

	// And check
	if masterref != clonedmasterref {
		t.Errorf("Master ref (%s) does not equal cloned master ref (%s)", masterref, clonedmasterref)
	}
}
