package functional_tests

import (
	"strings"
	"testing"
)

func TestBWrapHook(t *testing.T) {
	runForTestedCloneMethods(t, performBWrapHookTest)
}

func performBWrapHookTest(t *testing.T, method cloneMethod) {
	defer testCleanup(t)
	nodea := nodeNrType(1)
	nodeb := nodeNrType(2)
	nodec := nodeNrType(3)

	useBubbleWrap = true

	createNodes(t, nodea, nodeb, nodec)

	createRepo(t, nodea, "test1", false)
	runCommand(
		t, nodeb.Name(),
		"admin", "repo", "edit", "test1", "--hook-pre-receive", "blobs/test.sh",
	)

	wdir := clone(t, method, nodea, "test1", "admin", true)
	writeTestFiles(t, wdir, 0, 2)
	runRawCommand(t, "git", wdir, nil, "commit", "-sm", "Writing our tests")

	out := runRawCommand(t, "git", wdir, nil, "push")

	if !strings.Contains(out, "RUNNING HOOK") {
		t.Fatal("Hook did not run")
	}
	if !strings.Contains(out, "PS: 3") {
		t.Fatal("Did not get bubble wrapped")
	}
	if !strings.Contains(out, "Hostname: myhostname") {
		t.Fatal("Did not get bubble wrapped")
	}

	writeTestFiles(t, wdir, 3, 3)
	runRawCommand(t, "git", wdir, nil, "commit", "-sm", "Writing our tests")
	runCommand(
		t, nodec.Name(),
		"admin", "repo", "edit", "test1", "--hook-pre-receive", "blobs/test-blocking.sh",
	)

	out = runFailingRawCommand(t, "git", wdir, nil, "push")

	if !strings.Contains(out, "RUNNING HOOK") {
		t.Fatal("Hook did not run")
	}
	if !strings.Contains(out, "BLOCKING THE PUSH") {
		t.Fatal("Push was not blocked by hook?")
	}

	runCommand(
		t, nodea.Name(),
		"admin", "repo", "edit", "test1", "--hook-pre-receive", "blobs/test.sh",
	)

	out = runRawCommand(t, "git", wdir, nil, "push")

	if !strings.Contains(out, "RUNNING HOOK") {
		t.Fatal("Hook did not run")
	}

	writeTestFiles(t, wdir, 4, 4)
	runRawCommand(t, "git", wdir, nil, "commit", "-sm", "Writing our tests")
	runCommand(
		t, nodeb.Name(),
		"admin", "repo", "edit", "test1", "--hook-update", "blobs/test-blocking.sh",
	)

	out = runFailingRawCommand(t, "git", wdir, nil, "push")

	if !strings.Contains(out, "RUNNING HOOK") {
		t.Fatal("Hook did not run")
	}
	if !strings.Contains(out, "BLOCKING THE PUSH") {
		t.Fatal("Push was not blocked by hook?")
	}

	runCommand(
		t, nodec.Name(),
		"admin", "repo", "edit", "test1", "--hook-update", "blobs/test.sh",
	)
	runCommand(
		t, nodeb.Name(),
		"admin", "repo", "edit", "test1", "--hook-post-receive", "blobs/test.sh",
	)

	out = runRawCommand(t, "git", wdir, nil, "push")

	if !strings.Contains(out, "RUNNING HOOK") {
		t.Fatal("Hook did not run")
	}
}
