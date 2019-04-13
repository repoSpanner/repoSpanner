package functional_tests

import (
	"strings"
	"testing"
)

func TestSpawn(t *testing.T) {
	if !runCloneMethodIndependentTest(t) {
		return
	}

	defer testCleanup(t)
	nodea := nodeNrType(1)
	createNodeCert(t, nodea)

	out := runFailingCommand(t, nodea.Name(), "serve")
	if !strings.Contains(out, "No state found ") {
		t.Fatal("Output of initial serve wrong")
	}

	out = runCommand(t, nodea.Name(), "serve", "--spawn")
	if !strings.Contains(out, "No state existed") {
		t.Fatal("No new state was created")
	}
	if !strings.Contains(out, "Starting serving") {
		t.Fatal("Did not spawn correctly")
	}
	if !strings.Contains(out, "Shutdown complete") {
		t.Fatal("Did not shut down properly")
	}
	if !strings.Contains(out, "became leader at term ") {
		t.Fatal("Did not become leader")
	}
}

func TestSimpleRun(t *testing.T) {
	if !runCloneMethodIndependentTest(t) {
		return
	}

	defer testCleanup(t)
	nodea := nodeNrType(1)
	spawnNode(t, nodea)
}
