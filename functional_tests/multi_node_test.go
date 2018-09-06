package functional_tests

import (
	"strings"
	"testing"
)

func nodeInfoVerification(t *testing.T, node nodeNrType) {
	nodeinfo := runCommand(t, node.Name(), "admin", "nodestatus", "--json")
	t.Log("Node info: ", nodeinfo)
	if !strings.Contains(nodeinfo, "https://node1.regiona.repospanner.local:1444") {
		t.Error("Node A url not in node info")
	}
	if !strings.Contains(nodeinfo, "https://node2.regiona.repospanner.local:2444") {
		t.Error("Node B url not in node info")
	}
	if !strings.Contains(nodeinfo, "https://node3.regiona.repospanner.local:3444") {
		t.Error("Node C url not in node info")
	}
}

func TestJoin(t *testing.T) {
	defer testCleanup(t)
	nodea := nodeNrType(1)
	nodeb := nodeNrType(2)
	nodec := nodeNrType(3)

	createNodes(t, nodea, nodeb, nodec)

	nodeInfoVerification(t, nodea)
	nodeInfoVerification(t, nodeb)
	nodeInfoVerification(t, nodec)
}
