package functional_tests

import (
	"strings"
	"testing"
)

func nodeInfoVerification(t *testing.T, node nodeNrType) {
	nodeinfo, _ := _runCommand(t, node.Name(), "admin", "nodestatus", "--json")
	t.Log("Node info: ", nodeinfo)
	if !strings.Contains(nodeinfo, nodeNrType(1).RPCBase()) {
		t.Error("Node A url not in node info")
	}
	if !strings.Contains(nodeinfo, nodeNrType(2).RPCBase()) {
		t.Error("Node B url not in node info")
	}
	if !strings.Contains(nodeinfo, nodeNrType(3).RPCBase()) {
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
