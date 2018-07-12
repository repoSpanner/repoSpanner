package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
	"repospanner.org/repospanner/server/datastructures"
)

var adminNodeInfoCmd = &cobra.Command{
	Use:   "nodeinfo",
	Short: "Get node info",
	Long:  `Print information about a node.`,
	Run:   runAdminNodeInfo,
	Args:  cobra.ExactArgs(0),
}

func runAdminNodeInfo(cmd *cobra.Command, args []string) {
	clnt := getAdminClient()
	var resp datastructures.NodeInfo

	shouldExit := clnt.Perform(
		"admin/nodeinfo",
		&resp,
	)
	if shouldExit {
		return
	}

	fmt.Printf("Node ID: %d\n", resp.NodeID)
	fmt.Printf("Node name: %s\n", resp.NodeName)
}

func init() {
	adminCmd.AddCommand(adminNodeInfoCmd)
}
