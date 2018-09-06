package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
	"repospanner.org/repospanner/server/datastructures"
)

var adminNodeInfoCmd = &cobra.Command{
	Use:   "nodestatus",
	Short: "Get node status",
	Long:  `Print information about a node.`,
	Run:   runAdminNodeStatus,
	Args:  cobra.ExactArgs(0),
}

func runAdminNodeStatus(cmd *cobra.Command, args []string) {
	clnt := getAdminClient()
	var resp datastructures.NodeInfo

	shouldExit := clnt.Perform(
		"admin/nodestatus",
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
