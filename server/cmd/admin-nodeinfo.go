package cmd

import (
	"fmt"
	"time"

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
	var resp datastructures.NodeStatus

	shouldExit := clnt.Perform(
		"admin/nodestatus",
		&resp,
	)
	if shouldExit {
		return
	}

	lastselfping := resp.PeerPings[resp.NodeID]
	lastselfpingtime := time.Unix(0, *lastselfping.Timestamp)
	timesinceping := time.Since(lastselfpingtime)

	fmt.Printf("Node ID: %d\n", resp.NodeID)
	fmt.Printf("Node name: %s\n", resp.NodeName)
	fmt.Printf("Time since last ping: %s\n", timesinceping.Truncate(time.Second))

}

func init() {
	adminCmd.AddCommand(adminNodeInfoCmd)
}
