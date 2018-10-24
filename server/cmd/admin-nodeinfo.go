package cmd

import (
	"fmt"
	"os"
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

func outputRawStatus(resp datastructures.NodeStatus, timesinceping time.Duration) {
	fmt.Printf("Node ID: %d\n", resp.NodeID)
	fmt.Printf("Node name: %s\n", resp.NodeName)
	fmt.Printf("Time since last ping: %s\n", timesinceping.Truncate(time.Second))
}

func outputNagiosStatus(resp datastructures.NodeStatus, timesinceping time.Duration) {
	msg := fmt.Sprintf("Node %s has last pinged %s ago", resp.NodeName, timesinceping.Truncate(time.Second))

	if timesinceping >= (15 * time.Second) {
		fmt.Println("CRITICAL:", msg)
		os.Exit(2)
	} else if timesinceping >= (5 * time.Second) {
		fmt.Println("WARNING:", msg)
		os.Exit(2)
	} else {
		fmt.Println("OK:", msg)
		os.Exit(0)
	}
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

	if val, _ := cmd.Flags().GetBool("nagios"); val {
		outputNagiosStatus(resp, timesinceping)
	} else {
		outputRawStatus(resp, timesinceping)
	}
}

func init() {
	adminCmd.AddCommand(adminNodeInfoCmd)

	adminNodeInfoCmd.Flags().Bool("nagios", false, "Return nagios-formatted output")
}
