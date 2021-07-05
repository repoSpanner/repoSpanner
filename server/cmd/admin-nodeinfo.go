package cmd

import (
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"
	"github.com/repoSpanner/repoSpanner/server/datastructures"
)

var adminNodeInfoCmd = &cobra.Command{
	Use:   "nodestatus",
	Short: "Get node status",
	Long:  `Print information about a node.`,
	Run:   runAdminNodeStatus,
	Args:  cobra.ExactArgs(0),
}

const warnTime = 10 * time.Second
const errTime = 15 * time.Second

func exitStatus(d time.Duration) {
	if d >= errTime {
		os.Exit(2)
	} else if d >= warnTime {
		os.Exit(1)
	} else {
		os.Exit(0)
	}
}

func outputRawStatus(resp datastructures.NodeStatus, timesinceping time.Duration) {
	fmt.Printf("Node ID: %d\n", resp.NodeID)
	fmt.Printf("Node name: %s\n", resp.NodeName)
	fmt.Printf("Time since last ping: %s\n", timesinceping.Truncate(time.Second))
}

func outputNagiosStatus(resp datastructures.NodeStatus, timesinceping time.Duration) {
	msg := fmt.Sprintf("Node %s has last pinged %s ago", resp.NodeName, timesinceping.Truncate(time.Second))

	if timesinceping >= errTime {
		fmt.Println("CRITICAL:", msg)
	} else if timesinceping >= warnTime {
		fmt.Println("WARNING:", msg)
	} else {
		fmt.Println("OK:", msg)
	}
}

func runAdminNodeStatus(cmd *cobra.Command, args []string) {
	defer func() {
		if r := recover(); r != nil {
			if val, _ := cmd.Flags().GetBool("nagios"); val {
				fmt.Println("CRITICAL: Error checking status:", r)
			} else {
				fmt.Println("Error while checking:", r)
			}
			os.Exit(3)
		}
	}()

	clnt := getAdminClient()
	var resp datastructures.NodeStatus

	shouldExit := clnt.Perform(
		"admin/nodestatus",
		&resp,
	)
	lastselfping := resp.PeerPings[resp.NodeID]
	timesinceping := time.Duration(50 * time.Hour)
	if lastselfping.Timestamp != nil {
		lastselfpingtime := time.Unix(0, *lastselfping.Timestamp)
		timesinceping = time.Since(lastselfpingtime)
	}
	if shouldExit {
		exitStatus(timesinceping)
		return
	}

	if val, _ := cmd.Flags().GetBool("nagios"); val {
		outputNagiosStatus(resp, timesinceping)
	} else {
		outputRawStatus(resp, timesinceping)
	}

	exitStatus(timesinceping)
}

func init() {
	adminCmd.AddCommand(adminNodeInfoCmd)

	adminNodeInfoCmd.Flags().Bool("nagios", false, "Return nagios-formatted output")
}
