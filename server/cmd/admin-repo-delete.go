package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/repoSpanner/repoSpanner/server/datastructures"
)

var adminDeleteRepoCmd = &cobra.Command{
	Use:   "delete",
	Short: "Repo deleting",
	Long:  `Delete a repository.`,
	Run:   runAdminDeleteRepo,
	Args:  cobra.ExactArgs(1),
}

func runAdminDeleteRepo(cmd *cobra.Command, args []string) {
	request := datastructures.RepoDeleteRequest{
		Reponame: args[0],
	}

	clnt := getAdminClient()
	var resp datastructures.CommandResponse
	shouldExit := clnt.PerformWithRequest(
		"admin/deleterepo",
		request,
		&resp,
	)
	if shouldExit {
		return
	}

	if !resp.Success {
		fmt.Fprintf(os.Stderr, "Error deleting repository: %s\n", resp.Error)
		os.Exit(1)
	}
	fmt.Println("Repo deleted successfully")
}

func init() {
	adminRepoCmd.AddCommand(adminDeleteRepoCmd)
}
