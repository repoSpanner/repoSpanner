package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"repospanner.org/repospanner/server/datastructures"
)

var adminCreateRepoCmd = &cobra.Command{
	Use:   "create",
	Short: "Repo creation",
	Long:  `Create a repository.`,
	Run:   runAdminCreateRepo,
	Args:  cobra.ExactArgs(1),
}

func runAdminCreateRepo(cmd *cobra.Command, args []string) {
	reponame := args[0]
	ispublic, _ := cmd.Flags().GetBool("public")

	req := datastructures.RepoRequestInfo{
		Reponame: reponame,
		Public:   ispublic,
	}

	clnt := getAdminClient()
	var resp datastructures.CommandResponse

	shouldExit := clnt.PerformWithRequest(
		"admin/createrepo",
		req,
		&resp,
	)
	if shouldExit {
		return
	}

	if !resp.Success {
		fmt.Fprintf(os.Stderr, "Error creating repository: %s\n", resp.Error)
		os.Exit(1)
	}
	fmt.Println("Repo created successfully")
}

func init() {
	adminRepoCmd.AddCommand(adminCreateRepoCmd)

	adminCreateRepoCmd.Flags().Bool("public", false,
		"Create the repository as publicly readable")
}
