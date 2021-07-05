package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/repoSpanner/repoSpanner/server/datastructures"
)

var adminListReposCmd = &cobra.Command{
	Use:   "list",
	Short: "Repo listing",
	Long:  `Lists all repository.`,
	Run:   runAdminListRepos,
	Args:  cobra.ExactArgs(0),
}

func runAdminListRepos(cmd *cobra.Command, args []string) {
	clnt := getAdminClient()
	var resp datastructures.RepoList

	shouldExit := clnt.Perform(
		"admin/listrepos",
		&resp,
	)
	if shouldExit {
		return
	}

	for name, repo := range resp.Repos {
		fmt.Printf("Repo %s\n", name)
		fmt.Printf("\tPublic: %t\n", repo.Public)
		fmt.Printf("\tRefs:\n")
		for refname, refval := range repo.Refs {
			fmt.Printf("\t\t%s -> %s\n", refname, refval)
		}
		fmt.Printf("\tSymrefs:\n")
		for refname, refval := range repo.Symrefs {
			fmt.Printf("\t\t%s -> %s\n", refname, refval)
		}
		fmt.Printf("\tHooks:\n")
		fmt.Printf("\t\tPre-Receive: \t%s\n", repo.Hooks.PreReceive)
		fmt.Printf("\t\tUpdate: \t%s\n", repo.Hooks.Update)
		fmt.Printf("\t\tPost-Receive: \t%s\n", repo.Hooks.PostReceive)
	}
}

func init() {
	adminRepoCmd.AddCommand(adminListReposCmd)
}
