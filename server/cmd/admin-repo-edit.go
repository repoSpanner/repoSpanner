package cmd

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"repospanner.org/repospanner/server/datastructures"
	"repospanner.org/repospanner/server/storage"
)

var adminEditRepoCmd = &cobra.Command{
	Use:   "edit",
	Short: "Repo editing",
	Long:  `Edit a repository.`,
	Run:   runAdminEditRepo,
	Args:  cobra.MinimumNArgs(1),
}

func addHook(cmd *cobra.Command, req *datastructures.RepoUpdateRequest, reponame, flagname string) {
	hookname := flagname[len("hook-"):]
	var field datastructures.RepoUpdateField
	switch hookname {
	case "pre-receive":
		field = datastructures.RepoUpdateHookPreReceive
	case "update":
		field = datastructures.RepoUpdateHookUpdate
	case "post-receive":
		field = datastructures.RepoUpdateHookPostReceive
	default:
		panic("Invalid flag name")
	}

	val, _ := cmd.Flags().GetString("hook-" + hookname)
	if val == "" {
		// Clear out hook
		req.UpdateRequest[field] = string(storage.ZeroID)
	} else {
		// Upload the hook
		info, err := os.Stat(val)
		if err != nil {
			panic(err)
		}

		reader, err := os.Open(val)
		if err != nil {
			panic(err)
		}

		clnt := getAdminClient()
		var resp datastructures.CommandResponse
		clnt.PerformUpload(
			"admin/hook/"+reponame+".git/upload",
			int(info.Size()),
			reader,
			&resp,
		)

		if !resp.Success {
			fmt.Fprintf(os.Stderr, "Error uploading hook: %s\n", resp.Error)
			os.Exit(1)
		}
		fmt.Println("Hook uploaded successfully")
		req.UpdateRequest[field] = resp.Info
	}
}

func runAdminEditRepo(cmd *cobra.Command, args []string) {
	reponame := args[0]

	request := datastructures.RepoUpdateRequest{
		Reponame:      args[0],
		UpdateRequest: make(map[datastructures.RepoUpdateField]string),
	}

	cmd.Flags().Visit(func(f *pflag.Flag) {
		if f.Name == "public" {
			ispublic, _ := cmd.Flags().GetBool("public")
			if ispublic {
				request.UpdateRequest[datastructures.RepoUpdatePublic] = "true"
			} else {
				request.UpdateRequest[datastructures.RepoUpdatePublic] = "false"
			}
		} else if strings.HasPrefix(f.Name, "hook-") {
			addHook(cmd, &request, reponame, f.Name)
		}
	})

	if len(args) > 1 {
		// Parse symref updates
		symrefupdate := make([]string, 0)

		for _, arg := range args[1:] {
			if !strings.Contains(arg, "=") {
				fmt.Println("Symref rename argument", arg, "invalid: no = separator")
				return
			}
			if strings.Contains(arg, " ") {
				fmt.Println("Symref rename argument", arg, "invalid: contains space")
				return
			}

			symrefupdate = append(symrefupdate, arg)
		}

		request.UpdateRequest[datastructures.RepoUpdateSymref] = strings.Join(symrefupdate, " ")
	}

	if len(request.UpdateRequest) == 0 {
		fmt.Println("No update request provided")
		return
	}

	clnt := getAdminClient()
	var resp datastructures.CommandResponse
	shouldExit := clnt.PerformWithRequest(
		"admin/editrepo",
		request,
		&resp,
	)
	if shouldExit {
		return
	}

	if !resp.Success {
		fmt.Fprintf(os.Stderr, "Error editing repository: %s\n", resp.Error)
		os.Exit(1)
	}
	fmt.Println("Repo edited successfully")
}

func init() {
	adminRepoCmd.AddCommand(adminEditRepoCmd)

	adminEditRepoCmd.Flags().Bool("public", false,
		"Make the repository publicly readable")
	adminEditRepoCmd.Flags().String("hook-pre-receive", "",
		"Set a pre-receive hook")
	adminEditRepoCmd.Flags().String("hook-update", "",
		"Set an update hook")
	adminEditRepoCmd.Flags().String("hook-post-receive", "",
		"Set a post-receive hook")
}
