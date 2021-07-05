package cmd

import (
	"fmt"

	"github.com/repoSpanner/repoSpanner/server/constants"

	"github.com/spf13/cobra"
)

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Get the repoSpanner version",
	Long:  `Print the repoSpanner version.`,
	Run: func(cmd *cobra.Command, args []string) {
		public, _ := cmd.Flags().GetBool("public")

		if public {
			fmt.Println("repoSpanner", constants.PublicVersionString())
		} else {
			fmt.Println("repoSpanner", constants.VersionString())
		}
	},
}

func init() {
	rootCmd.AddCommand(versionCmd)

	versionCmd.Flags().Bool("public", false, "Return the public version string")
}
