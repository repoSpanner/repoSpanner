package cmd

import (
	"crypto/rand"
	"fmt"
	"io"
	mathrand "math/rand"
	"os"

	"github.com/spf13/cobra"
)

var caCmd = &cobra.Command{
	Use:   "ca",
	Short: "Perform certificate authority tasks.",
	Long:  `Certificate Authority management tasks.`,
}

func getRandReader() io.Reader {
	weak, err := caCmd.Flags().GetBool("very-insecure-weak-keys")
	if err != nil {
		panic(err)
	}
	if weak {
		for i := 0; i < 10; i++ {
			fmt.Fprintln(os.Stderr, "WEAK KEY GENERATION USED")
			fmt.Println("WEAK KEY GENERATION USED")
		}
		// These keys are *INCREDIBLY* weak, and should never, ever be used.
		// This option is purely here for the functional tests, since otherwise they
		// would need tons of entropy
		return mathrand.New(mathrand.NewSource(0))
	}
	return rand.Reader
}

func init() {
	caCmd.PersistentFlags().Bool("very-insecure-weak-keys", false, "Generate weak keys")
	caCmd.PersistentFlags().MarkHidden("very-insecure-weak-keys")

	rootCmd.AddCommand(caCmd)
}
