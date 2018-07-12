package main

import (
	"fmt"
	"log"
	"os"

	"repospanner.org/repospanner/client"
	"repospanner.org/repospanner/server/constants"
)

func main() {
	if !constants.VersionBuiltIn() {
		log.Print("Build made incorrectly")
		os.Exit(1)
	}
	if len(os.Args) == 1 {
		fmt.Println("repoSpanner client " + constants.PublicVersionString())
		os.Exit(0)
	}

	client.ExecuteClient()
}
