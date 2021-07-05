package main

import (
	"fmt"
	"log"
	"os"

	"github.com/repoSpanner/repoSpanner/bridge"
	"github.com/repoSpanner/repoSpanner/server/constants"
)

func main() {
	if !constants.VersionBuiltIn() {
		log.Print("Build made incorrectly")
		os.Exit(1)
	}
	if len(os.Args) == 1 {
		fmt.Println("repoSpanner bridge " + constants.PublicVersionString())
		if bridge.HasH2() {
			fmt.Println("This bridge is HTTP/2 enabled")
		} else {
			fmt.Println("This bridge is deprived of HTTP/2 goodness")
		}
		os.Exit(0)
	}

	bridge.ExecuteBridge()
}
