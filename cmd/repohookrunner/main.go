package main

import (
	"fmt"
	"os"

	"repospanner.org/repospanner/hookrun"
)

func main() {
	controlfile := os.NewFile(3, "controlfile")
	if controlfile == nil {
		panic("Unable to open control file")
	}
	err := hookrun.Run(controlfile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error running hook protocol: %s", err)
		os.Exit(1)
	}
}
