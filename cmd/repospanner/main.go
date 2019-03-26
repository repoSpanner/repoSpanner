//go:generate protoc -I ../../server/protobuf --go_out=../../server/protobuf ../../server/protobuf/pushrequest.proto ../../server/protobuf/pingmessage.proto

package main

import (
	"log"

	"repospanner.org/repospanner/server/cmd"
	"repospanner.org/repospanner/server/constants"
)

func main() {
	if !constants.VersionBuiltIn() {
		log.Print("Build made incorrectly")
	}

	cmd.Execute()
}
