// +build prof

package constants

import (
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"strconv"
	"strings"

	"github.com/spf13/viper"
)

const hasProfiling = true

func RunProfiling() {
	rpclisten := viper.GetString("listen.rpc")
	split := strings.Split(rpclisten, ":")

	rpchost := ""
	rpcport := ""
	if len(split) == 1 {
		// Just port
		rpcport = split[0]
	} else {
		rpchost = split[0]
		rpcport = split[1]
	}
	var err error
	portnum, err := strconv.Atoi(rpcport)
	if err != nil {
		panic(err)
	}
	proflisten := fmt.Sprintf("%s:%d", rpchost, portnum+1)

	go func() {
		fmt.Println("RUNNING PROFILING ON ", proflisten)

		panic(http.ListenAndServe(proflisten, nil))
	}()
}
