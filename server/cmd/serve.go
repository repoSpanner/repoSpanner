package cmd

import (
	"crypto/tls"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"repospanner.org/repospanner/server/constants"
	"repospanner.org/repospanner/server/service"
)

var serveCmd = &cobra.Command{
	Use:   "serve",
	Short: "Start the repoSpanner server",
	Long:  `Command to actually run a repoSpanner server.`,
	Run:   runServer,
}

func runServer(cmd *cobra.Command, args []string) {
	constants.RunProfiling()

	debug, _ := cmd.Flags().GetBool("debug")
	spawning, _ := cmd.Flags().GetBool("spawn")
	joinnode, _ := cmd.Flags().GetString("joinnode")

	cfg := &service.Service{
		ClientCaCertFile: viper.GetString("certificates.ca"),
		ServerCerts:      make(map[string]tls.Certificate),
		ListenRPC:        viper.GetString("listen.rpc"),
		ListenHTTP:       viper.GetString("listen.http"),
		StateStorageDir:  viper.GetString("storage.state"),
		GitStorageConfig: viper.GetStringMapString("storage.git"),
		Debug:            debug,
	}

	cfg.ClientCertificate = viper.GetString("certificates.client.cert")
	cfg.ClientKey = viper.GetString("certificates.client.key")
	rpccert, err := tls.LoadX509KeyPair(
		viper.GetString("certificates.client.cert"),
		viper.GetString("certificates.client.key"),
	)
	if err != nil {
		panic(err)
	}
	cfg.RPCServerCert = rpccert
	for servername, certint := range viper.GetStringMap("certificates.server") {
		certinfo, ok := certint.(map[string]interface{})
		if !ok {
			panic("Invalid configuration: certificate not a map")
		}
		certpath, ok := certinfo["cert"].(string)
		if !ok {
			panic("Certpath not a string")
		}
		keypath, ok := certinfo["key"].(string)
		if !ok {
			panic("Keypath not a string")
		}

		cert, err := tls.LoadX509KeyPair(certpath, keypath)
		if err != nil {
			panic(err)
		}
		cfg.ServerCerts[servername] = cert
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	signal.Notify(c, syscall.SIGTERM)
	go func() {
		<-c
		cfg.Shutdown()
	}()

	if err := cfg.RunServer(spawning, joinnode); err != nil {
		panic(err)
	}
}

func init() {
	rootCmd.AddCommand(serveCmd)

	serveCmd.Flags().Bool("debug", false, "Enable development logging")
	serveCmd.Flags().Bool("spawn", false, "Spawn a new region")
	serveCmd.Flags().String("joinnode", "", "Enter node of an existing region to join")
}
