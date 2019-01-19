package cmd

import (
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"io/ioutil"
	"os"
	"path"

	"repospanner.org/repospanner/server/constants"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var caInfoCmd = &cobra.Command{
	Use:   "info",
	Short: "Show information about a repoSpanner certificate",
	Long:  `Show information about a repoSpanner certificate.`,
	Run:   runCaInfo,
	Args:  cobra.ExactArgs(1),
}

func determineCertType(cert *x509.Certificate) string {
	if cert.IsCA {
		return "CA"
	}
	for _, ku := range cert.ExtKeyUsage {
		if ku == x509.ExtKeyUsageServerAuth {
			return "Node"
		}
	}
	return "Leaf"
}

func runCaInfo(cmd *cobra.Command, args []string) {
	capath := viper.GetString("ca.path")
	certpath := args[0]

	// Checks
	if capath == "" {
		panic("No ca path configured")
	}
	if _, err := os.Stat(capath); err == os.ErrNotExist {
		panic("CA path does not exist")
	}

	// Read files
	certpem, err := ioutil.ReadFile(certpath)
	if err != nil {
		panic(err)
	}
	capem, err := ioutil.ReadFile(path.Join(capath, "ca.crt"))
	if err != nil {
		panic(err)
	}

	// Parse certificates
	certblock, rest := pem.Decode(certpem)
	if len(rest) != 0 {
		panic("More data in cert file than expected")
	}
	cert, err := x509.ParseCertificate(certblock.Bytes)
	if err != nil {
		panic(err)
	}
	cablock, rest := pem.Decode(capem)
	if len(rest) != 0 {
		panic("More data in CA file than expected")
	}
	ca, err := x509.ParseCertificate(cablock.Bytes)
	if err != nil {
		panic(err)
	}

	// Verify cert is signed by CA
	err = cert.CheckSignatureFrom(ca)
	if err != nil {
		fmt.Println("WARNING: Certificate is not signed by CA:", err)
	}

	fmt.Println("Certificate information:")
	fmt.Println("Subject:", cert.Subject.CommonName)
	fmt.Println("Certificate type: ", determineCertType(cert))

	for _, ext := range cert.Extensions {
		if ext.Id.Equal(constants.OIDClusterName) {
			fmt.Println("repoSpanner cluster name:", string(ext.Value))
		} else if ext.Id.Equal(constants.OIDRegionName) {
			fmt.Println("repoSpanner region name:", string(ext.Value))
		} else if ext.Id.Equal(constants.OIDNodeName) {
			fmt.Println("repoSpanner Node Name:", string(ext.Value))
		} else if ext.Id.Equal(constants.OIDNodeID) {
			fmt.Println("repoSpanner Node ID:", string(ext.Value))
		} else if ext.Id.Equal(constants.OIDPermission) {
			fmt.Println("repoSpanner permission:", string(ext.Value))
		} else if ext.Id.Equal(constants.OIDRepoName) {
			fmt.Println("repoSpanner regions:", string(ext.Value))
		}
	}
}

func init() {
	caCmd.AddCommand(caInfoCmd)
}
