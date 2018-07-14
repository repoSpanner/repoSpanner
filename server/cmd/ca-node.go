package cmd

import (
	"crypto/x509"
	"crypto/x509/pkix"
	"fmt"
	"io/ioutil"
	"math/big"
	"os"
	"path"
	"time"

	"repospanner.org/repospanner/server/constants"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var caNodeCmd = &cobra.Command{
	Use:   "node",
	Short: "Create a node certificate",
	Long:  `Generating a node certificate.`,
	Run:   runCaNode,
	Args:  cobra.ExactArgs(2),
}

func runCaNode(cmd *cobra.Command, args []string) {
	capath := viper.GetString("ca.path")
	regionname := args[0]
	nodename := args[1]
	clustername, err := getClusterName()
	if err != nil {
		panic(err)
	}
	if _, err := os.Stat(path.Join(capath, nodename+"."+regionname+".crt")); err == nil {
		panic("Node certificate exists")
	}

	fqdn := nodename + "." + regionname + "." + clustername

	years, err := cmd.Flags().GetInt("years")
	if err != nil {
		panic(err)
	}
	nodeidi, err := cmd.Flags().GetInt64("nodeid")
	if err != nil {
		panic(err)
	}

	serial, err := getNextSerial()
	if err != nil {
		panic(err)
	}

	var nodeid *big.Int
	if nodeidi == 0 {
		// Default nodeID to x509 serial
		nodeid = serial
	} else {
		nodeid = big.NewInt(nodeidi)
	}

	nodeidext := pkix.Extension{
		Id:       constants.OIDNodeID,
		Critical: false,
		Value:    []byte(nodeid.Text(10)),
	}

	nodenameext := pkix.Extension{
		Id:       constants.OIDNodeName,
		Critical: false,
		Value:    []byte(nodename),
	}

	// Construct template
	cert := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: fqdn},
		DNSNames:     []string{fqdn},
		NotBefore:    time.Now(),
		NotAfter:     time.Now().AddDate(years, 0, 0),
		ExtKeyUsage: []x509.ExtKeyUsage{
			x509.ExtKeyUsageServerAuth,
			x509.ExtKeyUsageClientAuth,
		},
		ExtraExtensions: []pkix.Extension{
			clusterNameExtension(clustername),
			regionNameExtension(regionname),
			nodenameext,
			nodeidext,
		},
	}

	pemcert, pemkey, err := signCertificate(cert)
	if err != nil {
		panic(err)
	}
	if err := ioutil.WriteFile(path.Join(capath, nodename+"."+regionname+".crt"), pemcert, 0644); err != nil {
		panic(err)
	}
	if err := ioutil.WriteFile(path.Join(capath, nodename+"."+regionname+".key"), pemkey, 0600); err != nil {
		panic(err)
	}

	fmt.Println("Done")
}

func init() {
	caCmd.AddCommand(caNodeCmd)

	caNodeCmd.Flags().Int("years", 1, "Validity of the node certificate")
	caNodeCmd.Flags().Int64("nodeid", 0, "Node ID (for replacing existing node cert)")
}
