package cmd

import (
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"io/ioutil"
	"math/big"
	"os"
	"path"
	"runtime"
	"time"

	"repospanner.org/repospanner/server/constants"
	"repospanner.org/repospanner/server/service"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var caInitCmd = &cobra.Command{
	Use:   "init",
	Short: "Initialize a repoSpanner CA",
	Long:  `Generating a full repoSpanner CA.`,
	Run:   runCaInit,
	Args:  cobra.ExactArgs(1),
}

func runCaInit(cmd *cobra.Command, args []string) {
	capath := viper.GetString("ca.path")
	clustername := args[0]

	// Checks
	if capath == "" {
		panic("No ca path configured")
	}
	if _, err := os.Stat(capath); err == nil {
		panic("CA path exists")
	}

	// Perform generation
	fmt.Println("Generating private key")
	priv, err := rsa.GenerateKey(getRandReader(), 2048)
	if err != nil {
		panic(err)
	}
	marshalledPriv := x509.MarshalPKCS1PrivateKey(priv)

	fmt.Println("Generating template certificate")
	subname := pkix.Name{
		CommonName: "repoSpanner " + clustername + " cluster CA",
	}
	fmt.Println("Subject/issuer name: ", subname)
	years, err := cmd.Flags().GetInt("years")
	if err != nil {
		panic(err)
	}
	skipNameConstraint, err := cmd.Flags().GetBool("no-name-constraint")
	if err != nil {
		panic(err)
	}

	// Construct template
	serial := big.NewInt(1)
	cert := &x509.Certificate{
		SerialNumber: serial,
		Issuer:       subname,
		Subject:      subname,
		NotBefore:    time.Now(),
		NotAfter:     time.Now().AddDate(years, 0, 0),
		KeyUsage:     x509.KeyUsageCertSign | x509.KeyUsageCRLSign | x509.KeyUsageDigitalSignature,
		ExtraExtensions: []pkix.Extension{
			clusterNameExtension(clustername),
			permissionExtension(constants.CertPermissionCA),
		},
		BasicConstraintsValid: true,
		IsCA:           true,
		MaxPathLen:     0,
		MaxPathLenZero: true,
	}

	if !skipNameConstraint {
		atleast110, sure := service.IsAtLeastGo110(runtime.Version())
		if !atleast110 {
			fmt.Println("WARNING: Name constraint added, but repospanner built with Go1.9 or earlier!")
			fmt.Println("If nodes are also built with Go1.9 or earlier, they will refuse to start")
			if !sure {
				fmt.Println("INFO: We were unable to parse your Go version. The above message might not apply.")
			}
		}
		cert.PermittedDNSDomainsCritical = true
		cert.PermittedDNSDomains = []string{clustername}
	}

	fmt.Println("Signing certificate")
	signed, err := x509.CreateCertificate(getRandReader(), cert, cert, priv.Public(), priv)
	if err != nil {
		panic(err)
	}

	fmt.Println("Writing key and cert")
	if err := os.Mkdir(capath, 0700); err != nil {
		panic(err)
	}
	keyfile, err := os.OpenFile(path.Join(capath, "ca.key"), os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		panic(err)
	}
	defer keyfile.Close()
	pem.Encode(keyfile, &pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: marshalledPriv,
	})
	certfile, err := os.OpenFile(path.Join(capath, "ca.crt"), os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		panic(err)
	}
	defer certfile.Close()
	pem.Encode(certfile, &pem.Block{
		Type:  "CERTIFICATE",
		Bytes: signed,
	})
	if err := ioutil.WriteFile(path.Join(capath, "serial"), []byte(serial.Text(16)), 0644); err != nil {
		panic(err)
	}

	fmt.Println("Done")
}

func init() {
	caCmd.AddCommand(caInitCmd)

	caInitCmd.Flags().Int("years", 10, "Validity of the cluster CA crtificate")
	caInitCmd.Flags().Bool("no-name-constraint", false,
		"Do not add the DNSName constraint. Pass this if you expect to run nodes compiled with Go1.9 or earlier")
}
