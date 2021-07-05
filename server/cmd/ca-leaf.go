package cmd

import (
	"crypto/x509"
	"crypto/x509/pkix"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/repoSpanner/repoSpanner/server/constants"
)

var caLeafCmd = &cobra.Command{
	Use:   "leaf",
	Short: "Create a leaf certificate",
	Long:  `Generating a leaf certificate.`,
	Run:   runCaLeaf,
	Args:  cobra.ExactArgs(1),
}

func runCaLeaf(cmd *cobra.Command, args []string) {
	capath := viper.GetString("ca.path")
	subject := args[0]

	clustername, err := getClusterName()
	if err != nil {
		panic(err)
	}
	if _, err := os.Stat(path.Join(capath, subject+".crt")); err == nil {
		panic("Leaf certificate exists")
	}

	years, err := cmd.Flags().GetInt("years")
	if err != nil {
		panic(err)
	}

	exts := []pkix.Extension{
		clusterNameExtension(clustername),
	}

	regions, err := cmd.Flags().GetStringSlice("region")
	if err != nil {
		panic(err)
	}
	for _, region := range regions {
		ext := pkix.Extension{
			Id:       constants.OIDRegionName,
			Critical: false,
			Value:    []byte(region),
		}
		exts = append(exts, ext)
	}

	repos, err := cmd.Flags().GetStringSlice("repo")
	if err != nil {
		panic(err)
	}
	for _, repo := range repos {
		ext := pkix.Extension{
			Id:       constants.OIDRepoName,
			Critical: false,
			Value:    []byte(repo),
		}
		exts = append(exts, ext)
	}

	// Build key usages
	admin, err := cmd.Flags().GetBool("admin")
	if err != nil {
		panic(err)
	}
	monitor, err := cmd.Flags().GetBool("monitor")
	if err != nil {
		panic(err)
	}
	read, err := cmd.Flags().GetBool("read")
	if err != nil {
		panic(err)
	}
	write, err := cmd.Flags().GetBool("write")
	if err != nil {
		panic(err)
	}
	if admin {
		exts = append(exts, permissionExtension(constants.CertPermissionAdmin))
	}
	if monitor {
		exts = append(exts, permissionExtension(constants.CertPermissionMonitor))
	}
	if read {
		exts = append(exts, permissionExtension(constants.CertPermissionRead))
	}
	if write {
		exts = append(exts, permissionExtension(constants.CertPermissionWrite))
	}

	// Construct template
	cert := &x509.Certificate{
		Subject:   pkix.Name{CommonName: subject},
		NotBefore: time.Now(),
		NotAfter:  time.Now().AddDate(years, 0, 0),
		ExtKeyUsage: []x509.ExtKeyUsage{
			x509.ExtKeyUsageClientAuth,
		},
		ExtraExtensions: exts,
		// Due to the fact that we add name limitations to the CA cert,
		// the SAN extension must be added. Let's just add something.
		EmailAddresses: []string{subject + "@" + clustername},
	}

	pemcert, pemkey, err := signCertificate(cert)
	if err != nil {
		panic(err)
	}
	if err := ioutil.WriteFile(path.Join(capath, subject+".crt"), pemcert, 0644); err != nil {
		panic(err)
	}
	if err := ioutil.WriteFile(path.Join(capath, subject+".key"), pemkey, 0600); err != nil {
		panic(err)
	}

	fmt.Println("Done")
}

func init() {
	caCmd.AddCommand(caLeafCmd)

	caLeafCmd.Flags().Int("years", 1, "Validity of the crtificate")

	caLeafCmd.Flags().StringSlice("region", nil, "Region where this cert is valid (wildcards accepted)")
	caLeafCmd.MarkFlagRequired("region")

	caLeafCmd.Flags().StringSlice("repo", nil, "Region where this cert is valid (wildcards accepted)")
	caLeafCmd.MarkFlagRequired("repo")

	caLeafCmd.Flags().Bool("admin", false, "Whether this certificate has admin privileges")
	caLeafCmd.Flags().Bool("monitor", false, "Whether this certificate has monitor privileges")
	caLeafCmd.Flags().Bool("read", false, "Whether this certificate has read privileges")
	caLeafCmd.Flags().Bool("write", false, "Whether this certificate has write privileges")
}
