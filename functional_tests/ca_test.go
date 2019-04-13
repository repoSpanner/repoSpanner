package functional_tests

import (
	"crypto/x509"
	"encoding/pem"
	"io/ioutil"
	"path"
	"strings"
	"testing"
)

func TestWeakKeysNotInHelp(t *testing.T) {
	if !runCloneMethodIndependentTest(t) {
		return
	}

	defer testCleanup(t)
	createTestConfig(t, "ca", 0)

	out := runCommand(
		t,
		"ca",
		"ca",
		"init",
		"--help",
	)
	if strings.Contains(string(out), "weak-keys") {
		t.Fatal("weak-keys argument in ca command help!")
	}
}

func TestCaInit(t *testing.T) {
	if !runCloneMethodIndependentTest(t) {
		return
	}
	defer testCleanup(t)
	createTestConfig(t, "ca", 0)

	out := runCommand(t, "ca",
		"ca", "init", testCluster,
	)

	if strings.Contains(out, "WEAK KEYS USED") {
		t.Fatal("Weak key used")
	}

	capem, err := ioutil.ReadFile(path.Join(testDir, "ca", "ca.crt"))
	failIfErr(t, err, "reading CA PEM file")
	pemblock, rest := pem.Decode(capem)
	if len(rest) != 0 {
		t.Fatal("More than just CA Cert in ca.crt?")
	}
	if pemblock.Type != "CERTIFICATE" {
		t.Fatalf("CA Cert is not CERTIFICATE but %s", pemblock.Type)
	}
	x509cert, err := x509.ParseCertificate(pemblock.Bytes)
	failIfErr(t, err, "reading ca.crt")
	if !x509cert.IsCA {
		t.Fatal("CA cert is not a CA cert")
	}
	if x509cert.SerialNumber.Int64() != 1 {
		t.Fatalf("CA cert serial incorrect: %d", x509cert.SerialNumber)
	}
	if x509cert.KeyUsage&x509.KeyUsageCertSign == 0 {
		t.Fatal("CA cert is not allowed to sign cert")
	}
	if x509cert.KeyUsage&x509.KeyUsageCRLSign == 0 {
		t.Fatal("CA cert is not allowed to sign CRL")
	}
	if !x509cert.BasicConstraintsValid {
		t.Fatal("Basic constraints not marked valid")
	}
	if x509cert.MaxPathLen != 0 {
		t.Fatal("Path len is not 0")
	}
	if !x509cert.MaxPathLenZero {
		t.Fatal("Path len 0 not set")
	}
	if len(x509cert.PermittedDNSDomains) != 1 || x509cert.PermittedDNSDomains[0] != testCluster {
		t.Fatalf("Permitted DNS names incorrect: %s", x509cert.PermittedDNSDomains)
	}
	if x509cert.Subject.CommonName != "repoSpanner localdomain cluster CA" {
		t.Fatalf("Subject is not as expected: %s", x509cert.Subject.CommonName)
	}
}

func TestCaInitNoNameConstraint(t *testing.T) {
	if !runCloneMethodIndependentTest(t) {
		return
	}

	defer testCleanup(t)
	createTestConfig(t, "ca", 0)

	runCommand(t, "ca",
		"ca", "init", testCluster, "--no-name-constraint",
		insecureKeysFlag,
	)

	capem, err := ioutil.ReadFile(path.Join(testDir, "ca", "ca.crt"))
	failIfErr(t, err, "reading CA PEM file")
	pemblock, rest := pem.Decode(capem)
	if len(rest) != 0 {
		t.Fatal("More than just CA Cert in ca.crt?")
	}
	if pemblock.Type != "CERTIFICATE" {
		t.Fatalf("CA Cert is not CERTIFICATE but %s", pemblock.Type)
	}
	x509cert, err := x509.ParseCertificate(pemblock.Bytes)
	failIfErr(t, err, "reading ca.crt")
	if !x509cert.IsCA {
		t.Fatal("CA cert is not a CA cert")
	}
	if len(x509cert.PermittedDNSDomains) != 0 {
		t.Fatalf("Permitted DNS names incorrect: %s", x509cert.PermittedDNSDomains)
	}
}

func TestCaInitRandomCN(t *testing.T) {
	if !runCloneMethodIndependentTest(t) {
		return
	}

	defer testCleanup(t)
	createTestConfig(t, "ca", 0)

	out := runCommand(t, "ca",
		"ca", "init", testCluster,
		"--random-cn",
	)

	if strings.Contains(out, "WEAK KEYS USED") {
		t.Fatal("Weak key used")
	}

	capem, err := ioutil.ReadFile(path.Join(testDir, "ca", "ca.crt"))
	failIfErr(t, err, "reading CA PEM file")
	pemblock, rest := pem.Decode(capem)
	if len(rest) != 0 {
		t.Fatal("More than just CA Cert in ca.crt?")
	}
	if pemblock.Type != "CERTIFICATE" {
		t.Fatalf("CA Cert is not CERTIFICATE but %s", pemblock.Type)
	}
	x509cert, err := x509.ParseCertificate(pemblock.Bytes)
	failIfErr(t, err, "reading ca.crt")
	if !x509cert.IsCA {
		t.Fatal("CA cert is not a CA cert")
	}
	if x509cert.SerialNumber.Int64() != 1 {
		t.Fatalf("CA cert serial incorrect: %d", x509cert.SerialNumber)
	}
	if x509cert.KeyUsage&x509.KeyUsageCertSign == 0 {
		t.Fatal("CA cert is not allowed to sign cert")
	}
	if x509cert.KeyUsage&x509.KeyUsageCRLSign == 0 {
		t.Fatal("CA cert is not allowed to sign CRL")
	}
	if !x509cert.BasicConstraintsValid {
		t.Fatal("Basic constraints not marked valid")
	}
	if x509cert.MaxPathLen != 0 {
		t.Fatal("Path len is not 0")
	}
	if !x509cert.MaxPathLenZero {
		t.Fatal("Path len 0 not set")
	}
	if len(x509cert.PermittedDNSDomains) != 1 || x509cert.PermittedDNSDomains[0] != testCluster {
		t.Fatalf("Permitted DNS names incorrect: %s", x509cert.PermittedDNSDomains)
	}
	if x509cert.Subject.CommonName == "repoSpanner localdomain cluster CA" {
		t.Fatalf("Subject does not include random part: %s", x509cert.Subject.CommonName)
	}
}

func TestCANode(t *testing.T) {
	if !runCloneMethodIndependentTest(t) {
		return
	}

	defer testCleanup(t)
	createTestCA(t)

	runCommand(t, "ca",
		"ca", "node", testRegion, "nodea",
		insecureKeysFlag,
	)
}

func TestCaLeaf(t *testing.T) {
	if !runCloneMethodIndependentTest(t) {
		return
	}

	defer testCleanup(t)
	createTestCA(t)

	runCommand(t, "ca",
		"ca", "leaf", "myuser",
		"--read", "--write", "--admin",
		"--region", "*", "--repo", "*",
		insecureKeysFlag,
	)
}
