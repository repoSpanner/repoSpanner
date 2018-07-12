package cmd

import (
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"io/ioutil"
	"math/big"
	"path"

	"repospanner.org/repospanner/server/constants"

	"github.com/spf13/viper"
)

func clusterNameExtension(clustername string) pkix.Extension {
	return pkix.Extension{
		Id:       constants.OIDClusterName,
		Critical: false,
		Value:    []byte(clustername),
	}
}

func regionNameExtension(regionname string) pkix.Extension {
	return pkix.Extension{
		Id:       constants.OIDRegionName,
		Critical: false,
		Value:    []byte(regionname),
	}
}

func permissionExtension(perm constants.CertPermission) pkix.Extension {
	return pkix.Extension{
		Id:       constants.OIDPermission,
		Critical: false,
		Value:    []byte(perm),
	}
}

func getNextSerial() (*big.Int, error) {
	capath := viper.GetString("ca.path")

	serialb, err := ioutil.ReadFile(path.Join(capath, "serial"))
	if err != nil {
		return nil, err
	}
	serial := new(big.Int)
	serial, ok := serial.SetString(string(serialb), 16)
	if !ok {
		return nil, errors.New("Error loading serial")
	}
	serial = serial.Add(serial, big.NewInt(1))
	if err := ioutil.WriteFile(path.Join(capath, "serial"), []byte(serial.Text(16)), 0644); err != nil {
		return nil, err
	}
	return serial, nil
}

func readCertificate(pathname string) (*x509.Certificate, error) {
	certb, err := ioutil.ReadFile(pathname)
	if err != nil {
		return nil, err
	}
	block, rest := pem.Decode(certb)
	if len(rest) != 0 {
		return nil, errors.New("Rest data in cacert?")
	}
	if block.Type != "CERTIFICATE" {
		return nil, errors.New("Not a certificate in cacert?")
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return nil, err
	}
	return cert, nil
}

func getClusterName() (string, error) {
	cacert, err := readCertificate(path.Join(viper.GetString("ca.path"), "ca.crt"))
	if err != nil {
		return "", err
	}
	for _, ext := range cacert.Extensions {
		if ext.Id.Equal(constants.OIDClusterName) {
			return string(ext.Value), nil
		}
	}
	return "", errors.New("Certificate did not contain clustername extension")
}

func signCertificate(cert *x509.Certificate) ([]byte, []byte, error) {
	capath := viper.GetString("ca.path")

	if capath == "" {
		return nil, nil, errors.New("No ca path configured")
	}

	cakeyb, err := ioutil.ReadFile(path.Join(capath, "ca.key"))
	if err != nil {
		return nil, nil, err
	}
	block, rest := pem.Decode(cakeyb)
	if len(rest) != 0 {
		return nil, nil, errors.New("Rest data in cakey?")
	}
	if block.Type != "RSA PRIVATE KEY" {
		return nil, nil, errors.New("Not a private key in cakey?")
	}
	cakey, err := x509.ParsePKCS1PrivateKey(block.Bytes)
	if err != nil {
		return nil, nil, err
	}
	cacert, err := readCertificate(path.Join(capath, "ca.crt"))
	if err != nil {
		return nil, nil, err
	}

	// Override template fields
	cert.BasicConstraintsValid = true
	cert.IsCA = false
	cert.MaxPathLen = 0
	cert.MaxPathLenZero = true
	cert.KeyUsage = x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment

	if cert.SerialNumber == nil {
		serial, err := getNextSerial()
		if err != nil {
			return nil, nil, err
		}
		cert.SerialNumber = serial
	}

	// Perform generation
	priv, err := rsa.GenerateKey(getRandReader(), 2048)
	if err != nil {
		return nil, nil, err
	}
	marshalledPriv := x509.MarshalPKCS1PrivateKey(priv)

	signed, err := x509.CreateCertificate(getRandReader(), cert, cacert, priv.Public(), cakey)
	if err != nil {
		return nil, nil, err
	}

	pemcert := &pem.Block{
		Type:  "CERTIFICATE",
		Bytes: signed,
	}
	pemkey := &pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: marshalledPriv,
	}
	return pem.EncodeToMemory(pemcert), pem.EncodeToMemory(pemkey), nil
}
