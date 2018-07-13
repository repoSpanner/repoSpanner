package client

import (
	"crypto/tls"
	"crypto/x509"
	"io"
	"io/ioutil"
	"net/http"
	"os"

	"repospanner.org/repospanner/server/constants"

	"golang.org/x/net/http2"

	"github.com/spf13/viper"
)

var client *http.Client

func getClient() *http.Client {
	if client != nil {
		return client
	}

	cert, key := getCertAndKey()

	clientcert, err := tls.LoadX509KeyPair(
		cert,
		key,
	)
	checkError(err, "Error initializing client")

	var certpool *x509.CertPool
	capath := viper.GetString("ca")
	if capath != "" {
		cts, err := ioutil.ReadFile(capath)
		checkError(err, "Error initializing client ca")
		certpool = x509.NewCertPool()
		if ok := certpool.AppendCertsFromPEM(cts); !ok {
			exitWithError("Error initializing client ca")
		}
	}

	transport := &http.Transport{
		TLSClientConfig: &tls.Config{
			Certificates:             []tls.Certificate{clientcert},
			NextProtos:               []string{"h2"},
			PreferServerCipherSuites: true,
			MinVersion:               tls.VersionTLS12,
			RootCAs:                  certpool,
		},
	}
	err = http2.ConfigureTransport(transport)
	checkError(err, "Error initializing h2 transport")

	client = &http.Client{
		Transport: transport,
	}
	return client
}

func getURL(service, reponame string) string {
	return viper.GetString("baseurl") + "/repo/" + reponame + ".git/" + service
}

func bridge(r *http.Request) {
	r.Header["X-RepoClient-Version"] = []string{constants.VersionString()}

	resp, err := getClient().Do(r)
	checkError(
		err,
		"Error bridging request",
		"request", r,
	)
	defer resp.Body.Close()
	if resp.StatusCode == 404 {
		exitWithError("Repository does not exist")
	}
	if resp.StatusCode != 200 {
		exitWithError(
			"Server error",
			"request", r,
		)
	}
	_, err = io.Copy(os.Stdout, resp.Body)
	checkError(
		err,
		"Error streaming response",
		"request", r,
	)
}

func performRefDiscovery(service, reponame string) {
	url := getURL("info/refs", reponame) + "?service=" + service
	req, err := http.NewRequest("GET", url, nil)
	checkError(err, "Error preparing discovery request")
	bridge(req)
}

func performService(r io.ReadCloser, service, reponame string) {
	url := getURL(service, reponame)
	req, err := http.NewRequest("POST", url, r)
	checkError(err, "Error preparing service request")
	bridge(req)
}
