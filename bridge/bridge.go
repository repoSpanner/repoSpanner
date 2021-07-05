package bridge

import (
	"crypto/tls"
	"crypto/x509"
	"io"
	"io/ioutil"
	"net/http"
	"os"

	"github.com/repoSpanner/repoSpanner/server/constants"
)

var client *http.Client

func getClient() *http.Client {
	if client != nil {
		return client
	}

	cert, key := getCertAndKey()

	bridgecert, err := tls.LoadX509KeyPair(
		cert,
		key,
	)
	checkError(err, "Error initializing bridge")

	var certpool *x509.CertPool
	capath := configuration.Ca
	if capath != "" {
		cts, err := ioutil.ReadFile(capath)
		checkError(err, "Error initializing bridge ca")
		certpool = x509.NewCertPool()
		if ok := certpool.AppendCertsFromPEM(cts); !ok {
			exitWithError("Error initializing bridge ca")
		}
	}

	transport := &http.Transport{
		TLSClientConfig: &tls.Config{
			Certificates:             []tls.Certificate{bridgecert},
			NextProtos:               []string{"h2"},
			PreferServerCipherSuites: true,
			MinVersion:               tls.VersionTLS12,
			RootCAs:                  certpool,
		},
	}

	maybeConfigureH2(transport)

	client = &http.Client{
		Transport: transport,
	}
	return client
}

func getURL(service, reponame string) string {
	return configuration.BaseURL + "/repo/" + reponame + ".git/" + service
}

func bridge(r *http.Request) {
	r.Header["X-RepoBridge-Version"] = []string{constants.VersionString()}
	for key, val := range configuration.Extras {
		r.Header["X-Extra-"+key] = []string{val}
	}

	resp, err := getClient().Do(r)
	checkError(
		err,
		"Error bridging request",
	)
	defer resp.Body.Close()
	if resp.StatusCode == 404 {
		exitWithError("Repository does not exist")
	}
	if resp.StatusCode != 200 {
		exitWithError(
			"Server error",
			"statuscode", string(resp.StatusCode),
		)
	}
	_, err = io.Copy(os.Stdout, resp.Body)
	checkError(
		err,
		"Error streaming response",
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
