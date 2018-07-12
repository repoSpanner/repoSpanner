package cmd

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"

	"golang.org/x/net/http2"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var adminCmd = &cobra.Command{
	Use:   "admin",
	Short: "Perform administrative tasks",
	Long:  `Administrative tasks.`,
}

var adminRepoCmd = &cobra.Command{
	Use:   "repo",
	Short: "Repo management",
	Long:  `Perform repository management functions.`,
}

func init() {
	rootCmd.AddCommand(adminCmd)
	adminCmd.AddCommand(adminRepoCmd)

	adminCmd.PersistentFlags().Bool("json", false, "Output raw json output")
}

type adminClient struct {
	httpClient *http.Client
	baseurl    string
}

func (c *adminClient) getURL(url string) string {
	baseurl := c.baseurl
	if !strings.HasSuffix(baseurl, "/") {
		baseurl = baseurl + "/"
	}
	if strings.HasPrefix(url, "/") {
		url = url[1:]
	}
	return baseurl + url
}

func (c *adminClient) PerformUpload(url string, size int, body io.Reader, out interface{}) (shouldExit bool) {
	req, err := http.NewRequest(
		"POST",
		c.getURL(url),
		body,
	)
	if err != nil {
		panic(err)
	}
	req.Header["X-Object-Size"] = []string{strconv.Itoa(size)}
	resp, err := c.httpClient.Do(
		req,
	)
	if err != nil {
		panic(err)
	}
	if resp.StatusCode != 200 {
		panic("Server error occured")
	}
	defer resp.Body.Close()
	bodycts, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		panic(err)
	}
	if json, _ := adminCmd.Flags().GetBool("json"); json {
		fmt.Print(string(bodycts))
		shouldExit = true
	}
	err = json.Unmarshal(bodycts, &out)
	if err != nil {
		panic(err)
	}
	return
}

func (c *adminClient) Perform(url string, out interface{}) (shouldExit bool) {
	resp, err := c.httpClient.Get(c.getURL(url))
	if err != nil {
		panic(err)
	}
	if resp.StatusCode != 200 {
		panic("Server error occured")
	}
	defer resp.Body.Close()
	bodycts, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		panic(err)
	}
	if json, _ := adminCmd.Flags().GetBool("json"); json {
		fmt.Print(string(bodycts))
		shouldExit = true
	}
	err = json.Unmarshal(bodycts, &out)
	if err != nil {
		panic(err)
	}
	return
}

func (c *adminClient) PerformWithRequest(url string, in interface{}, out interface{}) (shouldExit bool) {
	req, err := json.Marshal(in)
	if err != nil {
		panic(err)
	}
	body := bytes.NewBuffer(req)
	return c.PerformUpload(url, body.Len(), body, out)
}

func getAdminClient() *adminClient {
	baseurl := viper.GetString("admin.url")
	if baseurl == "" {
		panic("No admin URL configured")
	}

	admincert, err := tls.LoadX509KeyPair(
		viper.GetString("admin.cert"),
		viper.GetString("admin.key"),
	)
	if err != nil {
		panic(err)
	}

	var certpool *x509.CertPool
	capath := viper.GetString("admin.ca")
	if capath != "" {
		cts, err := ioutil.ReadFile(capath)
		if err != nil {
			panic(err)
		}
		certpool = x509.NewCertPool()
		if ok := certpool.AppendCertsFromPEM(cts); !ok {
			panic("No CA certs loaded")
		}
	}

	transport := &http.Transport{
		TLSClientConfig: &tls.Config{
			Certificates:             []tls.Certificate{admincert},
			NextProtos:               []string{"h2"},
			PreferServerCipherSuites: true,
			MinVersion:               tls.VersionTLS12,
			RootCAs:                  certpool,
		},
	}
	if err := http2.ConfigureTransport(transport); err != nil {
		panic(err)
	}
	return &adminClient{
		httpClient: &http.Client{Transport: transport},
		baseurl:    baseurl,
	}
}
