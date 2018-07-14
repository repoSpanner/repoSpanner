package service

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"io/ioutil"
	"math/big"
	"net/http"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/coreos/etcd/pkg/transport"
	"github.com/coreos/etcd/raft"
	"github.com/coreos/pkg/capnslog"
	"github.com/pkg/errors"
	"golang.org/x/net/http2"

	"repospanner.org/repospanner/server/constants"
	"repospanner.org/repospanner/server/storage"
	"repospanner.org/repospanner/server/utils"
)

func getCertPoolFromFile(pemfile string) (*x509.CertPool, error) {
	pemcontents, err := ioutil.ReadFile(pemfile)
	if err != nil {
		return nil, errors.Wrap(err, "Error reading pemcert")
	}
	certpool := x509.NewCertPool()
	if ok := certpool.AppendCertsFromPEM(pemcontents); !ok {
		return nil, errors.New("No CA certs found")
	}
	return certpool, nil
}

var versionregex = regexp.MustCompile("^go([0-9]+)\\.([0-9]+)(\\.)?")

// IsAtLeastGo110 returns whether the current binary is compiled on a Golang newer than
// Version 1.10, and whether it is sure about this.
func IsAtLeastGo110(version string) (atleast110 bool, sure bool) {
	matches := versionregex.FindStringSubmatch(version)
	// Matches would be e.g.: [go1.10. 1 10 .]
	if len(matches) != 3 && len(matches) != 4 {
		// This is not a tagged go release version string
		return
	}
	major, err := strconv.Atoi(matches[1])
	if err != nil {
		return
	}
	minor, err := strconv.Atoi(matches[2])
	if err != nil {
		return
	}
	sure = true
	if major < 1 {
		return
	}
	if major == 1 && minor < 10 {
		return
	}
	atleast110 = true
	return
}

func (cfg *Service) checkPathConstraintToGo() error {
	atleast110, sure := IsAtLeastGo110(runtime.Version())
	if atleast110 {
		// On Go 1.10+, we can validate name constraints correctly
		return nil
	}

	pemcontents, err := ioutil.ReadFile(cfg.ClientCaCertFile)
	if err != nil {
		return err
	}
	pemblock, _ := pem.Decode(pemcontents)
	if pemblock.Type != "CERTIFICATE" {
		return fmt.Errorf("Pem type %s is not certificate", pemblock.Type)
	}
	cert, err := x509.ParseCertificate(pemblock.Bytes)
	if err != nil {
		return err
	}

	if len(cert.PermittedDNSDomains) > 0 {
		if !sure {
			cfg.log.Info("Node compiled with uncertain Go version and name constraints in client ca cert")
			cfg.log.Warn("The node might refuse valid leaf certificates")
			return nil
		}
		return errors.New("Node compiled with Go 1.9 or earlier, and name constraints in client ca cert")
	}
	return nil
}

// Service is the core of the repoSpanner service
type Service struct {
	ClientCaCertFile  string
	ClientCertificate string
	ClientKey         string
	RPCServerCert     tls.Certificate
	ServerCerts       map[string]tls.Certificate
	ListenRPC         string
	ListenHTTP        string
	StateStorageDir   string
	GitStorageConfig  map[string]string
	Debug             bool

	initialized bool

	serverTLSConfig   *tls.Config
	rpcTLSConfig      *tls.Config
	raftClientTLSInfo transport.TLSInfo

	rpcClient  *http.Client
	rpcServer  *http.Server
	httpServer *http.Server

	nodeid   uint64
	nodename string
	region   string
	cluster  string

	logwrapper *utils.LogWrapper
	log        *logrus.Logger
	statestore *stateStore
	gitstore   storage.StorageDriver
	sync       *syncer

	isrunning bool
}

func (cfg *Service) getCertificate(chi *tls.ClientHelloInfo) (*tls.Certificate, error) {
	hostcert, ok := cfg.ServerCerts[chi.ServerName]
	if ok {
		return &hostcert, nil
	}
	defcert, ok := cfg.ServerCerts["default"]
	if ok {
		return &defcert, nil
	}
	return nil, errors.New("unable to retrieve a certificate")
}

func (cfg *Service) Initialize() error {
	if cfg.initialized {
		return nil
	}

	logger := logrus.New()
	if cfg.Debug {
		logger.SetLevel(logrus.DebugLevel)
	} else {
		logger.SetLevel(logrus.InfoLevel)
	}
	cfg.log = logger

	cfg.logwrapper = utils.CreateLogWrapper(cfg.log)
	capnslog.SetFormatter(cfg.logwrapper)
	raft.SetLogger(cfg.logwrapper)

	if err := cfg.checkPathConstraintToGo(); err != nil {
		return err
	}

	clientcertpool, err := getCertPoolFromFile(cfg.ClientCaCertFile)
	if err != nil {
		return err
	}

	_, ok := cfg.ServerCerts["default"]
	if !ok {
		return errors.New("Default certificate not provided")
	}

	cts, err := ioutil.ReadFile(cfg.ClientCertificate)
	if err != nil {
		return errors.Wrap(err, "Error reading client certificate")
	}
	pemblock, _ := pem.Decode(cts)
	if pemblock.Type != "CERTIFICATE" {
		return errors.New("Client Certificate is not a cert")
	}
	clientcert, err := x509.ParseCertificate(pemblock.Bytes)
	if err != nil {
		return errors.Wrap(err, "Error parsing client certificate")
	}

	// Extract values from client cert
	for _, ext := range clientcert.Extensions {
		if ext.Id.Equal(constants.OIDClusterName) {
			if cfg.cluster != "" {
				return errors.New("Multiple regions in cert?")
			}
			cfg.cluster = string(ext.Value)
			continue
		}
		if ext.Id.Equal(constants.OIDRegionName) {
			if cfg.region != "" {
				return errors.New("Multiple regions in cert?")
			}
			cfg.region = string(ext.Value)
			continue
		}
		if ext.Id.Equal(constants.OIDNodeID) {
			if cfg.nodeid != 0 {
				return errors.New("Multiple node IDs in cert?")
			}
			nodeidb, ok := new(big.Int).SetString(string(ext.Value), 10)
			if !ok {
				return errors.New("Unable to parse nodeid from cert")
			}
			cfg.nodeid = nodeidb.Uint64()
			continue
		}
		if ext.Id.Equal(constants.OIDNodeName) {
			if cfg.nodename != "" {
				return errors.New("Multiple nodenames in cert?")
			}
			cfg.nodename = string(ext.Value)
			continue
		}
		if ext.Id.Equal(constants.OIDPermission) {
			if string(ext.Value) != string(constants.CertPermissionNode) {
				return errors.New("Non-node cert provided?")
			}
		}
	}

	if cfg.nodename == "" || cfg.nodeid == 0 || cfg.region == "" || cfg.cluster == "" {
		return errors.New("Certificate missing required values")
	}

	cfg.serverTLSConfig = &tls.Config{
		GetCertificate:           cfg.getCertificate,
		NextProtos:               []string{"h2", "http/1.1"},
		ClientAuth:               tls.VerifyClientCertIfGiven,
		ClientCAs:                clientcertpool,
		PreferServerCipherSuites: true,
		MinVersion:               tls.VersionTLS12,
	}
	cfg.rpcTLSConfig = &tls.Config{
		Certificates:             []tls.Certificate{cfg.RPCServerCert},
		NextProtos:               []string{"h2"},
		ClientAuth:               tls.RequireAndVerifyClientCert,
		VerifyPeerCertificate:    cfg.verifyNodeCert,
		ClientCAs:                clientcertpool,
		RootCAs:                  clientcertpool,
		PreferServerCipherSuites: true,
		MinVersion:               tls.VersionTLS12,
	}
	cfg.raftClientTLSInfo = transport.TLSInfo{
		CertFile:      cfg.ClientCertificate,
		KeyFile:       cfg.ClientKey,
		TrustedCAFile: cfg.ClientCaCertFile,
	}
	rpcTransport := &http.Transport{
		TLSClientConfig: cfg.rpcTLSConfig,
	}
	if err := http2.ConfigureTransport(rpcTransport); err != nil {
		return errors.Wrap(err, "Error configuring rpc transport for h2")
	}
	cfg.rpcClient = &http.Client{Transport: rpcTransport}

	syncer, err := cfg.createSyncer()
	if err != nil {
		return err
	}
	cfg.sync = syncer

	cfg.log.Debug("Initialization finished")

	cfg.initialized = true
	return nil
}

func (cfg *Service) Shutdown() {
	if !cfg.isrunning {
		cfg.log.Error("Shutting down before start finished")
		return
	}

	cfg.sync.Stop()
	cfg.httpServer.Shutdown(context.Background())
	var closer struct{}
	cfg.statestore.raftnode.stopc <- closer
	cfg.rpcServer.Shutdown(context.Background())
}

func (cfg *Service) findRPCURL() string {
	split := strings.Split(cfg.ListenRPC, ":")
	return fmt.Sprintf("https://%s.%s.%s:%s",
		cfg.nodename,
		cfg.region,
		cfg.cluster,
		split[1],
	)
}

func (cfg *Service) verifyNodeCert(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) error {
	if len(verifiedChains) != 1 {
		return errors.New("Unexpected number of verified chains returned")
	}
	peer := verifiedChains[0][0]
	var correctregion bool
	var correctcluster bool
	var isnode bool

	for _, ext := range peer.Extensions {
		if ext.Id.Equal(constants.OIDRegionName) {
			if string(ext.Value) == cfg.region {
				correctregion = true
				continue
			}
			return errors.Errorf("Incorrect region %s found", string(ext.Value))
		}
		if ext.Id.Equal(constants.OIDClusterName) {
			if string(ext.Value) == cfg.cluster {
				correctcluster = true
				continue
			}
			return errors.Errorf("Incorrect cluster name %s found", string(ext.Value))
		}
	}
	for _, usage := range peer.ExtKeyUsage {
		if usage == x509.ExtKeyUsageServerAuth {
			isnode = true
		}
	}

	if !correctregion {
		return errors.Errorf("No region extension found")
	}
	if !correctcluster {
		return errors.Errorf("No cluster extension found")
	}
	if !isnode {
		return errors.Errorf("Non-node cert used")
	}
	return nil
}

func (cfg *Service) openGitStorage() error {
	clustered, ok := cfg.GitStorageConfig["clustered"]
	if ok {
		delete(cfg.GitStorageConfig, "clustered")
	} else {
		// Default to clustered wrapper enabled
		clustered = "true"
	}
	gitstore, err := storage.InitializeStorageDriver(cfg.GitStorageConfig)
	if err != nil {
		return err
	}
	if clustered != "false" {
		cfg.log.Debug("Adding clustered storage driver layer")
		cfg.gitstore = &clusterStorageDriverInstance{
			inner: gitstore,
			cfg:   cfg,
		}
	} else {
		cfg.log.Debug("Not clustering storage driver layer")
		cfg.gitstore = gitstore
	}
	return nil
}

func (cfg *Service) GetPeerURL(peer uint64, relativeurl string) string {
	peerbase := cfg.statestore.Peers[peer]
	return peerbase + relativeurl
}

func (cfg *Service) openStorage(spawning bool, joinnode string) error {
	err := cfg.openGitStorage()
	if err != nil {
		return err
	}

	statestore, err := cfg.loadStateStore(spawning, joinnode, cfg.StateStorageDir)
	if err != nil {
		return err
	}
	cfg.statestore = statestore

	return nil
}

func (cfg *Service) runPreFlightChecks() error {
	// Verify the statestore is for us
	if cfg.cluster != cfg.statestore.ClusterName {
		return errors.New("The state store cluster is " + cfg.statestore.ClusterName + " not " + cfg.cluster)
	}
	if cfg.region != cfg.statestore.RegionName {
		return errors.New("The state store region is " + cfg.statestore.RegionName + " not " + cfg.region)
	}
	if cfg.nodename != cfg.statestore.NodeName {
		return errors.New("The state store nodename is " + cfg.statestore.NodeName + " not " + cfg.nodename)
	}
	if cfg.nodeid != cfg.statestore.NodeID {
		return errors.New("NodeID invalid. New cert without --nodeid?")
	}
	return nil
}

func (cfg *Service) RunServer(spawning bool, joinnode string) error {
	// Tiny wrapper that will use cfg.log.Fatal if log was setup already
	if err := cfg.runServer(spawning, joinnode); err != nil {
		if cfg.log == nil {
			return err
		}
		cfg.log.WithError(err).Fatal("Fatal error occured")
		return nil
	}
	return nil
}

const numServices = 4

func (cfg *Service) runServer(spawning bool, joinnode string) error {
	if err := cfg.Initialize(); err != nil {
		return err
	}

	if err := cfg.openStorage(spawning, joinnode); err != nil {
		return err
	}

	if err := cfg.runPreFlightChecks(); err != nil {
		return err
	}

	cfg.log.Info("Starting serving")

	// We create a stopped timer that we can start when we start getting closes
	shutdownTimer := time.NewTimer(5 * time.Second)
	if !shutdownTimer.Stop() {
		<-shutdownTimer.C
	}

	// Start serving
	raftstarted := make(chan struct{})

	errchan := make(chan error)
	go cfg.sync.Run(errchan)
	go cfg.statestore.RunStateStore(errchan, raftstarted)
	go cfg.runHTTP(errchan)

	<-raftstarted
	go cfg.runRPC(errchan)

	cfg.isrunning = true

	var gotcloses int
	for {
		select {
		case err := <-errchan:
			if err == nil {
				gotcloses++
				cfg.log.Debug("Service shut down")
				if gotcloses == numServices {
					// We got "nil" from everything, which means we're done!
					cfg.log.Info("Shutdown complete")
					return nil
				} else if gotcloses == 1 {
					// First service started shutting down. Start the shutdown timer
					shutdownTimer.Reset(20 * time.Second)
				}
			} else {
				// Any of the services returned an actual error. Crash and burn.
				return err
			}

		case <-shutdownTimer.C:
			return errors.New("Some services took too long to stop")
		}
	}
}

func (cfg *Service) runHTTP(errchan chan<- error) {
	lis, err := tls.Listen("tcp", cfg.ListenHTTP, cfg.serverTLSConfig)
	if err != nil {
		errchan <- err
		return
	}
	cfg.httpServer = &http.Server{Handler: cfg}
	err = cfg.httpServer.Serve(lis)
	cfg.log.WithError(err).Info("Git HTTP server shut down")
	if err != http.ErrServerClosed {
		errchan <- errors.Wrap(err, "Git HTTP error")
	} else {
		errchan <- nil
	}
	return
}
