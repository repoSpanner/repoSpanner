package bridge

import (
	"bytes"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"syscall"
)

type config struct {
	GitBinary string
	Ca        string
	BaseURL   string
	Certs     map[string]map[string]string
	Extras    map[string]string
}

var (
	username      string
	configuration config
	usesideband   bool
)

func sendPacket(w io.Writer, packet []byte) error {
	len, err := getPacketLen(packet)
	if err != nil {
		return err
	}
	if _, err := w.Write(len); err != nil {
		return err
	}
	if usesideband {
		if _, err := w.Write([]byte{byte(0x03)}); err != nil {
			return err
		}
	}
	if _, err := w.Write(packet); err != nil {
		return err
	}
	return nil
}

func sendFlushPacket(w io.Writer) error {
	_, err := w.Write([]byte{'0', '0', '0', '0'})
	return err
}

func getPacketLen(packet []byte) ([]byte, error) {
	pktlen := len(packet) + 4 // Funny detail: the 4 bytes with length are included in length
	if pktlen == 4 {
		// "Empty" packets are not allowed
		return nil, errors.New("Unable to send empty packet")
	}
	if pktlen > 65520 {
		return nil, errors.New("Packet too big")
	}
	if usesideband {
		pktlen++
	}
	len := fmt.Sprintf("%04x", pktlen)
	return []byte(len), nil
}

func exitWithError(errmsg string, extra ...string) {
	// When we get here, we have most likely not yet arrived at sending any requests,
	// or are still at the discovery stage.
	// Send a plain (non-sidebanded) git packet.
	// The only moment that this would be wrong
	if len(extra) > 0 {
		extras := make([]string, 0)
		for _, ex := range extra {
			extras = append(extras, ex)
		}
		errmsg = errmsg + ": " + strings.Join(extras, ",")
	}
	sendPacket(os.Stdout, []byte("ERR "+errmsg+"\n"))
	sendFlushPacket(os.Stdout)
	os.Exit(1)
}

func checkError(err error, errmsg string, extra ...string) {
	if err == nil {
		return
	}
	errmsg = errmsg + ": " + err.Error()
	exitWithError(errmsg)
}

func callGit(command, repo string) {
	gitbinary := configuration.GitBinary
	err := syscall.Exec(
		gitbinary,
		append(
			[]string{path.Base(gitbinary)},
			command,
			repo,
		),
		os.Environ(),
	)
	checkError(err, "Calling git failed")
	exitWithError("Git syscall returned?!")
	os.Exit(1)
}

func isRawGitRepo(path string) (rawgit bool, rsname string, err error) {
	if configuration.GitBinary != "" {
		_, err = os.Stat(path)
		if !os.IsNotExist(err) {
			// Either it existed, or we weren't able to check. Assume it's git either way
			rawgit = true
			err = nil
			return
		}
	}

	err = nil
	// It did not exist, assume it's a repospanner name.
	// In repospanner, we want to remove the trailing .git
	if strings.HasSuffix(path, ".git") {
		rsname = path[:len(path)-4]
	} else {
		rsname = path
	}
	return
}

func loadURLAndCAFromEnv(allenv bool) {
	if allenv || configuration.BaseURL == ":FROMENV:" {
		configuration.BaseURL = os.Getenv("REPOBRIDGE_BASEURL")
	}
	if allenv || configuration.Ca == ":FROMENV:" {
		configuration.Ca = os.Getenv("REPOBRIDGE_CA")
	}
}

func loadConfigFromEnv() {
	loadURLAndCAFromEnv(true)
	configuration.Certs = make(map[string]map[string]string)
	configuration.Certs["_default_"] = make(map[string]string)
	configuration.Certs["_default_"]["cert"] = os.Getenv("REPOBRIDGE_CERT")
	configuration.Certs["_default_"]["key"] = os.Getenv("REPOBRIDGE_KEY")
}

func loadConfig() {
	cfgFile := os.Getenv("REPOBRIDGE_CONFIG")
	if cfgFile == ":environment:" {
		loadConfigFromEnv()
	} else {
		if cfgFile == "" {
			cfgFile = "/etc/repospanner/bridge_config.json"
		}
		cts, err := ioutil.ReadFile(cfgFile)
		checkError(err, "Error reading configuration")
		err = json.Unmarshal(cts, &configuration)
		checkError(err, "Error parsing configuration")

		loadURLAndCAFromEnv(false)
	}

	if configuration.Extras == nil {
		// If nothing was configured, start with an empty map
		configuration.Extras = make(map[string]string)
	}
}

func parseArgs() (command, repo string) {
	inextra := 0
	var extrakey string

	for i, arg := range os.Args {
		if i == 0 {
			// This is the program name, we don't need that
			continue
		}
		if inextra == 1 {
			// We had the --extra, this is the key
			extrakey = arg
			inextra = 2
			_, ok := configuration.Extras[extrakey]
			if ok {
				exitWithError("Extra value " + extrakey + " overriding attempted")
			}
		} else if inextra == 2 {
			// We had the --extra and key, this is the value
			configuration.Extras[extrakey] = arg
			extrakey = ""
			inextra = 0
		} else {
			if arg == "--extra" {
				inextra = 1
			} else if command == "" {
				command = arg
			} else if repo == "" {
				if arg[0] == '\'' && arg[len(arg)-1] == '\'' {
					arg = arg[1 : len(arg)-1]
				}
				repo = arg
			} else {
				exitWithError("Too many arguments")
			}
		}
	}
	return
}

func ExecuteBridge() {
	username = os.Getenv("USER")
	if username == "" {
		exitWithError("Unable to determine username")
		os.Exit(1)
	}
	loadConfig()

	// Just call this to make sure we abort loudly early on if the user has no access
	cert, _ := getCertAndKey()
	checkForNonLeafCert(cert)

	if configuration.BaseURL == "" {
		exitWithError("Invalid configuration file")
		os.Exit(1)
	}

	command, repo := parseArgs()

	if repo == "" && command != "" {
		// This is used in a call by git remote-ext, probably in a test situation
		repo = command
		command = os.Getenv("GIT_EXT_SERVICE_NOPREFIX")
	}

	if command == "" || repo == "" {
		exitWithError("Invalid call arguments", "len", strings.Join(os.Args, " "))
	}

	if !strings.HasPrefix(command, "git-") {
		command = "git-" + command
	}
	if command != "git-receive-pack" && command != "git-upload-pack" {
		exitWithError("Invalid call")
	}

	// First, let's see if we need to do anything here.
	rawgit, reponame, err := isRawGitRepo(repo)
	checkError(err, "Error getting repo info")

	if rawgit {
		// This is a plain git repo
		callGit(command, repo)
	} else {
		// This might be a repo for us! Let's get to it.
		performRefDiscovery(command, reponame)

		isdone, r := shouldClose(os.Stdin)
		if isdone {
			os.Exit(0)
		}
		// From here on out, we need to send a sideband
		usesideband = true

		performService(r, command, reponame)
	}
}

type splitReadCloser struct {
	r io.Reader
	c io.Closer
}

func (s *splitReadCloser) Read(p []byte) (int, error) { return s.r.Read(p) }
func (s *splitReadCloser) Close() error               { return s.c.Close() }

func shouldClose(r io.ReadCloser) (bool, io.ReadCloser) {
	buf := make([]byte, 4)
	n, err := r.Read(buf)
	checkError(err, "Error determining whether to close channel")
	if n != 4 {
		exitWithError("Not enough bytes read to determine close status")
	}
	buffer := bytes.NewBuffer(buf)
	combined := &splitReadCloser{
		r: io.MultiReader(buffer, r),
		c: r,
	}
	if buf[0] == '0' && buf[1] == '0' && buf[2] == '0' && buf[3] == '0' {
		// This is a flush packet, we are done
		return true, combined
	}
	return false, combined
}

func getCertAndKey() (string, string) {
	user, hasuser := configuration.Certs[username]
	if hasuser {
		return user["cert"], user["key"]
	}

	def, hasdef := configuration.Certs["_default_"]
	if hasdef {
		return def["cert"], def["key"]
	}

	// Seems there was no configuration for this user, nor default... Abandon all hope
	exitWithError("User does not have access to this bridge")
	return "", ""
}

func checkForNonLeafCert(certpath string) {
	cts, err := ioutil.ReadFile(certpath)
	checkError(err, "Error opening client certificate")
	certblock, rest := pem.Decode(cts)
	if len(rest) != 0 {
		exitWithError("Client certificate has unexpected contents")
	}
	cert, err := x509.ParseCertificate(certblock.Bytes)
	checkError(err, "Error parsing client certificate")
	if cert.IsCA {
		exitWithError("Client certificate is a CA certificate?")
	}
	for _, ku := range cert.ExtKeyUsage {
		if ku != x509.ExtKeyUsageClientAuth {
			exitWithError("Client certificate is not a leaf certificate")
		}
	}
	// If everything is OK, we just return and the bridge code will take over
}
