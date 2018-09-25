package bridge

import (
	"bytes"
	"encoding/json"
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
}

var (
	username      string
	configuration config
)

func sendPacket(w io.Writer, packet []byte) error {
	len, err := getPacketLen(packet)
	if err != nil {
		return err
	}
	if _, err := w.Write([]byte(len)); err != nil {
		return err
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
	len := fmt.Sprintf("%04x", pktlen)
	return []byte(len), nil
}

func exitWithError(errmsg string, extra ...interface{}) {
	// When we get here, we have most likely not yet arrived at sending any requests,
	// or are still at the discovery stage.
	// Send a plain (non-sidebanded) git packet.
	// The only moment that this would be wrong
	sendPacket(os.Stdout, []byte("ERR "+errmsg+"\n"))
	sendFlushPacket(os.Stdout)
	os.Exit(1)
}

func checkError(err error, errmsg string, extra ...interface{}) {
	if err == nil {
		return
	}
	extra = append(extra, "error", err)
	exitWithError(errmsg, extra...)
}

func callGit(command, repo string) {
	gitbinary := configuration.GitBinary
	if gitbinary == "" {
		exitWithError("No fallback to git enabled")
	}
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
	_, err = os.Stat(path)
	if !os.IsNotExist(err) {
		// Either it existed, or we weren't able to check. Assume it's git either way
		rawgit = true
		err = nil
		return
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

func loadConfig() {
	cfgFile := os.Getenv("REPOBRIDGE_CONFIG")
	if cfgFile == "" {
		cfgFile = "/etc/repospanner/bridge_config.json"
	}
	cts, err := ioutil.ReadFile(cfgFile)
	checkError(err, "Error reading configuration")
	err = json.Unmarshal(cts, &configuration)
	checkError(err, "Error parsing configuration")
}

func ExecuteBridge() {
	username = os.Getenv("USER")
	if username == "" {
		exitWithError("Unable to determine username")
		os.Exit(1)
	}
	loadConfig()

	// Just call this to make sure we abort loudly early on if the user has no access
	getCertAndKey()

	if configuration.BaseURL == "" {
		exitWithError("Invalid configuration file")
		os.Exit(1)
	}

	var command string
	var repo string

	if len(os.Args) == 3 {
		command = os.Args[1]
		repo = os.Args[2]
	} else if len(os.Args) == 2 {
		// This is used in a call by git remote-ext, probably in a test situation
		command = os.Getenv("GIT_EXT_SERVICE_NOPREFIX")
		repo = os.Args[1]
	}

	if command == "" || repo == "" {
		exitWithError("Invalid call arguments", "len", len(os.Args))
	}

	if command != "receive-pack" && command != "upload-pack" {
		exitWithError("Invalid call")
	}
	command = "git-" + command

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
