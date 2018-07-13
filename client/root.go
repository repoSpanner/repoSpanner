package client

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"syscall"

	"github.com/spf13/viper"
)

var (
	username string
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
	gitbinary := viper.GetString("gitbinary")
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

func isRawGitRepo(path string) (rawgit bool, gsname string, err error) {
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
		gsname = path[:len(path)-4]
	} else {
		gsname = path
	}
	return
}

func checkConfigured(options ...string) {
	missing := false
	for _, opt := range options {
		if !viper.IsSet(opt) {
			missing = true
			fmt.Fprintln(os.Stderr, "Required option not configured:", opt)
		}
	}
	if missing {
		exitWithError("Invalid configuration file")
	}
}

func ExecuteClient() {
	username = os.Getenv("USER")
	cfgFile := os.Getenv("REPOCLIENT_CONFIG")
	if username == "" {
		exitWithError("Unable to determine username")
		os.Exit(1)
	}
	viper.SetConfigName("client_config")
	if cfgFile != "" {
		viper.SetConfigFile(cfgFile)
	} else {
		viper.AddConfigPath("/etc/repospanner")
	}
	if err := viper.ReadInConfig(); err != nil {
		exitWithError("Unable to read config file")
		os.Exit(1)
	}

	// Just call this to make sure we abort loudly early on if the user has no access
	getCertAndKey()

	checkConfigured(
		"baseurl",
		"gitbinary",
	)

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
	usercertopt := fmt.Sprintf("certs.%s.cert", username)
	userkeyopt := fmt.Sprintf("certs.%s.key", username)

	if viper.IsSet(usercertopt) && viper.IsSet(userkeyopt) {
		return viper.GetString(usercertopt), viper.GetString(userkeyopt)
	}

	defcertopt := "certs._default_.cert"
	defkeyopt := "certs._default_.key"

	if viper.IsSet(defcertopt) && viper.IsSet(defkeyopt) {
		return viper.GetString(defcertopt), viper.GetString(defkeyopt)
	}

	// Seems there was no configuration for this user, nor default... Abandon all attempts
	exitWithError("User does not have access to this client")
	return "", ""
}
