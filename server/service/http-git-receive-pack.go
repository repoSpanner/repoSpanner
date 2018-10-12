package service

import (
	"bufio"
	"crypto/sha1"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"strings"

	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

func (cfg *Service) serveGitReceivePack(w http.ResponseWriter, r *http.Request, reqlogger *logrus.Entry, reponame string) {
	bodyreader := bufio.NewReader(r.Body)
	rw := newWrappedResponseWriter(w)

	reqlogger.Debug("git-receive-pack requested")
	projectstore := cfg.gitstore.GetProjectStorage(reponame)

	capabs, toupdate, err := cfg.readDownloadPacketRequestHeader(bodyreader, reqlogger, reponame)
	if err != nil {
		reqlogger.WithError(err).Info("Invalid request received")
		sendPacket(rw, []byte("ERR Invalid request"))
		return
	}
	reqlogger = reqlogger.WithFields(logrus.Fields{
		"capabs":   capabs,
		"toupdate": fmt.Sprintf("%s", toupdate),
	})
	hasStatus := hasCapab(capabs, "report-status")
	sbstatus, err := getSideBandStatus(capabs)
	if err != nil {
		reqlogger.WithError(err).Info("Invalid request received")
		sendPacket(rw, []byte("ERR Invalid request"))
		return
	}
	reqlogger = reqlogger.WithField(
		"sbstatus", sbstatus,
	)
	infosender := sideBandSender{
		w:        rw,
		sbstatus: sbstatus,
		sb:       sideBandProgress,
	}
	errsender := sideBandSender{
		w:        rw,
		sbstatus: sbstatus,
		sb:       sideBandProgress,
	}

	cfg.maybeSayHello(rw, sbstatus)

	// Get extra information to pass into any hooks we might run
	hookExtras := make(map[string]string)
	for key, value := range r.Header {
		if strings.HasPrefix(key, "X-Extra-") {
			key = key[8:]
			hookExtras[strings.ToLower(key)] = value[0]
		}
	}

	// Perform a pre-check to determine whether there's any chance of success after we parse all the objects
	cfg.debugPacket(rw, sbstatus, "Performing pre-check...")
	precheckresult := cfg.statestore.getPushResult(toupdate)
	cfg.debugPacket(rw, sbstatus, "Pre-check results in")

	if !precheckresult.success {
		sendPushResult(rw, hasStatus, sbstatus, precheckresult)
		return
	}

	if rw.IsClosed() {
		reqlogger.Debug("Connection closed")
		return
	}

	pusher := projectstore.GetPusher(toupdate.UUID())
	pushresultc := pusher.GetPushResultChannel()

	if toupdate.ExpectPackFile() {
		packhasher := sha1.New()
		packreader := &hashWriter{r: bodyreader, w: packhasher}
		version, numobjects, err := getPackHeader(packreader)
		if err != nil {
			reqlogger.WithError(err).Info("Unable to get packfile header")
			sendSideBandPacket(rw, sbstatus, sideBandProgress, []byte("ERR Invalid packfile\n"))
			return
		}
		reqlogger = reqlogger.WithField(
			"pack-numobjects", numobjects,
		)
		if version != 2 {
			reqlogger.Info("Invalid pack version received")
			sendSideBandPacket(rw, sbstatus, sideBandProgress, []byte("ERR Invalid packfile version"))
			sendUnpackFail(rw, hasStatus, sbstatus, toupdate)
			return
		}
		reqlogger.Debug("Got receive-pack request header")

		deltasqueue, err := ioutil.TempFile("", "repospanner_deltaqueue_")
		if err != nil {
			reqlogger.WithError(err).Info("Unable to create deltaqueue")
			sendSideBandPacket(w, sbstatus, sideBandProgress, []byte("ERR Internal error\n"))
			sendUnpackFail(w, hasStatus, sbstatus, toupdate)
			return
		}
		defer deltasqueue.Close()
		defer os.Remove(deltasqueue.Name())
		var deltaqueuesize int

		var gotObjects uint32
		for gotObjects < numobjects {
			select {
			case syncerr := <-pushresultc:
				reqlogger.WithError(syncerr).Info("Error syncing object out to enough nodes")
				sendSideBandPacket(w, sbstatus, sideBandProgress, []byte("ERR Object sync failed\n"))
				sendUnpackFail(w, hasStatus, sbstatus, toupdate)
				return

			default:
				if rw.IsClosed() {
					reqlogger.Debug("Connection closed")
					return
				}
				_, _, resolve, err := getSingleObjectFromPack(packreader, pusher)
				if err != nil {
					reqlogger.WithError(err).Info("Error getting object")
					sendSideBandPacket(w, sbstatus, sideBandProgress, []byte("ERR Invalid packfile\n"))
					sendUnpackFail(w, hasStatus, sbstatus, toupdate)
					return
				}
				if resolve.baseobj != "" {
					fmt.Fprintf(deltasqueue, "%s %s\n", resolve.deltaobj, resolve.baseobj)
					deltaqueuesize++
				}
				gotObjects++
			}
		}
		expectedSum := make([]byte, packhasher.Size())
		if _, err := io.ReadFull(bodyreader, expectedSum); err != nil {
			reqlogger.WithError(err).Info("Error reading expected checksum")
			sendSideBandPacket(rw, sbstatus, sideBandProgress, []byte("ERR Packfile checksum failed\n"))
			sendUnpackFail(rw, hasStatus, sbstatus, toupdate)
			return
		}

		if !checksumsMatch(packhasher.Sum(nil), expectedSum, reqlogger) {
			// Checksum failed, checksumsMatch already logs
			sendSideBandPacket(rw, sbstatus, sideBandProgress, []byte("ERR Packfile checksum failed\n"))
			sendUnpackFail(rw, hasStatus, sbstatus, toupdate)
			return
		}

		rw.isfullyread = true

		// Resolve deltas
		err = processDeltas(reqlogger, deltaqueuesize, deltasqueue, pushresultc, rw, sbstatus, projectstore, pusher)
		if err != nil {
			reqlogger.WithError(err).Info("Error processing deltas")
			sendSideBandPacket(w, sbstatus, sideBandProgress, []byte("ERR Delta processing failed\n"))
			sendUnpackFail(w, hasStatus, sbstatus, toupdate)
			return
		}

		cfg.debugPacket(rw, sbstatus, "Delta resolving finished")
		reqlogger.Debug("Pack file accepted, checksum matches")
	}
	if rw.IsClosed() {
		reqlogger.Debug("Connection closed")
		return
	}

	rw.isfullyread = true

	// TODO: Determine when to be paranoid and check objects all the way down
	paranoid := viper.GetBool("checks.paranoid")
	cfg.debugPacket(rw, sbstatus, "Validating objects...")
	reqlogger.Debug("Validating all objects are reachable and sufficient")
	if err := validateObjects(projectstore, toupdate, paranoid); err != nil {
		reqlogger.WithError(err).Info("Object validation failure")
		sendSideBandPacket(rw, sbstatus, sideBandProgress, []byte("ERR Object validation failed\n"))
		sendUnpackFail(rw, hasStatus, sbstatus, toupdate)
		return
	}
	cfg.debugPacket(rw, sbstatus, "Objects validated")
	reqlogger.Debug("Objects in request are sufficient")

	if rw.IsClosed() {
		reqlogger.Debug("Connection closed")
		return
	}

	// Inform the pusher we have sent all the objects we're going to send it
	pusher.Done()

	cfg.statestore.AddFakeRefs(reponame, toupdate)
	defer cfg.statestore.RemoveFakeRefs(reponame, toupdate)

	cfg.debugPacket(rw, sbstatus, "Running pre-receive hook...")
	err = cfg.runHook(
		hookTypePreReceive,
		errsender,
		infosender,
		reponame,
		toupdate,
		hookExtras,
	)
	if err != nil {
		reqlogger.WithError(err).Debug("Pre-receive hook refused push")
		sendSideBandPacket(w, sbstatus, sideBandProgress, []byte("ERR Pre-receive hook refused push\n"))
		sendUnpackFail(w, hasStatus, sbstatus, toupdate)
		return
	}
	cfg.debugPacket(rw, sbstatus, "Pre-receive hook done")

	cfg.debugPacket(rw, sbstatus, "Running update hook...")
	err = cfg.runHook(
		hookTypeUpdate,
		errsender,
		infosender,
		reponame,
		toupdate,
		hookExtras,
	)
	if err != nil {
		reqlogger.WithError(err).Debug("Update hook refused push")
		sendSideBandPacket(w, sbstatus, sideBandProgress, []byte("ERR Update hook refused push\n"))
		sendUnpackFail(w, hasStatus, sbstatus, toupdate)
		return
	}
	cfg.debugPacket(rw, sbstatus, "Update hook done")

	// We wait for either the first error, or the waitgroup to be fully done, which will
	// close the channel, returning a <nil> value.
	cfg.debugPacket(rw, sbstatus, "Syncing objects...")
	syncerr, isopen := <-pushresultc
	if syncerr == nil && isopen == true {
		// Someone sent <nil> over the channel. That is a coding error.
		sendSideBandPacket(w, sbstatus, sideBandProgress, []byte("ERR Object sync failed\n"))
		sendUnpackFail(w, hasStatus, sbstatus, toupdate)
		// This is a definite coding error. Let's panic to be really, *really* obnoxious in logs.
		// The http.Server should capture it, and prevent the server from crashing alltogether.
		panic("syncerr channel got nil without close, coding error")
	}
	if syncerr != nil {
		reqlogger.WithError(syncerr).Info("Error syncing object out to enough nodes")
		sendSideBandPacket(w, sbstatus, sideBandProgress, []byte("ERR Object sync failed\n"))
		sendUnpackFail(w, hasStatus, sbstatus, toupdate)
		return
	}
	cfg.debugPacket(rw, sbstatus, "Objects synced")

	cfg.debugPacket(rw, sbstatus, "Requesting push...")
	pushresult := cfg.statestore.performPush(toupdate)
	cfg.debugPacket(rw, sbstatus, "Push results in")

	reqlogger.WithFields(logrus.Fields{
		"success":     pushresult.success,
		"refresults":  pushresult.branchresults,
		"clienterror": pushresult.clienterror,
		"error":       pushresult.logerror,
	}).Debug("Push results computed")
	if !pushresult.success {
		reqlogger.WithError(pushresult.logerror).Info("Push failed")
	}

	cfg.debugPacket(rw, sbstatus, "Running post-receive hook...")
	err = cfg.runHook(
		hookTypePostReceive,
		errsender,
		infosender,
		reponame,
		toupdate,
		hookExtras,
	)
	if err != nil {
		reqlogger.WithError(err).Debug("Post-receive hook failed")
	}
	cfg.debugPacket(rw, sbstatus, "Post-receive hook done")

	sendPushResult(rw, hasStatus, sbstatus, pushresult)
	reqlogger.Debug("Push result sent, we are all done")
	// And... we are done! That was a ride
	return
}
