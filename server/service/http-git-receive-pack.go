package service

import (
	"bufio"
	"context"
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

func (cfg *Service) serveGitReceivePack(ctx context.Context, w http.ResponseWriter, r *http.Request, reponame string) {
	reqlogger := loggerFromCtx(ctx)

	bodyreader := bufio.NewReader(r.Body)
	rw := newWrappedResponseWriter(w)

	reqlogger.Debug("git-receive-pack requested")
	projectstore := cfg.gitstore.GetProjectStorage(reponame)

	capabs, toupdate, err := cfg.readDownloadPacketRequestHeader(ctx, bodyreader, reponame)
	if err != nil {
		reqlogger.WithError(err).Info("Invalid request received")
		sendPacket(rw, []byte("ERR Invalid request"))
		return
	}
	ctx, reqlogger = expandCtxLogger(ctx, logrus.Fields{
		"capabs":   capabs,
		"toupdate": fmt.Sprintf("%s", toupdate),
	})
	ctx, reqlogger, err = addCapabsToCtx(ctx, capabs)
	if err != nil {
		reqlogger.WithError(err).Info("Invalid request received")
		sendPacket(rw, []byte("ERR Invalid request"))
		return
	}
	infosender := sideBandSender{
		ctx: ctx,
		w:   rw,
		sb:  sideBandProgress,
	}
	errsender := sideBandSender{
		ctx: ctx,
		w:   rw,
		sb:  sideBandProgress,
	}

	cfg.maybeSayHello(ctx, rw)

	// Get extra information to pass into any hooks we might run
	hookExtras := make(map[string]string)
	for key, value := range r.Header {
		if strings.HasPrefix(key, "X-Extra-") {
			key = key[8:]
			hookExtras[strings.ToLower(key)] = value[0]
		}
	}

	// Perform a pre-check to determine whether there's any chance of success after we parse all the objects
	cfg.debugPacket(ctx, rw, "Performing pre-check...")
	precheckresult := cfg.statestore.getPushResult(toupdate)
	cfg.debugPacket(ctx, rw, "Pre-check results in")

	if !precheckresult.success {
		sendPushResult(ctx, rw, precheckresult)
		return
	}

	if ctx.Err() != nil {
		reqlogger.Debug("Connection closed")
		return
	}

	pusher := projectstore.GetPusher(toupdate.UUID())
	ctx, cancel := context.WithCancel(ctx)
	pushresultc := pusher.GetPushResultChannel()
	go func() {
		select {
		case <-ctx.Done():
			return
		case syncerr := <-pushresultc:
			if syncerr != nil {
				reqlogger.WithError(syncerr).Info("Error syncing object out to enough nodes")
				sendSideBandPacket(ctx, w, sideBandProgress, []byte("ERR Object sync failed\n"))
				sendUnpackFail(ctx, w, toupdate, "sync-fail")
				cancel()
			}
		}
	}()

	if toupdate.ExpectPackFile() {
		packhasher := sha1.New()
		packreader := &hashWriter{r: bodyreader, w: packhasher}
		version, numobjects, err := getPackHeader(packreader)
		if err != nil {
			reqlogger.WithError(err).Info("Unable to get packfile header")
			sendSideBandPacket(ctx, rw, sideBandProgress, []byte("ERR Invalid packfile\n"))
			sendUnpackFail(ctx, w, toupdate, "pack-fail")
			return
		}
		reqlogger = reqlogger.WithField(
			"pack-numobjects", numobjects,
		)
		if version != 2 {
			reqlogger.Info("Invalid pack version received")
			sendSideBandPacket(ctx, rw, sideBandProgress, []byte("ERR Invalid packfile version"))
			sendUnpackFail(ctx, rw, toupdate, "pack-version-fail")
			return
		}
		reqlogger.Debug("Got receive-pack request header")

		deltasqueue, err := ioutil.TempFile("", "repospanner_deltaqueue_")
		if err != nil {
			reqlogger.WithError(err).Info("Unable to create deltaqueue")
			sendSideBandPacket(ctx, w, sideBandProgress, []byte("ERR Internal error\n"))
			sendUnpackFail(ctx, w, toupdate, "internal-error")
			return
		}
		defer deltasqueue.Close()
		os.Remove(deltasqueue.Name())
		var deltaqueuesize int

		var gotObjects uint32
		for gotObjects < numobjects {
			select {
			case syncerr := <-pushresultc:
				reqlogger.WithError(syncerr).Info("Error syncing object out to enough nodes")
				sendSideBandPacket(ctx, w, sideBandProgress, []byte("ERR Object sync failed\n"))
				sendUnpackFail(ctx, w, toupdate, "sync-fail")
				return

			case <-ctx.Done():
				reqlogger.Debug("Connection closed")
				return

			default:
				_, _, resolve, err := getSingleObjectFromPack(packreader, pusher)
				if err != nil {
					reqlogger.WithError(err).Info("Error getting object")
					sendSideBandPacket(ctx, w, sideBandProgress, []byte("ERR Invalid packfile\n"))
					sendUnpackFail(ctx, w, toupdate, "parse-fail")
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
			sendSideBandPacket(ctx, rw, sideBandProgress, []byte("ERR Packfile checksum failed\n"))
			sendUnpackFail(ctx, rw, toupdate, "internal-fail")
			return
		}

		if !checksumsMatch(ctx, packhasher.Sum(nil), expectedSum) {
			// Checksum failed, checksumsMatch already logs
			sendSideBandPacket(ctx, rw, sideBandProgress, []byte("ERR Packfile checksum failed\n"))
			sendUnpackFail(ctx, rw, toupdate, "checksum-fail")
			return
		}

		rw.isfullyread = true

		// Resolve deltas
		err = processDeltas(ctx, deltaqueuesize, deltasqueue, rw, projectstore, pusher)
		if err != nil {
			reqlogger.WithError(err).Info("Error processing deltas")
			sendSideBandPacket(ctx, w, sideBandProgress, []byte("ERR Delta processing failed\n"))
			sendUnpackFail(ctx, w, toupdate, "delta-fail")
			return
		}

		cfg.debugPacket(ctx, rw, "Delta resolving finished")
		reqlogger.Debug("Pack file accepted, checksum matches")
	}
	if ctx.Err() != nil {
		reqlogger.Debug("Connection closed")
		return
	}

	rw.isfullyread = true

	// TODO: Determine when to be paranoid and check objects all the way down
	paranoid := true
	if viper.IsSet("checks.paranoid") {
		paranoid = viper.GetBool("checks.paranoid")
	}
	cfg.debugPacket(ctx, rw, "Validating objects...")
	reqlogger.Debug("Validating all objects are reachable and sufficient")
	if err := validateObjects(projectstore, toupdate, paranoid); err != nil {
		reqlogger.WithError(err).Info("Object validation failure")
		sendSideBandPacket(ctx, rw, sideBandProgress, []byte("ERR Object validation failed\n"))
		sendUnpackFail(ctx, rw, toupdate, "object-validation-fail")
		return
	}
	cfg.debugPacket(ctx, rw, "Objects validated")
	reqlogger.Debug("Objects in request are sufficient")

	if ctx.Err() != nil {
		reqlogger.Debug("Connection closed")
		return
	}

	// Inform the pusher we have sent all the objects we're going to send it
	pusher.Done()

	cfg.statestore.AddFakeRefs(reponame, toupdate)
	defer cfg.statestore.RemoveFakeRefs(reponame, toupdate)

	cfg.debugPacket(ctx, rw, "Running pre-receive hook...")
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
		sendSideBandPacket(ctx, w, sideBandProgress, []byte("ERR Pre-receive hook refused push\n"))
		sendUnpackFail(ctx, w, toupdate, "pre-receive-hook-refuse")
		return
	}
	cfg.debugPacket(ctx, rw, "Pre-receive hook done")

	cfg.debugPacket(ctx, rw, "Running update hook...")
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
		sendSideBandPacket(ctx, w, sideBandProgress, []byte("ERR Update hook refused push\n"))
		sendUnpackFail(ctx, w, toupdate, "update-hook-refuse")
		return
	}
	cfg.debugPacket(ctx, rw, "Update hook done")

	// We wait for either the first error, or the waitgroup to be fully done, which will
	// close the channel, returning a <nil> value.
	cfg.debugPacket(ctx, rw, "Syncing objects...")
	syncerr, isopen := <-pushresultc
	if syncerr == nil && isopen == true {
		// Someone sent <nil> over the channel. That is a coding error.
		sendSideBandPacket(ctx, w, sideBandProgress, []byte("ERR Object sync failed\n"))
		sendUnpackFail(ctx, w, toupdate, "internal-error")
		// This is a definite coding error. Let's panic to be really, *really* obnoxious in logs.
		// The http.Server should capture it, and prevent the server from crashing alltogether.
		panic("syncerr channel got nil without close, coding error")
	}
	if syncerr != nil {
		reqlogger.WithError(syncerr).Info("Error syncing object out to enough nodes")
		sendSideBandPacket(ctx, w, sideBandProgress, []byte("ERR Object sync failed\n"))
		sendUnpackFail(ctx, w, toupdate, "sync-fail")
		return
	}
	cfg.debugPacket(ctx, rw, "Objects synced")

	cfg.debugPacket(ctx, rw, "Requesting push...")
	pushresult := cfg.statestore.performPush(toupdate)
	cfg.debugPacket(ctx, rw, "Push results in")

	reqlogger.WithFields(logrus.Fields{
		"success":     pushresult.success,
		"refresults":  pushresult.branchresults,
		"clienterror": pushresult.clienterror,
		"error":       pushresult.logerror,
	}).Debug("Push results computed")
	if !pushresult.success {
		reqlogger.WithError(pushresult.logerror).Info("Push failed")
	}

	cfg.debugPacket(ctx, rw, "Running post-receive hook...")
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
	cfg.debugPacket(ctx, rw, "Post-receive hook done")

	sendPushResult(ctx, rw, pushresult)
	reqlogger.Debug("Push result sent, we are all done")
	// And... we are done! That was a ride
	return
}
