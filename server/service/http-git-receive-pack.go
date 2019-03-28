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

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	pb "repospanner.org/repospanner/server/protobuf"
)

var errDisconnected = errors.New("Client disconnected during request processing")

func (cfg *Service) serveGitReceivePack(ctx context.Context, w http.ResponseWriter, r *http.Request, reponame string) {
	reqlogger := loggerFromCtx(ctx)

	bodyreader := bufio.NewReader(r.Body)
	rw := newWrappedResponseWriter(w)

	reqlogger.Debug("git-receive-pack requested")

	capabs, toupdate, err := cfg.readDownloadPacketRequestHeader(ctx, bodyreader, reponame)
	if err != nil {
		// TODO: Log inner error
		reqlogger.WithError(err).Info("Invalid request received")
		sendPacket(ctx, rw, []byte("ERR Invalid request"))
		return
	}
	ctx, reqlogger, err = addCapabsToCtx(ctx, capabs)
	if err != nil {
		reqlogger.WithError(err).Info("Invalid request received")
		sendPacket(ctx, rw, []byte("ERR Invalid request"))
		return
	}
	ctx, reqlogger = expandCtxLogger(ctx, logrus.Fields{
		"capabs":   capabs,
		"toupdate": fmt.Sprintf("%s", toupdate),
	})
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
	cfg.debugPacket(ctx, rw, "Performing pre-check...")
	precheckresult := cfg.statestore.getPushResult(toupdate)
	cfg.debugPacket(ctx, rw, "Pre-check results in")
	if !precheckresult.success {
		sendPushResult(ctx, rw, precheckresult)
		return
	}
	hookrun, err := cfg.prepareHookRunner(ctx, r.Header, infosender, errsender, reponame, toupdate)
	if err != nil {
		// TODO
		userErr := getUserError(err)
		reqlogger.WithError(getLogError(err)).Info(userErr)
		sendSideBandPacket(ctx, w, sideBandProgress, []byte(fmt.Sprintf("%s\n", userErr)))
		sendUnpackFail(ctx, w, toupdate, userErr)
		return
	}
	defer hookrun.close()
	err = cfg.performReceivePack(ctx, r.Header, reqlogger, bodyreader, rw, reponame, capabs, toupdate, hookrun)
	if err == errDisconnected {
		reqlogger.Debug("Client disconnected")
	} else if err != nil {
		// TODO
		userErr := getUserError(err)
		reqlogger.WithError(getLogError(err)).Info(userErr)
		sendSideBandPacket(ctx, w, sideBandProgress, []byte(fmt.Sprintf("ERR %s\n", userErr)))
		sendUnpackFail(ctx, w, toupdate, fmt.Sprintf("unpack FAIL: %s", userErr))
		return
	}
}

func getLogError(err error) error {
	pe, ok := err.(pushError)
	if ok {
		return pe.err
	}
	return err
}

func getUserError(err error) string {
	pe, ok := err.(pushError)
	if ok {
		return pe.msg
	}
	return err.Error()
}

type pushError struct {
	err error
	msg string
}

func wrapPushError(err error, msg string) pushError {
	return pushError{
		err: err,
		msg: msg,
	}
}

func (p pushError) Error() string {
	return fmt.Sprintf("Push error: %s (msg: %s)", p.err, p.msg)
}

func (cfg *Service) prepareHookRunner(ctx context.Context, hdrs http.Header, infosender, errsender sideBandSender, reponame string, toupdate *pb.PushRequest) (hookRunning, error) {
	// Get extra information to pass into any hooks we might run
	hookExtras := make(map[string]string)
	for key, value := range hdrs {
		if strings.HasPrefix(key, "X-Extra-") {
			key = key[8:]
			hookExtras[strings.ToLower(key)] = value[0]
		}
	}

	// Prepare hook runners
	hookrun, err := cfg.prepareHookRunning(
		ctx,
		errsender,
		infosender,
		reponame,
		toupdate,
		hookExtras,
	)
	if err != nil {
		return nil, wrapPushError(err, "Error preparing hook")
	}
	if hookrun == nil {
		return nil, wrapPushError(nil, "Hook preparing failed")
	}
	return hookrun, nil
}

func (cfg *Service) performReceivePack(ctx context.Context, hdrs http.Header, reqlogger *logrus.Entry, bodyreader io.Reader, rw *wrappedResponseWriter, reponame string, capabs []string, toupdate *pb.PushRequest, hookrun hookRunning) error {
	projectstore := cfg.gitstore.GetProjectStorage(reponame)

	cfg.maybeSayHello(ctx, rw)

	if ctx.Err() != nil {
		return errDisconnected
	}

	// Now get and store objects
	pusher := projectstore.GetPusher(toupdate.UUID())
	pushresultc := pusher.GetPushResultChannel()

	if toupdate.ExpectPackFile() {
		packhasher := sha1.New()
		packreader := &hashWriter{r: bodyreader, w: packhasher}
		version, numobjects, err := getPackHeader(packreader)
		if err != nil {
			return wrapPushError(err, "Unable to get packfile header")
		}
		reqlogger = reqlogger.WithField(
			"pack-numobjects", numobjects,
		)
		if version != 2 {
			return wrapPushError(nil, "Invalid pack version received")
		}
		reqlogger.Debug("Got receive-pack request header")

		deltasqueue, err := ioutil.TempFile("", "repospanner_deltaqueue_")
		if err != nil {
			return wrapPushError(err, "Unable to create deltaqueue")
		}
		if ctx.Err() != nil {
			return errDisconnected
		}
		defer deltasqueue.Close()
		os.Remove(deltasqueue.Name())
		var deltaqueuesize int

		var gotObjects uint32
		for gotObjects < numobjects {
			select {
			case syncerr := <-pushresultc:
				return wrapPushError(syncerr, "Error syncing object out to enough nodes")

			case <-ctx.Done():
				return errDisconnected

			default:
				_, _, resolve, err := getSingleObjectFromPack(packreader, pusher)
				if err != nil {
					return wrapPushError(err, "Error getting object")
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
			return wrapPushError(err, "Error reading expected checksum")
		}

		if !checksumsMatch(ctx, packhasher.Sum(nil), expectedSum) {
			// Checksum failed, checksumsMatch already logs
			return wrapPushError(nil, "Checksum has failed")
		}

		rw.isfullyread = true

		// Resolve deltas
		err = processDeltas(ctx, deltaqueuesize, deltasqueue, rw, projectstore, pusher)
		if err != nil {
			return wrapPushError(err, "Error processing deltas")
		}

		cfg.debugPacket(ctx, rw, "Delta resolving finished")
		reqlogger.Debug("Pack file accepted, checksum matches")
	}
	if ctx.Err() != nil {
		return errDisconnected
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
		return wrapPushError(err, "Object validation failure")
	}
	cfg.debugPacket(ctx, rw, "Objects validated")
	reqlogger.Debug("Objects in request are sufficient")

	if ctx.Err() != nil {
		return errDisconnected
	}

	// Inform the pusher we have sent all the objects we're going to send it
	pusher.Done()

	cfg.debugPacket(ctx, rw, "Finishing hook runner preparation...")
	err := hookrun.finishPreparing()
	if err != nil {
		if ctx.Err() != nil {
			return errDisconnected
		}
		return wrapPushError(err, "Hook preparation finish failed")
	}

	if ctx.Err() != nil {
		return errDisconnected
	}
	cfg.statestore.AddFakeRefs(reponame, toupdate)
	defer cfg.statestore.RemoveFakeRefs(reponame, toupdate)

	cfg.debugPacket(ctx, rw, "Telling hook runner to grab new contents...")
	err = hookrun.fetchFakeRefs()
	if err != nil {
		if ctx.Err() != nil {
			return errDisconnected
		}
		return wrapPushError(err, "Hook fetching failed")
	}

	if ctx.Err() != nil {
		return errDisconnected
	}
	cfg.debugPacket(ctx, rw, "Running pre-receive hook...")
	err = hookrun.runHook(hookTypePreReceive)
	if err != nil {
		if ctx.Err() != nil {
			return errDisconnected
		}
		return wrapPushError(err, "Pre-receive hook refused push")
	}
	cfg.debugPacket(ctx, rw, "Pre-receive hook done")

	if ctx.Err() != nil {
		return errDisconnected
	}
	cfg.debugPacket(ctx, rw, "Running update hook...")
	err = hookrun.runHook(hookTypeUpdate)
	if err != nil {
		if ctx.Err() != nil {
			return errDisconnected
		}
		return wrapPushError(err, "Update hook refused push")
	}
	cfg.debugPacket(ctx, rw, "Update hook done")

	// We wait for either the first error, or the waitgroup to be fully done, which will
	// close the channel, returning a <nil> value.
	cfg.debugPacket(ctx, rw, "Syncing objects...")
	syncerr, isopen := <-pushresultc
	if syncerr == nil && isopen == true {
		return fmt.Errorf("INTERNAL ERROR: syncerr channel got nil without close")
	}
	if syncerr != nil {
		return wrapPushError(syncerr, "Error syncing object out to enough nodes")
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
	err = hookrun.runHook(hookTypePostReceive)
	if err != nil {
		reqlogger.WithError(err).Debug("Post-receive hook failed")
	}
	cfg.debugPacket(ctx, rw, "Post-receive hook done")

	sendPushResult(ctx, rw, pushresult)
	reqlogger.Debug("Push result sent, we are all done")
	// And... we are done! That was a ride
	return nil
}
