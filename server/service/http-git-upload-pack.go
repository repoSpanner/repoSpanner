package service

import (
	"bufio"
	"context"
	"crypto/sha1"
	"fmt"
	"io"
	"net/http"

	"github.com/sirupsen/logrus"
	"repospanner.org/repospanner/server/storage"
)

func maybeSendReply(ctx context.Context, reply []byte, stateless bool, replies [][]byte, rw io.Writer) [][]byte {
	if stateless {
		return append(replies, reply)
	}

	sendPacket(ctx, rw, reply)
	return replies
}

func determineToSend(ctx context.Context, projectstore storage.ProjectStorageDriver, w io.Writer, r io.Reader, wants []storage.ObjectID, isStateless bool) (stopSending bool, tosend objectIDSearcher, common objectIDSearcher) {
	reqlogger := loggerFromCtx(ctx)

	multiAck := 0
	if hasCapab(ctx, "multi_ack_detailed") {
		multiAck = 2
	} else if hasCapab(ctx, "multi_ack") {
		multiAck = 1
	}
	tosend = newObjectIDSearch()
	common = newObjectIDSearch()
	noDone := hasCapab(ctx, "no-done")

	common = newObjectIDSearch()
	nrHaves := 0
	lastHex := storage.ZeroID
	gotCommon := false
	gotOther := false
	isReady := false
	sentReady := false

	for {
		have, isdone, iseof, err := readHavePacket(ctx, r)
		if err != nil {
			panic(err)
		}

		if iseof {
			if multiAck == 2 && gotCommon && !gotOther && isReady {
				sentReady = true
				sendPacket(ctx, w, []byte("ACK "+lastHex+" ready\n"))
			}
			if nrHaves == 0 || multiAck > 0 {
				sendPacket(ctx, w, []byte("NAK\n"))
			}
			if noDone && sentReady {
				sendPacket(ctx, w, []byte("ACK "+lastHex+"\n"))
				return
			}
			if isStateless {
				stopSending = true
				return
			}
			gotCommon = false
			gotOther = false
			continue
		}

		if isdone {
			if nrHaves > 0 {
				if multiAck > 0 {
					sendPacket(ctx, w, []byte("ACK "+lastHex+"\n"))
					return
				}
			}
			sendPacket(ctx, w, []byte("NAK\n"))
			return
		}

		// We got a valid "have"
		has, err := hasObject(projectstore, have)
		if err != nil {
			panic(err)
		}
		nrHaves++
		lastHex = have
		if has {
			gotCommon = true
			if multiAck == 2 {
				sendPacket(ctx, w, []byte("ACK "+have+" common\n"))
			} else if multiAck == 1 {
				sendPacket(ctx, w, []byte("ACK "+have+" continue\n"))
			} else {
				sendPacket(ctx, w, []byte("ACK "+have+"\n"))
			}

			var enough bool
			common.Add(have)
			enough, tosend, err = hasEnoughHaves(ctx, projectstore, wants, common)
			if err != nil {
				panic(err)
			}
			if enough {
				reqlogger.Info("Has enough info!")
				isReady = true
			}
		} else {
			// Not a common object
			gotOther = true
			if multiAck > 0 && isReady {
				if multiAck == 2 {
					sendPacket(ctx, w, []byte("ACK "+have+" ready\n"))
				} else {
					sendPacket(ctx, w, []byte("ACK "+have+" continue\n"))
				}
			}
		}
	}
}

func (cfg *Service) serveGitUploadPack(ctx context.Context, w http.ResponseWriter, r *http.Request, reponame string) {
	reqlogger := loggerFromCtx(ctx)

	reqlogger.Debug("Read requested")
	bodyreader := bufio.NewReader(r.Body)
	rw := newWrappedResponseWriter(w)
	projectstore := cfg.gitstore.GetProjectStorage(reponame)

	capabs, wants, err := readUploadPackRequest(ctx, bodyreader)
	if err != nil {
		reqlogger.WithField("err", err).Debug("Invalid request received")
		w.WriteHeader(400)
		w.Write([]byte("Error processing your request"))
		return
	}

	ctx, reqlogger, err = addCapabsToCtx(ctx, capabs)
	if err != nil {
		reqlogger.WithField("err", err).Debug("Invalid request received")
		w.WriteHeader(400)
		w.Write([]byte("Error processing your request"))
		return
	}
	ctx, reqlogger = expandCtxLogger(ctx, logrus.Fields{
		"wants": wants,
	})

	reqlogger.Debug("Got request wants")

	isrepobridge := len(r.Header[http.CanonicalHeaderKey("X-RepoBridge-Version")]) == 1
	isStateless := !isrepobridge
	stopSending, commitsToSend, commonObjects := determineToSend(ctx, projectstore, rw, bodyreader, wants, isStateless)

	reqlogger.WithFields(logrus.Fields{
		"stopSending":   stopSending,
		"commitsToSend": commitsToSend,
		"commonObjects": commonObjects,
	}).Info("Got send information")

	if stopSending {
		// We shouldn't be sending, so we're done
		return
	}

	rw.isfullyread = true

	cfg.maybeSayHello(ctx, rw)
	cfg.debugPacket(ctx, rw, "Building packfile")

	lastNumObjectsPackedReported := 0
	numObjectsPacked := 0
	reportObjectPacked := func(numobjects int) {
		numObjectsPacked += numobjects
		if numObjectsPacked-lastNumObjectsPackedReported > 100 {
			lastNumObjectsPackedReported = numobjects
			sendSideBandPacket(
				ctx,
				rw,
				sideBandProgress,
				[]byte(fmt.Sprintf(
					"Packed %d objects...\r",
					numObjectsPacked,
				)),
			)
		}
	}

	var commitList []storage.ObjectID
	recursive := false
	if commitsToSend == nil || commitsToSend.NumEntries() == 0 || commonObjects.NumEntries() == 0 {
		// We were unable to find anything, just send all wants
		recursive = true
		commitList = wants
	} else {
		commitList = commitsToSend.List()
	}

	packfile, numobjects, err := writeTemporaryPackFile(reportObjectPacked, projectstore, commitList, commonObjects, recursive)
	if err != nil {
		panic(err)
	}
	defer packfile.Close()
	cfg.debugPacket(ctx, rw, "Packfile built, sending")
	reqlogger.Debug("Temporary packfile generated")

	sbsender := sideBandSender{
		ctx: ctx,
		w:   rw,
		sb:  sideBandData,
	}
	packhasher := sha1.New()
	packwriter := io.MultiWriter(packhasher, sbsender)

	hdr := buildPackHeader(numobjects)
	_, err = packwriter.Write(hdr)
	if err != nil {
		panic(err)
	}

	var buf [1024]byte
	for {
		if ctx.Err() != nil {
			reqlogger.Debug("Connection closed")
			return
		}

		in, err := packfile.Read(buf[:])
		if err != nil {
			if err == io.EOF {
				break
			}
			panic(err)
		}
		out, err := packwriter.Write(buf[:in])
		if err != nil {
			panic(err)
		}
		if in != out {
			panic("Not everything written?")
		}
	}
	sum := packhasher.Sum(nil)
	_, err = sbsender.Write(sum)
	if err != nil {
		panic(err)
	}

	cfg.debugPacket(ctx, rw, "Packfile sent")
	err = sendFlushPacket(ctx, w)

	reqlogger.Debug("Pull result sent, we are all done")

	return
}
