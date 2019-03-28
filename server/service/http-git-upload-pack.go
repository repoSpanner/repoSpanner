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

	multiAck := hasCapab(ctx, "multi_ack")
	multiAckDetailed := hasCapab(ctx, "multi_ack_detailed")
	sentdone := false
	commonObjects := newObjectIDSearch()
	firstCommon := storage.ZeroID
	isReady := false
	sentReady := false
	var commitsToSend objectIDSearcher
	for {
		have, isdone, iseof, err := readHavePacket(ctx, bodyreader)
		if err != nil {
			panic(err)
		}
		if isdone {
			sentdone = true
			break
		}
		if iseof {
			break
		}

		if isReady {
			// We have enough info to generate a pack, ack blindly
			if multiAckDetailed {
				sendPacket(ctx, rw, []byte("ACK "+have+" common\n"))
			} else if multiAck {
				sendPacket(ctx, rw, []byte("ACK "+have+"\n"))
			}
			continue
		}

		has, err := hasObject(projectstore, have)
		if err != nil {
			panic(err)
		}
		if has {
			commonObjects.Add(have)
			enough, tosend, err := hasEnoughHaves(ctx, projectstore, wants, commonObjects)
			if err != nil {
				panic(err)
			}
			if enough {
				reqlogger.Debug("We now have enough info to generate a packfile")
				// We have enough
				isReady = true
				commitsToSend = tosend
			}
			firstCommon = have
			if multiAckDetailed {
				sendPacket(ctx, rw, []byte("ACK "+have+" common\n"))
				if enough && !sentReady {
					sendPacket(ctx, rw, []byte("ACK "+have+" ready\n"))
					sentReady = true
				}
			} else if multiAck {
				sendPacket(ctx, rw, []byte("ACK "+have+"\n"))
			}
		}
	}
	if firstCommon == storage.ZeroID {
		// send NAK
		sendPacket(ctx, rw, []byte("NAK\n"))
	} else {
		sendPacket(ctx, rw, []byte("ACK "+firstCommon+"\n"))
	}
	if !sentdone && !hasCapab(ctx, "no-done") {
		cfg.debugPacket(ctx, rw, "Not sending pack per request")
		return
	}

	rw.isfullyread = true

	cfg.maybeSayHello(ctx, rw)
	cfg.debugPacket(ctx, rw, "Building packfile")

	numObjectsPacked := 0
	reportObjectPacked := func() {
		numObjectsPacked++
		if numObjectsPacked%100 == 0 {
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
	if commitsToSend == nil {
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

	for {
		if ctx.Err() != nil {
			reqlogger.Debug("Connection closed")
			return
		}

		buf := make([]byte, 1024)
		in, err := packfile.Read(buf)
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
