package service

import (
	"bufio"
	"crypto/sha1"
	"io"
	"net/http"

	"github.com/sirupsen/logrus"
	"repospanner.org/repospanner/server/storage"
)

func (cfg *Service) serveGitUploadPack(w http.ResponseWriter, r *http.Request, reqlogger *logrus.Entry, reponame string) {
	reqlogger.Debug("Read requested")
	bodyreader := bufio.NewReader(r.Body)
	rw := newWrappedResponseWriter(w)
	projectstore := cfg.gitstore.GetProjectStorage(reponame)

	capabs, wants, err := readUploadPackRequest(bodyreader, reqlogger)
	if err != nil {
		panic(err)
	}

	reqlogger = reqlogger.WithFields(logrus.Fields{
		"capabilities": capabs,
		"wants":        wants,
	})

	reqlogger.Debug("Got request wants")

	sbstatus, err := getSideBandStatus(capabs)
	if err != nil {
		panic(err)
	}

	multiAck := hasCapab(capabs, "multi_ack")
	multiAckDetailed := hasCapab(capabs, "multi_ack_detailed")
	sentdone := false
	commonObjects := newObjectIDSearch()
	firstCommon := storage.ZeroID
	isReady := false
	sentReady := false
	var commitsToSend objectIDSearcher
	for {
		have, isdone, iseof, err := readHavePacket(bodyreader, reqlogger)
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
				sendPacket(rw, []byte("ACK "+have+" common\n"))
			} else if multiAck {
				sendPacket(rw, []byte("ACK "+have+"\n"))
			}
			continue
		}

		has, err := hasObject(projectstore, have)
		if err != nil {
			panic(err)
		}
		if has {
			commonObjects.Add(have)
			enough, tosend, err := hasEnoughHaves(projectstore, reqlogger, wants, commonObjects)
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
				sendPacket(rw, []byte("ACK "+have+" common\n"))
				if enough && !sentReady {
					sendPacket(rw, []byte("ACK "+have+" ready\n"))
					sentReady = true
				}
			} else if multiAck {
				sendPacket(rw, []byte("ACK "+have+"\n"))
			}
		}
	}
	if firstCommon == storage.ZeroID {
		// send NAK
		sendPacket(rw, []byte("NAK\n"))
	} else {
		sendPacket(rw, []byte("ACK "+firstCommon+"\n"))
	}
	if !sentdone && !hasCapab(capabs, "no-done") {
		cfg.debugPacket(rw, sbstatus, "Not sending pack per request")
		return
	}

	rw.isfullyread = true

	cfg.maybeSayHello(rw, sbstatus)
	cfg.debugPacket(rw, sbstatus, "Building packfile")

	var commitList []storage.ObjectID
	recursive := false
	if commitsToSend == nil {
		// We were unable to find anything, just send all wants
		recursive = true
		commitList = wants
	} else {
		commitList = commitsToSend.List()
	}

	packfile, numobjects, err := writeTemporaryPackFile(projectstore, commitList, commonObjects, recursive)
	if err != nil {
		panic(err)
	}
	defer packfile.Close()
	cfg.debugPacket(rw, sbstatus, "Packfile built, sending")
	reqlogger.Debug("Temporary packfile generated")

	sbsender := sideBandSender{
		w:        rw,
		sbstatus: sbstatus,
		sb:       sideBandData,
	}
	packhasher := sha1.New()
	packwriter := io.MultiWriter(packhasher, sbsender)

	hdr := buildPackHeader(numobjects)
	_, err = packwriter.Write(hdr)
	if err != nil {
		panic(err)
	}

	for {
		if rw.IsClosed() {
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

	cfg.debugPacket(rw, sbstatus, "Packfile sent")
	err = sendFlushPacket(w)

	reqlogger.Debug("Pull result sent, we are all done")

	return
}
