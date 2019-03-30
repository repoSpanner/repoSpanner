package service

import (
	"bytes"
	"compress/zlib"
	"context"
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"hash"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"

	"github.com/pkg/errors"
	"repospanner.org/repospanner/server/constants"
	pb "repospanner.org/repospanner/server/protobuf"
	"repospanner.org/repospanner/server/storage"
)

type sideBand byte

const (
	sideBandData     sideBand = 0x01
	sideBandProgress          = 0x02
	sideBandFatal             = 0x03
)

func (s sideBand) String() string {
	switch s {
	case sideBandData:
		return "sideBandData"
	case sideBandProgress:
		return "sideBandProgress"
	case sideBandFatal:
		return "sideBandFatal"
	}
	return "Invalid sideBand"
}

type sideBandStatus int

const (
	sideBandStatusNot sideBandStatus = iota
	sideBandStatusSmall
	sideBandStatusLarge
)

func (s sideBandStatus) String() string {
	switch s {
	case sideBandStatusNot:
		return "sideBandStatusNot"
	case sideBandStatusSmall:
		return "sideBandStatusSmall"
	case sideBandStatusLarge:
		return "sideBandStatusLarge"
	}
	return "Invalid sideBandStatus"
}

func sendSideBandFlushPacket(ctx context.Context, w io.Writer, sb sideBand) error {
	lock := sbLockFromCtx(ctx)
	lock.Lock()
	defer lock.Unlock()

	sbstatus := sbStatusFromCtx(ctx)
	if sbstatus == sideBandStatusNot {
		return sendFlushPacket(ctx, w)
	}
	_, err := w.Write([]byte{byte(sb), '0', '0', '0', '0'})
	return err
}

func sendStatusPacket(ctx context.Context, w io.Writer, packets ...string) error {
	// For some reason, the result packet is expected to be multiple packets inside a
	// single sidebanded packet....
	status := new(bytes.Buffer)

	for _, spacket := range packets {
		packet := []byte(spacket + "\n")
		len, err := getPacketLen(packet)
		if err != nil {
			return err
		}
		if _, err := status.Write(len); err != nil {
			return err
		}
		if _, err := status.Write(packet); err != nil {
			return err
		}
	}
	// First in-sideband flush....
	status.Write([]byte{'0', '0', '0', '0'})

	if err := sendSideBandPacket(ctx, w, sideBandData, status.Bytes()); err != nil {
		return err
	}
	// Then an out-of-sideband flush
	return sendFlushPacket(ctx, w)
}

func sendSideBandPacket(ctx context.Context, w io.Writer, sb sideBand, packet []byte) error {
	sbstatus := sbStatusFromCtx(ctx)

	if sbstatus == sideBandStatusNot {
		// We have no sideband, ignore anything except for sideband data
		if sb == sideBandData {
			return sendPacket(ctx, w, packet)
		}
		// No sideband, no data... Ignore
		return nil
	}

	var maxsize int
	if sbstatus == sideBandStatusSmall {
		maxsize = 990
	} else {
		// This is smaller than the max packet size of 65520, to account
		// for the overhead of packet length and sideband indicator.
		maxsize = 65500
	}

	// Break up in chunks of at most maxsize bytes
	for start := 0; start < len(packet); start += maxsize {
		end := start + maxsize
		if end >= len(packet) {
			end = len(packet)
		}
		tosend := append([]byte{byte(sb)}, packet[start:end]...)
		if err := sendPacket(ctx, w, tosend); err != nil {
			return err
		}
	}
	return nil
}

func (cfg *Service) debugPacket(ctx context.Context, w io.Writer, msg string) {
	if cfg.Debug {
		sendSideBandPacket(ctx, w, sideBandProgress, []byte(msg+"\n"))
	}
}

func (cfg *Service) maybeSayHello(ctx context.Context, w io.Writer) {
	sendSideBandPacket(
		ctx,
		w,
		sideBandProgress,
		[]byte(fmt.Sprintf(
			"Welcome to repoSpanner %s, node %s.%s.%s\n",
			constants.PublicVersionString(),
			cfg.nodename,
			cfg.region,
			cfg.cluster,
		)),
	)
}

type sideBandSender struct {
	ctx context.Context
	w   io.Writer
	sb  sideBand
}

func (s sideBandSender) Write(packet []byte) (int, error) {
	return len(packet), sendSideBandPacket(s.ctx, s.w, s.sb, packet)
}

var extensions = []string{
	"delete-refs",
	"no-thin",
	"no-done",
	"side-band",
	"side-band-64k",
	"report-status",
	"allow-tip-sha1-in-want",
	"multi_ack",
	"multi_ack_detailed",
	"allow-reachable-sha1-in-want",
	"agent=repoSpanner/" + constants.PublicVersionString(),
}

func sendPacketWithExtensions(ctx context.Context, w io.Writer, packet []byte, symrefs map[string]string) error {
	packet = append(packet, byte('\x00'))
	packet = append(packet, []byte(strings.Join(extensions, " "))...)
	for symref, target := range symrefs {
		packet = append(packet, []byte(" symref="+symref+":"+target)...)
	}
	packet = append(packet, byte('\n'))
	return sendPacket(ctx, w, packet)
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

type wrappedResponseWriter struct {
	writer      io.Writer
	flusher     http.Flusher
	ishttp2     bool
	isfullyread bool
}

func (w *wrappedResponseWriter) Write(p []byte) (int, error) {
	return w.writer.Write(p)
}

func (w *wrappedResponseWriter) Flush() {
	if (w.ishttp2 || w.isfullyread) && w.flusher != nil {
		w.flusher.Flush()
	}
}

func newWrappedResponseWriter(inner io.Writer) *wrappedResponseWriter {
	_, ishttp2 := inner.(http.Pusher)
	flusher, hasflusher := inner.(http.Flusher)
	if !hasflusher {
		flusher = nil
	}
	return &wrappedResponseWriter{
		writer:      inner,
		flusher:     flusher,
		ishttp2:     ishttp2,
		isfullyread: false,
	}
}

func possiblyFlush(w io.Writer) {
	f, ok := w.(http.Flusher)
	if ok {
		f.Flush()
	}
}

func sendPacket(ctx context.Context, w io.Writer, packet []byte) error {
	lock := sbLockFromCtx(ctx)
	lock.Lock()
	defer lock.Unlock()

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
	possiblyFlush(w)
	return nil
}

func sendFlushPacket(ctx context.Context, w io.Writer) error {
	lock := sbLockFromCtx(ctx)
	lock.Lock()
	defer lock.Unlock()

	_, err := w.Write([]byte{'0', '0', '0', '0'})
	possiblyFlush(w)
	return err
}

func readPacket(r io.Reader) ([]byte, error) {
	var rawlen [4]byte
	n, err := io.ReadFull(r, rawlen[:])
	if err != nil {
		return nil, err
	}
	if n != 4 {
		return nil, fmt.Errorf("Expected 4 bytes, got %d", n)
	}
	len, err := strconv.ParseInt(string(rawlen[:]), 16, 0)
	if err != nil {
		return nil, err
	}

	if len >= 65516 {
		return nil, fmt.Errorf("Too large packet received, len: %d", len)
	}
	if len == 0 {
		// This was a "flush" packet
		return []byte{}, nil
	}
	if len <= 4 {
		return nil, fmt.Errorf("Invalid length sent: %d", len)
	}
	len = len - 4
	buff := make([]byte, len)
	read, err := io.ReadFull(r, buff)
	if int64(read) != len {
		return nil, fmt.Errorf("Expected to read %d, read %d", len, read)
	}
	return buff, nil
}

func sendUnpackFail(ctx context.Context, w io.Writer, toupdate *pb.PushRequest, msg string) {
	if !hasCapab(ctx, "report-status") {
		sendFlushPacket(ctx, w)
		return
	}

	fails := []string{msg}
	for _, req := range toupdate.Requests {
		fails = append(fails, fmt.Sprintf("ng %s %s", req.GetRef(), msg))
	}
	sendStatusPacket(
		ctx,
		w,
		fails...)
}

func sendPushResult(ctx context.Context, w io.Writer, result PushResult) {
	if !hasCapab(ctx, "report-status") {
		sendFlushPacket(ctx, w)
		return
	}

	var msgs []string
	if result.success {
		msgs = []string{"unpack ok"}
	} else {
		msgs = []string{result.clienterror.Error()}
	}
	for refname, refmsg := range result.branchresults {
		if result.success {
			msgs = append(msgs, "ok "+refname+" "+refmsg)
		} else {
			msgs = append(msgs, "ng "+refname+" "+refmsg)
		}
	}
	sendStatusPacket(
		ctx,
		w,
		msgs...)
}

var refRegexp = regexp.MustCompile("^[a-fA-F0-9]{40}$")

func isValidRef(ref string) bool {
	return refRegexp.MatchString(ref)
}

func isValidRefName(refname string) bool {
	if refname == "HEAD" {
		return true
	}
	if !strings.HasPrefix(refname, "refs/") {
		return false
	}
	if strings.Contains(refname, "/.") {
		return false
	}
	if !strings.Contains(refname[len("refs/"):], "/") {
		return false
	}
	if strings.Contains(refname, "..") {
		return false
	}
	if strings.Contains(refname, " ") {
		return false
	}
	if strings.Contains(refname, "~") {
		return false
	}
	if strings.Contains(refname, "^") {
		return false
	}
	if strings.Contains(refname, ":") {
		return false
	}
	// TODO: ASCII control chars
	if strings.HasSuffix(refname, "/") {
		return false
	}
	if strings.HasSuffix(refname, ".") {
		return false
	}
	if strings.HasSuffix(refname, ".lock") {
		return false
	}
	if strings.Contains(refname, "@{") {
		return false
	}
	if strings.Contains(refname, "\\\\") {
		return false
	}
	return true
}

func (cfg *Service) readDownloadPacketRequestHeader(ctx context.Context, r io.Reader, reponame string) (capabs []string, toupdate *pb.PushRequest, err error) {
	reqlogger := loggerFromCtx(ctx)

	// Even though the documentation says we need to expect "commands", git does not
	// actually seem to send those, and instead just sends <to> <from> <refname> lines
	toupdate = pb.NewPushRequest(cfg.nodeid, reponame)
	hadcapabs := false

	for {
		pkt, err := readPacket(r)
		if err != nil {
			if err == io.EOF {
				reqlogger.Debug("Got EOF")
				return capabs, toupdate, nil
			}
			return nil, nil, err
		}

		strpkt := string(pkt)

		if len(pkt) == 0 {
			reqlogger.Debug("Got flush")
			return capabs, toupdate, nil
		}

		split := strings.Split(strpkt, " ")
		if len(split) < 3 {
			return nil, nil, fmt.Errorf("Invalid length of command received: %d", len(split))
		}
		reffrom := split[0]
		refto := split[1]
		refname := split[2]
		pkthascapabs := false

		if refname[len(refname)-1] == '\x00' {
			pkthascapabs = true
			refname = refname[:len(refname)-1]
		}

		if !isValidRef(reffrom) {
			return nil, nil, fmt.Errorf("Invalid reffrom received: %s", reffrom)
		}
		if !isValidRef(refto) {
			return nil, nil, fmt.Errorf("Invalid refto received: %s", refto)
		}
		if !isValidRefName(refname) {
			return nil, nil, fmt.Errorf("Invalid refname received: %s", refname)
		}
		if toupdate.HasRef(refname) {
			return nil, nil, fmt.Errorf("Multiple updates sent for %s", refname)
		}
		toupdate.AddRequest(pb.NewUpdateRequest(refname, reffrom, refto))

		if pkthascapabs {
			if !hadcapabs {
				// Parse capabilities
				if len(split) >= 3 {
					capabs = split[3:]
				}
				hadcapabs = true
			} else {
				return nil, nil, errors.New("Capabilities received unexpectedly")
			}
		}
	}
}

func commitIDFromTag(reader io.Reader) (storage.ObjectID, error) {
	info, err := readTag(reader)
	if err != nil {
		return storage.ZeroID, errors.Wrap(err, "Error reading tag to get commit ID")
	}
	return info.object, nil
}

func wantIsReachableFromCommons(p storage.ProjectStorageDriver, want storage.ObjectID, common, tosend objectIDSearcher) (bool, error) {
	if common.Contains(want) {
		return true, nil
	}
	if tosend.Contains(want) {
		return true, nil
	}
	tosend.Add(want)
	otype, _, reader, err := p.ReadObject(want)
	if err != nil {
		return false, err
	}
	if otype == storage.ObjectTypeTag {
		commit, err := commitIDFromTag(reader)
		reader.Close()
		if err != nil {
			return false, errors.Wrap(err, "Error determining want reachability")
		}
		otype, _, reader, err = p.ReadObject(commit)
		if err != nil {
			return false, errors.Wrap(err, "Error deterining want reachability")
		}
	}
	if otype != storage.ObjectTypeCommit {
		reader.Close()
		return false, errors.New("Non-commit found in chain")
	}
	info, err := readCommit(reader)
	reader.Close()

	if len(info.parents) == 0 {
		// No further parents to look up, and we weren't common. No commons :(
		return false, nil
	}
	for _, parent := range info.parents {
		parentReachable, err := wantIsReachableFromCommons(p, parent, common, tosend)
		if err != nil {
			return false, err
		}
		if !parentReachable {
			return false, nil
		}
	}
	// If we got here, we have found a common ancestor for all parents, great!
	return true, nil
}

func hasEnoughHaves(ctx context.Context, p storage.ProjectStorageDriver, wants []storage.ObjectID, common objectIDSearcher) (bool, objectIDSearcher, error) {
	tosend := newObjectIDSearch()
	for _, want := range wants {
		wantIsReachable, err := wantIsReachableFromCommons(p, want, common, tosend)
		if err != nil {
			return false, nil, err
		}
		if !wantIsReachable {
			return false, nil, nil
		}
	}
	return true, tosend, nil
}

func readHavePacket(ctx context.Context, r io.Reader) (have storage.ObjectID, isdone, iseof bool, err error) {
	reqlogger := loggerFromCtx(ctx)

	have = storage.ZeroID

	var pkt []byte
	pkt, err = readPacket(r)
	if err != nil {
		if err == io.EOF {
			// EOF means that we're not getting any further wants, so we must be done
			err = nil
			iseof = true
			return
		}
		return
	}

	strpkt := strings.TrimSpace(string(pkt))

	if len(pkt) == 0 {
		isdone = true
		return
	}

	if strpkt == "done" {
		isdone = true
		return
	}

	split := strings.Split(strpkt, " ")
	if split[0] != "have" || len(split) != 2 {
		err = errors.New("Invalid packet received")
		return
	}

	haveS := split[1]
	if !isValidRef(haveS) {
		reqlogger.Debugf("Invalid have ref: %s", have)
		err = errors.New("Invalid have value")
		return
	}

	have = storage.ObjectID(haveS)
	return
}

func readUploadPackRequest(ctx context.Context, r io.Reader) (capabs []string, wants []storage.ObjectID, err error) {
	reqlogger := loggerFromCtx(ctx)

	hadcapabs := false

	for {
		var pkt []byte
		pkt, err = readPacket(r)
		if err != nil {
			if err == io.EOF {
				err = errors.New("EOF while waiting for wants")
				return
			}

			return
		}

		strpkt := strings.TrimSpace(string(pkt))

		if len(pkt) == 0 {
			reqlogger.Debug("End of wants")
			return
		}

		if strpkt == "done" {
			// This is unexpected...
			err = errors.New("Done received while in wants phase")
			return
		}

		split := strings.Split(strpkt, " ")
		if split[0] == "want" {
			want := split[1]
			if !isValidRef(want) {
				reqlogger.Debugf("Invalid want ref: %s", want)
				err = fmt.Errorf("Invalid want: %s", want)
				return
			}
			wants = append(wants, storage.ObjectID(want))
		} else {
			err = errors.New("Invalid packet read in wants phase")
		}

		if len(split) > 2 {
			if !hadcapabs {
				// Parse capabilities
				capabs = split[2:]
				hadcapabs = true
			} else {
				err = errors.New("Capabilities received unexpectedly")
				return
			}
		}
	}
}

func concatSlices(slices ...[]byte) (ret []byte) {
	var slen int
	for _, s := range slices {
		slen += len(s)
	}
	ret = make([]byte, slen)
	var i int
	for _, s := range slices {
		i += copy(ret[i:], s)
	}
	return
}

func getPackHeader(r io.Reader) (version uint32, numobjects uint32, err error) {
	var rawhdr [4]byte
	if _, err = io.ReadFull(r, rawhdr[:]); err != nil {
		return 0, 0, errors.Wrap(err, "Error reading pack header")
	}
	if string(rawhdr[:]) != "PACK" {
		return 0, 0, errors.New("Non-PACK header received")
	}
	version, err = getNetworkByteOrderInt32(r)
	if err != nil {
		return 0, 0, errors.Wrap(err, "Error parsing pack version")
	}
	numobjects, err = getNetworkByteOrderInt32(r)
	if err != nil {
		return 0, 0, errors.Wrap(err, "Error parsing number of pack objects")
	}
	return
}

func getNetworkByteOrderInt32(r io.Reader) (uint32, error) {
	var buf [4]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint32(buf[:]), nil
}

func checksumsMatch(ctx context.Context, calculated, expected []byte) bool {
	reqlogger := loggerFromCtx(ctx)
	if len(calculated) != len(expected) {
		return false
	}
	matches := true
	for i, v := range calculated {
		matches = matches && v == expected[i]
	}
	if !matches {
		reqlogger.Infof("Packfile checksums don't match: %s != %s",
			calculated,
			expected,
		)
	}
	return matches
}

func getSingleObjectTypeSizeFromPack(r io.Reader) (objtype storage.ObjectType, objsize uint, err error) {
	objtype = storage.ObjectTypeBad
	objsize = 0

	firstbyte := true
	var shift uint = 4
	var buf [1]byte
	for {
		if _, err = r.Read(buf[:]); err != nil {
			return
		}
		val := uint(buf[0])

		if firstbyte {
			// This must be the first byte, which means this is the objtype+size byte
			objtype = storage.ObjectType((val >> 4) & 7)
			objsize = (val & 15)
			firstbyte = false
		} else {
			objsize += (val & 0x7F) << shift
			shift += 7
		}

		if val&0x80 == 00 {
			// No more size bytes
			break
		}
	}

	if objtype < storage.ObjectTypeCommit || objtype > storage.ObjectTypeRefDelta {
		err = fmt.Errorf("Invalid object type %s", objtype)
		return
	}

	return
}

type hashWriter struct {
	r io.Reader
	w hash.Hash
}

func (rw *hashWriter) Read(buf []byte) (int, error) {
	in, inerr := rw.r.Read(buf)
	rw.w.Write(buf[:in])
	return in, inerr
}

func (rw *hashWriter) ReadByte() (byte, error) {
	r, ok := rw.r.(io.ByteReader)
	if !ok {
		panic("Huh?")
	}
	in, inerr := r.ReadByte()
	rw.w.Write([]byte{in})
	return in, inerr
}

func getSingleObjectFromPack(r io.Reader, s storage.ProjectStoragePushDriver) (storage.ObjectID, storage.ObjectType, resolveInfo, error) {
	var toresolve resolveInfo

	objtype, objsize, err := getSingleObjectTypeSizeFromPack(r)
	if err != nil {
		return storage.ZeroID, 0, toresolve, err
	}

	if objtype == storage.ObjectTypeRefDelta {
		var buf [20]byte
		if _, err := io.ReadFull(r, buf[:]); err != nil {
			return storage.ZeroID, 0, toresolve, err
		}
		toresolve.baseobj = storage.ObjectIDFromRaw(buf[:])
	}

	zreader, err := zlib.NewReader(r)
	if err != nil {
		return storage.ZeroID, 0, toresolve, err
	}
	defer zreader.Close()

	stager, err := s.StageObject(objtype, objsize)
	if err != nil {
		return storage.ZeroID, 0, toresolve, err
	}
	defer stager.Close()
	oidwriter := getObjectIDStart(objtype, objsize)
	combwriter := io.MultiWriter(oidwriter, stager)

	var buf [1024]byte
	for {
		n, err := zreader.Read(buf[:])
		_, outerr := combwriter.Write(buf[:n])
		if err == nil {
			if outerr != nil {
				return storage.ZeroID, 0, toresolve, outerr
			}
			continue
		} else if err == io.EOF {
			break
		} else {
			return storage.ZeroID, 0, toresolve, err
		}
	}

	objectid := getObjectIDFinish(oidwriter)
	toresolve.deltaobj = objectid

	_, err = stager.Finalize(objectid)
	if err != nil {
		return storage.ZeroID, 0, toresolve, err
	}

	return objectid, objtype, toresolve, nil
}

func getObjectIDStart(objtype storage.ObjectType, objsize uint) hash.Hash {
	hasher := sha1.New()
	fmt.Fprintf(hasher, "%s %d\x00", objtype.HdrName(), objsize)
	return hasher
}

func getObjectIDFinish(hasher hash.Hash) storage.ObjectID {
	return storage.ObjectIDFromRaw(hasher.Sum(nil))
}

func hasCapab(ctx context.Context, capab string) bool {
	capabs, ok := capabsFromCtx(ctx)
	if !ok {
		return false
	}
	for _, s := range capabs {
		if s == capab {
			return true
		}
	}
	return false
}

func validateObjects(p storage.ProjectStorageDriver, toupdate *pb.PushRequest, recurse bool) error {
	for _, updinfo := range toupdate.Requests {
		if updinfo.ToObject() == storage.ZeroID {
			// Not much to verify for a deletion request
			continue
		}
		if err := validateCommitOrTag(p, updinfo.ToObject(), updinfo.FromObject(), true, recurse); err != nil {
			return err
		}
	}
	return nil
}

func validateCommitOrTag(p storage.ProjectStorageDriver, commitstart storage.ObjectID, commitend storage.ObjectID, allowtag bool, recursive bool) error {
	if commitstart == commitend {
		// We assume that the refto is already in our database.
		// If it isn't, it will get rejected at the update
		return nil
	}

	objtype, _, r, err := p.ReadObject(commitstart)
	if err != nil {
		return err
	}
	defer r.Close()

	if objtype == storage.ObjectTypeCommit {
		return validateCommitContents(p, r, commitend, recursive)
	} else if allowtag && objtype == storage.ObjectTypeTag {
		return validateTag(p, r, commitend)
	} else if !allowtag {
		return errors.New("Tag object found in tag object?")
	}
	return errors.New("Non-commit-non-tag object passed in refto chain")
}

func validateTag(p storage.ProjectStorageDriver, r io.ReadCloser, commitend storage.ObjectID) error {
	taginf, err := readTag(r)
	if err != nil {
		return err
	}
	r.Close()

	if taginf.objecttype != storage.ObjectTypeCommit {
		return errors.New("Non-commit tag found")
	}
	if taginf.object == "" || !isValidRef(string(taginf.object)) {
		return errors.New("Non-valid tag object received")
	}
	if taginf.tagname == "" {
		return errors.New("Empty tagname received")
	}

	return validateCommitOrTag(p, taginf.object, storage.ZeroID, false, false)
}

func validateCommitContents(p storage.ProjectStorageDriver, r io.ReadCloser, commitend storage.ObjectID, recursive bool) error {
	cominf, err := readCommit(r)
	if err != nil {
		return err
	}
	r.Close()

	if cominf.tree != "" {
		if err := validateTree(p, cominf.tree, []storage.ObjectID{}); err != nil {
			return err
		}
	}

	if !recursive {
		// We expect that all contents have already been verified
		return nil
	}

	for _, parent := range cominf.parents {
		if err := validateCommitOrTag(p, parent, commitend, false, true); err != nil {
			return err
		}
	}

	return nil
}

func validateBlob(p storage.ProjectStorageDriver, blobid storage.ObjectID) error {
	objtype, _, r, err := p.ReadObject(blobid)
	if err != nil {
		return err
	}
	defer r.Close()
	if objtype != storage.ObjectTypeBlob {
		return errors.New("Not a blob object found")
	}
	return nil
}

func validateTree(p storage.ProjectStorageDriver, treeid storage.ObjectID, seentrees []storage.ObjectID) error {
	for _, seen := range seentrees {
		if treeid == seen {
			// We have already verified this tree
			return nil
		}
	}
	objtype, _, r, err := p.ReadObject(treeid)
	if err != nil {
		return err
	}
	defer r.Close()
	if objtype != storage.ObjectTypeTree {
		return err
	}
	treeinfo, err := readTree(r)
	if err != nil {
		return err
	}
	r.Close()

	for _, entry := range treeinfo.entries {
		if entry.isGitSubmodule() {
			// This is a Git submodule, it is very likely we won't have the commit actually, so let's
			// not validate it... The Git client itself will also silently ignore it anyway.
		} else if entry.mode.IsDir() {
			// This is a subtree
			if err := validateTree(p, entry.objectid, append(seentrees, treeid)); err != nil {
				return err
			}
		} else {
			if err := validateBlob(p, entry.objectid); err != nil {
				return err
			}
		}
	}

	return nil
}

// TODO: Replace objectIDSearch with a binary search tree
type objectIDSearcher interface {
	Add(o storage.ObjectID)
	Contains(o storage.ObjectID) bool
	List() []storage.ObjectID
}

func newObjectIDSearch() objectIDSearcher {
	return &listObjectIDSearch{[]storage.ObjectID{}}
}

func newObjectIDSearchFromSlice(o []storage.ObjectID) objectIDSearcher {
	return &listObjectIDSearch{o}
}

type listObjectIDSearch struct {
	s []storage.ObjectID
}

func (t *listObjectIDSearch) Add(o storage.ObjectID) {
	if t.Contains(o) {
		return
	}
	t.s = append(t.s, o)
}

func (t *listObjectIDSearch) Contains(o storage.ObjectID) bool {
	for _, oid := range t.s {
		if oid == o {
			return true
		}
	}
	return false
}

func (t *listObjectIDSearch) List() []storage.ObjectID {
	return t.s
}

func hasObject(p storage.ProjectStorageDriver, objid storage.ObjectID) (bool, error) {
	_, _, _, err := p.ReadObject(objid)
	if err == storage.ErrObjectNotFound {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func getCommonObjects(p storage.ProjectStorageDriver, objid storage.ObjectID, havesearch objectIDSearcher, commitsearch objectIDSearcher, commonobjects objectIDSearcher) error {
	if objid == storage.ZeroID {
		return errors.New("ZeroID encountered determining common")
	}
	if commonobjects.Contains(objid) {
		return nil
	} else if commitsearch.Contains(objid) {
		return nil
	}
	commonobjects.Add(objid)

	objtype, _, r, err := p.ReadObject(objid)
	if err != nil {
		return err
	}
	defer r.Close()

	if objtype == storage.ObjectTypeBlob {
		// Nothing to do for blobs
		return nil
	} else if objtype == storage.ObjectTypeTag {
		tag, err := readTag(r)
		if err != nil {
			return err
		}
		if tag.object == "" {
			return errors.New("Unknown tag object")
		}
		return getCommonObjects(p, tag.object, havesearch, commitsearch, commonobjects)
	} else if objtype == storage.ObjectTypeCommit {
		commit, err := readCommit(r)
		if err != nil {
			return err
		}
		if commit.tree != "" {
			if err := getCommonObjects(p, commit.tree, havesearch, commitsearch, commonobjects); err != nil {
				return err
			}
		}
		for _, parent := range commit.parents {
			if err := getCommonObjects(p, parent, havesearch, commitsearch, commonobjects); err != nil {
				return err
			}
		}
		return nil
	} else if objtype == storage.ObjectTypeTree {
		tree, err := readTree(r)
		if err != nil {
			return err
		}
		for _, entry := range tree.entries {
			if err := getCommonObjects(p, entry.objectid, havesearch, commitsearch, commonobjects); err != nil {
				return err
			}
		}
		return nil
	}
	return fmt.Errorf("Unexpected object type %s encountered", objtype)
}

func getAcks(p storage.ProjectStorageDriver, want storage.ObjectID, havesearch objectIDSearcher, commitsearch objectIDSearcher, commonobjects objectIDSearcher) ([]storage.ObjectID, error) {
	if commonobjects.Contains(want) {
		return []storage.ObjectID{}, nil
	} else if havesearch.Contains(want) {
		// From here on out, we assume that all recursive objects (trees, blobs) are known
		return []storage.ObjectID{want}, getCommonObjects(p, want, havesearch, commitsearch, commonobjects)
	} else if commitsearch.Contains(want) {
		return []storage.ObjectID{}, nil
	}

	// Send this current commit
	commitsearch.Add(want)

	objtype, _, r, err := p.ReadObject(want)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	if objtype == storage.ObjectTypeCommit {
		return getAcksForCommit(p, r, havesearch, commitsearch, commonobjects)
	} else if objtype == storage.ObjectTypeTag {
		return getAcksForTag(p, r, havesearch, commitsearch, commonobjects)
	} else {
		return nil, errors.New("Non-commit-non-ack traversed for ack")
	}
}

func getAcksForTag(p storage.ProjectStorageDriver, r io.Reader, havesearch objectIDSearcher, commitsearch objectIDSearcher, commonobjects objectIDSearcher) ([]storage.ObjectID, error) {
	tag, err := readTag(r)
	if err != nil {
		return nil, err
	}

	if tag.object == "" {
		return nil, errors.New("Unknown tag object")
	}

	return getAcks(p, tag.object, havesearch, commitsearch, commonobjects)
}

func getAcksForCommit(p storage.ProjectStorageDriver, r io.Reader, havesearch objectIDSearcher, commitsearch objectIDSearcher, commonobjects objectIDSearcher) ([]storage.ObjectID, error) {
	commit, err := readCommit(r)
	if err != nil {
		return nil, err
	}

	var acks []storage.ObjectID
	for _, parent := range commit.parents {
		parentacks, err := getAcks(p, parent, havesearch, commitsearch, commonobjects)
		if err != nil {
			return nil, err
		}
		acks = append(acks, parentacks...)
	}

	return acks, nil
}

func getAcksAndCommits(p storage.ProjectStorageDriver, wants []storage.ObjectID, haves []storage.ObjectID) (acks []storage.ObjectID, commits []storage.ObjectID, commonobjects objectIDSearcher, err error) {
	acks = []storage.ObjectID{}
	havesearch := newObjectIDSearchFromSlice(haves)
	commitsearch := newObjectIDSearch()
	commonobjects = newObjectIDSearch()

	for _, want := range wants {
		var newacks []storage.ObjectID
		newacks, err = getAcks(p, want, havesearch, commitsearch, commonobjects)
		if err != nil {
			return
		}
		if len(newacks) != 0 {
			acks = append(acks, newacks...)
		}
	}

	commits = commitsearch.List()
	return
}

func buildPackHeader(numo uint32) (buf []byte) {
	buf = make([]byte, 4+4+4)

	// Pack header
	buf[0] = 'P'
	buf[1] = 'A'
	buf[2] = 'C'
	buf[3] = 'K'
	// Pack version
	binary.BigEndian.PutUint32(buf[4:], 2)
	// Num objects
	binary.BigEndian.PutUint32(buf[8:], numo)

	return
}

type packedReportFunc func(numobjects int)

func writeTemporaryPackFile(r packedReportFunc, p storage.ProjectStorageDriver, commits []storage.ObjectID, commonobjects objectIDSearcher, recursive bool) (packfile *os.File, numobjects uint32, err error) {
	packfile, err = ioutil.TempFile("", "repospanner_pack_")
	if err != nil {
		return
	}
	defer func() {
		if err != nil {
			// We are going to return an error, clean up the file
			packfile.Close()
		} else {
			// Make sure to seek to the front of the file
			var off int64
			off, err = packfile.Seek(0, 0)
			if off != 0 {
				err = errors.New("Seek() did not go to the start of the file")
			}
		}
	}()
	// We want to unlink it as soon as possible, so that if something goes wrong, we don't keep it around
	err = os.Remove(packfile.Name())
	if err != nil {
		return
	}

	written := newObjectIDSearch()
	for _, commit := range commits {
		var numObjectsInCommit uint32
		numObjectsInCommit, err = writeCommitOrTagToPack(packfile, r, p, commit, written, commonobjects, recursive)
		if err != nil {
			return
		}
		numobjects += numObjectsInCommit
	}

	if len(written.List()) != int(numobjects) {
		return nil, 0, fmt.Errorf("written != numwritten: %d != %d", len(written.List()), numobjects)
	}

	return
}

func flushToFrom(w io.Writer, r io.Reader, expected uint) (int, error) {
	totalsent := 0
	var buf [1024]byte
	for {
		r, err := r.Read(buf[:])
		if err == io.EOF {
			break
		}
		if err != nil {
			return totalsent, err
		}
		w, err := w.Write(buf[:r])
		if err != nil {
			return totalsent, err
		}
		if w != r {
			return totalsent, errors.New("Not full object written")
		}
		totalsent += w
	}
	if expected != 0 && uint(totalsent) != expected {
		return totalsent, errors.New("Not expected number of bytes flushed")
	}
	return totalsent, nil
}

func writeObjectHeader(w io.Writer, objtype storage.ObjectType, objsize uint) error {
	var buf []byte

	lastbyte := byte((objtype)<<4) | byte(objsize&0xF)
	objsize = objsize >> 4
	for objsize != 0 {
		lastbyte = lastbyte | 0x80
		buf = append(buf, lastbyte)
		lastbyte = byte(objsize & 0x7F)
		objsize = objsize >> 7
	}
	buf = append(buf, lastbyte)

	out, err := w.Write(buf)
	if err != nil {
		return err
	}
	if out != len(buf) {
		return errors.New("Not full object header written")
	}

	return nil
}

func writeTreeToPack(w io.Writer, rep packedReportFunc, p storage.ProjectStorageDriver, treeid storage.ObjectID, written objectIDSearcher, commonobjects objectIDSearcher) (uint32, error) {
	if written.Contains(treeid) {
		// This tree was probably already somewhere else in the chain, and has already been sent
		return 0, nil
	}
	if commonobjects.Contains(treeid) {
		// This tree was determined to be already on the client
		return 0, nil
	}
	rep(1)
	written.Add(treeid)

	objtype, objsize, r, err := p.ReadObject(treeid)
	if err != nil {
		return 0, err
	}
	if objtype != storage.ObjectTypeTree {
		return 0, fmt.Errorf("Objects %s not a tree?", treeid)
	}
	err = writeObjectHeader(w, objtype, objsize)
	if err != nil {
		return 0, err
	}
	zwriter := zlib.NewWriter(w)
	treader := &treeReader{r: r}

	_, err = flushToFrom(zwriter, treader, objsize)
	if err != nil {
		return 0, err
	}
	zwriter.Flush()
	zwriter.Close()

	if len(treader.info.entries) == 0 && objsize != 0 {
		// This should indicate that we have made an error parsing the
		// tree object.
		return 0, errors.Errorf("Tree object %s misparsed", treeid)
	}

	// We start at 1 entry: we just sent ourselves
	var entriessent uint32 = 1
	for _, entry := range treader.info.entries {
		if entry.isGitSubmodule() {
			numsent, err := writeCommitOrTagToPack(w, rep, p, entry.objectid, written, commonobjects, true)
			if err == nil {
				entriessent += numsent
			} else if err == storage.ErrObjectNotFound {
				// This could be because we don't have the Git objects...
				// Git clients should be able to get these back from the .gitmodules source repo.
			} else {
				return entriessent, err
			}
		} else if entry.mode.IsDir() {
			// This is a subtree
			numsent, err := writeTreeToPack(w, rep, p, entry.objectid, written, commonobjects)
			if err != nil {
				return entriessent, err
			}
			entriessent += numsent
		} else {
			sent, err := writeBlobToPack(w, p, entry.objectid, written, commonobjects)
			if err != nil {
				return entriessent, err
			}
			if sent {
				entriessent++
			}
		}
	}

	return entriessent, nil
}

func writeBlobToPack(w io.Writer, p storage.ProjectStorageDriver, blobid storage.ObjectID, written objectIDSearcher, commonobjects objectIDSearcher) (sent bool, err error) {
	if written.Contains(blobid) {
		return
	}
	if commonobjects.Contains(blobid) {
		return
	}
	written.Add(blobid)
	sent = true

	objtype, objsize, r, err := p.ReadObject(blobid)
	if err != nil {
		return sent, err
	}
	if objtype != storage.ObjectTypeBlob {
		err = errors.New("Non-blob sending as blob")
		return
	}
	err = writeObjectHeader(w, objtype, objsize)
	if err != nil {
		return
	}
	zwriter := zlib.NewWriter(w)

	_, err = flushToFrom(zwriter, r, objsize)
	if err != nil {
		return
	}
	zwriter.Flush()
	zwriter.Close()

	return
}

func writeCommitOrTagToPack(w io.Writer, rep packedReportFunc, p storage.ProjectStorageDriver, commitid storage.ObjectID, written objectIDSearcher, commonobjects objectIDSearcher, recursive bool) (uint32, error) {
	if written.Contains(commitid) {
		return 0, nil
	} else if commonobjects.Contains(commitid) {
		return 0, nil
	}
	rep(1)

	objtype, objsize, r, err := p.ReadObject(commitid)
	if err != nil {
		return 0, err
	}
	if objtype != storage.ObjectTypeCommit && objtype != storage.ObjectTypeTag {
		return 0, fmt.Errorf("Object %s not a commit or tag?", commitid)
	}
	written.Add(commitid)

	err = writeObjectHeader(w, objtype, objsize)
	if err != nil {
		return 0, err
	}
	zwriter := zlib.NewWriter(w)
	creader := &headerObjectReader{r: r}

	_, err = flushToFrom(zwriter, creader, objsize)
	if err != nil {
		return 0, err
	}
	zwriter.Flush()
	zwriter.Close()

	// We start at 1 object: We just wrote a commit or tag
	var numwritten uint32 = 1

	if objtype == storage.ObjectTypeCommit {
		cominf := parseCommitInfo(creader.headers)
		if cominf.tree != "" {
			written, err := writeTreeToPack(w, rep, p, cominf.tree, written, commonobjects)
			if err != nil {
				return 0, err
			}
			numwritten += written
		}
		if recursive {
			for _, parent := range cominf.parents {
				written, err := writeCommitOrTagToPack(w, rep, p, parent, written, commonobjects, true)
				if err != nil {
					return 0, err
				}
				numwritten += written
			}
		}
	} else if objtype == storage.ObjectTypeTag {
		taginf := parseTagInfo(creader.headers)
		if taginf.object != "" {
			written, err := writeCommitOrTagToPack(w, rep, p, taginf.object, written, commonobjects, recursive)
			if err != nil {
				return 0, err
			}
			numwritten += written
		}
	}

	return numwritten, nil
}
