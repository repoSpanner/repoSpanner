package service

import (
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"syscall"

	"repospanner.org/repospanner/server/storage"
)

type commitInfo struct {
	parents []storage.ObjectID
	tree    storage.ObjectID
}

type treeEntry struct {
	mode     os.FileMode
	name     string
	objectid storage.ObjectID
}

const s_IFGITLINK = 0160000

func (t treeEntry) isGitSubmodule() bool {
	return (t.mode & syscall.S_IFMT) == s_IFGITLINK
}

type treeInfo struct {
	entries []treeEntry
}

type tagInfo struct {
	object     storage.ObjectID
	objecttype storage.ObjectType
	tagname    string
}

type headerObjectReader struct {
	headers map[string][]string

	r          io.Reader
	postheader bool
	curline    string
}

func (r *headerObjectReader) updateHeader(header, val string) {
	if r.headers == nil {
		r.headers = make(map[string][]string)
	}

	curval, exists := r.headers[header]
	if !exists {
		r.headers[header] = []string{val}
	} else {
		r.headers[header] = append(curval, val)
	}
}

func (r *headerObjectReader) parseLine() error {
	if r.curline == "" {
		// Empty line -> end of header
		r.postheader = true
		return nil
	}

	split := strings.Split(r.curline, " ")
	if len(split) < 2 {
		return fmt.Errorf("Line %s not valid header", r.curline)
	}

	r.updateHeader(split[0], strings.Join(split[1:], " "))
	return nil
}

func (r *headerObjectReader) ParseFullHeader() error {
	// Call this function if you're not interested in the contents, and just want the header parsed
	for {
		buf := make([]byte, 1024)
		_, err := r.Read(buf)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

func (r *headerObjectReader) Read(buf []byte) (read int, err error) {
	if r.postheader {
		// If we parsed the header entirely, we no longer need to parse anything
		in, err := r.r.Read(buf)
		if err != nil {
			return in, err
		}
		return in, err
	}
	read, err = r.r.Read(buf)
	if err != nil && err != io.EOF {
		return
	}

	toparse := buf[:read]
	var i int
	for i < len(toparse) {
		if toparse[i] == '\n' {
			err = r.parseLine()
			if err != nil {
				return
			}
			r.curline = ""
		} else {
			r.curline += string(toparse[i])
		}
		if r.postheader {
			// This block contained the end-of-header. Return everything
			return
		}
		i++
	}
	return
}

func parseTagInfo(headers map[string][]string) tagInfo {
	var info tagInfo
	if headers == nil {
		return info
	}

	object, hasobject := headers["object"]
	if hasobject {
		if len(object) > 1 {
			panic("More than 1 object in tag?")
		}
		info.object = storage.ObjectID(object[0])
	}

	objtype, hastype := headers["type"]
	if hastype {
		if len(objtype) > 1 {
			panic("More than 1 object type in tag?")
		}
		info.objecttype = storage.ObjectTypeFromHdrName(objtype[0])
	}

	tag, hastag := headers["tag"]
	if hastag {
		if len(tag) > 1 {
			panic("More than 1 tagname?")
		}
		info.tagname = tag[0]
	}

	return info
}

func parseCommitInfo(headers map[string][]string) commitInfo {
	var info commitInfo
	if headers == nil {
		return info
	}

	tree, hastree := headers["tree"]
	if hastree {
		if len(tree) > 1 {
			panic("Commit with more than 1 tree?")
		}
		info.tree = storage.ObjectID(tree[0])
	}
	parents, hasparents := headers["parent"]
	if hasparents {
		for _, parent := range parents {
			info.parents = append(info.parents, storage.ObjectID(parent))
		}
	}
	return info
}

func readCommit(r io.Reader) (info commitInfo, err error) {
	reader := headerObjectReader{r: r}
	err = reader.ParseFullHeader()
	info = parseCommitInfo(reader.headers)
	return
}

func readTag(r io.Reader) (info tagInfo, err error) {
	reader := headerObjectReader{r: r}
	err = reader.ParseFullHeader()
	info = parseTagInfo(reader.headers)
	return
}

type treeReader struct {
	r io.Reader

	info treeInfo

	buffer []byte

	currentMode os.FileMode
	currentName string
}

func (t *treeReader) resetEntry() {
	t.currentMode = 0
	t.currentName = ""
}

func (t *treeReader) parseEntries() error {
	for {
		if t.currentMode == 0 {
			// Read until first space, expect <=7 characters
			modebuf := make([]byte, 8)
			var i int
			for {
				if i >= 8 {
					return errors.New("Mode was more than expected length?")
				}
				if len(t.buffer) < i+1 {
					// Not full mode received yet
					return nil
				}
				if t.buffer[i] == ' ' {
					// End of mode
					break
				}
				modebuf[i] = t.buffer[i]
				i++
			}
			t.buffer = t.buffer[i+1:]
			mode, err := strconv.ParseInt(string(modebuf[:i]), 8, 32)
			if err != nil {
				return err
			}

			// Git thinks it's being funny with inventing its own mode bits....
			if (mode & syscall.S_IFMT) == s_IFGITLINK {
				// This is a Git submodule link....
			} else if (mode & 0040000) != 0 {
				mode = (mode & ^0040000) | int64(os.ModeDir)
			}

			t.currentMode = os.FileMode(mode)
		}
		if t.currentName == "" {
			if len(t.buffer) < 2 {
				// Not enough bytes for a filename + null
				return nil
			}
			tempname := ""
			var i int
			for {
				if i >= len(t.buffer) {
					// We didn't have the null byte yet
					return nil
				}
				if t.buffer[i] == '\x00' {
					// Found the null
					break
				}
				tempname += string(t.buffer[i])
				i++
			}
			t.buffer = t.buffer[i+1:]
			t.currentName = tempname
		}
		if len(t.buffer) < 20 {
			// Not enough bytes for 20-byte object ID
			return nil
		}
		objectid := hex.EncodeToString(t.buffer[:20])
		if !isValidRef(objectid) {
			return fmt.Errorf("Invalid object ID '%s' in tree", objectid)
		}
		t.buffer = t.buffer[20:]
		// We havce a full entry!
		entry := treeEntry{
			mode:     t.currentMode,
			name:     t.currentName,
			objectid: storage.ObjectID(objectid),
		}

		t.info.entries = append(t.info.entries, entry)
		t.resetEntry()

		if len(t.buffer) <= 0 {
			// No more bytes to read for now. More coming in the future!
			return nil
		}
	}
}

func (t *treeReader) Read(buf []byte) (int, error) {
	read, rerr := t.r.Read(buf)
	if rerr != nil && rerr != io.EOF {
		return 0, rerr
	}
	if rerr == io.EOF && (t.currentMode != 0 || t.currentName != "") {
		// Trailing filemode or trailing currentName are no good
		return 0, errors.New("Invalid tree object")
	}

	t.buffer = append(t.buffer, buf[:read]...)
	err := t.parseEntries()
	if err != nil {
		return 0, err
	}
	return read, rerr
}

func (t *treeReader) ParseFullTree() error {
	// Call this function if you're not interested in the contents, and just want the parsed tree
	for {
		buf := make([]byte, 1024)
		_, err := t.Read(buf)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

func readTree(r io.Reader) (info treeInfo, err error) {
	treader := &treeReader{r: r}
	err = treader.ParseFullTree()
	info = treader.info
	return
}
