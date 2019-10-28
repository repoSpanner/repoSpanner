package storage

import (
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
)

// ObjectID is a Git Object ID (sha1 digest in hex format without prefix)
type ObjectID string

// ZeroID is the "null" object ID, indicating there's no object here
// Git uses this for example in update requests where a new ref is created (source ZeroID) or delete (destination ZeroID)
const ZeroID ObjectID = "0000000000000000000000000000000000000000"

// ObjectIDLength defines the length of an Object ID
const ObjectIDLength = 40

// ObjectIDFromRaw parses the bytes of a sha1 digest and returns the standard object ID format representation
func ObjectIDFromRaw(rawid []byte) ObjectID {
	return ObjectID(hex.EncodeToString(rawid))
}

type Reference string

// ObjectType represents the type of a Git Object
// In Pack files, this is represented by 3 bits.
type ObjectType int

const (
	// ObjectTypeBad is an invalid object type outside of specifically defined internal API cases and should result in an error when used anywhere else
	ObjectTypeBad ObjectType = -1
	// ObjectTypeNone should never occur, as it's an encoded value for invalid object
	ObjectTypeNone ObjectType = 0
	// ObjectTypeCommit encodes a "Commit" object
	ObjectTypeCommit ObjectType = 1
	// ObjectTypeTree encodes a "tree" object
	ObjectTypeTree ObjectType = 2
	// ObjectTypeBlob encodes a "blob" object
	ObjectTypeBlob ObjectType = 3
	// ObjectTypeTag encodes a "tag" object
	ObjectTypeTag ObjectType = 4

	// 5 is "for future expansion"

	// ObjectTypeOfsDelta encodes an "offset delta" object
	// These are deltas that refer to another object in the same pack by offset from the base
	ObjectTypeOfsDelta ObjectType = 6
	// ObjectTypeRefDelta encodes a "reference delta" object
	// These are deltas that refer to another object in the same repository by encoding the base object ID
	ObjectTypeRefDelta ObjectType = 7
	// ObjectTypeAny should never occur, as it's an encoded value for "any" object (anything goes), which we don't want
	ObjectTypeAny ObjectType = 8
	// ObjectTypeMax should never occur, as it's higher than available in the 3 bits reserved for object types
	ObjectTypeMax ObjectType = 9
)

func (o ObjectType) String() string {
	return o.HdrName()
}

// HdrName returns the name to use in a "tree" object file to indicate object type
func (o ObjectType) HdrName() string {
	switch o {
	case ObjectTypeCommit:
		return "commit"
	case ObjectTypeTree:
		return "tree"
	case ObjectTypeBlob:
		return "blob"
	case ObjectTypeTag:
		return "tag"
	case ObjectTypeRefDelta:
		return "refdelta"
	default:
		panic(fmt.Errorf("Invalid object type %d requested", o))
	}
}

// ObjectTypeFromHdrName returns the ObjectType that encodes the HdrName requested
func ObjectTypeFromHdrName(hdrname string) ObjectType {
	switch hdrname {
	case "commit":
		return ObjectTypeCommit
	case "tree":
		return ObjectTypeTree
	case "blob":
		return ObjectTypeBlob
	case "tag":
		return ObjectTypeTag
	case "refdelta":
		return ObjectTypeRefDelta
	default:
		panic(fmt.Errorf("Invalid object header %s requested", hdrname))
	}
}

// StorageDriver represents the highest layer of an initialized storage driver
// It is only used to get project-scoped references
type StorageDriver interface {
	// GetProjectStorage returns a scoped instance for a particular project
	GetProjectStorage(project string) ProjectStorageDriver
}

// ErrObjectNotFound is an error encoding an object was not found by the storage driver
var ErrObjectNotFound = errors.New("Object not found")

// ProjectStorageDriver is a scoped storage driver instance to a single project
// SECURITY PROPERTY: a ProjectStorageDriver should not be able to find any objects that were never pushed to this specific project,
// so if object A was pushed to project X but not Y, running a ReadObject() from a ProjectStorageDriver scoped to project Y *must*
// return ErrObjectNotFound. This avoids possible attacks for getting objects from repositories an attacker should not have access to:
// - Perform a Pull operation where the "want" list contains the object ID
// - Perform a Push where one of the Commit objects points to the other object as its Parent or Tree, which would result in us returning
//   that object on the next pull
type ProjectStorageDriver interface {
	// ReadObject is used to read the Object objectid, and returns the object's type, size and a ReadCloser (make sure to close after reading)
	ReadObject(objectid ObjectID) (ObjectType, uint, io.ReadCloser, error)
	// ListObjects returns an iterator over the objects in the project
	ListObjects() ProjectStorageObjectLister

	// GetPusher initializes a ProjectStoragePushDriver to initiate a new object push
	GetPusher(pushuuid string) ProjectStoragePushDriver
}

// ProjectStorageObjectLister is an iterator over the objects in a project
type ProjectStorageObjectLister interface {
	Objects() <-chan ObjectID
	Err() error
}

// ProjectStoragePushDriver is a project-scoped storage driver instance that is accepting new object pushes
type ProjectStoragePushDriver interface {
	// StageObject is a function that creates a StagedObject instance for creating a new object in storage
	StageObject(objtype ObjectType, objsize uint) (StagedObject, error)
	// GetPushResultChannel returns a channel that is used to return error conditions to the push controller
	GetPushResultChannel() <-chan error
	// Done is a function that gets called after the last object for this push was submitted to the storage driver
	Done()
}

// StagedObject is an object that is currently being written to storage
// NOTE: Finalize *must* be called before Close is called! Otherwise the underlying storage implementation must toss the object
// and regard it as "incomplete".
type StagedObject interface {
	io.Writer
	io.Closer
	// Finalize informs the storage driver the full object was written via the Write function, and an object ID can be computed.
	// The provided objid will be compared to the computed object ID, and if they don't match, Finalize returns an error.
	// This can be used if the object ID is known, so we can be sure that the correct object was written to disk.
	Finalize(objid ObjectID) (ObjectID, error)
}

// InitializeStorageDriver initializes the storage driver configured and returns an initialized instance
func InitializeStorageDriver(config map[string]string) (StorageDriver, error) {
	clustered, ok := config["clustered"]
	if !ok || clustered != "false" {
		// Unless "clustered" is set to "false", we actually return a clusteredStorageDriver
		// with the real backend
	}
	storagetype, ok := config["type"]
	if !ok {
		return nil, errors.New("No storage driver type provided")
	}

	switch storagetype {
	case "tree":
		dirname, ok := config["directory"]
		if !ok {
			return nil, errors.New("No directory provided")
		}
		compressmethod, ok := config["compressmethod"]
		if !ok {
			compressmethod = "none"
		}
		return newTreeStoreDriver(dirname, compressmethod)
	default:
		return nil, errors.New("Unknown storage driver " + storagetype)
	}
}

// oidWriter is a Writer that will, while an object is being written, compute the Object ID
type oidWriter struct {
	hasher hash.Hash
	inner  io.WriteCloser
	writer io.Writer
}

func (w *oidWriter) Write(buf []byte) (int, error) {
	return w.writer.Write(buf)
}

func (w *oidWriter) Close() error {
	return w.inner.Close()
}

// getObjectID computes the Object ID of the object that was written via the oidWriter.
// This must be called after Close() is called, to make sure buffers are flushed.
func (w *oidWriter) getObjectID() ObjectID {
	return ObjectIDFromRaw(w.hasher.Sum(nil))
}

// createOidWriter creates a new oidWriter instance
func createOidWriter(objtype ObjectType, objsize uint, inner io.WriteCloser) *oidWriter {
	hasher := sha1.New()
	fmt.Fprintf(hasher, "%s %d\x00", objtype.HdrName(), objsize)

	w := &oidWriter{
		hasher: hasher,
		inner:  inner,
		writer: io.MultiWriter(hasher, inner),
	}

	return w
}
