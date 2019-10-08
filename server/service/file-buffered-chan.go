/* Provides a service that accepts objects via a chan and publishes them again via a second chan.
   Objects are buffered with a file on disk rather than in memory. This allows senders to send
   objects faster than receivers can process them without consuming much memory. Note that this is
   not intended to achieve persistence, as a program crash will still lose objects. It is solely
   intended for fast publishing with low memory usage. */

package service

import (
	"bufio"
	"errors"
	"fmt"
	"os"

	"repospanner.org/repospanner/server/storage"
)

type objectIDBufferedChan struct {
	// The path of the file backing the chan
	path string
	// A chan we can send errors into when there are problems
	errors chan<- error
	// Internally, this is the chan we receive objects on. Note that this
	// means that the external API users will be sending with it.
	recv <-chan storage.ObjectID
	// Internally, this is the chan we send objects on. Note that this
	// means that the external API users will be receiving with it.
	send chan<- storage.ObjectID
	// This chan is used to send open files from the writer to the
	// reader.
	files chan os.File
}

// NewObjectIDBufferedChan initializes the buffered chan. The path is a path to a file that can be
// used to temporarily buffer objects to storage to avoid keeping them in memory. errors is a chan
// that is used to send any errors encountered during operation, and will not be closed by the
// buffered chan when done. The two chans that are returned are meant for the caller to use to send
// and receive, respectively.
func NewObjectIDBufferedChan(path string, errors chan<- error) (chan<- storage.ObjectID, <-chan storage.ObjectID) {
	send := make(chan storage.ObjectID, 100)
	recv := make(chan storage.ObjectID, 100)
	ochan := objectIDBufferedChan{
		path:   path,
		errors: errors,
		recv:   recv,
		send:   send,
		files:  make(chan os.File, 100),
	}
	go ochan.read()
	go ochan.write()

	return recv, send
}

// This is intended to be run as a goroutine, reading from ochan.recv and writing what it sees
// to path. When it's written enough objects, it passes the file
// handle to path to the ochan.files chan to be read by read() later. It is responsible for closing
// the files chan when done.
func (ochan *objectIDBufferedChan) write() {
	defer close(ochan.files)

	file, writer, err := ochan.startFile()
	if err != nil {
		ochan.errors <- err
		// Since we encountered an error we should stop.
		return
	}

	// This is a counter to keep track of how many objects we've written into the current file
	// handle so that we know when to start a new file.
	numRecvd := 0
	for oid := range ochan.recv {
		fmt.Fprintf(writer, "%s\n", oid)
		// The number chosen here is a bit arbitary. If we make it too
		// small, we end up creating too many file handles. If we make it
		// too big, we increase the lag of objects from input to output.
		if numRecvd++; numRecvd >= 10000 {
			// We've now written enough objects to the file to start a new one. Send this file off
			// to a nice farm upstate (read()), reset our counter, and start a new file.
			err = ochan.finishFile(writer, file)
			if err != nil {
				ochan.errors <- err
				// Since we encountered an error we should stop.
				return
			}
			numRecvd = 0
			file, writer, err = ochan.startFile()
			if err != nil {
				ochan.errors <- err
				// Since we encountered an error we should stop.
				return
			}
		}
	}
	err = ochan.finishFile(writer, file)
	if err != nil {
		ochan.errors <- err
		// Since we encountered an error we should stop.
		return
	}
}

// Open a new buffer file. Returns a pointer to the file handle, a pointer to a Writer that can be
// used to buffer writes to the file, and an error, if any.
func (ochan *objectIDBufferedChan) startFile() (*os.File, *bufio.Writer, error) {
	file, err := os.Create(ochan.path)
	if err != nil {
		return nil, nil, err
	}

	// By removing the file immediately after creating it, we ensure that
	// we won't accidentally leave files lying around on disk. It also
	// allows us to reuse the same filename each time.
	err = os.Remove(ochan.path)
	if err != nil {
		return nil, nil, err
	}

	w := bufio.NewWriter(file)
	return file, w, err
}

// Flush the file's writer buffer and send the file to the files chan so
// the reader can start processing it.
func (ochan *objectIDBufferedChan) finishFile(w *bufio.Writer, file *os.File) error {
	err := w.Flush()
	if err != nil {
		return err
	}

	ochan.files <- *file
	return nil
}

// This is intended to be run as a goroutine and receives from ochan.file for file handles that
// are ready to be read for objects to send into ochan.send. It is responsible for closing the send
// channel before exiting.
func (ochan *objectIDBufferedChan) read() {
	defer close(ochan.send)

	for file := range ochan.files {
		err := ochan.readFile(file)
		if err != nil {
			ochan.errors <- err
			// Since we encountered an error we should stop.
			return
		}
	}
}

// Read an individual file handle, send its contents into the send
// chan, and close it.
func (ochan *objectIDBufferedChan) readFile(file os.File) error {
	// We don't need to remove the file here because the file was
	// unlinked right after we opened it. The kernel will remove it
	// once we close it here.
	defer file.Close()

	offset, err := file.Seek(0, 0)
	if err != nil || offset != 0 {
		return errors.New("Unable to seek to beginning of file")
	}

	scanner := bufio.NewScanner(&file)
	for scanner.Scan() {
		ochan.send <- storage.ObjectID(scanner.Text())
	}

	err = scanner.Err()
	if err != nil {
		return err
	}

	return nil
}
