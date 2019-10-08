package service

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"repospanner.org/repospanner/server/storage"
)

// Test that error handling works appropriately if the tempfile path does not exist
func TestObjectIDBufferedChan_dne(t *testing.T) {
	errors := make(chan error, 10)

	// Let's try to write a file to a path that does not exist
	send, recv := NewObjectIDBufferedChan("/doesnotexist/repospanner_oidbufferedchan.test", errors)

	send <- "test"
	close(send)
	var values []storage.ObjectID
	for value := range recv {
		values = append(values, value)
	}

	if len(values) != 0 {
		t.Fatalf("Wrong value received. Expected [], but received %s", values)
	}

	// There should be exactly one error in the errors chan.
	if len(errors) != 1 {
		t.Fatalf("Expected exactly 1 error, but there were %d errors instead", len(errors))
	}
	err := <-errors
	if err.Error() != "open /doesnotexist/repospanner_oidbufferedchan.test: no such file or directory" {
		t.Fatalf("Incorrect error message: %s", err.Error())
	}

	// write() stops when it can't open the file, so the send chan should still have our object in
	// it.
	if len(send) != 1 {
		t.Fatalf("write() should have halted but it continued")
	}
}

// This is the simplest test case - we send one object in and ensure that
// the same object comes back out.
func TestObjectIDBufferedChan_one_objectid(t *testing.T) {
	errors := make(chan error, 10)

	send, recv := NewObjectIDBufferedChan("/tmp/repospanner_oidbufferedchan.test", errors)

	send <- "test"
	close(send)
	var values []storage.ObjectID
	for value := range recv {
		values = append(values, value)
	}

	if !reflect.DeepEqual(values, []storage.ObjectID{"test"}) {
		t.Fatalf("Wrong value received. Expected {\"test\"}, but received %s", values)
	}

	// Ensure there were no errors in the errors chan.
	if len(errors) != 0 {
		t.Fatalf("There was an error in the errors chan")
	}
}

// Test with a fast writer and a slow reader. The writer sends enough
// objects to require several files to be written, and the reader sleeps
// to simulate a slow read.
func TestObjectIDBufferedChan_slow_reader(t *testing.T) {
	errors := make(chan error, 10)

	send, recv := NewObjectIDBufferedChan("/tmp/repospanner_oidbufferedchan.test", errors)

	var expectedValues []storage.ObjectID
	// The number to send here is a bit arbitary, but the goal is to make
	// sure we fill up a few files worth of objects to ensure that the
	// reader and writer goroutines are properly passing the data into and
	// out of the files. We also want to use a number that ends up with
	// a file that is partially full (i.e., we don't want to use a
	// mulitple of the number of objects per file that the writer is
	// using so we can make sure the last partial set is handled
	// correctly.)
	go func() {
		for i := 0; i < 10042; i++ {
			value := storage.ObjectID(fmt.Sprintf("%d", i))
			send <- value
			expectedValues = append(expectedValues, value)
		}
		close(send)
	}()
	var values []storage.ObjectID
	done := make(chan int)
	go func() {
		for value := range recv {
			values = append(values, value)
			time.Sleep(time.Microsecond)
		}
		close(done)
	}()

	<-done

	if !reflect.DeepEqual(values, expectedValues) {
		t.Fatalf("Wrong value received. Expected %#v, but received %#v", expectedValues, values)
	}

	// Ensure there were no errors in the errors chan.
	if len(errors) != 0 {
		t.Fatalf("There was an error in the errors chan")
	}
}
