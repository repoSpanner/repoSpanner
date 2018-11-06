package storage

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"testing"
)

func expectObjects(t *testing.T, d ProjectStorageDriver, allowextra bool, oids ...ObjectID) {
	t.Log("Looking for objects: ", oids)
	l := d.ListObjects()
	for obj := range l.Objects() {
		t.Log("Got object: ", obj)
		found := -1
		for nr, oid := range oids {
			if oid == obj {
				found = nr
			}
		}
		if found == -1 && !allowextra {
			t.Fatal("Extra object found: ", obj)
			return
		}
		oids = append(oids[:found], oids[found+1:]...)
	}
	if l.Err() != nil {
		t.Fatal("Error listing objects: ", l.Err())
		return
	}
	if len(oids) != 0 {
		t.Fatal("Objects not found: ", oids)
		return
	}
}

func testStorageDriver(name string, instance StorageDriver, t *testing.T) {
	// No objects stored yet. This should ay NoSuchObjectError
	p1 := instance.GetProjectStorage("test/project1")
	_, _, _, err := p1.ReadObject("object1")
	if err != ErrObjectNotFound {
		t.Fatal("New Object ID did not return expected error")
	}

	// Write a first object
	p1w := p1.GetPusher("")
	staged, err := p1w.StageObject(ObjectTypeBlob, 4)
	if err != nil {
		t.Fatal(fmt.Sprintf("StageObject returned error: %s", err))
	}
	n, err := staged.Write([]byte("test"))
	if err != nil {
		t.Fatal(fmt.Sprintf("Staged write returned error: %s", err))
	}
	if n != 4 {
		t.Fatal(fmt.Sprintf("Staged writed did not write expected number of bytes: %d", n))
	}
	_, err = staged.Finalize("30d74d258442c7c65512eafab474568dd706c430")
	if err != nil {
		t.Fatal(fmt.Sprintf("Unexpected error from finalizing: %s", err))
	}
	err = staged.Close()
	if err != nil {
		t.Fatal(fmt.Sprintf("Unexpected error on close: %s", err))
	}
	doneC := p1w.GetPushResultChannel()
	select {
	case err := <-doneC:
		t.Fatal(fmt.Sprintf("Unexpected error on close channel: %s", err))
	default:
		// No message is good here
		break
	}
	p1w.Done()
	select {
	case err, isopen := <-doneC:
		if err != nil {
			t.Fatal(fmt.Sprintf("Unexpected error when we expected close: %s", err))
		}
		if !isopen {
			// Channel closed correctly
			break
		}
	default:
		t.Fatal("Done channel not closed")
	}
	expectObjects(t, p1, false, "30d74d258442c7c65512eafab474568dd706c430")

	// Re-write the first object
	p1w = p1.GetPusher("")
	staged, err = p1w.StageObject(ObjectTypeBlob, 4)
	if err != nil {
		t.Fatal(fmt.Sprintf("StageObject returned error: %s", err))
	}
	n, err = staged.Write([]byte("test"))
	if err != nil {
		t.Fatal(fmt.Sprintf("Staged write returned error: %s", err))
	}
	if n != 4 {
		t.Fatal(fmt.Sprintf("Staged writed did not write expected number of bytes: %d", n))
	}
	_, err = staged.Finalize("30d74d258442c7c65512eafab474568dd706c430")
	if err != nil {
		t.Fatal(fmt.Sprintf("Unexpected error from finalizing: %s", err))
	}
	err = staged.Close()
	if err != nil {
		t.Fatal(fmt.Sprintf("Unexpected error on close: %s", err))
	}
	doneC = p1w.GetPushResultChannel()
	select {
	case err := <-doneC:
		t.Fatal(fmt.Sprintf("Unexpected error on close channel: %s", err))
	default:
		// No message is good here
		break
	}
	p1w.Done()
	select {
	case err, isopen := <-doneC:
		if err != nil {
			t.Fatal(fmt.Sprintf("Unexpected error when we expected close: %s", err))
		}
		if !isopen {
			// Channel closed correctly
			break
		}
	default:
		t.Fatal("Done channel not closed")
	}
	expectObjects(t, p1, false, "30d74d258442c7c65512eafab474568dd706c430")

	staged, err = p1w.StageObject(ObjectTypeBlob, 5)
	if err != nil {
		t.Fatal(fmt.Sprintf("StageObject returned error: %s", err))
	}
	n, err = staged.Write([]byte("test1"))
	if err != nil {
		t.Fatal(fmt.Sprintf("Staged write returned error: %s", err))
	}
	if n != 5 {
		t.Fatal(fmt.Sprintf("Staged writed did not write expected number of bytes: %d", n))
	}
	_, err = staged.Finalize("30d74d258442c7c65512eafab474568dd706c430")
	if err.Error() != "Calculated object does not match provided: f079749c42ffdcc5f52ed2d3a6f15b09307e975e != 30d74d258442c7c65512eafab474568dd706c430" {
		t.Fatal(fmt.Sprintf("Unexpected error returned on close: %s", err))
	}
	err = staged.Close()
	if err != nil {
		t.Fatal(fmt.Sprintf("Unexpected error on close: %s", err))
	}

	objtype, objsize, objr, err := p1.ReadObject("30d74d258442c7c65512eafab474568dd706c430")
	if err != nil {
		t.Fatal(fmt.Sprintf("Error when getting just-written object: %s", err))
	}
	if objsize != 4 {
		t.Fatal(fmt.Sprintf("Incorrect number of bytes returned: %d != 4", objsize))
	}
	if objtype != ObjectTypeBlob {
		t.Fatal(fmt.Sprintf("Incorrect object type returned: %s != ObjectTypeBlob", objtype))
	}
	buf := make([]byte, 6)
	n, err = objr.Read(buf)
	if err != nil && err != io.EOF {
		t.Fatal(fmt.Sprintf("Error reading object: %s", err))
	}
	if n != 4 {
		t.Fatal(fmt.Sprintf("Unexpected number of bytes read: %d", n))
	}
	if buf[0] != 't' || buf[1] != 'e' || buf[2] != 's' || buf[3] != 't' {
		t.Fatal(fmt.Sprintf("Incorrect object returned: %s", buf))
	}
	err = objr.Close()
	if err != nil {
		t.Fatal(fmt.Sprintf("Error when closing read object: %s", err))
	}

	// Create a second project
	p2 := instance.GetProjectStorage("test/project2")
	p2w := p2.GetPusher("")
	staged, err = p2w.StageObject(ObjectTypeBlob, 4)
	if err != nil {
		t.Fatal(fmt.Sprintf("StageObject returned error: %s", err))
	}
	n, err = staged.Write([]byte("foo"))
	if err != nil {
		t.Fatal(fmt.Sprintf("Staged write returned error: %s", err))
	}
	if n != 3 {
		t.Fatal(fmt.Sprintf("Staged writed did not write expected number of bytes: %d", n))
	}
	_, err = staged.Finalize("5c40945c981bd37a6912f5e2d3b2ceabfec5452a")
	if err != nil {
		t.Fatal(fmt.Sprintf("Unexpected error from finalizing: %s", err))
	}
	err = staged.Close()
	if err != nil {
		t.Fatal(fmt.Sprintf("Unexpected error on close: %s", err))
	}
	doneC = p2w.GetPushResultChannel()
	select {
	case err := <-doneC:
		t.Fatal(fmt.Sprintf("Unexpected error on close channel: %s", err))
	default:
		// No message is good here
		break
	}
	p2w.Done()
	select {
	case err, isopen := <-doneC:
		if err != nil {
			t.Fatal(fmt.Sprintf("Unexpected error when we expected close: %s", err))
		}
		if !isopen {
			// Channel closed correctly
			break
		}
	default:
		t.Fatal("Done channel not closed")
	}

	objtype, objsize, objr, err = p2.ReadObject("5c40945c981bd37a6912f5e2d3b2ceabfec5452a")
	if err != nil {
		t.Fatal(fmt.Sprintf("Error when getting just-written object: %s", err))
	}
	if objsize != 4 {
		t.Fatal(fmt.Sprintf("Incorrect number of bytes returned: %d != 4", objsize))
	}
	if objtype != ObjectTypeBlob {
		t.Fatal(fmt.Sprintf("Incorrect object type returned: %s != ObjectTypeBlob", objtype))
	}
	buf = make([]byte, 6)
	n, err = objr.Read(buf)
	if err != nil && err != io.EOF {
		t.Fatal(fmt.Sprintf("Error reading object: %s", err))
	}
	if n != 3 {
		t.Fatal(fmt.Sprintf("Unexpected number of bytes read: %d", n))
	}
	if buf[0] != 'f' || buf[1] != 'o' || buf[2] != 'o' {
		t.Fatal(fmt.Sprintf("Incorrect object returned: %s", buf))
	}
	err = objr.Close()
	if err != nil {
		t.Fatal(fmt.Sprintf("Error when closing read object: %s", err))
	}

	// Make sure that getting the p2 object on p1 is ErrObjectNotFound
	objtype, objsize, objr, err = p1.ReadObject("5c40945c981bd37a6912f5e2d3b2ceabfec5452a")
	if err != ErrObjectNotFound {
		t.Fatal(fmt.Sprintf("p2 object available via p1"))
	}

	// And p1 via p2
	objtype, objsize, objr, err = p2.ReadObject("30d74d258442c7c65512eafab474568dd706c430")
	if err != ErrObjectNotFound {
		t.Fatal(fmt.Sprintf("p1 object available via p2"))
	}

	// And check all object lists
	expectObjects(t, p1, false, "30d74d258442c7c65512eafab474568dd706c430")
	expectObjects(t, p2, false, "5c40945c981bd37a6912f5e2d3b2ceabfec5452a")
}

func TestGzipCompressedTreeStorageDriver(t *testing.T) {
	dir, err := ioutil.TempDir("", "repospanner_test_")
	if err != nil {
		panic(err)
	}
	//defer os.RemoveAll(dir)
	fmt.Println("Dir: ", dir)
	t.Run("tree", func(t *testing.T) {
		treeconf := map[string]string{}
		treeconf["type"] = "tree"
		treeconf["directory"] = dir
		treeconf["clustered"] = "false"
		treeconf["compressmethod"] = "gzip"
		instance, err := InitializeStorageDriver(treeconf)
		if err != nil {
			panic(err)
		}
		testStorageDriver("tree", instance, t)
	})
}

func TestZlibCompressedTreeStorageDriver(t *testing.T) {
	dir, err := ioutil.TempDir("", "repospanner_test_")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)
	t.Run("tree", func(t *testing.T) {
		treeconf := map[string]string{}
		treeconf["type"] = "tree"
		treeconf["directory"] = dir
		treeconf["clustered"] = "false"
		treeconf["compressmethod"] = "zlib"
		instance, err := InitializeStorageDriver(treeconf)
		if err != nil {
			panic(err)
		}
		testStorageDriver("tree", instance, t)
	})
}

func TestUncompressedTreeStorageDriver(t *testing.T) {
	dir, err := ioutil.TempDir("", "repospanner_test_")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)
	t.Run("tree", func(t *testing.T) {
		treeconf := map[string]string{}
		treeconf["type"] = "tree"
		treeconf["directory"] = dir
		treeconf["clustered"] = "false"
		treeconf["compressmethod"] = "none"
		instance, err := InitializeStorageDriver(treeconf)
		if err != nil {
			panic(err)
		}
		testStorageDriver("tree", instance, t)
	})
}
