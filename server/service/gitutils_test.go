package service

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
)

type maxwritable struct {
	maxlen int
}

func (w maxwritable) Write(b []byte) (int, error) {
	if len(b) > w.maxlen {
		return 0, errors.New("Too much data")
	}
	return len(b), nil
}

func TestSendPacketFromDocks(t *testing.T) {
	ctx := context.Background()
	ctx = addSBLockToCtx(ctx)

	b := new(bytes.Buffer)

	// These tests come straight from the protocol-common docs
	b.Reset()
	if err := sendPacket(ctx, b, []byte("a\n")); err != nil {
		t.Fatalf("Error returned when writing: %s", err)
	}
	if b.String() != "0006a\n" {
		t.Fatal("Incorrect packet written")
	}

	b.Reset()
	if err := sendPacket(ctx, b, []byte("a")); err != nil {
		t.Fatalf("Error returned when writing: %s", err)
	}
	if b.String() != "0005a" {
		t.Fatal("Incorrect packet written")
	}

	b.Reset()
	if err := sendPacket(ctx, b, []byte("foobar\n")); err != nil {
		t.Fatalf("Error returned when writing: %s", err)
	}
	if b.String() != "000bfoobar\n" {
		t.Fatal("Incorrect packet written")
	}
}

func TestSendPacket(t *testing.T) {
	ctx := context.Background()
	ctx = addSBLockToCtx(ctx)

	b := new(bytes.Buffer)
	if err := sendPacket(ctx, b, []byte("hello")); err != nil {
		t.Fatalf("Error returned when writing: %s", err)
	}
	if b.String() != "0009hello" {
		t.Fatalf("sendPacket wrote incorrect packet: %v", b.String())
	}

	b.Reset()
	err := sendPacket(ctx, b, []byte(""))
	if err == nil {
		t.Fatalf("sendPacket of empty packet didn't fail?")
	}
	if err.Error() != "Unable to send empty packet" {
		t.Fatalf("Error message for empty packet incorrect")
	}
	if b.Len() != 0 {
		t.Fatalf("Empty packet wrote some data")
	}

	b.Reset()
	err = sendPacket(ctx, b, make([]byte, 65517))
	if err == nil {
		t.Fatalf("Too long packet written")
	}
	if err.Error() != "Packet too big" {
		t.Fatalf("Error message for too big packet incorrect")
	}
	if b.Len() != 0 {
		t.Fatalf("Too big packet wrote some data")
	}

	b.Reset()
	err = sendPacket(ctx, b, make([]byte, 65516))
	if err != nil {
		t.Fatalf("Max length packet not accepted")
	}
	sent := b.String()
	if len(sent) != 65520 {
		t.Fatalf("Max packet did not get written correctly. Len: %v", len(sent))
	}
	if !strings.HasPrefix(sent, "fff0") {
		t.Fatalf("Max length string has invalid length byte: %v", sent[:4])
	}

	w := maxwritable{}
	err = sendPacket(ctx, w, []byte("hello"))
	if err == nil {
		t.Fatalf("Non-writable error not returned")
	}
	if err.Error() != "Too much data" {
		t.Fatalf("Incorrect error returned: %v", err)
	}

	w = maxwritable{maxlen: 4}
	err = sendPacket(ctx, w, []byte("hello"))
	if err == nil {
		t.Fatalf("Non-writable error not returned")
	}
	if err.Error() != "Too much data" {
		t.Fatalf("Incorrect error returned: %v", err)
	}

	w = maxwritable{maxlen: 5}
	err = sendPacket(ctx, w, []byte("hello"))
	if err != nil {
		t.Fatalf("More data written than expected")
	}
}

func TestSendPacketWithExtensions(t *testing.T) {
	ctx := context.Background()
	ctx = addSBLockToCtx(ctx)

	b := new(bytes.Buffer)
	if err := sendPacketWithExtensions(ctx, b, []byte("hello"), make(map[string]string)); err != nil {
		t.Errorf("Error returned when writing: %s", err)
	}
	if !strings.Contains(b.String(), "hello\x00delete-refs") {
		t.Errorf("Written packet not as expected: %v", b.String())
	}
	if !strings.Contains(b.String(), " agent=repoSpanner/") {
		t.Errorf("Agent was not in extensions")
	}
}

func TestSendFlushPacket(t *testing.T) {
	ctx := context.Background()
	ctx = addSBLockToCtx(ctx)

	b := new(bytes.Buffer)
	if err := sendFlushPacket(ctx, b); err != nil {
		t.Errorf("Error returned when writing: %s", err)
	}
	if b.String() != "0000" {
		t.Errorf("Incorrect data written: %v", b.String())
	}
}
