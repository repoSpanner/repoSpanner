package utils

import "io"

// innerReadCloser is a structure used to automatically close two related
// ReadClosers' when done reading a single Reader.
// NOTE: This should only be used if the outer Close() function does *not* call the inner Close() function,
// like is the case with zlib.Reader and gzip.Reader.
type innerReadCloser struct {
	closeOuter bool
	inner      io.ReadCloser
	outer      io.ReadCloser
}

// NewInnerReadCloser returns a new innerReadCloser structure, with the indicated inner and outer readers
// If set to true, closeOuter will close the outer reader when NewInnerReadCloser.Close() is used.
// Make sure to read the notes in innerReadCloser before use.
func NewInnerReadCloser(outer io.ReadCloser, inner io.ReadCloser, closeOuter bool) io.ReadCloser {
	return &innerReadCloser{outer: outer, inner: inner, closeOuter: closeOuter}
}

// Read reads from the Outer reader
func (i *innerReadCloser) Read(p []byte) (int, error) {
	return i.outer.Read(p)
}

// Close() first closes the outer Reader if closeOuter was set, and after that closes the inner reader.
func (i *innerReadCloser) Close() error {
	if i.closeOuter {
		err := i.outer.Close()
		if err != nil {
			return err
		}
	}
	return i.inner.Close()
}
