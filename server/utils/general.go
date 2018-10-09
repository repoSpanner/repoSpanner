package utils

import "io"

type innerReadCloser struct {
	closeOuter bool
	inner      io.ReadCloser
	outer      io.ReadCloser
}

func NewInnerReadCloser(outer io.ReadCloser, inner io.ReadCloser, closeOuter bool) io.ReadCloser {
	return &innerReadCloser{outer: outer, inner: inner, closeOuter: closeOuter}
}

func (i *innerReadCloser) Read(p []byte) (int, error) {
	return i.outer.Read(p)
}

func (i *innerReadCloser) Close() error {
	if i.closeOuter {
		err := i.outer.Close()
		if err != nil {
			return err
		}
	}
	return i.inner.Close()
}
