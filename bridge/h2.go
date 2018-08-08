// +build !nonh2

package bridge

import (
	"net/http"

	"golang.org/x/net/http2"
)

// HasH2 returns whether this bridge was compiled with h2 support
func HasH2() bool {
	return true
}

func maybeConfigureH2(transport *http.Transport) {
	err := http2.ConfigureTransport(transport)
	checkError(err, "Error initializing HTTP/2 transport")
}
