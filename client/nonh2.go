// +build nonh2

package client

import (
	"net/http"
)

// HasH2 returns whether this client was compiled with h2 support
func HasH2() bool {
	return false
}

func maybeConfigureH2(transport *http.Transport) {
	// Not configuring h2
}