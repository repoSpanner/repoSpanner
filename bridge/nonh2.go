// +build nonh2

package bridge

import (
	"net/http"
)

// HasH2 returns whether this bridge was compiled with h2 support
func HasH2() bool {
	return false
}

func maybeConfigureH2(transport *http.Transport) {
	// Not configuring h2
}
