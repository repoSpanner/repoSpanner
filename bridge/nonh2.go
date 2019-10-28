// +build nonh2

package bridge

import (
	"net/http"
)

// HasH2 returns whether this bridge was compiled with h2 support
func HasH2() bool {
	return false
}

// maybeConfigureH2 configures the http transport if HTTP/2 support was compiled in
func maybeConfigureH2(transport *http.Transport) {
	// Not configuring h2
}
