package service

import (
	"context"
	"fmt"
	"net/http"

	"repospanner.org/repospanner/server/constants"
	"repospanner.org/repospanner/server/storage"
)

func (cfg *Service) serveGitDiscovery(ctx context.Context, w http.ResponseWriter, r *http.Request, reponame string, fakerefs bool) {
	reqlogger := loggerFromCtx(ctx)

	// Git smart protocol handshake
	services := r.URL.Query()["service"]
	if len(services) != 1 {
		reqlogger.Info("No service requested? Non-smart?")
		// TODO: Return client error code?
		http.NotFound(w, r)
		return
	}
	isrepobridge := len(r.Header[http.CanonicalHeaderKey("X-RepoBridge-Version")]) == 1
	service := services[0]
	w.Header()["Content-Type"] = []string{"application/x-" + service + "-advertisement"}
	reqlogger = reqlogger.WithField("service", service)

	if service == "git-upload-pack" || service == "git-receive-pack" {
		read := service == "git-upload-pack"

		if !read && !cfg.checkAccess(ctx, reponame, constants.CertPermissionWrite) {
			// Write access denied
			reqlogger.Info("Unauthorized request")
			http.NotFound(w, r)
			return
		}

		w.WriteHeader(200)

		if !isrepobridge {
			if err := sendPacket(w, []byte("# service="+service+"\n")); err != nil {
				http.NotFound(w, r)
				return
			}
			if err := sendFlushPacket(w); err != nil {
				http.NotFound(w, r)
				return
			}
		}

		refs := cfg.statestore.getGitRefs(reponame)
		symrefs := cfg.statestore.getSymRefs(reponame)

		if fakerefs {
			cfg.statestore.mux.RLock()
			frefs, hasfrefs := cfg.statestore.fakerefs[reponame]
			if hasfrefs {
				realrefs := refs
				refs = make(map[string]string)
				for refname, refval := range realrefs {
					refs[refname] = refval
				}
				for refname, refval := range frefs {
					refs[refname] = refval
				}
			}
			cfg.statestore.mux.RUnlock()
		}

		if len(refs) == 0 {
			// Empty repo
			if service == "git-receive-pack" {
				pkt := []byte(fmt.Sprintf("%s capabilities^{}", storage.ZeroID))
				sendPacketWithExtensions(w, pkt, symrefs)
			}
			sendFlushPacket(w)
			return
		}
		sentexts := false
		for refname, refval := range refs {
			if !isValidRef(refval) {
				continue
			}
			pkt := []byte(fmt.Sprintf("%s %s", refval, refname))
			var err error
			if !sentexts {
				err = sendPacketWithExtensions(w, pkt, symrefs)
				sentexts = true
			} else {
				err = sendPacket(w, pkt)
			}
			if err != nil {
				http.NotFound(w, r)
				return
			}
		}
		for symref, target := range symrefs {
			refval, ok := refs[target]
			if !ok {
				continue
			}
			pkt := []byte(fmt.Sprintf("%s %s", refval, symref))
			var err error
			err = sendPacket(w, pkt)
			if err != nil {
				http.NotFound(w, r)
				return
			}
		}

		if err := sendFlushPacket(w); err != nil {
			http.NotFound(w, r)
			return
		}

		return
	}

	reqlogger.Info("Invalid service requested")
	http.NotFound(w, r)
	return
}
