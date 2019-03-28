package service

import (
	"context"
	"net/http"
	"sync"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type key int

const (
	logKey      key = 0
	permKey     key = 1
	capabsKey   key = 2
	sbstatusKey key = 3
	sbLockKey   key = 4
)

func watchCloser(ctx context.Context, w http.ResponseWriter) context.Context {
	closer, hascloser := w.(http.CloseNotifier)
	// If this is an HTTP function with detection of closing, determine that
	if hascloser {
		var cancel context.CancelFunc
		ctx, cancel = context.WithCancel(ctx)
		closenotif := closer.CloseNotify()
		// Now start a new goroutine that will cancel the context when the conn
		// closes
		go func() {
			select {
			case <-ctx.Done():
				break
			case <-closenotif:
				cancel()
				break
			}
		}()
	}

	return ctx
}

func addHTTPHeaderInfo(reqlogger *logrus.Entry, r *http.Request) *logrus.Entry {
	reqlogger = reqlogger.WithFields(logrus.Fields{
		"protocol": r.Proto,
		"method":   r.Method,
		"client":   r.RemoteAddr,
		"url":      r.URL,
	})
	hdr, ok := r.Header["User-Agent"]
	if ok && len(hdr) >= 1 {
		reqlogger = reqlogger.WithField(
			"useragent", hdr[0],
		)
	}
	repobridge := r.Header[http.CanonicalHeaderKey("X-RepoBridge-Version")]
	if len(repobridge) != 0 {
		reqlogger = reqlogger.WithField(
			"RepoBridge-Version", repobridge,
		)
	}

	return reqlogger
}

func (cfg *Service) ctxFromReq(w http.ResponseWriter, r *http.Request, server string) context.Context {
	ctx := context.Background()

	ctx = watchCloser(ctx, w)

	cfg.addSecurityHeaders(w)

	reqlogger := cfg.log.WithField("server", server)
	reqlogger = addHTTPHeaderInfo(reqlogger, r)
	reqlogger = resolveCompression(r, reqlogger)

	if r.TLS != nil {
		perminfo := cfg.getPermissionInfo(r.TLS)
		ctx = context.WithValue(ctx, permKey, perminfo)
		reqlogger = cfg.addPermToLogger(perminfo, reqlogger)
	}

	ctx = context.WithValue(ctx, logKey, reqlogger)

	return ctx
}

func permFromCtx(ctx context.Context) (permissionInfo, bool) {
	perminfo, ok := ctx.Value(permKey).(permissionInfo)
	return perminfo, ok
}

func loggerFromCtx(ctx context.Context) *logrus.Entry {
	logger, ok := ctx.Value(logKey).(*logrus.Entry)
	if !ok {
		panic("No logger in context?")
	}
	return logger
}

func expandCtxLogger(ctx context.Context, extra logrus.Fields) (context.Context, *logrus.Entry) {
	current := loggerFromCtx(ctx).WithFields(extra)
	ctx = context.WithValue(ctx, logKey, current)
	return ctx, current
}

func addCapabsToCtx(ctx context.Context, capabs []string) (context.Context, *logrus.Entry, error) {
	sbstatus := sideBandStatusNot
	for _, capab := range capabs {
		if capab == "side-band-64k" {
			if sbstatus != sideBandStatusNot {
				return ctx, loggerFromCtx(ctx), errors.New("Multiple side-bands requested")
			}
			sbstatus = sideBandStatusLarge
		} else if capab == "side-band" {
			if sbstatus != sideBandStatusNot {
				return ctx, loggerFromCtx(ctx), errors.New("Multiple side-bands requested")
			}
			sbstatus = sideBandStatusSmall
		}
	}
	ctx, reqlogger := expandCtxLogger(ctx, logrus.Fields{
		"sbstatus": sbstatus,
	})
	ctx = context.WithValue(ctx, sbstatusKey, sbstatus)
	ctx = context.WithValue(ctx, capabsKey, capabs)
	return ctx, reqlogger, nil
}

func addSBLockToCtx(ctx context.Context) context.Context {
	return context.WithValue(ctx, sbLockKey, new(sync.Mutex))
}

func capabsFromCtx(ctx context.Context) ([]string, bool) {
	capabs, ok := ctx.Value(capabsKey).([]string)
	return capabs, ok
}

func sbStatusFromCtx(ctx context.Context) sideBandStatus {
	sbstatus, ok := ctx.Value(sbstatusKey).(sideBandStatus)
	if !ok {
		return sideBandStatusNot
	}
	return sbstatus
}

func sbLockFromCtx(ctx context.Context) sync.Locker {
	sblock, _ := ctx.Value(sbLockKey).(*sync.Mutex)
	if sblock == nil {
		panic("SB Lock requested for context without one")
	}
	return sblock
}
