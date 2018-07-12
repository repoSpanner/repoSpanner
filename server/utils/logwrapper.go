package utils

import (
	"fmt"

	"github.com/coreos/pkg/capnslog"
	"go.uber.org/zap"
)

// LogWrapper is a utility to wrap both coreos capnslog and coreos.raft.Logger for zap
type LogWrapper struct {
	inner *zap.SugaredLogger

	sentInitialized bool
	HasInitialized  chan struct{}
}

func CreateLogWrapper(in *zap.SugaredLogger) *LogWrapper {
	return &LogWrapper{
		inner: in,

		sentInitialized: false,
		HasInitialized:  make(chan struct{}),
	}
}

// This part is for capnslog
func (w *LogWrapper) Flush() {}

func (w *LogWrapper) Format(pkg string, level capnslog.LogLevel, depth int, entries ...interface{}) {
	msg := fmt.Sprint(entries...)

	switch level {
	case capnslog.CRITICAL:
		w.inner.Fatalw(msg,
			"pkg", pkg,
			"level", level.String(),
			"depth", depth,
		)
	case capnslog.ERROR:
		w.inner.Errorw(msg,
			"pkg", pkg,
			"level", level.String(),
			"depth", depth,
		)
	case capnslog.WARNING:
		w.inner.Warnw(msg,
			"pkg", pkg,
			"level", level.String(),
			"depth", depth,
		)
	case capnslog.NOTICE:
		w.inner.Infow(msg,
			"pkg", pkg,
			"level", level.String(),
			"depth", depth,
		)
	case capnslog.INFO:
		w.inner.Infow(msg,
			"pkg", pkg,
			"level", level.String(),
			"depth", depth,
		)
	case capnslog.DEBUG:
		w.inner.Debugw(msg,
			"pkg", pkg,
			"level", level.String(),
			"depth", depth,
		)
	case capnslog.TRACE:
		w.inner.Debugw(msg,
			"pkg", pkg,
			"level", level.String(),
			"depth", depth,
		)
	default:
		panic("Unhandled capnslog loglevel")
	}
}

// raft.Logger interface
// All this because zap calls it Warn while raft.Logger wants Warning........
func (w *LogWrapper) Debug(v ...interface{}) {
	w.inner.Debug(v...)
}

func (w *LogWrapper) Debugf(format string, v ...interface{}) {
	w.inner.Debugf(format, v...)
}

func (w *LogWrapper) Error(v ...interface{}) {
	w.inner.Error(v...)
}

func (w *LogWrapper) Errorf(format string, v ...interface{}) {
	w.inner.Errorf(format, v...)
}

func (w *LogWrapper) Info(v ...interface{}) {
	w.inner.Info(v...)
}

func (w *LogWrapper) Infof(format string, v ...interface{}) {
	// This is an ugly method to determine whether we have had a leader (i.e. initialized)
	if !w.sentInitialized {
		if format == "%x became leader at term %d" || format == "raft.node: %x elected leader %x at term %d" {
			close(w.HasInitialized)
			w.sentInitialized = true
		}
	}

	w.inner.Infof(format, v...)
}

func (w *LogWrapper) Warning(v ...interface{}) {
	w.inner.Warn(v...)
}

func (w *LogWrapper) Warningf(format string, v ...interface{}) {
	w.inner.Warnf(format, v...)
}

func (w *LogWrapper) Fatal(v ...interface{}) {
	w.inner.Fatal(v...)
}

func (w *LogWrapper) Fatalf(format string, v ...interface{}) {
	w.inner.Fatalf(format, v...)
}

func (w *LogWrapper) Panic(v ...interface{}) {
	w.inner.Panic(v...)
}

func (w *LogWrapper) Panicf(format string, v ...interface{}) {
	w.inner.Panicf(format, v...)
}
