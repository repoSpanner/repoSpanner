package utils

import (
	"fmt"

	"github.com/coreos/pkg/capnslog"
	"github.com/sirupsen/logrus"
)

// LogWrapper is a utility to wrap both coreos capnslog and coreos.raft.Logger for logrus
type LogWrapper struct {
	inner *logrus.Logger

	sentInitialized bool
	HasInitialized  chan struct{}
}

func CreateLogWrapper(in *logrus.Logger) *LogWrapper {
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
	log := w.inner.WithFields(logrus.Fields{
		"pkg":   pkg,
		"level": level.String(),
		"depth": depth,
	})

	switch level {
	case capnslog.CRITICAL:
		log.Fatal(msg)
	case capnslog.ERROR:
		log.Error(msg)
	case capnslog.WARNING:
		log.Warn(msg)
	case capnslog.NOTICE:
		log.Info(msg)
	case capnslog.INFO:
		log.Info(msg)
	case capnslog.DEBUG:
		log.Debug(msg)
	case capnslog.TRACE:
		log.Debug(msg)
	default:
		panic("Unhandled capnslog loglevel")
	}
}

// raft.Logger interface
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
