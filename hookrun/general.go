package hookrun

import (
	"os"
	"os/signal"
	"syscall"
)

var (
	debug       bool
	keysDeleted bool

	rmWorkdir    string
	shutdownChan chan<- os.Signal
)

func shutdown() {
	os.RemoveAll(rmWorkdir)
	os.Exit(0)
}

func armShutdownHandler(workdir string) {
	rmWorkdir = workdir
	c := make(chan os.Signal, 10)
	go func() {
		<-c
		shutdown()
	}()
	signal.Notify(c, syscall.SIGTERM)
	shutdownChan = c
}
