package hookrun

import (
	"fmt"
	"net/rpc"
	"os"

	"github.com/pkg/errors"
	"github.com/repoSpanner/repoSpanner/server/datastructures"
)

type HookServer struct {
	request *datastructures.HookRunRequest

	workdir string
}

func (s *HookServer) Prepare(noop_request, noop_reply *int) (err error) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Fprintln(os.Stderr, "Error occured")
			err = fmt.Errorf("Error occured: %s", r)
			return
		}
	}()

	// At this moment, we have lowered privileges. Clone the repo
	workdir, err := cloneRepository(*s.request)
	if err != nil {
		return errors.Wrap(err, "Error cloning repository")
	}
	s.workdir = workdir

	// Get the hook scripts
	err = getScripts(*s.request, s.workdir)

	return errors.Wrap(err, "Error getting scripts")
}

func (s *HookServer) FetchFakeRefs(noop_request, noop_reply *int) (err error) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Fprintln(os.Stderr, "Error occured")
			err = fmt.Errorf("Error occured: %s", r)
			return
		}
	}()

	err = fetchFakeRefs(*s.request, s.workdir)
	if err != nil {
		return errors.Wrap(err, "Error fetching fake refs")
	}
	err = deleteKeys(s.request, s.workdir)

	return errors.Wrap(err, "Error deleting keys")
}

func (s *HookServer) RunHook(hookname *string, noop_reply *int) (err error) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Fprintln(os.Stderr, "Error occured")
			err = fmt.Errorf("Error occured: %s", r)
			return
		}
	}()

	if *hookname == "update" {
		for branch, req := range s.request.Requests {
			err = runHook(*s.request, s.workdir, *hookname, branch, req)
			if err != nil {
				return
			}
		}
	} else {
		err = runHook(*s.request, s.workdir, *hookname, "", [2]string{"", ""})
		if err != nil {
			return
		}
	}

	return nil
}

func Run(control *os.File) error {
	request, err := prepareRequest()
	if err != nil {
		return err
	}
	server := new(HookServer)
	server.request = request

	rpc.RegisterName("Hook", server)
	rpc.ServeConn(control)
	return nil
}
