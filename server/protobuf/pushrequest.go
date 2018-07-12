package protobuf

import (
	fmt "fmt"
	"math/rand"
	"time"

	"repospanner.org/repospanner/server/storage"
)

func NewUpdateRequest(ref, from, to string) *UpdateRequest {
	return &UpdateRequest{
		Ref:  &ref,
		From: &from,
		To:   &to,
	}
}

func (m *UpdateRequest) FromObject() storage.ObjectID {
	return storage.ObjectID(m.GetFrom())
}

func (m *UpdateRequest) ToObject() storage.ObjectID {
	return storage.ObjectID(m.GetTo())
}

func NewPushRequest(pushnode uint64, reponame string) *PushRequest {
	timestamp := time.Now().UTC().UnixNano()
	pushid := rand.Int63()
	return &PushRequest{
		Pushnode: &pushnode,
		Pushtime: &timestamp,
		// This Push "ID" only needs to be unique on the same node on the same nanosecond...
		Pushid:   &pushid,
		Reponame: &reponame,
		Requests: make([]*UpdateRequest, 0),
	}
}

func (p *PushRequest) UUID() string {
	return fmt.Sprintf("push-%d-%d-%d",
		p.GetPushnode(),
		p.GetPushtime(),
		p.GetPushid(),
	)
}

func (p *PushRequest) HasRef(ref string) bool {
	for _, req := range p.GetRequests() {
		if req.GetRef() == ref {
			return true
		}
	}
	return false
}

func (p *PushRequest) AddRequest(r *UpdateRequest) {
	p.Requests = append(p.Requests, r)
}

func (p *PushRequest) ExpectPackFile() bool {
	for _, request := range p.Requests {
		if request.ToObject() != storage.ZeroID {
			return true
		}
	}
	return false
}

func (p *PushRequest) Equals(o *PushRequest) bool {
	if p == nil && o == nil {
		return true
	}
	if p == nil && o != nil {
		return false
	}
	if p != nil && o == nil {
		return false
	}

	if p.Reponame != o.Reponame {
		return false
	}

	if len(p.Requests) != len(o.Requests) {
		return false
	}

	for _, reqP := range p.Requests {
		hitit := false
		for _, reqO := range o.Requests {
			if reqO.GetRef() != reqP.GetRef() {
				continue
			}
			if reqO.GetTo() != reqP.GetTo() {
				return false
			}
			if reqO.GetFrom() != reqP.GetFrom() {
				return false
			}
			hitit = true
		}
		if !hitit {
			return false
		}
	}
	// If p has as many requests as o, and all requests in p match requests in o, they are equal
	return true
}
