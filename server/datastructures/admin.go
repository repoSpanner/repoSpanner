package datastructures

import (
	pb "repospanner.org/repospanner/server/protobuf"
)

type RepoRequestInfo struct {
	Reponame string
	Public   bool
}

type CommandResponse struct {
	Success bool
	Error   string
	Info    string
}

type RepoHookInfo struct {
	PreReceive  string
	Update      string
	PostReceive string
}

type RepoInfo struct {
	Symrefs      map[string]string
	Refs         map[string]string
	Hooks        RepoHookInfo
	LastPushNode uint64
	Public       bool
}

type RepoUpdateField string

const (
	RepoUpdatePublic          RepoUpdateField = "public"
	RepoUpdateHookPreReceive  RepoUpdateField = "hook-prereceive"
	RepoUpdateHookUpdate      RepoUpdateField = "hook-update"
	RepoUpdateHookPostReceive RepoUpdateField = "hook-postreceive"
	RepoUpdateSymref          RepoUpdateField = "update-symref"
)

type RepoUpdateRequest struct {
	Reponame      string
	UpdateRequest map[RepoUpdateField]string
}

type RepoDeleteRequest struct {
	Reponame string
}

type RepoList struct {
	Repos map[string]RepoInfo
}

type NodeInfo struct {
	NodeID      uint64
	NodeName    string
	RegionName  string
	ClusterName string
	Version     string
	Peers       map[uint64]string
}

type NodeStatus struct {
	NodeInfo

	PeerPings map[uint64]pb.PingMessage

	LeaderNode uint64

	Status string
}

type HookRunRequest struct {
	Debug        bool
	RPCURL       string
	ProjectName  string
	PushUUID     string
	BwrapConfig  map[string]interface{}
	User         int
	HookObjects  map[string]string
	Requests     map[string][2]string
	ClientCaCert string
	ClientCert   string
	ClientKey    string
	ExtraEnv     map[string]string
}
