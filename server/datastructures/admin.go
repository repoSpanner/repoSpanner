package datastructures

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
)

type RepoUpdateRequest struct {
	Reponame      string
	UpdateRequest map[RepoUpdateField]string
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

type HookRunRequest struct {
	RPCURL       string
	ProjectName  string
	PushUUID     string
	BwrapConfig  map[string]interface{}
	User         int
	Hook         string
	HookObject   string
	Requests     map[string][2]string
	ClientCaCert string
	ClientCert   string
	ClientKey    string
	ExtraEnv     map[string]string
}
