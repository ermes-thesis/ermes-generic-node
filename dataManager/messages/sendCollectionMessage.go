package messages

type SendCollectionMessage struct {
	CollectionId   string            `json:"collectionId"`
	Records        map[string]string `json:"records"`
	Groups         []string          `json:"groups"`
	GroupsSequence uint64            `json:"groupsSequence"`
	IsStrong       bool              `json:"isStrong"`
	IsLeader       bool              `json:"isLeader"`
	CollectionSize uint64            `json:"collectionSize"`
	HostToRemove   bool              `json:"hostToRemove"`
	SenderIp       string            `json:"hostIp"`
}
