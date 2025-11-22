package messages

import "db"

type SendCollectionMessage struct {
	CollectionId   string                    `json:"collectionId"`
	Records        map[string]db.RecordValue `json:"records"`
	SequenceNumber uint64                    `json:"sequenceNumber"`
	Groups         []string                  `json:"groups"`
	Owner          string                    `json:"owner"`
	GroupsSequence uint64                    `json:"groupsSequence"`
	IsStrong       bool                      `json:"isStrong"`
	IsLeader       bool                      `json:"isLeader"`
	CollectionSize uint64                    `json:"collectionSize"`
	HostToRemove   bool                      `json:"hostToRemove"`
	SenderIp       string                    `json:"hostIp"`
}
