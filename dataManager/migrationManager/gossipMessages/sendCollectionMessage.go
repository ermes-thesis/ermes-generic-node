package gossipmessages

type SendCollectionMessage struct {
	CollectionId string            `json:"collectionId"`
	Records      map[string]string `json:"records"`
	Groups       []string          `json:"groups"`
	IsStrong     bool              `json:"isStrong"`
	IsLeader     bool              `json:"isLeader"`
}
