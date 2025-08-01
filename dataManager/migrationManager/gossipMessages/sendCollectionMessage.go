package gossipmessages

type SendCollectionMessage struct {
	CollectionId string            `json:"collectionId"`
	Res          map[string]string `json:"res"`
	Groups       []string          `json:"groups"`
	IsStrong     bool              `json:"isStrong"`
}
