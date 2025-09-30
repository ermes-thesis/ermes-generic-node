package messages

type RemoteAccessRequest struct {
	CollectionId string   `json:"collectionId"`
	RecordNames  []string `json:"recordNames"`
}

type RemoteAccessResponse struct {
	Records []any `json:"records"`
}

type RemoteUpdateRequest struct {
	CollectionId string   `json:"collectionId"`
	RecordNames  []string `json:"recordNames"`
	Record       []string `json:"records"`
	SenderIp     string   `json:"senderIp"`
	IsDelete     bool     `json:"isDelete"`
}

type CollectionUpdate struct {
	CollectionId string   `json:"collectionId"`
	RecordNames  []string `json:"recordNames"`
	RecordValues []string `json:"recordValue"`
}

type CollectionUpdateMessage struct {
	CollectionId string   `json:"collectionId"`
	RecordNames  []string `json:"recordNames"`
	RecordValues []string `json:"recordValue"`
	Time         int64    `json:"time"`
	UpdateHostIp string   `json:"updateHostIp"`
	SenderIp     string   `json:"senderIp"`
	IsDelete     bool     `json:"isDelete"`
}

type NewCollectionMessage struct {
	CollectionId string `json:"collectionId"`
	IsStrong     bool   `json:"isStrong"`
	SenderIp     string `json:"senderIp"`
}
