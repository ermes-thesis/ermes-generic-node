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
}

type CollectionUpdate struct {
	CollectionId string   `json:"collectionId"`
	RecordNames  []string `json:"recordNames"`
	RecordValues []string `json:"recordValue"`
}

// TODO: add machine id for disambiguity
type CollectionUpdateMessage struct {
	CollectionId string   `json:"collectionId"`
	RecordNames  []string `json:"recordNames"`
	RecordValues []string `json:"recordValue"`
	Time         int64    `json:"time"`
	UpdateHostIp string   `json:"updateHostIp"`
	SenderIp     string   `json:"senderIp"`
}
