package messages

type RemoteAccessRequest struct {
	CollectionId string   `json:"collectionId"`
	RecordNames  []string `json:"recordNames"`
}

type RemoteAccessResponse struct {
	Records []any `json:"records"`
}
