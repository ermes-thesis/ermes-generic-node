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
