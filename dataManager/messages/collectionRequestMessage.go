package messages

type CollectionRequestInfo struct {
	CollectionId   string
	IsWrite        bool
	RequestLatency int
}

type CollectionRequestInfoMessage struct {
	CollectionId   string `json:"collectionId"`
	IsWrite        bool   `json:"isWrite"`
	RequestLatency int    `json:"requestLatency"`
	Sender         string `json:"sender"`
}

type CollectionRequestInfoMessages struct {
	Messages []CollectionRequestInfoMessage `json:"messages"`
}

func ToCollectionRequestInfoMessages(info []CollectionRequestInfo, sender string) CollectionRequestInfoMessages {
	var messages []CollectionRequestInfoMessage
	for _, i := range info {
		message := CollectionRequestInfoMessage{
			CollectionId:   i.CollectionId,
			IsWrite:        i.IsWrite,
			RequestLatency: i.RequestLatency,
			Sender:         sender,
		}
		messages = append(messages, message)
	}
	return CollectionRequestInfoMessages{Messages: messages}
}
