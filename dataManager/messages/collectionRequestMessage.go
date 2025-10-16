package messages

type CollectionRequestInfo struct {
	CollectionId   string
	NodeName       string
	IsWrite        bool
	RequestLatency int
}

type CollectionRequestInfoMessage struct {
	CollectionId   string `json:"collectionId"`
	NodeName       string `json:"nodeName"`
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
			NodeName:       i.NodeName,
			IsWrite:        i.IsWrite,
			RequestLatency: i.RequestLatency,
			Sender:         sender,
		}
		messages = append(messages, message)
	}
	return CollectionRequestInfoMessages{Messages: messages}
}
