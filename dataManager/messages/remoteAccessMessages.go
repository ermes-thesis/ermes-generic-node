package messages

import (
	"time"
)

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
	IsDelete     bool     `json:"isDelete"`
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

type CollectionUpdateMessages []CollectionUpdateMessage

func ToCollectionUpdateMessages(
	updates []CollectionUpdate,
	updateHostIp string,
	senderIp string,
) []CollectionUpdateMessage {
	result := make([]CollectionUpdateMessage, 0, len(updates))
	time := time.Now().Unix()

	for _, u := range updates {
		msg := CollectionUpdateMessage{
			CollectionId: u.CollectionId,
			RecordNames:  u.RecordNames,
			RecordValues: u.RecordValues,
			Time:         time,
			UpdateHostIp: updateHostIp,
			SenderIp:     senderIp,
			IsDelete:     u.IsDelete,
		}
		result = append(result, msg)
	}

	return result
}

type NewCollectionMessage struct {
	CollectionId string `json:"collectionId"`
	IsStrong     bool   `json:"isStrong"`
	SenderIp     string `json:"senderIp"`
}
