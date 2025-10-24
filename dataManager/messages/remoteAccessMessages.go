package messages

import (
	"time"
)

// get

type RemoteAccessRequest struct {
	CollectionId string   `json:"collectionId"`
	RecordNames  []string `json:"recordNames"`
	Groups       []string `json:"groups"`
	UserId       string   `json:"userId"`
}

type RemoteAccessResponse struct {
	Records []any `json:"records"`
}

type RemoteUpdateRequest struct {
	CollectionId string   `json:"collectionId"`
	RecordNames  []string `json:"recordNames"`
	Record       []string `json:"records"`
	Groups       []string `json:"groups"`
	UserId       string   `json:"userId"`
	SenderIp     string   `json:"senderIp"`
	IsDelete     bool     `json:"isDelete"`
}

// put and delete records

type UpdateMessage interface {
	GetSequence() uint64
	SetSequence(seq uint64)
	IncrementSequence()
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
	Sequence     uint64   `json:"sequence"`
}

func (c *CollectionUpdateMessage) GetSequence() uint64 {
	return c.Sequence
}

func (c *CollectionUpdateMessage) SetSequence(seq uint64) {
	c.Sequence = seq
}

func (c *CollectionUpdateMessage) IncrementSequence() {
	c.Sequence++
}

type CollectionUpdateMessages struct {
	Messages []CollectionUpdateMessage
	Groups   []string
	UserId   string
}

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

// put and delete groups

type CollectionGroupUpdate struct {
	CollectionId string   `json:"collectionId"`
	GroupNames   []string `json:"recordNames"`
	Sequence     uint64   `json:"sequence"`
}

type CollectionGroupUpdateMessage struct {
	CollectionId string   `json:"collectionId"`
	GroupNames   []string `json:"recordNames"`
	Sequence     uint64   `json:"sequence"`
	SenderIp     string   `json:"senderIp"`
}

func ToCollectionGroupUpdateMessages(updates []CollectionGroupUpdate, senderIp string) []CollectionGroupUpdateMessage {
	result := make([]CollectionGroupUpdateMessage, 0, len(updates))

	for _, u := range updates {
		msg := CollectionGroupUpdateMessage{
			CollectionId: u.CollectionId,
			GroupNames:   u.GroupNames,
			Sequence:     u.Sequence,
			SenderIp:     senderIp,
		}
		result = append(result, msg)
	}

	return result
}

func (c *CollectionGroupUpdateMessage) GetSequence() uint64 {
	return 0
}

func (c *CollectionGroupUpdateMessage) SetSequence(seq uint64) { //do nothing
}

func (c *CollectionGroupUpdateMessage) IncrementSequence() { //do nothing
}

type CollectionGroupUpdateMessages []CollectionGroupUpdateMessage

// collection management

type NewCollectionMessage struct {
	CollectionId        string `json:"collectionId"`
	CollectionName      string `json:"collectionName"`
	CollectionOwner     string `json:"collectionOwner"`
	IsStrong            bool   `json:"isStrong"`
	SenderIp            string `json:"senderIp"`
	SkipUniquenessCheck bool   `json:"skipUniquenessCheck"`
}

type RemoveCollectionMessage struct {
	CollectionId string   `json:"collectionId"`
	SenderIp     string   `json:"senderIp"`
	Groups       []string `json:"groups"`
	UserId       string   `json:"userId"`
}
