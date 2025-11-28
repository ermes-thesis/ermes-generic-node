package messages

import (
	"db"
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
	ToForward    bool     `json:"toForward"`
	Sequence     uint64   `json:"sequence"`
}

func (c *CollectionUpdateMessage) GetSequence() uint64 {
	return c.Sequence
}

func (c *CollectionUpdateMessage) SetSequence(seq uint64) {
	c.Sequence = seq
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
	time := time.Now().UnixMicro()

	for _, u := range updates {
		msg := CollectionUpdateMessage{
			CollectionId: u.CollectionId,
			RecordNames:  u.RecordNames,
			RecordValues: u.RecordValues,
			Time:         time,
			UpdateHostIp: updateHostIp,
			SenderIp:     senderIp,
			IsDelete:     u.IsDelete,
			ToForward:    true,
		}
		result = append(result, msg)
	}

	return result
}

func (m CollectionUpdateMessage) Copy() CollectionUpdateMessage {
	newNames := make([]string, len(m.RecordNames))
	copy(newNames, m.RecordNames)

	newValues := make([]string, len(m.RecordValues))
	copy(newValues, m.RecordValues)

	return CollectionUpdateMessage{
		CollectionId: m.CollectionId,
		RecordNames:  newNames,
		RecordValues: newValues,
		Time:         m.Time,
		UpdateHostIp: m.UpdateHostIp,
		SenderIp:     m.SenderIp,
		IsDelete:     m.IsDelete,
		ToForward:    m.ToForward,
		Sequence:     m.Sequence,
	}
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
	ToForward    bool     `json:"toForward"`
}

func ToCollectionGroupUpdateMessages(updates []CollectionGroupUpdate, senderIp string) []CollectionGroupUpdateMessage {
	result := make([]CollectionGroupUpdateMessage, 0, len(updates))

	for _, u := range updates {
		msg := CollectionGroupUpdateMessage{
			CollectionId: u.CollectionId,
			GroupNames:   u.GroupNames,
			Sequence:     u.Sequence,
			SenderIp:     senderIp,
			ToForward:    true,
		}
		result = append(result, msg)
	}

	return result
}

func (m CollectionGroupUpdateMessage) Copy() CollectionGroupUpdateMessage {
	newSlice := make([]string, len(m.GroupNames))
	copy(newSlice, m.GroupNames)

	return CollectionGroupUpdateMessage{
		CollectionId: m.CollectionId,
		GroupNames:   newSlice,
		Sequence:     m.Sequence,
		SenderIp:     m.SenderIp,
		ToForward:    m.ToForward,
	}
}

func (c *CollectionGroupUpdateMessage) GetSequence() uint64 {
	return 0
}

func (c *CollectionGroupUpdateMessage) SetSequence(seq uint64) { //do nothing
}

type CollectionGroupUpdateMessages []CollectionGroupUpdateMessage

// collection management

type NewCollectionMessage struct {
	CollectionId        string `json:"collectionId"`
	CollectionName      string `json:"collectionName"`
	CollectionOwner     string `json:"collectionOwner"`
	IsStrong            bool   `json:"isStrong"`
	LeaderIp            string `json:"leaderIp"`
	SenderIp            string `json:"senderIp"`
	SkipUniquenessCheck bool   `json:"skipUniquenessCheck"`
}

type RemoveCollectionMessage struct {
	CollectionId string   `json:"collectionId"`
	SenderIp     string   `json:"senderIp"`
	Groups       []string `json:"groups"`
	UserId       string   `json:"userId"`
}

type SetCollectionMessage struct {
	CollectionId        string                    `json:"collectionId"`
	CollectionName      string                    `json:"collectionName"`
	Records             map[string]db.RecordValue `json:"records"`
	Groups              []string                  `json:"groups"`
	UserId              string                    `json:"userId"`
	IsStrong            bool                      `json:"isStrong"`
	SkipUniquenessCheck bool                      `json:"skipUniquenessCheck"`
}
