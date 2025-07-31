package gossipmessages

import (
	"encoding/json"
)

type GossipMessage interface {
	GetMessageType() string
	GetSenderID() string
	ToJSON() ([]byte, error)
	FromJSON(data []byte) error
}

type BaseGossipMessage struct {
	MessageType string `json:"messageType"`
	SenderID    string `json:"senderId"`
}

func (b *BaseGossipMessage) GetMessageType() string {
	return b.MessageType
}

func (b *BaseGossipMessage) GetSenderID() string {
	return b.SenderID
}

func (c *ChildStateMessage) ToJSON() ([]byte, error) {
	return json.Marshal(c)
}

func (c *ChildStateMessage) FromJSON(data []byte) error {
	return json.Unmarshal(data, c)
}

// HwStaChildStateMessageteMessage represents the hardware state of a node + contains info of a new collection created in the child node.
type ChildStateMessage struct {
	BaseGossipMessage
	//CPUUsage          float64 `json:"cpuUsage"`
	FreeRedisMemory float64 `json:"freeRedisMemory"`
}

func NewChildStateMessage(senderID string, freeRedisMemory float64) *ChildStateMessage {
	return &ChildStateMessage{
		BaseGossipMessage: BaseGossipMessage{
			MessageType: "HW_STATE",
			SenderID:    senderID,
		},
		FreeRedisMemory: freeRedisMemory,
	}
}

// LeaderDelegaMessage represents a leadership delegation message include info to update forces
type LeaderDelegaMessage struct {
	BaseGossipMessage
	//TODO
}

func NewLeaderDelegaMessage(senderID string) *LeaderDelegaMessage {
	return &LeaderDelegaMessage{
		BaseGossipMessage: BaseGossipMessage{
			MessageType: "LEADER_DELEGA",
			SenderID:    senderID,
		},
	}
}

func (l *LeaderDelegaMessage) ToJSON() ([]byte, error) {
	return json.Marshal(l)
}

func (l *LeaderDelegaMessage) FromJSON(data []byte) error {
	return json.Unmarshal(data, l)
}
