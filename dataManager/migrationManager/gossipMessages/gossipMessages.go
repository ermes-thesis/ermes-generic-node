package gossipmessages

import (
	"encoding/json"
)

type GossipMessage interface {
	GetMessageType() string
	GetSenderID() string
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

func (b *BaseGossipMessage) FromJSON(data []byte) error {
	return json.Unmarshal(data, b)
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

func (c *ChildStateMessage) FromJSON(data []byte) error {
	return json.Unmarshal(data, c)
}

// LeaderDelegaMessage represents a leadership delegation message include info to update forces
type LeaderDelegaMessage struct {
	BaseGossipMessage
	FreeRedisMemory float64 `json:"freeRedisMemory"`
}

func NewLeaderDelegaMessage(senderID string, freeRedisMemory float64) *LeaderDelegaMessage {
	return &LeaderDelegaMessage{
		BaseGossipMessage: BaseGossipMessage{
			MessageType: "LEADER_DELEGA",
			SenderID:    senderID,
		},
		FreeRedisMemory: freeRedisMemory,
	}
}

func (l *LeaderDelegaMessage) FromJSON(data []byte) error {
	return json.Unmarshal(data, l)
}
