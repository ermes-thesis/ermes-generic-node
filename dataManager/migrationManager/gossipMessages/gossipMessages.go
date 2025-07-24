package gossipmessages

type GossipMessage interface {
	GetMessageType() string
	GetSenderID() string
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

// HwStateMessage represents the hardware state of a node
type HwStateMessage struct {
	BaseGossipMessage
	//CPUUsage          float64 `json:"cpuUsage"`
	MemoryUsage float64 `json:"memoryUsage"`
	DiskUsage   float64 `json:"diskUsage"`
}

func NewHwStateMessage(senderID string, memoryUsage float64, diskUsage float64) *HwStateMessage {
	return &HwStateMessage{
		BaseGossipMessage: BaseGossipMessage{
			MessageType: "HW_STATE",
			SenderID:    senderID,
		},
		MemoryUsage: memoryUsage,
		DiskUsage:   diskUsage,
	}
}

// NewCollectionsMessage represents information about new collections
type NewCollectionsMessage struct {
	BaseGossipMessage
	//boh ci vanno i metadati delle collezioni create nel mentre -> fare dopo merge pr
}

func NewNewCollectionsMessage(senderID string) *NewCollectionsMessage {
	return &NewCollectionsMessage{
		BaseGossipMessage: BaseGossipMessage{
			MessageType: "NEW_COLLECTIONS",
			SenderID:    senderID,
		},
	}
}

// LeaderDelegaMessage represents a leadership delegation message include info to update forces
type LeaderDelegaMessage struct {
	BaseGossipMessage
	CollectionID      string  `json:"collectionId"`
	AggregatedTension float64 `json:"aggregatedTension"`
}

func NewLeaderDelegaMessage(senderID string, collectionID string, aggregatedTension float64) *LeaderDelegaMessage {
	return &LeaderDelegaMessage{
		BaseGossipMessage: BaseGossipMessage{
			MessageType: "LEADER_DELEGA",
			SenderID:    senderID,
		},
		CollectionID:      collectionID,
		AggregatedTension: aggregatedTension,
	}
}
