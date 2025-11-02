package strongPolicy

import (
	gossip "dataManager/messages"
)

type StrongCollectionPolicy interface {
	SetDefaultState()
	ManageGossipMessage(msg gossip.GossipMessage)
	RemoveInfo(collectionId string)
	ManageNewStrongRequest(collectionId string, childName string, isWrite bool, requestLatency int)
}

type CurrentPolicy int

const (
	Pending CurrentPolicy = iota
	ERMESPOLICY
	DONOTHINGPOLICY
)

// dumb policy for comparison in evaluation
type DoNothingPolicy struct{}

func NewDoNothingPolicy() DoNothingPolicy {
	policy := DoNothingPolicy{}

	return policy
}

func (d DoNothingPolicy) ManageGossipMessage(msg gossip.GossipMessage) {}
func (d DoNothingPolicy) RemoveInfo(collectionId string)               {}
func (d DoNothingPolicy) ManageNewStrongRequest(collectionId string, childName string, isWrite bool, requestLatency int) {
}
func (d DoNothingPolicy) SetDefaultState() {}
