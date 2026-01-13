package dataManagerShared

import (
	"sync"

	"github.com/ermes-thesis/ermes-common/logger"
)

// Collection Info
type Collection interface {
	CollectionId() string //duplicated?
	IsLocal() bool
	IsStrong() bool
	AddHost(hostId string)
	RemoveHost(hostId string)
	HostsId() map[string]struct{}
	SetToLocal(uint64)
	SetToRemote()
	GetCollectionState() CollectionState
}

type PostponedDecision struct {
	SendFunc   func()
	DeleteFunc func()
}

// consistency levels

// StrongCollection strong collection
type StrongCollection struct {
	collectionId               string
	collectionState            CollectionState
	IsLeader                   bool
	LeaderId                   string
	WriteMutex                 *sync.Mutex
	ConsistencyClock           uint64
	postponedDecisionBehaviour PostponedDecision
}

func NewStrongCollection(collectionId string, size uint64, isRemote bool, isLeader bool, leaderId string) Collection {
	hosts := make(map[string]struct{})
	if isRemote {
		return &StrongCollection{
			collectionId:               collectionId,
			collectionState:            NewRemoteCollection(hosts),
			IsLeader:                   false,
			LeaderId:                   leaderId,
			WriteMutex:                 &sync.Mutex{},
			postponedDecisionBehaviour: PostponedDecision{},
		}
	}
	c := &StrongCollection{
		collectionId:               collectionId,
		collectionState:            NewLocalCollection(size, hosts),
		IsLeader:                   isLeader,
		WriteMutex:                 &sync.Mutex{},
		postponedDecisionBehaviour: PostponedDecision{},
	}
	if isLeader {
		c.LeaderId = leaderId
	}
	return c
}

func (s *StrongCollection) AddPostponedSendBehaviour(f func()) {
	s.postponedDecisionBehaviour.SendFunc = f
}

func (s *StrongCollection) AddPostponedDeleteBehaviour(f func()) {
	s.postponedDecisionBehaviour.DeleteFunc = f
}

func (s *StrongCollection) ExecutePostponedDecisionBehaviour(log logger.Logger) {
	log.Info("ExecutePostponedDecisionBehaviour called", "collectionId", s.collectionId)
	if s.postponedDecisionBehaviour.SendFunc != nil {
		log.Info("Executing postponed send behaviour", "collectionId", s.collectionId)
		s.postponedDecisionBehaviour.SendFunc()
	}
	if s.postponedDecisionBehaviour.DeleteFunc != nil {
		log.Info("Executing postponed delete behaviour", "collectionId", s.collectionId)
		s.postponedDecisionBehaviour.DeleteFunc()
	}
	s.postponedDecisionBehaviour = PostponedDecision{}
}

func (s *StrongCollection) HasPostponedDecisionBehaviour() bool {
	return s.postponedDecisionBehaviour.SendFunc != nil || s.postponedDecisionBehaviour.DeleteFunc != nil
}

func (s *StrongCollection) RemovePostponedDeleteBehaviour() {
	s.postponedDecisionBehaviour.DeleteFunc = nil
}

func (s *StrongCollection) GetCollectionState() CollectionState {
	return s.collectionState
}

func (s *StrongCollection) SetToLocal(size uint64) {
	if s.collectionState.IsLocal() {
		return
	}
	collectionState := NewLocalCollection(size, s.collectionState.HostsId())
	collectionState.(*LocalCollection).CollectionLock.Lock()
	collectionState.setCollectionSendingTo(s.GetCollectionState().getCollectionSendingTo())
	s.collectionState = collectionState
}

func (s *StrongCollection) SetToRemote() {
	if !s.collectionState.IsLocal() {
		return
	}
	s.IsLeader = false
	collectionState := NewRemoteCollection(s.collectionState.HostsId())
	collectionState.setCollectionSendingTo(s.GetCollectionState().getCollectionSendingTo())
	s.collectionState = collectionState
}

func (s *StrongCollection) CollectionId() string {
	return s.collectionId
}
func (s *StrongCollection) AddHost(hostId string) {
	s.collectionState.AddHost(hostId)
}
func (s *StrongCollection) IsLocal() bool {
	return s.collectionState.IsLocal()
}
func (s *StrongCollection) IsStrong() bool {
	return true
}
func (s *StrongCollection) HostsId() map[string]struct{} {
	return s.collectionState.HostsId()
}

func (s *StrongCollection) RemoveHost(hostId string) {
	s.collectionState.RemoveHost(hostId)
}

// WeakCollection weak collection
type WeakCollection struct {
	collectionId    string
	collectionState CollectionState
	WriteMutex      sync.Mutex
}

func NewWeakCollection(collectionId string, size uint64, isRemote bool) Collection {
	hosts := make(map[string]struct{})
	if isRemote {
		return &WeakCollection{
			collectionId:    collectionId,
			collectionState: NewRemoteCollection(hosts),
			WriteMutex:      sync.Mutex{},
		}
	}
	return &WeakCollection{
		collectionId:    collectionId,
		collectionState: NewLocalCollection(size, hosts),
		WriteMutex:      sync.Mutex{},
	}
}

func (w *WeakCollection) GetCollectionState() CollectionState {
	return w.collectionState
}

func (w *WeakCollection) SetToLocal(size uint64) {
	if w.collectionState.IsLocal() {
		return
	}
	collectionState := NewLocalCollection(size, w.collectionState.HostsId())
	collectionState.(*LocalCollection).CollectionLock.Lock()
	collectionState.setCollectionSendingTo(w.GetCollectionState().getCollectionSendingTo())
	w.collectionState = collectionState
}

func (w *WeakCollection) SetToRemote() {
	if !w.collectionState.IsLocal() {
		return
	}
	collectionState := NewRemoteCollection(w.collectionState.HostsId())
	collectionState.setCollectionSendingTo(w.GetCollectionState().getCollectionSendingTo())
	w.collectionState = collectionState
}

func (w *WeakCollection) CollectionId() string {
	return w.collectionId
}
func (w *WeakCollection) AddHost(hostId string) {
	w.collectionState.AddHost(hostId)
}
func (w *WeakCollection) IsLocal() bool {
	return w.collectionState.IsLocal()
}
func (w *WeakCollection) IsStrong() bool {
	return false
}
func (w *WeakCollection) HostsId() map[string]struct{} {
	return w.collectionState.HostsId()
}

func (w *WeakCollection) RemoveHost(hostId string) {
	w.collectionState.RemoveHost(hostId)
}
