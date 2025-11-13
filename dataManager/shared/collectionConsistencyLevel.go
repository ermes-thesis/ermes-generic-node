package dataManagerShared

import (
	"sync"
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

// consistency levels

// StrongCollection strong collection
type StrongCollection struct {
	collectionId               string
	collectionState            CollectionState
	IsLeader                   bool
	LeaderId                   string
	WriteMutex                 *sync.Mutex
	ConsistencyClock           uint64
	postponedDecisionBehaviour func()
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
			postponedDecisionBehaviour: nil,
		}
	}
	c := &StrongCollection{
		collectionId:               collectionId,
		collectionState:            NewLocalCollection(size, hosts),
		IsLeader:                   isLeader,
		WriteMutex:                 &sync.Mutex{},
		postponedDecisionBehaviour: nil,
	}
	if isLeader {
		c.LeaderId = leaderId
	}
	return c
}

func (s *StrongCollection) AddPostponedDecisionBehaviour(f func()) {
	oldBehaviour := s.postponedDecisionBehaviour
	if oldBehaviour == nil {
		s.postponedDecisionBehaviour = f
	} else {
		s.postponedDecisionBehaviour = func() {
			oldBehaviour()
			f()
		}
	}
}

func (s *StrongCollection) ExecutePostponedDecisionBehaviour() {
	if s.postponedDecisionBehaviour != nil {
		s.postponedDecisionBehaviour()
		s.postponedDecisionBehaviour = nil
	}
}

func (s *StrongCollection) GetCollectionState() CollectionState {
	return s.collectionState
}

func (s *StrongCollection) SetToLocal(size uint64) {
	if s.collectionState.IsLocal() {
		return
	}
	collectionState := NewLocalCollection(size, s.collectionState.HostsId())
	s.collectionState = collectionState
}

func (s *StrongCollection) SetToRemote() {
	if !s.collectionState.IsLocal() {
		return
	}
	s.IsLeader = false
	collectionState := NewRemoteCollection(s.collectionState.HostsId())
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
}

func NewWeakCollection(collectionId string, size uint64, isRemote bool) Collection {
	hosts := make(map[string]struct{})
	if isRemote {
		return &WeakCollection{
			collectionId:    collectionId,
			collectionState: NewRemoteCollection(hosts),
		}
	}
	return &WeakCollection{
		collectionId:    collectionId,
		collectionState: NewLocalCollection(size, hosts),
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
	w.collectionState = collectionState
}

func (w *WeakCollection) SetToRemote() {
	if !w.collectionState.IsLocal() {
		return
	}
	collectionState := NewRemoteCollection(w.collectionState.HostsId())
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
