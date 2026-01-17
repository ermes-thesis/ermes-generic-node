package dataManagerShared

import (
	"errors"
	"fmt"
	"maps"
	"sync"
)

// collection types

type CollectionState interface {
	IsLocal() bool
	HostsId() map[string]struct{}
	AddHost(hostId string)
	RemoveHost(hostId string)

	//migration safty
	IsCollectionBeingMigrated() bool
	AddCollectionMigrationHost(hostId string)
	RemoveCollectionMigrationHostAndExecutePostponedUpdates(hostId string) error
	HasPostponedUpdate(hostId string) bool
	RemoveCollectionMigrationHostExecutingPostponedUpdatesLocally(hostId string) error
	AddPostponedUpdate(hostId string, remote func(), local func())

	setCollectionSendingTo(map[string]DoubleFunctionQueue, *sync.RWMutex)
	getCollectionSendingTo() (map[string]DoubleFunctionQueue, *sync.RWMutex)
}

type LocalCollection struct {
	size uint64

	//a collection can be stored locally and on children simultaneously
	hostsId     map[string]struct{} //map to guarantee univocity
	hostsIdLock sync.RWMutex

	CollectionLock sync.RWMutex //if  lock is taken, a collection can't be moved from this node => wasm functions that use a collection must take the lock

	collectionSendingTo     map[string]DoubleFunctionQueue //map to guarantee univocity
	collectionSendingToLock *sync.RWMutex
}

type DoubleFunctionQueue struct {
	remoteUpdates FunctionQueue
	localUpdates  FunctionQueue
}

func (l *LocalCollection) String() string {
	l.hostsIdLock.RLock()
	defer l.hostsIdLock.RUnlock()

	return fmt.Sprintf("{size:%d hostsId:%v local:%t}", l.size, l.hostsId, l.IsLocal())
}

func NewLocalCollection(size uint64, hostNames map[string]struct{}) CollectionState {
	return &LocalCollection{
		size:                    size,
		hostsId:                 hostNames,
		collectionSendingTo:     make(map[string]DoubleFunctionQueue),
		collectionSendingToLock: &sync.RWMutex{},
	}
}
func (l *LocalCollection) AddHost(hostId string) {
	l.hostsIdLock.Lock()
	defer l.hostsIdLock.Unlock()

	l.hostsId[hostId] = struct{}{}
}
func (l *LocalCollection) IsLocal() bool {
	return true
}
func (l *LocalCollection) Size() uint64 {
	return l.size
}

func (l *LocalCollection) IncSize(size int64) {
	l.size = uint64(int64(l.size) + size)
}

func (l *LocalCollection) HostsId() map[string]struct{} {
	l.hostsIdLock.RLock()
	defer l.hostsIdLock.RUnlock()

	return maps.Clone(l.hostsId)
}

func (l *LocalCollection) RemoveHost(hostId string) {
	l.hostsIdLock.Lock()
	defer l.hostsIdLock.Unlock()

	delete(l.hostsId, hostId)
}

func (l *LocalCollection) IsCollectionBeingMigrated() bool {
	l.collectionSendingToLock.RLock()
	defer l.collectionSendingToLock.RUnlock()

	return len(l.collectionSendingTo) > 0
}

func (l *LocalCollection) AddCollectionMigrationHost(hostId string) {
	l.collectionSendingToLock.Lock()
	defer l.collectionSendingToLock.Unlock()

	if _, exists := l.collectionSendingTo[hostId]; exists {
		return
	}
	l.collectionSendingTo[hostId] = DoubleFunctionQueue{
		remoteUpdates: NewFunctionQueue(),
		localUpdates:  NewFunctionQueue(),
	}
}

func (l *LocalCollection) RemoveCollectionMigrationHostAndExecutePostponedUpdates(hostId string) error {
	l.collectionSendingToLock.Lock()
	defer l.collectionSendingToLock.Unlock()

	postponed, ok := l.collectionSendingTo[hostId]
	if !ok {
		return errors.New("hostId not found in collectionSendingTo")
	}

	postponed.remoteUpdates.ExecuteFunctions()

	delete(l.collectionSendingTo, hostId)
	return nil
}
func (l *LocalCollection) HasPostponedUpdate(hostId string) bool {
	l.collectionSendingToLock.RLock()
	defer l.collectionSendingToLock.RUnlock()

	_, ok := l.collectionSendingTo[hostId]
	return ok
}

func (l *LocalCollection) RemoveCollectionMigrationHostExecutingPostponedUpdatesLocally(hostId string) error {
	l.collectionSendingToLock.Lock()
	defer l.collectionSendingToLock.Unlock()

	postponed, ok := l.collectionSendingTo[hostId]
	if !ok {
		return errors.New("hostId not found in collectionSendingTo")
	}

	postponed.localUpdates.ExecuteFunctions()

	delete(l.collectionSendingTo, hostId)
	return nil
}

func (l *LocalCollection) AddPostponedUpdate(hostId string, remote func(), local func()) {
	l.collectionSendingToLock.Lock()
	defer l.collectionSendingToLock.Unlock()

	if _, ok := l.collectionSendingTo[hostId]; !ok {
		return
	}

	l.collectionSendingTo[hostId].remoteUpdates.AddFunction(remote)
	l.collectionSendingTo[hostId].localUpdates.AddFunction(local)
}

func (l *LocalCollection) setCollectionSendingTo(collectionSendingTo map[string]DoubleFunctionQueue, mutex *sync.RWMutex) {
	l.collectionSendingTo = collectionSendingTo
	l.collectionSendingToLock = mutex
}

func (l *LocalCollection) getCollectionSendingTo() (map[string]DoubleFunctionQueue, *sync.RWMutex) {
	return l.collectionSendingTo, l.collectionSendingToLock
}

type RemoteCollection struct {
	hostsId     map[string]struct{} //map to guarantee univocity
	hostsIdLock sync.RWMutex

	collectionSendingTo     map[string]DoubleFunctionQueue //map to guarantee univocity
	collectionSendingToLock *sync.RWMutex
}

func (l *RemoteCollection) String() string {
	l.hostsIdLock.RLock()
	defer l.hostsIdLock.RUnlock()

	return fmt.Sprintf("{hostsId:%v local:%t}", l.hostsId, l.IsLocal())
}

func NewRemoteCollection(hostNames map[string]struct{}) CollectionState {
	return &RemoteCollection{
		hostsId:                 hostNames,
		collectionSendingTo:     make(map[string]DoubleFunctionQueue),
		collectionSendingToLock: &sync.RWMutex{},
	}
}

func (l *RemoteCollection) IsLocal() bool {
	return false
}
func (l *RemoteCollection) AddHost(hostId string) {
	l.hostsIdLock.Lock()
	defer l.hostsIdLock.Unlock()

	l.hostsId[hostId] = struct{}{}
}
func (l *RemoteCollection) HostsId() map[string]struct{} {
	l.hostsIdLock.RLock()
	defer l.hostsIdLock.RUnlock()

	return maps.Clone(l.hostsId)
}

func (l *RemoteCollection) RemoveHost(hostId string) {
	l.hostsIdLock.Lock()
	defer l.hostsIdLock.Unlock()

	delete(l.hostsId, hostId)
}

func (l *RemoteCollection) IsCollectionBeingMigrated() bool {
	l.collectionSendingToLock.RLock()
	defer l.collectionSendingToLock.RUnlock()

	return len(l.collectionSendingTo) > 0
}

func (l *RemoteCollection) AddCollectionMigrationHost(hostId string) {
	l.collectionSendingToLock.Lock()
	defer l.collectionSendingToLock.Unlock()

	if _, exists := l.collectionSendingTo[hostId]; exists {
		return
	}
	l.collectionSendingTo[hostId] = DoubleFunctionQueue{
		remoteUpdates: NewFunctionQueue(),
		localUpdates:  NewFunctionQueue(),
	}
}

func (l *RemoteCollection) RemoveCollectionMigrationHostAndExecutePostponedUpdates(hostId string) error {
	l.collectionSendingToLock.Lock()
	defer l.collectionSendingToLock.Unlock()

	postponed, ok := l.collectionSendingTo[hostId]
	if !ok {
		return errors.New("hostId not found in collectionSendingTo")
	}

	postponed.remoteUpdates.ExecuteFunctions()

	delete(l.collectionSendingTo, hostId)
	return nil
}

func (l *RemoteCollection) HasPostponedUpdate(hostId string) bool {
	l.collectionSendingToLock.RLock()
	defer l.collectionSendingToLock.RUnlock()

	_, ok := l.collectionSendingTo[hostId]
	return ok
}

func (l *RemoteCollection) RemoveCollectionMigrationHostExecutingPostponedUpdatesLocally(hostId string) error {
	l.collectionSendingToLock.Lock()
	defer l.collectionSendingToLock.Unlock()

	postponed, ok := l.collectionSendingTo[hostId]
	if !ok {
		return errors.New("hostId not found in collectionSendingTo")
	}

	postponed.localUpdates.ExecuteFunctions()

	delete(l.collectionSendingTo, hostId)
	return nil
}

func (l *RemoteCollection) setCollectionSendingTo(collectionSendingTo map[string]DoubleFunctionQueue, mutex *sync.RWMutex) {
	l.collectionSendingTo = collectionSendingTo
	l.collectionSendingToLock = mutex
}

func (l *RemoteCollection) getCollectionSendingTo() (map[string]DoubleFunctionQueue, *sync.RWMutex) {
	return l.collectionSendingTo, l.collectionSendingToLock
}

func (l *RemoteCollection) AddPostponedUpdate(hostId string, remote func(), local func()) {
	l.collectionSendingToLock.Lock()
	defer l.collectionSendingToLock.Unlock()

	if _, ok := l.collectionSendingTo[hostId]; !ok {
		return
	}

	l.collectionSendingTo[hostId].remoteUpdates.AddFunction(remote)
	l.collectionSendingTo[hostId].localUpdates.AddFunction(local)
}
