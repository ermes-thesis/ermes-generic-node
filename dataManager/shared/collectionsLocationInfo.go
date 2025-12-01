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
	IsCollectionBeingMigratedTo(hostId string) bool
	AddCollectionMigrationHost(hostId string)
	RemoveCollectionMigrationHostAndExecutePostponedUpdates(hostId string) error
	AddPostponedUpdate(hostId string, f func())

	setCollectionSendingTo(map[string]FunctionQueue)
	getCollectionSendingTo() map[string]FunctionQueue
}

type LocalCollection struct {
	size uint64
	//a collection can be stored locally and on children simultaneously
	hostsId        map[string]struct{} //map to guarantee univocity
	CollectionLock sync.RWMutex        //if  lock is taken, a collection can't be moved from this node => wasm functions that use a collection must take the lock

	collectionSendingTo map[string]FunctionQueue //map to guarantee univocity
}

func (l *LocalCollection) String() string {
	return fmt.Sprintf("{size:%d hostsId:%v local:%t}", l.size, l.hostsId, l.IsLocal())
}

func NewLocalCollection(size uint64, hostNames map[string]struct{}) CollectionState {
	return &LocalCollection{
		size:                size,
		hostsId:             hostNames,
		collectionSendingTo: make(map[string]FunctionQueue),
	}
}
func (l *LocalCollection) AddHost(hostId string) {
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
	return maps.Clone(l.hostsId)
}

func (l *LocalCollection) RemoveHost(hostId string) {
	delete(l.hostsId, hostId)
}

func (l *LocalCollection) IsCollectionBeingMigrated() bool {
	return len(l.collectionSendingTo) > 0
}

func (l *LocalCollection) AddCollectionMigrationHost(hostId string) {
	if _, exists := l.collectionSendingTo[hostId]; exists {
		return
	}
	l.collectionSendingTo[hostId] = NewFunctionQueue()
}

func (l *LocalCollection) RemoveCollectionMigrationHostAndExecutePostponedUpdates(hostId string) error {
	postponed, ok := l.collectionSendingTo[hostId]
	if !ok {
		return errors.New("hostId not found in collectionSendingTo")
	}

	postponed.ExecuteFunctions()

	delete(l.collectionSendingTo, hostId)
	return nil
}

func (l *LocalCollection) IsCollectionBeingMigratedTo(hostId string) bool {
	_, ok := l.collectionSendingTo[hostId]
	return ok
}

func (l *LocalCollection) AddPostponedUpdate(hostId string, f func()) {
	l.collectionSendingTo[hostId].AddFunction(f)
}

func (l *LocalCollection) setCollectionSendingTo(collectionSendingTo map[string]FunctionQueue) {
	l.collectionSendingTo = collectionSendingTo
}

func (l *LocalCollection) getCollectionSendingTo() map[string]FunctionQueue {
	return l.collectionSendingTo
}

type RemoteCollection struct {
	hostsId map[string]struct{} //map to guarantee univocity

	collectionSendingTo map[string]FunctionQueue //map to guarantee univocity
}

func (l *RemoteCollection) String() string {
	return fmt.Sprintf("{hostsId:%v local:%t}", l.hostsId, l.IsLocal())
}

func NewRemoteCollection(hostNames map[string]struct{}) CollectionState {
	return &RemoteCollection{
		hostsId:             hostNames,
		collectionSendingTo: make(map[string]FunctionQueue),
	}
}

func (l *RemoteCollection) IsLocal() bool {
	return false
}
func (l *RemoteCollection) AddHost(hostId string) {
	l.hostsId[hostId] = struct{}{}
}
func (l *RemoteCollection) HostsId() map[string]struct{} {
	return maps.Clone(l.hostsId)
}

func (l *RemoteCollection) RemoveHost(hostId string) {
	delete(l.hostsId, hostId)
}

func (l *RemoteCollection) IsCollectionBeingMigrated() bool {
	return len(l.collectionSendingTo) > 0
}

func (l *RemoteCollection) AddCollectionMigrationHost(hostId string) {
	l.collectionSendingTo[hostId] = NewFunctionQueue()
}

func (l *RemoteCollection) RemoveCollectionMigrationHostAndExecutePostponedUpdates(hostId string) error {
	postponed, ok := l.collectionSendingTo[hostId]
	if !ok {
		return errors.New("hostId not found in collectionSendingTo")
	}

	postponed.ExecuteFunctions()

	delete(l.collectionSendingTo, hostId)
	return nil
}

func (l *RemoteCollection) setCollectionSendingTo(collectionSendingTo map[string]FunctionQueue) {
	l.collectionSendingTo = collectionSendingTo
}

func (l *RemoteCollection) getCollectionSendingTo() map[string]FunctionQueue {
	return l.collectionSendingTo
}

func (l *RemoteCollection) IsCollectionBeingMigratedTo(hostId string) bool {
	_, ok := l.collectionSendingTo[hostId]
	return ok
}

func (l *RemoteCollection) AddPostponedUpdate(hostId string, f func()) {
	l.collectionSendingTo[hostId].AddFunction(f)
}
