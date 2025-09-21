package dataManagerShared

import "sync"

// collection types

type CollectionState interface {
	IsLocal() bool
	HostsId() map[string]struct{}
	AddHost(hostId string)
}

type LocalCollection struct {
	size uint64
	//a collection can be stored locally and on children simultaneously
	hostsId        map[string]struct{} //map to guarantee univocity
	CollectionLock sync.RWMutex        //if  lock is taken, a collection can't be moved from this node => wasm functions that use a collection must take the lock
}

func NewLocalCollection(size uint64, hostNames map[string]struct{}) CollectionState {
	return &LocalCollection{
		size:    size,
		hostsId: hostNames,
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
func (l *LocalCollection) HostsId() map[string]struct{} {
	return l.hostsId
}

type RemoteCollection struct {
	hostsId map[string]struct{} //map to guarantee univocity
	//TODO maybe insert latency with host or maybe not idk
}

func NewRemoteCollection(hostNames map[string]struct{}) CollectionState {
	return &RemoteCollection{
		hostsId: hostNames,
	}
}

func (l *RemoteCollection) IsLocal() bool {
	return false
}
func (l *RemoteCollection) AddHost(hostId string) {
	l.hostsId[hostId] = struct{}{}
}
func (l *RemoteCollection) HostsId() map[string]struct{} {
	return l.hostsId
}

type CollectionPartialView struct {
	//sync
	Mutex sync.Mutex
	//data
	KnownCollections map[string]Collection //maps collectionID to info related to that collection
}
