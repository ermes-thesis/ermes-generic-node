package dataManagerShared

import "sync"

// collection types

type CollectionState interface {
	CollectionId() string //TODO -> refacotr and move up in Collection type only
	IsLocal() bool
	HostsId() map[string]struct{}
	AddHost(hostId string)
}

type LocalCollection struct {
	collectionId string
	size         int
	hostsId      map[string]struct{} //map to guarantee univocity
	//a collection can be stored locally and on children simultaneously
}

func NewLocalCollection(collectionId string, size int, hostNames map[string]struct{}) CollectionState {
	return &LocalCollection{
		collectionId: collectionId,
		size:         size,
		hostsId:      hostNames,
	}
}
func (l *LocalCollection) AddHost(hostId string) {
	l.hostsId[hostId] = struct{}{}
}
func (l *LocalCollection) CollectionId() string {
	return l.collectionId
}
func (l *LocalCollection) IsLocal() bool {
	return true
}
func (l *LocalCollection) Size() int {
	return l.size
}
func (l *LocalCollection) HostsId() map[string]struct{} {
	return l.hostsId
}

type RemoteCollection struct {
	collectionId string
	hostsId      map[string]struct{} //map to guarantee univocity
	//TODO maybe insert latency with host or maybe not idk
}

func NewRemoteCollection(collectionId string, hostNames map[string]struct{}) CollectionState {
	return &RemoteCollection{
		collectionId: collectionId,
		hostsId:      hostNames,
	}
}

func (l *RemoteCollection) CollectionId() string {
	return l.collectionId
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

// partialView
type CollectionPartialView struct {
	//sync
	Mutex sync.Mutex
	//data
	KnowedCollections map[string]Collection //maps collectionID to info related to that collection
}
