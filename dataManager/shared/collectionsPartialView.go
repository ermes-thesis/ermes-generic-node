package dataManagerShared

import "sync"

// collection Info
type Collection interface {
	CollectionId() string //duplicated?
	IsLocal() bool
	HostsId() map[string]struct{}
}

type LocalCollection struct {
	collectionId string
	size         int
	hostsId      map[string]struct{} //map to guarantee univocity
	//a collection can be stored locally and on children simultaneously
}

func NewLocalCollection(collectionId string, size int, childNames map[string]struct{}) Collection {
	return &LocalCollection{
		collectionId: collectionId,
		size:         size,
		hostsId:      childNames,
	}
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

func NewRemoteCollection(collectionId string, childNames map[string]struct{}) Collection {
	return &RemoteCollection{
		collectionId: collectionId,
		hostsId:      childNames,
	}
}

func (l *RemoteCollection) CollectionId() string {
	return l.collectionId
}
func (l *RemoteCollection) IsLocal() bool {
	return false
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
