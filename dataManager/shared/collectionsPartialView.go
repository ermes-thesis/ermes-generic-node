package dataManagerShared

import (
	"config"
	"fmt"
	"maps"
	"server"
	"sync"
)

// collection types

type CollectionState interface {
	IsLocal() bool
	HostsId() map[string]struct{}
	AddHost(hostId string)
	RemoveHost(hostId string)
}

type LocalCollection struct {
	size uint64
	//a collection can be stored locally and on children simultaneously
	hostsId        map[string]struct{} //map to guarantee univocity
	CollectionLock sync.RWMutex        //if  lock is taken, a collection can't be moved from this node => wasm functions that use a collection must take the lock
}

func (l *LocalCollection) String() string {
	return fmt.Sprintf("{size:%d hostsId:%v local:%t}", l.size, l.hostsId, l.IsLocal())
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

func (l *LocalCollection) IncSize(size int64) {
	l.size = uint64(int64(l.size) + size)
}

func (l *LocalCollection) HostsId() map[string]struct{} {
	return maps.Clone(l.hostsId)
}

func (l *LocalCollection) RemoveHost(hostId string) {
	delete(l.hostsId, hostId)
}

type RemoteCollection struct {
	hostsId map[string]struct{} //map to guarantee univocity
	//TODO maybe insert latency with host or maybe not idk
}

func (l *RemoteCollection) String() string {
	return fmt.Sprintf("{hostsId:%v local:%t}", l.hostsId, l.IsLocal())
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
	return maps.Clone(l.hostsId)
}

func (l *RemoteCollection) RemoveHost(hostId string) {
	delete(l.hostsId, hostId)
}

type CollectionPartialView struct {
	//sync
	Mutex sync.Mutex
	//data
	KnownCollections map[string]Collection //maps collectionID to info related to that collection
}

func (c *CollectionPartialView) RestoreInfoFromSettings(configData []config.CollectionSettingsInfo) error {
	//sync
	c.Mutex.Lock()
	defer c.Mutex.Unlock()
	//imp

	c.KnownCollections = make(map[string]Collection)
	for _, coll := range configData {
		if coll.IsStrong {
			c.restoreStrongCollectionFromSettings(coll)
		} else {
			c.restoreWeakCollectionFromSettings(coll)
		}
	}

	return nil
}

func (c *CollectionPartialView) restoreStrongCollectionFromSettings(info config.CollectionSettingsInfo) {
	CollectionSize := *info.CollectionSize
	if info.IsRemote {
		CollectionSize = 0
	}

	coll := NewStrongCollection(info.CollectionID, CollectionSize, info.IsRemote, info.IsLeader != nil && *info.IsLeader)

	//set leader id
	if info.LeaderID != nil {
		coll.(*StrongCollection).LeaderId = *info.LeaderID
	}

	//add hosts
	for _, host := range info.HostsID {
		coll.AddHost(host)
	}

	c.KnownCollections[info.CollectionID] = coll
}

func (c *CollectionPartialView) restoreWeakCollectionFromSettings(info config.CollectionSettingsInfo) {
	CollectionSize := *info.CollectionSize
	if info.IsRemote {
		CollectionSize = 0
	}

	coll := NewWeakCollection(info.CollectionID, CollectionSize, info.IsRemote)

	//add hosts
	for _, host := range info.HostsID {
		coll.AddHost(host)
	}

	c.KnownCollections[info.CollectionID] = coll
}

func (c *CollectionPartialView) GetClosestChild(hostSet map[string]struct{}, serverInfo server.ServerInfo) string {
	var minLatency int
	var minLatencyHost string

	for _, child := range serverInfo.Children {
		if _, ok := hostSet[child.HostName]; !ok {
			continue
		}
		if minLatencyHost == "" || child.Latency < minLatency {
			minLatency = child.Latency
			minLatencyHost = child.HostName
		}
	}

	return minLatencyHost
}
