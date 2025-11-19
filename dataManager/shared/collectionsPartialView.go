package dataManagerShared

import (
	"cmp"
	"config"
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"server"
	"slices"
	"sync"
	"time"
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

type collectionDump struct {
	Timestamp      string   `json:"timestamp"`
	CollectionID   string   `json:"collection_id"`
	IsStrong       bool     `json:"is_strong"`
	IsLocal        bool     `json:"is_local"`
	IsLeader       bool     `json:"is_leader"`
	SequenceNumber uint64   `json:"sequence_number"`
	CollectionSize uint64   `json:"collection_size"`
	LeaderID       string   `json:"leader_id,omitempty"`
	HostsID        []string `json:"hosts_id"`
}

func (c *CollectionPartialView) DumpToJsonFile(path string) error {
	timestamp := time.Now().Format("2006-01-02 15:04:05")

	c.Mutex.Lock()

	snapshot := make([]collectionDump, 0, len(c.KnownCollections))

	for id, collection := range c.KnownCollections {
		hosts := maps.Clone(collection.HostsId())
		isStrong := collection.IsStrong()
		isLocal := collection.IsLocal()

		// Default size = 0 if remote (we can’t measure it)
		var size uint64 = 0
		if state, ok := collection.GetCollectionState().(*LocalCollection); ok {
			size = state.Size()
		}

		entry := collectionDump{
			Timestamp:      timestamp,
			CollectionID:   id,
			IsStrong:       isStrong,
			IsLocal:        isLocal,
			CollectionSize: size,
			HostsID:        slices.Sorted(maps.Keys(hosts)),
		}

		// For strong collections, include leader info
		if s, ok := collection.(*StrongCollection); ok {
			entry.IsLeader = s.IsLeader
			entry.LeaderID = s.LeaderId
			entry.SequenceNumber = s.ConsistencyClock
		}

		snapshot = append(snapshot, entry)
	}
	c.Mutex.Unlock()

	slices.SortFunc(snapshot, func(e collectionDump, e2 collectionDump) int {
		return cmp.Compare(e.CollectionID, e2.CollectionID)
	})
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	for _, d := range snapshot {
		err = encoder.Encode(d)
		if err != nil {
			return fmt.Errorf("failed to marshal known collections: %w", err)
		}
	}

	_, err = file.WriteString("\n")
	if err != nil {
		return fmt.Errorf("failed to write known collections: %w", err)
	}
	return nil
}

func (c *CollectionPartialView) RestoreInfoFromSettings(configData []config.CollectionSettingsInfo, serverInfo server.ServerInfo) error {
	//sync
	c.Mutex.Lock()
	defer c.Mutex.Unlock()
	//imp

	c.KnownCollections = make(map[string]Collection)
	for _, coll := range configData {
		if coll.IsStrong {
			c.restoreStrongCollectionFromSettings(coll, serverInfo)
		} else {
			c.restoreWeakCollectionFromSettings(coll)
		}
	}

	return nil
}

func (c *CollectionPartialView) restoreStrongCollectionFromSettings(info config.CollectionSettingsInfo, serverInfo server.ServerInfo) {
	CollectionSize := *info.CollectionSize
	if info.IsRemote {
		CollectionSize = 0
	}

	coll := NewStrongCollection(info.CollectionID, CollectionSize, info.IsRemote, info.IsLeader != nil && *info.IsLeader, serverInfo.HostName)

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
