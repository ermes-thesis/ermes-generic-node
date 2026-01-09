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

type CollectionPartialView struct {
	//sync
	mutex sync.RWMutex
	//data
	knownCollections map[string]Collection //maps collectionID to info related to that collection

	resourceUsageMutex sync.RWMutex
	nodeResourceUsage  map[string]NodeStateInfo //maps nodeID to resource usage info
}

func NewCollectionPartialView() *CollectionPartialView {
	return &CollectionPartialView{
		knownCollections:  make(map[string]Collection),
		nodeResourceUsage: make(map[string]NodeStateInfo),
	}
}

func (c *CollectionPartialView) GetCollection(collectionId string) (Collection, bool) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	collection, exists := c.knownCollections[collectionId]
	return collection, exists
}

func (c *CollectionPartialView) SetCollection(collectionId string, collection Collection) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.knownCollections[collectionId] = collection
}

func (c *CollectionPartialView) RemoveCollection(collectionId string) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	delete(c.knownCollections, collectionId)
}

func (c *CollectionPartialView) GetNodeResourceUsage(nodeId string) (NodeStateInfo, bool) {
	c.resourceUsageMutex.RLock()
	defer c.resourceUsageMutex.RUnlock()

	info, exists := c.nodeResourceUsage[nodeId]
	return info, exists
}

func (c *CollectionPartialView) SetNodeResourceUsage(nodeId string, usage NodeStateInfo) {
	c.resourceUsageMutex.Lock()
	defer c.resourceUsageMutex.Unlock()

	c.nodeResourceUsage[nodeId] = usage
}

type NodeStateInfo struct {
	FreeRedisMemory float64
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

	c.mutex.RLock()

	snapshot := make([]collectionDump, 0, len(c.knownCollections))

	for id, collection := range c.knownCollections {
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
	c.mutex.RUnlock()

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
	c.mutex.Lock()
	defer c.mutex.Unlock()
	//imp

	c.knownCollections = make(map[string]Collection)
	c.nodeResourceUsage = make(map[string]NodeStateInfo)

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

	c.knownCollections[info.CollectionID] = coll
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

	c.knownCollections[info.CollectionID] = coll
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
