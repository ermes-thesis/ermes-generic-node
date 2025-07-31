package db

import (
	"maps"
	"strconv"
	"strings"
	"time"
)

// Consistency defines the consistency level for data collections in the Ermes framework.
// It determines the guarantees provided when reading or writing data.
type Consistency int

const (
	// STRONG_CONSISTENT ensures sequential reads and writes.
	// Writes are visible immediately and consistently across all nodes,
	// but may require coordination with a leader.
	//
	// Use this Consistency if you prefer strong guarantees on the data of the
	// collection at the expenses of serverless functions performances
	STRONG_CONSISTENT Consistency = iota

	// EVENTUAL_CONSISTENT provides lower latency and higher availability by allowing
	// reads and writes to occur on any replica, with eventual convergence across replicas.
	//
	// Use this Consistency if you prefer higher serverless functions performances
	// at the expanses of the data consistency
	EVENTUAL_CONSISTENT
)

type CollectionInfo struct {
	Id       string
	Metadata Metadata
}
type Metadata struct {
	Name           string
	Owner          string
	CreatedAt      time.Time
	Groups         []string
	Consistency    Consistency
	Geolocation    string
	CustomMetadata map[string]string
}

func (m Metadata) toList() []any {
	var res []any

	res = append(res, "name", m.Name, "owner", m.Owner, "created_at", m.CreatedAt.UnixMilli(), "groups",
		strings.Join(m.Groups, ","), "consistency", int(m.Consistency), "geolocation", m.Geolocation)
	for k, v := range m.CustomMetadata {
		res = append(res, k, v)
	}
	return res
}

func fromMap(metadataMap map[string]string) (*Metadata, error) {
	i, err := strconv.ParseInt(metadataMap["created_at"], 10, 64)
	if err != nil {
		return nil, err
	}

	consistency, err := strconv.ParseInt(metadataMap["consistency"], 10, 8)
	if err != nil {
		return nil, err
	}
	name := metadataMap["name"]
	owner := metadataMap["owner"]
	groups := metadataMap["groups"]
	geolocation := metadataMap["geolocation"]

	maps.DeleteFunc(metadataMap, func(key string, value string) bool {
		return key == "created_at" || key == "consistency" || key == "name" || key == "owner" || key == "groups" || key == "geolocation"
	})

	metadata := &Metadata{
		Name:           name,
		Owner:          owner,
		CreatedAt:      time.UnixMilli(i),
		Groups:         strings.Split(groups, ","),
		Consistency:    Consistency(consistency),
		Geolocation:    geolocation,
		CustomMetadata: metadataMap,
	}
	return metadata, nil
}

type MemStats struct {
	AllocationStats
	ReplicationBacklogSize uint64
	LuaScriptsCacheSize    uint64
	KeysCount              uint64
	BytesKeysRatio         uint64
}

type AllocationStats struct {
	UsedMemoryPeak              uint64
	CurrentUsedMemory           uint64
	UsedMemoryOverhead          uint64
	UsedMemoryStartup           uint64
	UsedMemoryDataset           uint64
	UsedMemoryDatasetPercentage float64
}
