//go:build test
// +build test

package tests

import (
	"config"
	lru "dataManager/lru"
	shared "dataManager/shared"
	"db"
	mockDb "db/mocks"
	"server/info"
	"testing"

	"github.com/stretchr/testify/mock"
)

func SetupGetMemoryStatsMock(mockDB *mockDb.GenericDB) {
	memStats := &db.MemStats{
		AllocationStats: db.AllocationStats{
			UsedMemoryPeak:              1024 * 1024 * 100, // 100MB
			CurrentUsedMemory:           1024 * 1024 * 50,  // 50MB
			UsedMemoryOverhead:          1024 * 1024 * 10,  // 10MB
			UsedMemoryStartup:           1024 * 1024 * 5,   // 5MB
			UsedMemoryDataset:           1024 * 1024 * 40,  // 40MB
			UsedMemoryDatasetPercentage: 80.0,              // 80%
		},
		ReplicationBacklogSize: 1024 * 100, // 100KB
		LuaScriptsCacheSize:    1024 * 50,  // 50KB
		KeysCount:              1000,       // 1000 keys
		BytesKeysRatio:         512,        // 512 bytes per key average
	}

	mockDB.On("GetMemoryStats").Return(memStats, nil).Maybe()
}

func TestSetUp(t *testing.T) (config.MigrationManagerConfig, *shared.CollectionPartialView, info.ServerInfo, *lru.LRUCache[shared.Collection], *mockDb.GenericDB) {
	migrationManagerConfig := config.MigrationManagerConfig{
		LeaderThreshold:   0,
		FollowerThreshold: 1,
		ViewSize:          3,
	}
	db := mockDb.NewGenericDB(t)
	db.On("SetCollection", mock.AnythingOfType("string"), mock.AnythingOfType("map[string]string"), mock.AnythingOfType("[]string")).Return(nil).Maybe()

	collections := &shared.CollectionPartialView{
		KnowedCollections: make(map[string]shared.Collection),
	}
	lruCache := lru.LRUCacheConstructor[shared.Collection](1000)
	serverinfo := info.ServerInfo{
		Node:   info.Node{Address: "Name", MetadataDomain: "Domain"},
		Father: &info.Node{Address: "fName", MetadataDomain: "fDomain"},
		Children: []info.Node{
			{Address: "child1", MetadataDomain: "Domain"},
			{Address: "child2", MetadataDomain: "Domain"},
		},
		Secret: "ermes-secret",
	}

	SetupGetMemoryStatsMock(db)
	return migrationManagerConfig, collections, serverinfo, &lruCache, db
}
