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
	mockServer "server/mocks"
	"testing"

	"github.com/stretchr/testify/mock"
)

func SetupGetMemoryStatsMock(mockDB *mockDb.GenericDB, server *mockServer.ServerType) {
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

	mockDB.On("GetMemoryStats").Return(memStats, nil).Maybe() // This mock is for the DB
	server.On("SendGenericHTTPRequest", mock.AnythingOfType("string"), mock.AnythingOfType("string"), mock.Anything).Return(200, nil).Maybe()
}

func TestSetUp(t *testing.T) (config.MigrationManagerConfig, config.LruConfig, *shared.CollectionPartialView, info.ServerInfo, *lru.LRUCache[shared.Collection], *mockDb.GenericDB, *mockServer.ServerType) {
	migrationManagerConfig := config.MigrationManagerConfig{
		LeaderThreshold:   0,
		FollowerThreshold: 1, // This is the threshold for the follower to become a leader
		ViewSize:          3,
	}
	lurConfig := config.LruConfig{
		Size:      100000,
		SafeLevel: 0.8,
	}

	db := mockDb.NewGenericDB(t)
	db.On("GetCollectionMemoryUsage", mock.AnythingOfType("string")).Return(uint64(10), nil).Maybe()
	db.On("SetCollection", mock.AnythingOfType("string"), mock.AnythingOfType("map[string]string"), mock.AnythingOfType("[]string")).Return(nil).Maybe()

	server := mockServer.NewServerType(t)
	server.On("AddCustomRoute", mock.AnythingOfType("string"), mock.AnythingOfType("bool"), mock.AnythingOfType("http.HandlerFunc")).Return(nil).Maybe()

	collections := &shared.CollectionPartialView{
		KnowedCollections: make(map[string]shared.Collection),
	}
	lruCache := lru.LRUCacheConstructor[shared.Collection](lurConfig.Size)
	serverinfo := info.ServerInfo{
		Node:   info.Node{Address: "Name", MetadataDomain: "Domain"},
		Father: &info.Node{Address: "fName", MetadataDomain: "fDomain"},
		Children: []info.Node{
			{Address: "child1", MetadataDomain: "Domain"},
			{Address: "child2", MetadataDomain: "Domain"},
		},
		Secret: "ermes-secret",
	}

	SetupGetMemoryStatsMock(db, server)
	return migrationManagerConfig, lurConfig, collections, serverinfo, &lruCache, db, server
}
