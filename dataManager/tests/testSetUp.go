//go:build test
// +build test

package tests

import (
	"config"
	"dataManager/lru"
	shared "dataManager/shared"
	"db"
	mockDb "mocks/db"
	mockServer "mocks/server"
	"server/info"
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

}

func TestSetUp(t *testing.T) (config.MigrationManagerConfig, config.LruConfig, *shared.CollectionPartialView, info.ServerInfo, *lru.LRUCache[shared.Collection], *mockDb.GenericDB, *mockServer.ServerType) {
	migrationManagerConfig := config.MigrationManagerConfig{
		IdleTimeoutMs:     1000000000000000000,
		LeaderThreshold:   0,
		FollowerThreshold: 1,
		ViewSize:          3,
		GravityConstant:   0.981,
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
	server.On("SendGenericHTTPRequest", mock.AnythingOfType("string"), mock.AnythingOfType("string"), mock.Anything).Return(200, nil).Maybe()

	collections := &shared.CollectionPartialView{
		KnownCollections: make(map[string]shared.Collection),
	}
	lruCache := lru.LRUCacheConstructor[shared.Collection](lurConfig.Size)
	serverInfo := info.ServerInfo{
		Node:   info.Node{Address: "Name", MetadataDomain: "Domain"},
		Father: &info.Node{Address: "fName", MetadataDomain: "fDomain"},
		Children: []info.Node{
			{Address: "child1", MetadataDomain: "Domain"},
			{Address: "child2", MetadataDomain: "Domain"},
		},
		Secret: "ermes-secret",
	}

	SetupGetMemoryStatsMock(db, server)
	return migrationManagerConfig, lurConfig, collections, serverInfo, &lruCache, db, server
}

func CycleTestSetUp(t *testing.T) (config.MigrationManagerConfig, config.LruConfig, *shared.CollectionPartialView, info.ServerInfo, *lru.LRUCache[shared.Collection], *mockDb.GenericDB, *mockServer.ServerType) {
	migrationManagerConfig := config.MigrationManagerConfig{
		IdleTimeoutMs:     1000000000000000000,
		LeaderThreshold:   0,
		FollowerThreshold: 1,
		ViewSize:          3,
		GravityConstant:   0.981,
	}
	lurConfig := config.LruConfig{
		Size:      10,
		SafeLevel: 0.9,
	}

	db := mockDb.NewGenericDB(t)
	db.On("GetCollectionMemoryUsage", "repoId0").Return(uint64(3), nil).Maybe()
	db.On("GetCollectionMemoryUsage", "repoId1").Return(uint64(2), nil).Maybe()
	db.On("GetCollectionMemoryUsage", "repoId2").Return(uint64(2), nil).Maybe()
	db.On("GetCollectionMemoryUsage", "repoId3").Return(uint64(1), nil).Maybe()
	db.On("GetCollectionMemoryUsage", "repoId4").Return(uint64(5), nil).Maybe()
	db.On("GetCollectionMemoryUsage", "repoId5").Return(uint64(10), nil).Maybe()
	db.On("DeleteCollection", mock.AnythingOfType("string")).Return(nil).Maybe()
	db.On("SetCollection", mock.AnythingOfType("string"), mock.AnythingOfType("map[string]string"), mock.AnythingOfType("[]string")).Return(nil).Maybe()
	db.On("GetCollection", mock.AnythingOfType("string")).Return(map[string]string{"a": "1", "b": "2"}, []string{"a", "b"}, nil).Maybe()

	server := mockServer.NewServerType(t)
	server.On("AddCustomRoute", mock.AnythingOfType("string"), mock.AnythingOfType("bool"), mock.AnythingOfType("http.HandlerFunc")).Return(nil).Maybe()

	collections := &shared.CollectionPartialView{
		KnownCollections: make(map[string]shared.Collection),
	}
	lruCache := lru.LRUCacheConstructor[shared.Collection](lurConfig.Size)
	serverInfo := info.ServerInfo{
		Node:   info.Node{Address: "0000", MetadataDomain: "Domain"},
		Father: &info.Node{Address: "1111", MetadataDomain: "fDomain", Latency: 5},
		Children: []info.Node{
			{Address: "2222", MetadataDomain: "Domain", Latency: 3},
			{Address: "3333", MetadataDomain: "Domain", Latency: 4},
		},
		Secret: "ermes-secret",
	}

	SetupGetMemoryStatsMock(db, server)
	return migrationManagerConfig, lurConfig, collections, serverInfo, &lruCache, db, server
}
