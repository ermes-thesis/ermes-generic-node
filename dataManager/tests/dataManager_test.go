//go:build test
// +build test

package tests

import (
	"config"
	clDiscoverer "dataManager/collectionsDiscoverer"
	lru "dataManager/lru"
	migrationManager "dataManager/migrationManager"
	shared "dataManager/shared"
	"server/info"
	"testing"
)

func myTPenalityFunc(bufferPos int, currentValue int) int {
	return currentValue
}

func testSetUp() (config.MigrationManagerConfig, *shared.CollectionPartialView, info.ServerInfo, *lru.LRUCache[shared.Collection]) {
	migrationManagerConfig := config.MigrationManagerConfig{
		LeaderThreshold:   0,
		FollowerThreshold: 1,
		ViewSize:          3,
	}
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

	return migrationManagerConfig, collections, serverinfo, &lruCache
}

func TestFromRemoteToLocalCollection(t *testing.T) {
	migrationManagerConfig, collections, serverinfo, lruCache := testSetUp()

	mig := migrationManager.GetMigrationManagerInstance()
	mig.Init(collections, myTPenalityFunc, migrationManagerConfig, serverinfo, lruCache)
	clDiscoverer := clDiscoverer.GetCollectionsDiscovererInstance()
	clDiscoverer.Init(collections)
	mig.ManageNewRequest("repoId", "childName", true, 3)

	b, c := clDiscoverer.IsCollectionLocallyPresent("repoId")

	if c != nil {
		t.Fatal("unexpected non null collection")
	}

	mig.NewCollectionReceived("repoId", 10, true)
	b, c = clDiscoverer.IsCollectionLocallyPresent("repoId")
	if c == nil {
		t.Fatal("unexpected null collection")
	}
	if !b {
		t.Error("unexpected remote collection")
	}

	t.Cleanup(func() {
		migrationManager.ResetSingleton()
		//clDiscoverer.ResetSingleton()
	})
}
