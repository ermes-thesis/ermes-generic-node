//go:build test
// +build test

package tests

import (
	"config"
	clDiscoverer "dataManager/collectionsDiscoverer"
	lru "dataManager/lru"
	migrationManager "dataManager/migrationManager"
	penality "dataManager/migrationManager/penalityFunction"
	shared "dataManager/shared"
	mockDb "db/mocks"
	"io"
	logger "logger"
	"os"
	"server/info"
	"testing"

	"github.com/stretchr/testify/mock"
)

func testSetUp(t *testing.T) (config.MigrationManagerConfig, *shared.CollectionPartialView, info.ServerInfo, *lru.LRUCache[shared.Collection], *mockDb.GenericDB) {
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

	return migrationManagerConfig, collections, serverinfo, &lruCache, db
}

func TestFromRemoteToLocalCollection(t *testing.T) {
	migrationManagerConfig, collections, serverinfo, lruCache, db := testSetUp(t)
	var loggerFile io.Writer = os.Stdout //TODO replace with file
	var logger = logger.NewLogger(logger.ERROR, &loggerFile)

	mig := migrationManager.GetMigrationManagerInstance(*logger)
	mig.Init(collections, penality.StupidPenalityFunc, migrationManagerConfig, serverinfo, lruCache, db, *logger)
	clDiscoverer := clDiscoverer.GetCollectionsDiscovererInstance(*logger)
	clDiscoverer.Init(collections)
	mig.ManageNewRequest("repoId", "childName", true, 3)

	b, c := clDiscoverer.IsCollectionLocallyPresent("repoId")

	if c != nil {
		t.Fatal("unexpected non null collection")
	}

	mig.NewCollectionReceived("repoId", 10, true, make(map[string]string), []string{})
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
