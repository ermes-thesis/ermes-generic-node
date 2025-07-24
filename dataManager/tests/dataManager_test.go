//go:build test
// +build test

package tests

import (
	"config"
	clDiscoverer "dataManager/collectionsDiscoverer"
	migrationManager "dataManager/migrationManager"
	shared "dataManager/shared"
	"server/info"
	"testing"
)

func myTPenalityFunc(bufferPos int, currentValue int) int {
	return currentValue
}

func testSetUp() (config.MigrationManagerConfig, *shared.CollectionPartialView, info.Serverinfo) {
	migrationManagerConfig := config.MigrationManagerConfig{
		LeaderThreshold:   0,
		FollowerThreshold: 1,
		ViewSize:          3,
	}
	collections := &shared.CollectionPartialView{
		KnowedCollections: make(map[string]shared.Collection),
	}
	serverinfo := info.Serverinfo{
		Name:     "Name",
		Father:   "fName",
		Children: []string{},
		Secret:   "ermes-secret",
	}

	return migrationManagerConfig, collections, serverinfo
}

func TestFromRemoteToLocalCollection(t *testing.T) {
	migrationManagerConfig, collections, serverinfo := testSetUp()

	mig := migrationManager.GetMigrationManagerInstance()
	mig.Init(collections, myTPenalityFunc, migrationManagerConfig, serverinfo)
	clDiscoverer := clDiscoverer.GetCollectionsDiscovererInstance()
	clDiscoverer.Init(collections)
	mig.ManageNewStrongRequest("repoId", "childName", true, 3)

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
