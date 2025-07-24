//go:build test
// +build test

package tests

import (
	"config"
	clDiscoverer "dataManager/collectionsDiscoverer"
	migrationManager "dataManager/migrationManager"
	shared "dataManager/shared"
	"testing"
)

func myTPenalityFunc(bufferPos int, currentValue int) int {
	return currentValue
}

func TestFromRemoteToLocalCollection(t *testing.T) {
	migrationManagerConfig := config.MigrationManagerConfig{
		LeaderThreshold:   0,
		FollowerThreshold: 1,
		ViewSize:          3,
	}
	collections := &shared.CollectionPartialView{
		KnowedCollections: make(map[string]shared.Collection),
	}

	mig := migrationManager.GetMigrationManagerInstance()
	mig.Init(collections, myTPenalityFunc, migrationManagerConfig)
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
