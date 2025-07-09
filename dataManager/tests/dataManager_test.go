//go:build test
// +build test

package tests

import (
	loockupManager "dataManager/lookupManager"
	migrationManager "dataManager/migrationManager"
	shared "dataManager/shared"
	"testing"
)

func myTPenalityFunc(bufferPos int, currentValue int) int {
	return currentValue
}

func TestFromRemoteToLocalCollection(t *testing.T) {
	collections := &shared.CollectionPartialView{
		KnowedCollections: make(map[string]shared.Collection),
	}

	mig := migrationManager.GetMigrationManagerInstance()
	mig.Init(collections, myTPenalityFunc)
	loock := loockupManager.GetLookupManagerInstance()
	loock.Init(collections)
	mig.ManageNewStrongRequest("repoId", "childName", true, 3)

	b, c := loock.IsCollectionLocallyPresent("repoId")

	if c == nil {
		t.Fatal("unexpected null collection")
	}
	if b {
		t.Error("unexpected local collection")
	}

	mig.NewCollectionReceived("repoId", 10)
	b, c = loock.IsCollectionLocallyPresent("repoId")
	if c == nil {
		t.Fatal("unexpected null collection")
	}
	if !b {
		t.Error("unexpected remote collection")
	}

	t.Cleanup(func() {
		migrationManager.ResetSingleton()
		loockupManager.ResetSingleton()
	})
}
