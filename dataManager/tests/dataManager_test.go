//go:build test
// +build test

package tests

import (
	clDiscoverer "dataManager/collectionsDiscoverer"
	migrationManager "dataManager/migrationManager"
	penality "dataManager/migrationManager/penalityFunction"
	"io"
	logger "logger"
	"os"
	"testing"
)

func TestFromRemoteToLocalCollection(t *testing.T) {
	migrationManagerConfig, collections, serverinfo, lruCache, db, ser := TestSetUp(t)
	var loggerFile io.Writer = os.Stdout //TODO replace with file
	var logger = logger.NewLogger(logger.ERROR, &loggerFile)

	mig := migrationManager.GetMigrationManagerInstance(*logger)
	mig.Init(collections, penality.StupidPenalityFunc, migrationManagerConfig, ser, serverinfo, lruCache, db, *logger)
	clDiscoverer := clDiscoverer.GetCollectionsDiscovererInstance(*logger)
	clDiscoverer.Init(collections)
	mig.ManageNewRequest("repoId", "childName", true, 3)

	b, c := clDiscoverer.IsCollectionLocallyPresent("repoId")

	if c != nil {
		t.Fatal("unexpected non null collection")
	}

	mig.NewCollectionReceived("repoId", true, false, make(map[string]string), []string{})
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
