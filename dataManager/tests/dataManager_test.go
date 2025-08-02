//go:build test
// +build test

package tests

import (
	clDiscoverer "dataManager/collectionsDiscoverer"
	migrationManager "dataManager/migrationManager"
	gossip "dataManager/migrationManager/gossipMessages"
	penality "dataManager/migrationManager/penalityFunction"
	"fmt"
	"io"
	logger "logger"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
)

func TestFromRemoteToLocalCollection(t *testing.T) {
	migrationManagerConfig, lruConfig, collections, serverinfo, lruCache, db, ser := TestSetUp(t)
	var loggerFile io.Writer = os.Stdout //TODO replace with file
	var logger = logger.NewLogger(logger.ERROR, &loggerFile)

	mig := migrationManager.GetMigrationManagerInstance(*logger)
	mig.Init(collections, penality.StupidPenalityFunc, migrationManagerConfig, lruConfig, ser, serverinfo, lruCache, db, *logger)
	clDiscovererImp := clDiscoverer.GetCollectionsDiscovererInstance(*logger)
	clDiscovererImp.Init(collections)
	mig.ManageNewRequest("repoId", "childName", true, 3)

	b, c := clDiscovererImp.IsCollectionLocallyPresent("repoId")

	if c != nil {
		t.Fatal("unexpected non null collection")
	}

	mig.NewCollectionReceived("repoId", true, false, make(map[string]string), []string{})
	b, c = clDiscovererImp.IsCollectionLocallyPresent("repoId")
	if c == nil {
		t.Fatal("unexpected null collection")
	}
	if !b {
		t.Error("unexpected remote collection")
	}

	t.Cleanup(func() {
		migrationManager.ResetSingleton()
		clDiscoverer.ResetSingleton()
	})
}

func TestIterationModule(t *testing.T) {
	migrationManagerConfig, lruConfig, collections, serverinfo, lruCache, db, ser := CycleTestSetUp(t)
	var loggerFile io.Writer = os.Stdout //TODO replace with file
	var logger = logger.NewLogger(logger.WARNING, &loggerFile)

	mig := migrationManager.GetMigrationManagerInstance(*logger)
	mig.Init(collections, penality.StupidPenalityFunc, migrationManagerConfig, lruConfig, ser, serverinfo, lruCache, db, *logger)
	clDiscovererImp := clDiscoverer.GetCollectionsDiscovererInstance(*logger)
	clDiscovererImp.Init(collections)

	//SET INITIAL STATE
	fmt.Println("SET INITIAL STATE")
	mig.NewCollectionCreated("repoId0", 3, true)
	mig.NewCollectionCreated("repoId1", 2, false)
	mig.NewCollectionCreated("repoId2", 2, false)

	// verify initial state
	lru := mig.(*migrationManager.MigrationManagerImp).GetLru()
	if !lru.CheckOrder([]string{"repoId2", "repoId1", "repoId0"}) {
		t.Fatal("fail in initial state")
	}

	//FIRST CYCLE
	fmt.Println("START FIRST CYCLE")
	ser.On("SendGenericHTTPRequest", "2222", "/repo", mock.Anything).Return(200, nil).Once()
	ser.On("SendGenericHTTPRequest", "1111", "/repo", mock.Anything).Return(200, nil).Once()
	ser.On("SendGenericHTTPRequest", mock.AnythingOfType("string"), "/gossip", mock.Anything).Return(200, nil).Times(3)
	//idle
	mig.ManageNewRequest("repoId2", "user", true, 10)

	mig.ManageNewRequest("repoId0", "2222", true, 20)
	mig.ManageNewRequest("repoId0", "1111", true, 20)

	mig.ManageNewRequest("repoId0", "user", false, 1)
	mig.ManageNewRequest("repoId0", "1111", false, 6)
	mig.ManageNewRequest("repoId0", "2222", false, 1)

	mig.ManageNewRequest("repoId1", "2222", true, 15)

	leaderGossip1 := gossip.NewLeaderDelegaMessage("1111", 0.90)
	mig.ManageGossipMessage(leaderGossip1)
	// gossipState
	mig.ManageNewRequest("repoId0", "3333", true, 20)
	child1Gossip1 := gossip.NewChildStateMessage("3333", 0.90)
	mig.ManageGossipMessage(child1Gossip1)
	child2Gossip1 := gossip.NewChildStateMessage("2222", 0.90)
	mig.ManageGossipMessage(child2Gossip1)
	// decisionState
	mig.NewCollectionReceived("repoId3", true, false, make(map[string]string), []string{})

	// verify first cycle
	time.Sleep(1 * time.Second)
	lru = mig.(*migrationManager.MigrationManagerImp).GetLru()
	if !lru.CheckOrder([]string{"repoId3", "repoId0", "repoId1", "repoId2"}) {
		t.Fatal("fail in first cycle")
	}

	t.Cleanup(func() {
		migrationManager.ResetSingleton()
		clDiscoverer.ResetSingleton()
	})
}
