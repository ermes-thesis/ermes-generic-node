//go:build test
// +build test

package tests

import (
	clDiscoverer "dataManager/collectionsDiscoverer"
	gossip "dataManager/messages"
	"dataManager/migrationManager"
	penality "dataManager/migrationManager/penalityFunction"
	database "db"
	"fmt"
	"testing"
	"time"

	"github.com/ermes-thesis/ermes-common/logger"
	"github.com/stretchr/testify/require"

	"github.com/stretchr/testify/mock"
)

func createNewLogger(t *testing.T) logger.Logger {
	log, err := logger.NewLogger(logger.Config{
		Level:  logger.EvalLevel,
		Format: "text",
	})
	require.NoError(t, err)
	return log
}

func TestFromRemoteToLocalCollection(t *testing.T) {
	migrationManagerConfig, lruConfig, collections, serverinfo, lruCache, db, ser, mutex := TestSetUp(t)

	log := createNewLogger(t)

	db.EXPECT().GetCollectionMemoryUsage("repoId").Return(10, nil)

	mig := migrationManager.GetMigrationManagerInstance()
	mig.Init(collections, penality.StupidPenalityFunc, migrationManagerConfig, lruConfig, ser, serverinfo, lruCache, db, log, mutex)
	clDiscovererImp := clDiscoverer.GetCollectionsDiscovererInstance()
	clDiscovererImp.Init(collections, serverinfo, ser, db, log, lruCache, mig, mutex)
	mig.UpdatesRequestsInfo([]gossip.CollectionRequestInfo{
		{
			CollectionId:   "repoId",
			IsWrite:        true,
			RequestLatency: 3,
		},
	}, "childName")
	mig.UpdateCollectionsLatestUsage([]string{"repoId"})

	b, c, d := clDiscovererImp.IsCollectionLocallyPresent("repoId", 0)

	if c != nil || d != nil {
		t.Fatal("unexpected non null collection")
	}

	mig.NewCollectionReceived("repoId", true, 0, false, make(map[string]database.RecordValue), []string{}, "", 0, 13, false, "host")
	b, c, d = clDiscovererImp.IsCollectionLocallyPresent("repoId", 0)
	if c == nil || d != nil {
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
	red := "\033[31m"
	reset := "\033[0m"

	migrationManagerConfig, lruConfig, collections, serverinfo, lruCache, db, ser, mutex := CycleTestSetUp(t)
	log := createNewLogger(t)

	db.EXPECT().GetCollectionMemoryUsage("repoId0").Return(10, nil)
	db.EXPECT().GetCollectionMemoryUsage("repoId1").Return(10, nil)
	db.EXPECT().GetCollectionMemoryUsage("repoId2").Return(10, nil)

	mig := migrationManager.GetMigrationManagerInstance()
	mig.Init(collections, penality.StupidPenalityFunc, migrationManagerConfig, lruConfig, ser, serverinfo, lruCache, db, log, mutex)
	defer mig.StopAsyncDump()
	clDiscovererImp := clDiscoverer.GetCollectionsDiscovererInstance()
	clDiscovererImp.Init(collections, serverinfo, ser, db, log, lruCache, mig, mutex)

	// SET INITIAL STATE
	fmt.Println("SET INITIAL STATE")
	mig.NewCollectionCreated("repoId0", 3, true)
	mig.NewCollectionCreated("repoId1", 2, false)
	mig.NewCollectionCreated("repoId2", 2, false)

	// verify initial state
	lru := mig.(*migrationManager.MigrationManagerImp).GetLru()
	if !lru.CheckOrder([]string{"repoId2", "repoId1", "repoId0"}) {
		t.Fatal("fail in initial state")
	}

	// FIRST CYCLE
	fmt.Println(red, "START FIRST CYCLE", reset)
	ser.On("SendGenericHTTPRequest", "2222", "/pvt-repo", mock.Anything).Return(200, nil).Once()
	ser.On("SendGenericHTTPRequest", "1111", "/pvt-repo", mock.Anything).Return(200, nil).Once()
	ser.On("SendGenericHTTPRequest", mock.AnythingOfType("string"), "/pvt-gossip", mock.Anything).Return(200, nil).Times(3)
	//idle
	mig.UpdatesRequestsInfo([]gossip.CollectionRequestInfo{
		{
			CollectionId:   "repoId2",
			IsWrite:        true,
			RequestLatency: 10,
		},
		{
			CollectionId:   "repoId0",
			IsWrite:        false,
			RequestLatency: 1,
		},
	}, "user")
	mig.UpdatesRequestsInfo([]gossip.CollectionRequestInfo{
		{
			CollectionId:   "repoId0",
			IsWrite:        true,
			RequestLatency: 20,
		},
		{
			CollectionId:   "repoId0",
			IsWrite:        false,
			RequestLatency: 6,
		},
	}, "1111")
	mig.UpdatesRequestsInfo([]gossip.CollectionRequestInfo{
		{
			CollectionId:   "repoId0",
			IsWrite:        true,
			RequestLatency: 20,
		},
		{
			CollectionId:   "repoId0",
			IsWrite:        false,
			RequestLatency: 1,
		},
		{
			CollectionId:   "repoId1",
			IsWrite:        true,
			RequestLatency: 15,
		},
	}, "2222")
	mig.UpdateCollectionsLatestUsage([]string{"repoId2", "repoId0", "repoId1"})
	leaderGossip1 := gossip.NewLeaderDelegaMessage("1111", 0.90)
	mig.ManageGossipMessage(leaderGossip1)
	// gossipState
	mig.UpdatesRequestsInfo([]gossip.CollectionRequestInfo{
		{
			CollectionId:   "repoId0",
			IsWrite:        true,
			RequestLatency: 20,
		},
	}, "3333")
	mig.UpdateCollectionsLatestUsage([]string{"repoId0"})
	child1Gossip1 := gossip.NewChildStateMessage("3333", 0.00)
	mig.ManageGossipMessage(child1Gossip1)
	child2Gossip1 := gossip.NewChildStateMessage("2222", 0.10)
	mig.ManageGossipMessage(child2Gossip1)
	// decisionState
	mig.NewCollectionReceived("repoId3", true, 0, false, make(map[string]database.RecordValue), []string{}, "", 0, 1, false, "host")

	// verify first cycle
	time.Sleep(2 * time.Second)
	lru = mig.(*migrationManager.MigrationManagerImp).GetLru()

	if !lru.CheckOrder([]string{"repoId3", "repoId0", "repoId1", "repoId2"}) {
		t.Fatal("fail in first cycle")
	}

	// SECOND CYCLE
	fmt.Println(red, "START SECOND CYCLE", reset)
	ser.On("SendGenericHTTPRequest", "2222", "/pvt-repo", mock.Anything).Return(200, nil).Once()
	ser.On("SendGenericHTTPRequest", "3333", "/pvt-repo", mock.Anything).Return(200, nil).Times(2)
	ser.On("SendGenericHTTPRequest", "1111", "/pvt-repo", mock.Anything).Return(200, nil).Times(2)
	ser.On("SendGenericHTTPRequest", mock.AnythingOfType("string"), "/pvt-gossip", mock.Anything).Return(200, nil).Times(3)
	// idle
	mig.UpdatesRequestsInfo([]gossip.CollectionRequestInfo{
		{
			CollectionId:   "repoId0",
			IsWrite:        false,
			RequestLatency: 10,
		},
		{
			CollectionId:   "repoId0",
			IsWrite:        false,
			RequestLatency: 10,
		},
		{
			CollectionId:   "repoId3",
			IsWrite:        false,
			RequestLatency: 1,
		},
	}, "2222")
	mig.UpdatesRequestsInfo([]gossip.CollectionRequestInfo{
		{
			CollectionId:   "repoId0",
			IsWrite:        false,
			RequestLatency: 5,
		},
		{
			CollectionId:   "repoId0",
			IsWrite:        false,
			RequestLatency: 15,
		},
		{
			CollectionId:   "repoId3",
			IsWrite:        false,
			RequestLatency: 5,
		},
	}, "3333")
	mig.UpdateCollectionsLatestUsage([]string{"repoId0", "repoId3"})
	child1Gossip1 = gossip.NewChildStateMessage("3333", 0.90)
	mig.ManageGossipMessage(child1Gossip1)
	// gossipState
	leaderGossip1 = gossip.NewLeaderDelegaMessage("1111", 0.90)
	mig.ManageGossipMessage(child1Gossip1)
	child2Gossip1 = gossip.NewChildStateMessage("2222", 0.70)
	mig.ManageGossipMessage(child2Gossip1)

	// decisionState
	mig.NewCollectionReceived("repoId4", true, 0, true, make(map[string]database.RecordValue), []string{}, "", 0, 4, false, "host")
	mig.NewCollectionReceived("repoId4", true, 0, true, make(map[string]database.RecordValue), []string{}, "", 0, 4, false, "host")

	// verify second cycle
	time.Sleep(2 * time.Second)
	lru = mig.(*migrationManager.MigrationManagerImp).GetLru()
	lru.PrintLRU()
	if !lru.CheckOrder([]string{"repoId4", "repoId3", "repoId0"}) {
		t.Fatal("fail in second cycle")
	}

	// THIRD CYCLE
	fmt.Println(string(red), "START THIRD CYCLE", string(reset))
	ser.On("SendGenericHTTPRequest", "2222", "/pvt-repo", mock.Anything).Return(200, nil).Once()
	ser.On("SendGenericHTTPRequest", "3333", "/pvt-repo", mock.Anything).Return(200, nil).Once()
	ser.On("SendGenericHTTPRequest", mock.AnythingOfType("string"), "/pvt-gossip", mock.Anything).Return(200, nil).Times(3)
	// idle
	mig.UpdatesRequestsInfo([]gossip.CollectionRequestInfo{
		{
			CollectionId:   "repoId3",
			IsWrite:        false,
			RequestLatency: 20,
		},
	}, "2222")
	mig.UpdatesRequestsInfo([]gossip.CollectionRequestInfo{
		{
			CollectionId:   "repoId4",
			IsWrite:        true,
			RequestLatency: 20,
		},
	}, "3333")
	mig.UpdateCollectionsLatestUsage([]string{"repoId3", "repoId4"})
	child1Gossip1 = gossip.NewChildStateMessage("3333", 0.90)
	mig.ManageGossipMessage(child1Gossip1)
	// gossipState
	leaderGossip1 = gossip.NewLeaderDelegaMessage("1111", 0.90)
	mig.ManageGossipMessage(child1Gossip1)
	child2Gossip1 = gossip.NewChildStateMessage("2222", 0.90)
	mig.ManageGossipMessage(child2Gossip1)

	// decisionState
	// ---

	// verify third cycle
	time.Sleep(2 * time.Second)
	lru = mig.(*migrationManager.MigrationManagerImp).GetLru()
	if !lru.CheckOrder([]string{"repoId0"}) {
		t.Fatal("fail in third cycle")
	}
	lru.PrintLRU()

	// FOURTH CYCLE
	fmt.Println(string(red), "START FOURTH CYCLE", string(reset))
	lru.PrintLRU()
	ser.On("SendGenericHTTPRequest", "1111", "/pvt-repo", mock.Anything).Return(200, nil).Times(2)
	ser.On("SendGenericHTTPRequest", mock.AnythingOfType("string"), "/pvt-gossip", mock.Anything).Return(200, nil).Times(3)

	// idle
	mig.NewCollectionReceived("repoId5", true, 0, true, make(map[string]database.RecordValue), []string{}, "", 0, 10, false, "host")

	// verify fourth cycle p1
	time.Sleep(2 * time.Second)
	lru = mig.(*migrationManager.MigrationManagerImp).GetLru()
	lru.PrintLRU()
	if !lru.CheckOrder([]string{"repoId5"}) {
		t.Fatal("fail in fourth cycle")
	}
	ser.On("SendGenericHTTPRequest", "1111", "/pvt-repo", mock.Anything).Return(200, nil).Once()

	child1Gossip1 = gossip.NewChildStateMessage("3333", 0.90)
	mig.ManageGossipMessage(child1Gossip1)
	// gossipState
	leaderGossip1 = gossip.NewLeaderDelegaMessage("1111", 0.90)
	mig.ManageGossipMessage(child1Gossip1)
	child2Gossip1 = gossip.NewChildStateMessage("2222", 0.90)
	mig.ManageGossipMessage(child2Gossip1)

	// verify fourth cycle p2
	time.Sleep(2 * time.Second)
	lru = mig.(*migrationManager.MigrationManagerImp).GetLru()
	lru.PrintLRU()

	t.Cleanup(func() {
		migrationManager.ResetSingleton()
		clDiscoverer.ResetSingleton()
	})
}
