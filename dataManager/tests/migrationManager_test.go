//go:build test
// +build test

package tests

import (
	"config"
	lru "dataManager/lru"
	migrationManager "dataManager/migrationManager"
	usage "dataManager/migrationManager/childUsage"
	gossip "dataManager/migrationManager/gossipMessages"
	policy "dataManager/migrationManager/strongPolicy"
	shared "dataManager/shared"
	mockDb "db/mocks"
	"server/info"
	"testing"

	"github.com/stretchr/testify/mock"
)

func myT1PenalityFunc(bufferPos int, currentValue int) int {
	return currentValue - bufferPos
}

func migTestSetUp(t *testing.T) (config.MigrationManagerConfig, *shared.CollectionPartialView, info.ServerInfo, *lru.LRUCache[shared.Collection], *mockDb.GenericDB) {
	migrationManagerConfig := config.MigrationManagerConfig{
		LeaderThreshold:   0,
		FollowerThreshold: 1,
		ViewSize:          3,
		IdleTimeoutMs:     100000000000,
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
	migrationManager.ResetSingleton()
	return migrationManagerConfig, collections, serverinfo, &lruCache, db
}

func TestSingletonInstance(t *testing.T) {
	migrationManagerConfig, collections, serverinfo, lruCache, db := migTestSetUp(t)

	instance1 := migrationManager.GetMigrationManagerInstance()
	instance1.Init(collections, myT1PenalityFunc, migrationManagerConfig, serverinfo, lruCache, db)
	instance2 := migrationManager.GetMigrationManagerInstance()
	instance2.Init(collections, myT1PenalityFunc, migrationManagerConfig, serverinfo, lruCache, db)

	if instance1 == nil {
		t.Fatal("Expected non-nil instance from GetMigrationManagerIstance")
	}

	if instance1 != instance2 {
		t.Error("Expected the same instance for both calls, but got different ones")
	}

	t.Cleanup(func() {
		migrationManager.ResetSingleton()
	})
}

func TestManageNewStrongRequest(t *testing.T) {
	migrationManagerConfig, collections, serverinfo, lruCache, db := migTestSetUp(t)

	instance1 := migrationManager.GetMigrationManagerInstance()
	instance1.Init(collections, myT1PenalityFunc, migrationManagerConfig, serverinfo, lruCache, db)
	instance1.NewCollectionReceived("repoId", 10, true, make(map[string]string), []string{})
	instance1.NewCollectionReceived("repoId1", 10, true, make(map[string]string), []string{})

	instance1.ManageNewRequest("repoId", "childName", true, 10)

	if instance1.(*migrationManager.MigrationManagerImp).StrongCollectionPolicy.(*policy.DefaultStrongCollectionPolicy).GetUnstableEquilibriumState()["repoId"]["childName"] == nil {
		t.Fatal("Expected non-nil entry - 1")
	}

	instance1.ManageNewRequest("repoId", "childName", true, 3)
	instance1.ManageNewRequest("repoId", "childName", false, 3)
	instance1.ManageNewRequest("repoId", "childName1", false, 7)
	instance1.ManageNewRequest("repoId1", "childName1", true, 7)

	if instance1.(*migrationManager.MigrationManagerImp).StrongCollectionPolicy.(*policy.DefaultStrongCollectionPolicy).GetUnstableEquilibriumState()["repoId"]["childName1"] == nil {
		t.Fatal("Expected non-nil entry - 2")
	}
	if instance1.(*migrationManager.MigrationManagerImp).StrongCollectionPolicy.(*policy.DefaultStrongCollectionPolicy).GetUnstableEquilibriumState()["repoId1"]["childName1"] == nil {
		t.Fatal("Expected non-nil entry - 3")
	}
	if instance1.(*migrationManager.MigrationManagerImp).StrongCollectionPolicy.(*policy.DefaultStrongCollectionPolicy).GetUnstableEquilibriumState()["repoId1"]["childName"] != nil {
		t.Fatal("Expected nil entry")
	}

	var equilibrium = instance1.(*migrationManager.MigrationManagerImp).StrongCollectionPolicy.(*policy.DefaultStrongCollectionPolicy).GetUnstableEquilibriumState()
	if equilibrium["repoId"]["childName"].Data[0].GetWrite().CumulativeLatency != 13 ||
		equilibrium["repoId"]["childName"].Data[0].GetRead().CumulativeLatency != 3 ||
		equilibrium["repoId"]["childName1"].Data[0].GetWrite().CumulativeLatency != 0 ||
		equilibrium["repoId"]["childName1"].Data[0].GetRead().CumulativeLatency != 7 ||
		equilibrium["repoId1"]["childName1"].Data[0].GetWrite().CumulativeLatency != 7 ||
		equilibrium["repoId1"]["childName1"].Data[0].GetRead().CumulativeLatency != 0 {
		t.Error("wrong latency values")
	}

	if collections.KnowedCollections["repoId"] == nil || collections.KnowedCollections["repoId1"] == nil {
		t.Fatal("collection not initialized")
	}

	t.Cleanup(func() {
		migrationManager.ResetSingleton()
	})
}

func TestManageNewStrongRequestComputeForces(t *testing.T) {
	migrationManagerConfig, collections, serverinfo, lruCache, db := migTestSetUp(t)

	instance1 := migrationManager.GetMigrationManagerInstance()
	instance1.Init(collections, myT1PenalityFunc, migrationManagerConfig, serverinfo, lruCache, db)
	instance1.NewCollectionReceived("repoId", 10, true, make(map[string]string), []string{})
	instance1.NewCollectionReceived("repoId1", 10, true, make(map[string]string), []string{})

	instance1.ManageNewRequest("repoId", "childName", true, 10)
	instance1.ManageNewRequest("repoId", "childName", true, 3)
	instance1.ManageNewRequest("repoId", "childName", false, 3)

	var equilibrium = instance1.(*migrationManager.MigrationManagerImp).StrongCollectionPolicy.(*policy.DefaultStrongCollectionPolicy).GetUnstableEquilibriumState()

	if usage.GetElasticForce(equilibrium["repoId"]["childName"].Data[0].GetWrite()) != 26 {
		t.Error("wrong write force")
	}
	if usage.GetElasticForce(equilibrium["repoId"]["childName"].Data[0].GetRead()) != 3 {
		t.Error("wrong read force")
	}

	t.Cleanup(func() {
		migrationManager.ResetSingleton()
	})
}

func TestComputeElasticForcesOnCircularBuffer(t *testing.T) {
	migrationManagerConfig, collections, serverinfo, lruCache, db := migTestSetUp(t)

	instance1 := migrationManager.GetMigrationManagerInstance()
	instance1.Init(collections, myT1PenalityFunc, migrationManagerConfig, serverinfo, lruCache, db)
	instance1.NewCollectionReceived("repoId", 10, true, make(map[string]string), []string{})
	instance1.NewCollectionReceived("repoId1", 10, true, make(map[string]string), []string{})

	instance1.ManageNewRequest("repoId", "childName", true, 10)
	instance1.ManageNewRequest("repoId", "childName", true, 3)
	instance1.ManageNewRequest("repoId", "childName", false, 7)

	instance1.(*migrationManager.MigrationManagerImp).StrongCollectionPolicy.(*policy.DefaultStrongCollectionPolicy).GetBuffer("repoId", "childName").Movehead() //skip to next window

	instance1.ManageNewRequest("repoId", "childName", true, 14)
	instance1.ManageNewRequest("repoId", "childName", false, 9)
	//fmt.Println(migrationManager.GetBuffer("repoId", "childName"))

	instance1.(*migrationManager.MigrationManagerImp).StrongCollectionPolicy.(*policy.DefaultStrongCollectionPolicy).SetState(instance1.(*migrationManager.MigrationManagerImp).StrongCollectionPolicy.(*policy.DefaultStrongCollectionPolicy).GetDecisionState())
	if decisionImpl, ok := instance1.(*migrationManager.MigrationManagerImp).StrongCollectionPolicy.(*policy.DefaultStrongCollectionPolicy).GetDecisionState().(*policy.MigrationManagerDecisionState); ok {
		feR := decisionImpl.ComputeReadElasticForce(instance1.(*migrationManager.MigrationManagerImp).StrongCollectionPolicy.(*policy.DefaultStrongCollectionPolicy).GetBuffer("repoId", "childName"))
		feW := decisionImpl.ComputeWriteElasticForce(instance1.(*migrationManager.MigrationManagerImp).StrongCollectionPolicy.(*policy.DefaultStrongCollectionPolicy).GetBuffer("repoId", "childName"))
		if feW != 39 {
			t.Error("wrong write elastic force")
		}
		if feR != 15 {
			t.Error("wrong read elastic force")
		}
	} else {
		t.Fatal("wrong state type")
	}

	t.Cleanup(func() {
		migrationManager.ResetSingleton()
	})
}

func TestMigrationFromIdleToGossipAfterReceiveAMessage(t *testing.T) {
	migrationManagerConfig, collections, serverinfo, lruCache, db := migTestSetUp(t)

	instance1 := migrationManager.GetMigrationManagerInstance()
	instance1.Init(collections, myT1PenalityFunc, migrationManagerConfig, serverinfo, lruCache, db)

	msg := gossip.NewChildStateMessage("senderId", 0.0, 0.0)

	_, isIdle := instance1.(*migrationManager.MigrationManagerImp).StrongCollectionPolicy.(*policy.DefaultStrongCollectionPolicy).GetCurrentState().(*policy.MigrationManagerIdleState)
	print(isIdle)

	instance1.ManageGossipMessage(msg)

	if _, isIdle := instance1.(*migrationManager.MigrationManagerImp).StrongCollectionPolicy.(*policy.DefaultStrongCollectionPolicy).GetCurrentState().(*policy.MigrationManagerIdleState); isIdle {
		t.Error("unexpected idle state")
	}

	if _, isDecision := instance1.(*migrationManager.MigrationManagerImp).StrongCollectionPolicy.(*policy.DefaultStrongCollectionPolicy).GetCurrentState().(*policy.MigrationManagerDecisionState); isDecision {
		t.Error("unexpected decision state")
	}

	if _, isGossip := instance1.(*migrationManager.MigrationManagerImp).StrongCollectionPolicy.(*policy.DefaultStrongCollectionPolicy).GetCurrentState().(*policy.MigrationManagerGossipState); !isGossip {
		t.Error("must be in gossip state")
	}

	t.Cleanup(func() {
		migrationManager.ResetSingleton()
	})
}
