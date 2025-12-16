//go:build test
// +build test

package tests

import (
	gossip "dataManager/messages"
	"dataManager/migrationManager"
	penality "dataManager/migrationManager/penalityFunction"
	policy "dataManager/migrationManager/strongPolicy"
	database "db"
	"testing"
)

func myT1PenalityFunc(bufferPos int, currentValue int) int {
	return currentValue - bufferPos
}

func TestSingletonInstance(t *testing.T) {
	migrationManagerConfig, lruConfig, collections, serverinfo, lruCache, db, ser := TestSetUp(t)
	log := createNewLogger(t)

	instance1 := migrationManager.GetMigrationManagerInstance()
	if instance1 == nil {
		t.Fatal("Expected non-nil instance from GetMigrationManagerInstance")
	}

	instance1.Init(collections, penality.StupidPenalityFunc, migrationManagerConfig, lruConfig, ser, serverinfo, lruCache, db, log)
	instance2 := migrationManager.GetMigrationManagerInstance()
	instance2.Init(collections, penality.StupidPenalityFunc, migrationManagerConfig, lruConfig, ser, serverinfo, lruCache, db, log)

	if instance1 != instance2 {
		t.Error("Expected the same instance for both calls, but got different ones")
	}

	t.Cleanup(func() {
		migrationManager.ResetSingleton()
	})
}

func TestManageNewStrongRequest(t *testing.T) {
	migrationManagerConfig, lruConfig, collections, serverinfo, lruCache, db, ser := TestSetUp(t)
	log := createNewLogger(t)

	db.EXPECT().GetCollectionMemoryUsage("repoId").Return(10, nil)
	db.EXPECT().GetCollectionMemoryUsage("repoId1").Return(10, nil)

	instance1 := migrationManager.GetMigrationManagerInstance()
	instance1.Init(collections, penality.StupidPenalityFunc, migrationManagerConfig, lruConfig, ser, serverinfo, lruCache, db, log)
	instance1.NewCollectionReceived("repoId", true, 0, false, make(map[string]database.RecordValue, 13), []string{}, "", 0, 13, false, "host")
	instance1.NewCollectionReceived("repoId1", true, 0, false, make(map[string]database.RecordValue, 14), []string{}, "", 0, 14, false, "host")

	instance1.UpdatesRequestsInfo([]gossip.CollectionRequestInfo{
		{
			CollectionId:   "repoId",
			IsWrite:        true,
			RequestLatency: 10,
		},
	}, "childName")
	if instance1.(*migrationManager.MigrationManagerImp).StrongCollectionPolicy.(*policy.ErmesStrongCollectionPolicy).GetBuffer("repoId", "childName") == nil {
		t.Fatal("Expected non-nil entry - 1")
	}

	instance1.UpdatesRequestsInfo([]gossip.CollectionRequestInfo{
		{
			CollectionId:   "repoId",
			IsWrite:        true,
			RequestLatency: 3,
		},
		{
			CollectionId:   "repoId",
			IsWrite:        false,
			RequestLatency: 3,
		},
	}, "childName")
	instance1.UpdatesRequestsInfo([]gossip.CollectionRequestInfo{
		{
			CollectionId:   "repoId",
			IsWrite:        false,
			RequestLatency: 7,
		},
		{
			CollectionId:   "repoId1",
			IsWrite:        true,
			RequestLatency: 7,
		},
	}, "childName1")

	if instance1.(*migrationManager.MigrationManagerImp).StrongCollectionPolicy.(*policy.ErmesStrongCollectionPolicy).GetBuffer("repoId", "childName1") == nil {
		t.Fatal("Expected non-nil entry - 2")
	}
	if instance1.(*migrationManager.MigrationManagerImp).StrongCollectionPolicy.(*policy.ErmesStrongCollectionPolicy).GetBuffer("repoId1", "childName1") == nil {
		t.Fatal("Expected non-nil entry - 3")
	}
	if instance1.(*migrationManager.MigrationManagerImp).StrongCollectionPolicy.(*policy.ErmesStrongCollectionPolicy).GetBuffer("repoId1", "childName") != nil {
		t.Fatal("Expected nil entry")
	}

	mig := instance1.(*migrationManager.MigrationManagerImp).StrongCollectionPolicy.(*policy.ErmesStrongCollectionPolicy)
	if mig.GetBuffer("repoId", "childName").Head().Write.CumulativeLatency != 13 ||
		mig.GetBuffer("repoId", "childName").Head().Read.CumulativeLatency != 3 ||
		mig.GetBuffer("repoId", "childName1").Head().Write.CumulativeLatency != 0 ||
		mig.GetBuffer("repoId", "childName1").Head().Read.CumulativeLatency != 7 ||
		mig.GetBuffer("repoId1", "childName1").Head().Write.CumulativeLatency != 7 ||
		mig.GetBuffer("repoId1", "childName1").Head().Read.CumulativeLatency != 0 {
		t.Error("wrong latency values")
	}

	if collections.KnownCollections["repoId"] == nil || collections.KnownCollections["repoId1"] == nil {
		t.Fatal("collection not initialized")
	}

	t.Cleanup(func() {
		migrationManager.ResetSingleton()
	})
}

func TestManageNewStrongRequestComputeForces(t *testing.T) {
	migrationManagerConfig, lruConfig, collections, serverinfo, lruCache, db, ser := TestSetUp(t)
	log := createNewLogger(t)
	db.EXPECT().GetCollectionMemoryUsage("repoId").Return(10, nil)
	db.EXPECT().GetCollectionMemoryUsage("repoId1").Return(10, nil)

	instance1 := migrationManager.GetMigrationManagerInstance()
	instance1.Init(collections, penality.StupidPenalityFunc, migrationManagerConfig, lruConfig, ser, serverinfo, lruCache, db, log)
	instance1.NewCollectionReceived("repoId", true, 0, false, make(map[string]database.RecordValue), []string{}, "", 0, 12, false, "host")
	instance1.NewCollectionReceived("repoId1", true, 0, false, make(map[string]database.RecordValue), []string{}, "", 0, 13, false, "host")

	instance1.UpdatesRequestsInfo([]gossip.CollectionRequestInfo{
		{
			CollectionId:   "repoId",
			IsWrite:        true,
			RequestLatency: 10,
		},
		{
			CollectionId:   "repoId",
			IsWrite:        true,
			RequestLatency: 3,
		},
		{
			CollectionId:   "repoId",
			IsWrite:        false,
			RequestLatency: 3,
		},
	}, "childName")
	mig := instance1.(*migrationManager.MigrationManagerImp).StrongCollectionPolicy.(*policy.ErmesStrongCollectionPolicy)
	val := mig.GetBuffer("repoId", "childName").Head().Write.GetElasticForce()
	if val != 13 {

		t.Error("wrong write force")
	}
	if mig.GetBuffer("repoId", "childName").Head().Read.GetElasticForce() != 3 {
		t.Error("wrong read force")
	}

	t.Cleanup(func() {
		migrationManager.ResetSingleton()
	})
}

func TestComputeElasticForcesOnCircularBuffer(t *testing.T) {
	migrationManagerConfig, lruConfig, collections, serverinfo, lruCache, db, ser := TestSetUp(t)
	log := createNewLogger(t)

	db.EXPECT().GetCollectionMemoryUsage("repoId").Return(10, nil)
	db.EXPECT().GetCollectionMemoryUsage("repoId1").Return(10, nil)

	instance1 := migrationManager.GetMigrationManagerInstance()
	instance1.Init(collections, penality.StupidPenalityFunc, migrationManagerConfig, lruConfig, ser, serverinfo, lruCache, db, log)
	instance1.NewCollectionReceived("repoId", true, 0, false, make(map[string]database.RecordValue), []string{}, "", 0, 13, false, "host")
	instance1.NewCollectionReceived("repoId1", true, 0, false, make(map[string]database.RecordValue), []string{}, "", 0, 14, false, "host")

	instance1.UpdatesRequestsInfo([]gossip.CollectionRequestInfo{
		{
			CollectionId:   "repoId",
			IsWrite:        true,
			RequestLatency: 10,
		},
		{
			CollectionId:   "repoId",
			IsWrite:        true,
			RequestLatency: 3,
		},
		{
			CollectionId:   "repoId",
			IsWrite:        false,
			RequestLatency: 7,
		},
	}, "childName")

	instance1.(*migrationManager.MigrationManagerImp).StrongCollectionPolicy.(*policy.ErmesStrongCollectionPolicy).GetBuffer("repoId", "childName").MoveHead() //skip to next window

	instance1.UpdatesRequestsInfo([]gossip.CollectionRequestInfo{
		{
			CollectionId:   "repoId",
			IsWrite:        true,
			RequestLatency: 14,
		},
		{
			CollectionId:   "repoId",
			IsWrite:        false,
			RequestLatency: 9,
		},
	}, "childName")
	//fmt.Println(migrationManager.GetBuffer("repoId", "childName"))

	instance1.(*migrationManager.MigrationManagerImp).StrongCollectionPolicy.(*policy.ErmesStrongCollectionPolicy).SetState(instance1.(*migrationManager.MigrationManagerImp).StrongCollectionPolicy.(*policy.ErmesStrongCollectionPolicy).GetDecisionState())
	if decisionImpl, ok := instance1.(*migrationManager.MigrationManagerImp).StrongCollectionPolicy.(*policy.ErmesStrongCollectionPolicy).GetDecisionState().(*policy.MigrationManagerDecisionState); ok {
		feR := decisionImpl.ComputeReadElasticForce(instance1.(*migrationManager.MigrationManagerImp).StrongCollectionPolicy.(*policy.ErmesStrongCollectionPolicy).GetBuffer("repoId", "childName"))
		feW := decisionImpl.ComputeWriteElasticForce(instance1.(*migrationManager.MigrationManagerImp).StrongCollectionPolicy.(*policy.ErmesStrongCollectionPolicy).GetBuffer("repoId", "childName"))
		if feW != 26 {
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
	migrationManagerConfig, lruConfig, collections, serverinfo, lruCache, db, ser := TestSetUp(t)
	log := createNewLogger(t)

	instance1 := migrationManager.GetMigrationManagerInstance()
	instance1.Init(collections, penality.StupidPenalityFunc, migrationManagerConfig, lruConfig, ser, serverinfo, lruCache, db, log)

	msg := gossip.NewChildStateMessage("senderId", 0.0)

	_, isIdle := instance1.(*migrationManager.MigrationManagerImp).StrongCollectionPolicy.(*policy.ErmesStrongCollectionPolicy).GetCurrentState().(*policy.MigrationManagerIdleState)
	print(isIdle)

	instance1.ManageGossipMessage(msg)

	if _, isIdle := instance1.(*migrationManager.MigrationManagerImp).StrongCollectionPolicy.(*policy.ErmesStrongCollectionPolicy).GetCurrentState().(*policy.MigrationManagerIdleState); isIdle {
		t.Error("unexpected idle state")
	}

	if _, isDecision := instance1.(*migrationManager.MigrationManagerImp).StrongCollectionPolicy.(*policy.ErmesStrongCollectionPolicy).GetCurrentState().(*policy.MigrationManagerDecisionState); isDecision {
		t.Error("unexpected decision state")
	}

	if _, isGossip := instance1.(*migrationManager.MigrationManagerImp).StrongCollectionPolicy.(*policy.ErmesStrongCollectionPolicy).GetCurrentState().(*policy.MigrationManagerGossipState); !isGossip {
		t.Error("must be in gossip state")
	}

	t.Cleanup(func() {
		migrationManager.ResetSingleton()
	})
}
