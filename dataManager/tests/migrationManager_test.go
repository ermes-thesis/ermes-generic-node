//go:build test
// +build test

package tests

import (
	migrationManager "dataManager/migrationManager"
	gossip "dataManager/migrationManager/gossipMessages"
	penality "dataManager/migrationManager/penalityFunction"
	policy "dataManager/migrationManager/strongPolicy"
	"io"
	logger "logger"
	"os"
	"testing"
)

func myT1PenalityFunc(bufferPos int, currentValue int) int {
	return currentValue - bufferPos
}

func TestSingletonInstance(t *testing.T) {
	migrationManagerConfig, lruConfig, collections, serverinfo, lruCache, db, ser := TestSetUp(t)
	var loggerFile io.Writer = os.Stdout //TODO replace with file
	var logger = logger.NewLogger(logger.ERROR, &loggerFile)

	instance1 := migrationManager.GetMigrationManagerInstance(*logger)
	instance1.Init(collections, penality.StupidPenalityFunc, migrationManagerConfig, lruConfig, ser, serverinfo, lruCache, db, *logger)
	instance2 := migrationManager.GetMigrationManagerInstance(*logger)
	instance2.Init(collections, penality.StupidPenalityFunc, migrationManagerConfig, lruConfig, ser, serverinfo, lruCache, db, *logger)

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
	migrationManagerConfig, lruConfig, collections, serverinfo, lruCache, db, ser := TestSetUp(t)
	var loggerFile io.Writer = os.Stdout //TODO replace with file
	var logger = logger.NewLogger(logger.ERROR, &loggerFile)

	instance1 := migrationManager.GetMigrationManagerInstance(*logger)
	instance1.Init(collections, penality.StupidPenalityFunc, migrationManagerConfig, lruConfig, ser, serverinfo, lruCache, db, *logger)
	instance1.NewCollectionReceived("repoId", true, false, make(map[string]string), []string{})
	instance1.NewCollectionReceived("repoId1", true, false, make(map[string]string), []string{})

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
	if equilibrium["repoId"]["childName"].Head().Write.CumulativeLatency != 13 ||
		equilibrium["repoId"]["childName"].Head().Read.CumulativeLatency != 3 ||
		equilibrium["repoId"]["childName1"].Head().Write.CumulativeLatency != 0 ||
		equilibrium["repoId"]["childName1"].Head().Read.CumulativeLatency != 7 ||
		equilibrium["repoId1"]["childName1"].Head().Write.CumulativeLatency != 7 ||
		equilibrium["repoId1"]["childName1"].Head().Read.CumulativeLatency != 0 {
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
	migrationManagerConfig, lruConfig, collections, serverinfo, lruCache, db, ser := TestSetUp(t)
	var loggerFile io.Writer = os.Stdout //TODO replace with file
	var logger = logger.NewLogger(logger.ERROR, &loggerFile)

	instance1 := migrationManager.GetMigrationManagerInstance(*logger)
	instance1.Init(collections, penality.StupidPenalityFunc, migrationManagerConfig, lruConfig, ser, serverinfo, lruCache, db, *logger)
	instance1.NewCollectionReceived("repoId", true, false, make(map[string]string), []string{})
	instance1.NewCollectionReceived("repoId1", true, false, make(map[string]string), []string{})

	instance1.ManageNewRequest("repoId", "childName", true, 10)
	instance1.ManageNewRequest("repoId", "childName", true, 3)
	instance1.ManageNewRequest("repoId", "childName", false, 3)

	var equilibrium = instance1.(*migrationManager.MigrationManagerImp).StrongCollectionPolicy.(*policy.DefaultStrongCollectionPolicy).GetUnstableEquilibriumState()

	if equilibrium["repoId"]["childName"].Head().Write.GetElasticForce() != 26 {
		t.Error("wrong write force")
	}
	if equilibrium["repoId"]["childName"].Head().Read.GetElasticForce() != 3 {
		t.Error("wrong read force")
	}

	t.Cleanup(func() {
		migrationManager.ResetSingleton()
	})
}

func TestComputeElasticForcesOnCircularBuffer(t *testing.T) {
	migrationManagerConfig, lruConfig, collections, serverinfo, lruCache, db, ser := TestSetUp(t)
	var loggerFile io.Writer = os.Stdout //TODO replace with file
	var logger = logger.NewLogger(logger.ERROR, &loggerFile)

	instance1 := migrationManager.GetMigrationManagerInstance(*logger)
	instance1.Init(collections, penality.StupidPenalityFunc, migrationManagerConfig, lruConfig, ser, serverinfo, lruCache, db, *logger)
	instance1.NewCollectionReceived("repoId", true, false, make(map[string]string), []string{})
	instance1.NewCollectionReceived("repoId1", true, false, make(map[string]string), []string{})

	instance1.ManageNewRequest("repoId", "childName", true, 10)
	instance1.ManageNewRequest("repoId", "childName", true, 3)
	instance1.ManageNewRequest("repoId", "childName", false, 7)

	instance1.(*migrationManager.MigrationManagerImp).StrongCollectionPolicy.(*policy.DefaultStrongCollectionPolicy).GetBuffer("repoId", "childName").MoveHead() //skip to next window

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
	migrationManagerConfig, lruConfig, collections, serverinfo, lruCache, db, ser := TestSetUp(t)
	var loggerFile io.Writer = os.Stdout //TODO replace with file
	var logger = logger.NewLogger(logger.ERROR, &loggerFile)

	instance1 := migrationManager.GetMigrationManagerInstance(*logger)
	instance1.Init(collections, penality.StupidPenalityFunc, migrationManagerConfig, lruConfig, ser, serverinfo, lruCache, db, *logger)

	msg := gossip.NewChildStateMessage("senderId", 0.0)

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
