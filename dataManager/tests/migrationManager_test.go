//go:build test
// +build test

package tests

import (
	"config"
	migrationManager "dataManager/migrationManager"
	shared "dataManager/shared"
	"testing"
)

func myT1PenalityFunc(bufferPos int, currentValue int) int {
	return currentValue - bufferPos
}

func TestSingletonInstance(t *testing.T) {
	migrationManagerConfig := config.MigrationManagerConfig{
		LeaderThreshold:   0,
		FollowerThreshold: 1,
		ViewSize:          3,
	}

	collections := &shared.CollectionPartialView{
		KnowedCollections: make(map[string]shared.Collection),
	}

	instance1 := migrationManager.GetMigrationManagerInstance()
	instance1.Init(collections, myT1PenalityFunc, migrationManagerConfig)
	instance2 := migrationManager.GetMigrationManagerInstance()
	instance2.Init(collections, myT1PenalityFunc, migrationManagerConfig)

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
	migrationManagerConfig := config.MigrationManagerConfig{
		LeaderThreshold:   0,
		FollowerThreshold: 1,
		ViewSize:          3,
	}
	collections := &shared.CollectionPartialView{
		KnowedCollections: make(map[string]shared.Collection),
	}

	instance1 := migrationManager.GetMigrationManagerInstance()
	instance1.Init(collections, myT1PenalityFunc, migrationManagerConfig)
	instance1.NewCollectionReceived("repoId", 10, true)
	instance1.NewCollectionReceived("repoId1", 10, true)

	instance1.ManageNewStrongRequest("repoId", "childName", true, 10)

	if instance1.GetUnstableEquilibriumState()["repoId"]["childName"] == nil {
		t.Fatal("Expected non-nil entry")
	}

	instance1.ManageNewStrongRequest("repoId", "childName", true, 3)
	instance1.ManageNewStrongRequest("repoId", "childName", false, 3)
	instance1.ManageNewStrongRequest("repoId", "childName1", false, 7)
	instance1.ManageNewStrongRequest("repoId1", "childName1", true, 7)

	if instance1.GetUnstableEquilibriumState()["repoId"]["childName1"] == nil {
		t.Fatal("Expected non-nil entry")
	}
	if instance1.GetUnstableEquilibriumState()["repoId1"]["childName1"] == nil {
		t.Fatal("Expected non-nil entry")
	}
	if instance1.GetUnstableEquilibriumState()["repoId1"]["childName"] != nil {
		t.Fatal("Expected nil entry")
	}

	var equilibrium = instance1.GetUnstableEquilibriumState()
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
	migrationManagerConfig := config.MigrationManagerConfig{
		LeaderThreshold:   0,
		FollowerThreshold: 1,
		ViewSize:          3,
	}
	collections := &shared.CollectionPartialView{
		KnowedCollections: make(map[string]shared.Collection),
	}

	instance1 := migrationManager.GetMigrationManagerInstance()
	instance1.Init(collections, myT1PenalityFunc, migrationManagerConfig)
	instance1.NewCollectionReceived("repoId", 10, true)
	instance1.NewCollectionReceived("repoId1", 10, true)

	instance1.ManageNewStrongRequest("repoId", "childName", true, 10)
	instance1.ManageNewStrongRequest("repoId", "childName", true, 3)
	instance1.ManageNewStrongRequest("repoId", "childName", false, 3)

	var equilibrium = instance1.GetUnstableEquilibriumState()

	if migrationManager.GetElasticForce(equilibrium["repoId"]["childName"].Data[0].GetWrite()) != 26 {
		t.Error("wrong write force")
	}
	if migrationManager.GetElasticForce(equilibrium["repoId"]["childName"].Data[0].GetRead()) != 3 {
		t.Error("wrong read force")
	}

	t.Cleanup(func() {
		migrationManager.ResetSingleton()
	})
}

func TestComputeElasticForcesOnCircularBuffer(t *testing.T) {
	migrationManagerConfig := config.MigrationManagerConfig{
		LeaderThreshold:   0,
		FollowerThreshold: 1,
		ViewSize:          3,
	}
	collections := &shared.CollectionPartialView{
		KnowedCollections: make(map[string]shared.Collection),
	}

	instance1 := migrationManager.GetMigrationManagerInstance()
	instance1.Init(collections, myT1PenalityFunc, migrationManagerConfig)
	instance1.NewCollectionReceived("repoId", 10, true)
	instance1.NewCollectionReceived("repoId1", 10, true)

	instance1.ManageNewStrongRequest("repoId", "childName", true, 10)
	instance1.ManageNewStrongRequest("repoId", "childName", true, 3)
	instance1.ManageNewStrongRequest("repoId", "childName", false, 7)

	migrationManager.GetBuffer("repoId", "childName").Movehead() //skip to next window

	instance1.ManageNewStrongRequest("repoId", "childName", true, 14)
	instance1.ManageNewStrongRequest("repoId", "childName", false, 9)
	//fmt.Println(migrationManager.GetBuffer("repoId", "childName"))

	instance1.SetState(instance1.GetDecisionState())
	if decisionImpl, ok := instance1.GetDecisionState().(*migrationManager.MigrationManagerDecisionState); ok {
		feR := decisionImpl.ComputeReadElasticForce(migrationManager.GetBuffer("repoId", "childName"))
		feW := decisionImpl.ComputeWriteElasticForce(migrationManager.GetBuffer("repoId", "childName"))
		if feW != 39 {
			t.Error("wrong write elastic force")
		}
		if feR != 15 {
			t.Error("wrong read elastic force")
		}
	} else {
		t.Fatal("wrong state type")
	}
}
