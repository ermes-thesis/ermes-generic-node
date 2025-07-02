//go:build test
// +build test

package migrationManager_test

import (
	migrationManager "dataManager/migrationManager"
	shared "dataManager/shared"
	"testing"
)

func TestSingletonInstance(t *testing.T) {
	collections := &shared.CollectionPartialView{
		KnowedCollections: make(map[string]*shared.Collection),
	}

	instance1 := migrationManager.GetMigrationManagerInstance()
	instance1.Init(collections)
	instance2 := migrationManager.GetMigrationManagerInstance()
	instance2.Init(collections)

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
	collections := &shared.CollectionPartialView{
		KnowedCollections: make(map[string]*shared.Collection),
	}

	instance1 := migrationManager.GetMigrationManagerInstance()
	instance1.Init(collections)
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

	if equilibrium["repoId"]["childName"].GetWrite().CumulativeLatency != 13 ||
		equilibrium["repoId"]["childName"].GetRead().CumulativeLatency != 3 ||
		equilibrium["repoId"]["childName1"].GetWrite().CumulativeLatency != 0 ||
		equilibrium["repoId"]["childName1"].GetRead().CumulativeLatency != 7 ||
		equilibrium["repoId1"]["childName1"].GetWrite().CumulativeLatency != 7 ||
		equilibrium["repoId1"]["childName1"].GetRead().CumulativeLatency != 0 {
		t.Error("wrong latency values")
	}

	t.Cleanup(func() {
		migrationManager.ResetSingleton()
	})
}

func TestManageNewStrongRequestComputeForces(t *testing.T) {
	collections := &shared.CollectionPartialView{
		KnowedCollections: make(map[string]*shared.Collection),
	}

	instance1 := migrationManager.GetMigrationManagerInstance()
	instance1.Init(collections)
	instance1.ManageNewStrongRequest("repoId", "childName", true, 10)
	instance1.ManageNewStrongRequest("repoId", "childName", true, 3)
	instance1.ManageNewStrongRequest("repoId", "childName", false, 3)

	var equilibrium = instance1.GetUnstableEquilibriumState()

	if migrationManager.GetElasticForce(equilibrium["repoId"]["childName"].GetWrite()) != 26 {
		t.Error("wrong write force")
	}
	if migrationManager.GetElasticForce(equilibrium["repoId"]["childName"].GetRead()) != 3 {
		t.Error("wrong read force")
	}

	t.Cleanup(func() {
		migrationManager.ResetSingleton()
	})
}
