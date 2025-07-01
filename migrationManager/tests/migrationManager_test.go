//go:build test
// +build test

package migrationManager_test

import (
	migrationManager "migrationManager"
	"testing"
)

func TestSingletonInstance(t *testing.T) {
	instance1 := migrationManager.GetMigrationManagerInstance()
	instance2 := migrationManager.GetMigrationManagerInstance()

	if instance1 == nil {
		t.Fatal("Expected non-nil instance from GetMigrationManagerIstance")
	}

	if instance1 != instance2 {
		t.Error("Expected the same instance for both calls, but got different ones")
	}
}

func TestManageNewStrongRequest(t *testing.T) {
	instance1 := migrationManager.GetMigrationManagerInstance()
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
}
