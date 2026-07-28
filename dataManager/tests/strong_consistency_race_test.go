//go:build test
// +build test

package tests

import (
	clDiscoverer "dataManager/collectionsDiscoverer"
	"dataManager/messages"
	"dataManager/migrationManager"
	penality "dataManager/migrationManager/penalityFunction"
	shared "dataManager/shared"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
)

// TestStrongCollectionConcurrentUpdatesOnReplica demonstrates that two concurrent
// updates on a strong collection replica can result in a lost update when they
// arrive out of order. The ConsistencyClock is a single monotonic counter per
// collection, so a message with a lower clock is discarded even if it touches
// completely different records.
func TestStrongCollectionConcurrentUpdatesOnReplica(t *testing.T) {
	migrationManagerConfig, lruConfig, collections, serverInfo, lruCache, db, ser, mutex := TestSetUp(t)

	log := createNewLogger(t)

	// Set up the replica: it has the strong collection locally but is NOT the leader.
	mig := migrationManager.GetMigrationManagerInstance()
	mig.Init(collections, penality.StupidPenalityFunc, migrationManagerConfig, lruConfig, ser, serverInfo, lruCache, db, log, mutex)
	cd := clDiscoverer.GetCollectionsDiscovererInstance()
	cd.Init(collections, serverInfo, ser, db, log, lruCache, mig, mutex)

	// Create a strong collection locally, NOT as leader (simulating a replica)
	coll := shared.NewStrongCollection("strongColl", 100, false, false, "leaderHost")
	coll.(*shared.StrongCollection).ConsistencyClock = 0
	coll.GetCollectionState().(*shared.LocalCollection).SetReady(true)
	collections.SetCollection("strongColl", coll)

	// Track which SetRecords calls actually happen
	var setRecordsMu sync.Mutex
	var setRecordsCalls []string

	db.On("IsAuthorized", mock.AnythingOfType("string"), mock.AnythingOfType("string"), mock.AnythingOfType("string")).Return(true, nil).Maybe()
	db.On("GetRecordsMemoryUsage", mock.AnythingOfType("string"), mock.AnythingOfType("[]string")).Return(uint64(5), nil).Maybe()
	db.On("SetRecords", mock.AnythingOfType("string"), mock.AnythingOfType("[]string"), mock.AnythingOfType("[]string"), mock.AnythingOfType("string")).
		Run(func(args mock.Arguments) {
			recordNames := args.Get(1).([]string) // recordNames which is a []string
			setRecordsMu.Lock()
			setRecordsCalls = append(setRecordsCalls, recordNames...)
			setRecordsMu.Unlock()
			// Simulate some work to increase the chance of interleaving
			time.Sleep(5 * time.Millisecond)
		}).Return(nil).Maybe()

	// Prepare two updates on DISJOINT records, with increasing sequence numbers.
	// Update A: clock=1, writes recordX
	updateA := messages.CollectionUpdateMessage{
		CollectionId: "strongColl",
		RecordNames:  []string{"recordX"},
		RecordValues: []string{"valueX"},
		Time:         time.Now().UnixMicro(),
		UpdateHostIp: "leaderHost",
		SenderIp:     "leaderHost",
		IsDelete:     false,
		ToForward:    false, // don't propagate further
		Sequence:     1,
	}

	// Update B: clock=2, writes recordY
	updateB := messages.CollectionUpdateMessage{
		CollectionId: "strongColl",
		RecordNames:  []string{"recordY"},
		RecordValues: []string{"valueY"},
		Time:         time.Now().UnixMicro(),
		UpdateHostIp: "leaderHost",
		SenderIp:     "leaderHost",
		IsDelete:     false,
		ToForward:    false,
		Sequence:     2,
	}

	// Simulate out-of-order delivery: send B first, then A concurrently.
	// This is what happens when two goroutines in the sender race.
	var wg sync.WaitGroup
	wg.Add(2)

	// Goroutine 1: sends update B (clock=2) — arrives first
	go func() {
		defer wg.Done()
		cd.UpdateCollectionsRecordsAndPropagate(
			[]messages.CollectionUpdateMessage{updateB},
			[]string{}, "", false, "leaderHost",
		)
	}()

	// Small delay to increase chance B is processed first
	time.Sleep(20 * time.Millisecond)

	// Goroutine 2: sends update A (clock=1) — arrives second
	go func() {
		defer wg.Done()
		cd.UpdateCollectionsRecordsAndPropagate(
			[]messages.CollectionUpdateMessage{updateA},
			[]string{}, "", false, "leaderHost",
		)
	}()

	wg.Wait()

	setRecordsMu.Lock()
	defer setRecordsMu.Unlock()

	hasRecordX := false
	hasRecordY := false
	for _, name := range setRecordsCalls {
		if name == "recordX" {
			hasRecordX = true
		}
		if name == "recordY" {
			hasRecordY = true
		}
	}

	if !hasRecordY {
		t.Error("recordY was not written — unexpected, update B should always succeed")
	}

	if !hasRecordX {
		t.Error("BUG CONFIRMED: recordX was lost!")
	} else {
		t.Log("Both records written — the race condition did not manifest in this run. ")
	}

	t.Cleanup(func() {
		migrationManager.ResetSingleton()
		clDiscoverer.ResetSingleton()
	})
}
