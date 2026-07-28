package race_test

import (
	"sync"
	"sync/atomic"
	"testing"
)

// Minimal reproduction of the ConsistencyClock race in StrongCollection.
// No project dependencies needed — just reproduces the logic from updateRecord.
func TestConsistencyClockRace(t *testing.T) {
	const iterations = 1000
	lostUpdates := atomic.Int32{}

	for i := 0; i < iterations; i++ {
		var clock uint64 = 0 // simulates StrongCollection.ConsistencyClock

		var updateAApplied, updateBApplied atomic.Bool
		var wg sync.WaitGroup
		wg.Add(2)

		// Goroutine simulating update B (clock=2)
		go func() {
			defer wg.Done()
			if clock > 2 {
				return
			}
			updateBApplied.Store(true)
			clock = 2
		}()

		// Goroutine simulating update A (clock=1)
		go func() {
			defer wg.Done()
			if clock > 1 {
				return
			}
			updateAApplied.Store(true)
			clock = 1
		}()

		wg.Wait()

		if updateBApplied.Load() && !updateAApplied.Load() {
			lostUpdates.Add(1)
		}
	}

	lost := lostUpdates.Load()
	t.Logf("Out of %d iterations, %d had a lost update", iterations, lost)

	if lost > 0 {
		t.Errorf("BUG CONFIRMED: %d/%d iterations lost an update. "+
			"When update B (clock=2) runs before A (clock=1), "+
			"A is discarded because clock (2) > sequence (1).", lost, iterations)
	}
}
