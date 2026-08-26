package workstealpool

import (
	"context"
	"testing"
	"time"
)

// Deliberately oversubscribed (many more workers than there is work),
// small/coarse workload, so nearly every worker should park at least
// once. Runs many trials under -race, each with a hard timeout, so any
// reintroduced deadlock (or a genuine lost-wakeup stall) fails loudly
// instead of hanging the whole test run.
func TestWorkerPool_ParkingUnderLightLoad(t *testing.T) {
	want := countPrimesSequential(0, 3000)

	for trial := range 100 {
		cfg := poolConfig{PoolSize: 32, InitialWorkerCap: 8, ResultBuffSize: 4, Threshold: 500}

		done := make(chan struct{})
		var got int
		var err error
		go func() {
			got, err = CountPrimesParallel(context.Background(), 0, 3000, cfg)
			close(done)
		}()

		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Fatalf("trial %d: hung for 3s (32 workers, light load - should force parking)", trial)
		}

		if err != nil {
			t.Fatalf("trial %d: unexpected error: %v", trial, err)
		}
		if got != want {
			t.Fatalf("trial %d: got %d, want %d", trial, got, want)
		}
	}
}
