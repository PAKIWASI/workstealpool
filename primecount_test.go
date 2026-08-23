package workstealpool

import (
	"context"
	"testing"
)

func TestCountPrimesSequential_KnownValues(t *testing.T) {
	// pi(n): number of primes below n, from standard tables.
	tests := []struct {
		hi   int
		want int
	}{
		{0, 0},
		{2, 0},
		{3, 1},   // {2}
		{10, 4},  // {2,3,5,7}
		{100, 25},
		{1000, 168},
	}

	for _, tc := range tests {
		if got := countPrimesSequential(0, tc.hi); got != tc.want {
			t.Errorf("countPrimesSequential(0, %d) = %d, want %d", tc.hi, got, tc.want)
		}
	}
}

// NOTE(-race): one of the two tests most likely to intermittently trigger
// the known benign LFdeque race - see README.md, "Known limitation:
// benign data race under -race". A -race report here alone is expected;
// a wrong count is a real bug.
func TestCountPrimesParallel_MatchesSequential(t *testing.T) {
	const lo, hi = 0, 50_000
	want := countPrimesSequential(lo, hi)

	configs := []poolConfig{
		{PoolSize: 1, InitialWorkerCap: 8, ResultBuffSize: 1, Threshold: 500},
		{PoolSize: 2, InitialWorkerCap: 8, ResultBuffSize: 4, Threshold: 500},
		{PoolSize: 4, InitialWorkerCap: 8, ResultBuffSize: 16, Threshold: 200},
		{PoolSize: 8, InitialWorkerCap: 8, ResultBuffSize: 64, Threshold: 50},
		{PoolSize: 8, InitialWorkerCap: 64, ResultBuffSize: 64, Threshold: 1},
		// threshold >= range width: the pool does one leaf, zero stealing.
		{PoolSize: 4, InitialWorkerCap: 8, ResultBuffSize: 4, Threshold: hi - lo},
	}

	for _, cfg := range configs {
		t.Run("", func(t *testing.T) {
			got, err := CountPrimesParallel(context.Background(), lo, hi, cfg)
			if err != nil {
				t.Fatalf("CountPrimesParallel(%+v) error: %v", cfg, err)
			}
			if got != want {
				t.Fatalf("CountPrimesParallel(%+v) = %d, want %d", cfg, got, want)
			}
		})
	}
}

func TestCountPrimesParallel_EmptyRange(t *testing.T) {
	cfg := poolConfig{PoolSize: 4, InitialWorkerCap: 8, ResultBuffSize: 4, Threshold: 100}
	got, err := CountPrimesParallel(context.Background(), 10, 10, cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != 0 {
		t.Fatalf("CountPrimesParallel on empty range = %d, want 0", got)
	}
}

func TestCountPrimesParallel_SingleWorker(t *testing.T) {
	// No stealing possible at all (pool size 1); exercises pure
	// spawn/PushBottom/PopBottom recursion on a single deque, including
	// its growth, with no thief ever touching it.
	const lo, hi = 0, 20_000
	want := countPrimesSequential(lo, hi)

	cfg := poolConfig{PoolSize: 1, InitialWorkerCap: 2, ResultBuffSize: 1, Threshold: 37}
	got, err := CountPrimesParallel(context.Background(), lo, hi, cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != want {
		t.Fatalf("got %d, want %d", got, want)
	}
}

func TestCountPrimesParallel_ThresholdOfOne(t *testing.T) {
	// Maximum fan-out: bisects all the way down to single numbers,
	// maximizing spawn/steal traffic relative to leaf work.
	const lo, hi = 0, 5_000
	want := countPrimesSequential(lo, hi)

	cfg := poolConfig{PoolSize: 8, InitialWorkerCap: 8, ResultBuffSize: 32, Threshold: 1}
	got, err := CountPrimesParallel(context.Background(), lo, hi, cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != want {
		t.Fatalf("got %d, want %d", got, want)
	}
}

// TestCountPrimesParallel_Repeated runs the same workload many times to
// catch any flakiness from the underlying deque/pool under real scheduling
// noise, rather than trusting a single lucky pass.
//
// NOTE(-race): the other test most likely to reproduce the known benign
// LFdeque race - see README.md, "Known limitation: benign data race under
// -race".
func TestCountPrimesParallel_Repeated(t *testing.T) {
	const lo, hi = 0, 8_000
	want := countPrimesSequential(lo, hi)
	cfg := poolConfig{PoolSize: 6, InitialWorkerCap: 4, ResultBuffSize: 8, Threshold: 23}

	for i := 0; i < 50; i++ {
		got, err := CountPrimesParallel(context.Background(), lo, hi, cfg)
		if err != nil {
			t.Fatalf("run %d: unexpected error: %v", i, err)
		}
		if got != want {
			t.Fatalf("run %d: got %d, want %d", i, got, want)
		}
	}
}
