package workstealpool

import (
	"context"
	"fmt"
	"testing"
)

// benchRange is the [0, hi) range counted in every benchmark below. Fixed
// so that different pool/buffer/threshold settings are being compared on
// identical work, rather than confounding "more workers" with "different
// problem".
const benchRangeHi = 200_000

// runCountPrimes is the shared benchmark body: run CountPrimesParallel
// b.N times under cfg, discarding the result (correctness is covered by
// primecount_test.go, not here).
func runCountPrimes(b *testing.B, cfg poolConfig) {
	b.Helper()
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := countPrimesParallel(ctx, 0, benchRangeHi, cfg); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkCountPrimes_PoolSize sweeps worker count at fixed buffer/threshold,
// the main "does stealing pay for itself" axis: GOMAXPROCS-bound speedup
// should show up here as pool size climbs, with diminishing (or negative,
// past the point of contention exceeding available cores) returns eventually.
func BenchmarkCountPrimes_PoolSize(b *testing.B) {
	for _, poolSize := range []int{1, 2, 4, 8, 16, 32} {
		cfg := poolConfig{
			PoolSize:         poolSize,
			InitialWorkerCap: 32,
			ResultBuffSize:   64,
			Threshold:        500,
		}
		b.Run(fmt.Sprintf("workers=%d", poolSize), func(b *testing.B) {
			runCountPrimes(b, cfg)
		})
	}
}

// BenchmarkCountPrimes_InitialWorkerCap sweeps each worker deque's starting
// capacity at fixed pool size/threshold. A too-small cap means every worker
// pays for several PushBottom-triggered resizeCopy calls early on; a large
// enough cap should flatten that cost out. This isolates that effect.
func BenchmarkCountPrimes_InitialWorkerCap(b *testing.B) {
	for _, cap := range []int{2, 8, 32, 128, 512} {
		cfg := poolConfig{
			PoolSize:         8,
			InitialWorkerCap: cap,
			ResultBuffSize:   64,
			Threshold:        500,
		}
		b.Run(fmt.Sprintf("cap=%d", cap), func(b *testing.B) {
			runCountPrimes(b, cfg)
		})
	}
}

// BenchmarkCountPrimes_ResultBuffSize sweeps the results channel's buffer
// size at fixed pool size/threshold. Threshold=500 over a 200k range
// produces ~400 leaves/results, so this sweep crosses from "every send
// blocks until the caller's drain loop catches up" (buffer=1) to "every
// send is buffered, drain loop entirely off the hot path" (buffer >= leaf
// count).
func BenchmarkCountPrimes_ResultBuffSize(b *testing.B) {
	for _, buf := range []int{1, 4, 16, 64, 256, 1024} {
		cfg := poolConfig{
			PoolSize:         8,
			InitialWorkerCap: 32,
			ResultBuffSize:   buf,
			Threshold:        500,
		}
		b.Run(fmt.Sprintf("buf=%d", buf), func(b *testing.B) {
			runCountPrimes(b, cfg)
		})
	}
}

// BenchmarkCountPrimes_Threshold sweeps leaf granularity at fixed pool
// size/buffer. Small threshold = more leaves = more spawn/steal/CAS
// overhead per unit of real work but finer-grained load balancing; large
// threshold = less overhead but coarser balancing (and, past a point, not
// enough leaves to keep every worker busy at all).
func BenchmarkCountPrimes_Threshold(b *testing.B) {
	for _, threshold := range []int{1, 10, 50, 200, 1000, 5000, benchRangeHi} {
		cfg := poolConfig{
			PoolSize:         8,
			InitialWorkerCap: 32,
			ResultBuffSize:   64,
			Threshold:        threshold,
		}
		b.Run(fmt.Sprintf("threshold=%d", threshold), func(b *testing.B) {
			runCountPrimes(b, cfg)
		})
	}
}

// BenchmarkCountPrimes_PoolSizeXThreshold cross-sweeps pool size and
// threshold together, since they interact: a small pool doesn't need fine
// granularity to stay busy, while a large pool needs enough leaves (i.e. a
// small enough threshold relative to the range) to actually have anything
// to steal.
func BenchmarkCountPrimes_PoolSizeXThreshold(b *testing.B) {
	poolSizes := []int{1, 4, 16}
	thresholds := []int{20, 500, 10_000}

	for _, poolSize := range poolSizes {
		for _, threshold := range thresholds {
			cfg := poolConfig{
				PoolSize:         poolSize,
				InitialWorkerCap: 32,
				ResultBuffSize:   64,
				Threshold:        threshold,
			}
			b.Run(fmt.Sprintf("workers=%d/threshold=%d", poolSize, threshold), func(b *testing.B) {
				runCountPrimes(b, cfg)
			})
		}
	}
}

// BenchmarkCountPrimes_RangeSize sweeps the total problem size at fixed
// pool/buffer/threshold, showing how fixed per-run overhead (pool
// construction, goroutine startup) amortizes as the workload grows.
func BenchmarkCountPrimes_RangeSize(b *testing.B) {
	for _, hi := range []int{1_000, 10_000, 100_000, 1_000_000} {
		cfg := poolConfig{
			PoolSize:         8,
			InitialWorkerCap: 32,
			ResultBuffSize:   64,
			Threshold:        500,
		}
		b.Run(fmt.Sprintf("hi=%d", hi), func(b *testing.B) {
			ctx := context.Background()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := countPrimesParallel(ctx, 0, hi, cfg); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkCountPrimes_Sequential is the non-parallel baseline every other
// benchmark in this file should be compared against - the whole point of
// the pool is to beat this, and by how much (and at what pool size it stops
// improving) is the interesting number.
func BenchmarkCountPrimes_Sequential(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		countPrimesSequential(0, benchRangeHi)
	}
}
