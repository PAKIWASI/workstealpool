package workstealpool

// primecount.go
//
// A divide-and-conquer workload used to exercise WorkerPool end-to-end:
// count the primes in [lo, hi) by recursively bisecting the range until a
// sub-range is small enough (<= threshold) to count sequentially by trial
// division, then summing the per-leaf counts.
//
// This shape - recursively spawn two children, no result at internal
// nodes, a result at every leaf - is exactly what Task[T, R] is built for:
// spawn(T) schedules more of the same T locally (so it stays on the
// spawning worker's deque and gets stolen from there if another worker
// goes idle), and leaf results are combined by the caller after draining
// the results channel, not by the tree of tasks itself.
//
// threshold controls granularity: too small and CPU time is dominated by
// task/CAS overhead per range; too large and there isn't enough work to
// go around for stealing to matter. It's one of the settings the
// benchmarks in primecount_bench_test.go sweep.

import (
	"context"
)

// primeRange is the unit of work: count primes in [Lo, Hi).
type primeRange struct {
	Lo, Hi int
}

// isPrime reports whether n is prime, via trial division up to sqrt(n).
// Deliberately simple (no sieve, no memoization) so leaf work is real,
// bounded CPU work rather than something the scheduler races through
// instantly - that's what makes pool size and threshold visible in the
// benchmarks below.
func isPrime(n int) bool {
	if n < 2 {
		return false
	}
	if n%2 == 0 {
		return n == 2
	}
	for d := 3; d*d <= n; d += 2 {
		if n%d == 0 {
			return false
		}
	}
	return true
}

// countPrimesSequential counts primes in [lo, hi) with no parallelism.
// Used as the correctness baseline and as the leaf computation itself.
func countPrimesSequential(lo, hi int) int {
	count := 0
	for n := lo; n < hi; n++ {
		if isPrime(n) {
			count++
		}
	}
	return count
}

// countPrimesTask returns a Task that recursively bisects its input range,
// spawning both halves, until a range is <= threshold wide, at which point
// it counts that range sequentially and returns the count as a leaf result.
func countPrimesTask(threshold int) Task[primeRange, int] {
	return func(ctx context.Context, workerID int, item primeRange, spawn func(...primeRange)) (int, bool, error) {
		width := item.Hi - item.Lo
		if width <= threshold {
			count := countPrimesSequential(item.Lo, item.Hi)
			return count, true, nil
		}

		mid := item.Lo + width/2
		spawn(primeRange{Lo: item.Lo, Hi: mid}, primeRange{Lo: mid, Hi: item.Hi})
		return 0, false, nil
	}
}

// poolConfig bundles the WorkerPool knobs this workload benchmarks against.
type poolConfig struct {
	PoolSize         int // number of worker goroutines / deques
	InitialWorkerCap int // initial capacity of each worker's LFdeque
	ResultBuffSize   int // buffering on the results channel
	Threshold        int // leaf granularity: range width that stops bisection
}

// CountPrimesParallel counts primes in [lo, hi) using a WorkerPool
// configured by cfg. It's the divide-and-conquer entry point: Submit seeds
// worker 0's deque with the whole range, Run starts every worker pulling
// and spawning, and the loop over results here does the "conquer" step -
// summing independent leaf counts as they arrive.
func CountPrimesParallel(ctx context.Context, lo, hi int, cfg poolConfig) (int, error) {
	pool := NewWorkerPool[primeRange, int](
		ctx,
		cfg.PoolSize,
		cfg.InitialWorkerCap,
		cfg.ResultBuffSize,
		countPrimesTask(cfg.Threshold),
	)

	pool.Submit(primeRange{Lo: lo, Hi: hi})

	results := pool.Run()

	total := 0
	for r := range results {
		total += r
	}

	if err := pool.Wait(); err != nil {
		return 0, err
	}

	return total, nil
}
