# worksteal

A lock-free work-stealing scheduler for recursive divide-and-conquer
workloads in Go, implementing the Chase-Lev deque (Lê, Pop, Cohen &
Nardelli, PPoPP 2013).

- **`deque.go`** — `LFdeque[T]`, a growable, array-based work-stealing
  deque. One owner goroutine pushes/pops from the bottom; any number of
  thieves steal from the top.
- **`work_pool.go`** — `WorkerPool[T, R]`, a pool of workers, each owning
  one `LFdeque[T]`. Workers run local work LIFO and steal from other
  workers' deques FIFO (in half-batches) when they run dry.
- **`primecount.go`** — a divide-and-conquer prime-counting workload used
  to exercise the pool end-to-end, and as the correctness/benchmark
  harness for the rest of the package.

## Install

```
go get github.com/PAKIWASI/work_steal_pool
```

## Usage

```go
pool := worksteal.NewWorkerPool[T, R](ctx, poolSize, initialCap, resultBuf, task)
pool.Submit(initialItem)
for r := range pool.Run() {
    // consume results as they arrive
}
if err := pool.Wait(); err != nil {
    // first error from any worker
}
```

A `Task[T, R]` either returns a result (leaf) or calls `spawn` to schedule
more work of the same type onto the calling worker's own deque (internal
node). See `primecount.go` for a worked example.

## Writing a Task function

```go
type Task[T, R any] func(ctx context.Context, item T, spawn func(T)) (result *R, err error)
```

Every call to your `Task` is one node in the recursion tree, and it must
do exactly one of two things:

- **Leaf**: do the real work for `item` and return `(&result, nil)`.
- **Internal node**: decide `item` is still too big, call `spawn(child)`
  one or more times to hand off smaller pieces, and return `(nil, nil)`.
  Don't do both — a node that spawns children shouldn't also return a
  result, since nothing would combine it with the children's results.

Worked example, from `primecount.go` (bisect a range until it's small
enough to count directly):

```go
func countPrimesTask(threshold int) Task[primeRange, int] {
    return func(ctx context.Context, item primeRange, spawn func(primeRange)) (*int, error) {
        width := item.Hi - item.Lo
        if width <= threshold {
            // Leaf: do the real work, return a result.
            count := countPrimesSequential(item.Lo, item.Hi)
            return &count, nil
        }

        // Internal node: split and hand off both halves.
        mid := item.Lo + width/2
        spawn(primeRange{Lo: item.Lo, Hi: mid})
        spawn(primeRange{Lo: mid, Hi: item.Hi})
        return nil, nil
    }
}
```

Rules to keep in mind:

- **Call `spawn` synchronously, from inside the `Task` call itself.**
  It schedules onto the *calling worker's own* deque — don't stash it and
  call it later, or call it from another goroutine.
- **Every result you want must come from a `return &result, nil`**, not
  from a side channel. The pool collects results only through the return
  value; combining/summing them (like the range-counting loop in
  `CountPrimesParallel`) is the caller's job after draining `Run()`, not
  the task tree's.
- **Return a non-nil `error` to abort the whole pool.** The first error
  from any worker cancels every other worker and is what `Wait()` returns
  — don't use it for expected/recoverable conditions, only real failures.
- **Check `ctx` in long-running leaf work** if you want it to be
  interruptible when the pool cancels (e.g. after another worker errors).
  Cheap leaves (like `countPrimesSequential` here) usually don't need to.
- **Pick a leaf-size threshold deliberately** — see Performance below.
  Too fine and CAS/spawn overhead dominates; too coarse and there isn't
  enough work to steal.

## What this is useful for

This pool is built for one specific shape of problem: recursive
divide-and-conquer work where you *don't know the shape of the tree up
front* — each item, once you look at it, may produce more items, and the
resulting subtrees can be wildly uneven in size. That's exactly the case
a fixed, evenly-partitioned worker pool handles badly: partition a lopsided
tree evenly across N workers up front and some of them finish early and
sit idle while one worker is still chewing through the big branch. Work
stealing fixes that by letting idle workers pull work from busy ones at
runtime, so load balances itself regardless of how the tree turns out.

Good fits:
- **Parallel recursive sorts** — quicksort/mergesort, where partitions
  split unevenly depending on the data.
- **Tree/graph search** — game tree search (e.g. alpha-beta), N-Queens,
  puzzle solvers, where branches prune to very different depths.
- **Recursive numeric subdivision** — this repo's own prime-counting
  example: bisect a range, recurse until small enough, sum leaf results.
- **Spatial/recursive structures** — ray tracing scene traversal, k-d
  tree or octree construction and queries, fractal rendering.

Poor fits:
- **Uniform, embarrassingly parallel batches** (e.g. "resize these 10,000
  images") — the work is already evenly sized and known up front, so a
  plain fixed-size worker pool with a shared channel is simpler and just
  as fast; there's nothing to steal because there's no imbalance.
- **I/O-bound work** — stealing balances *CPU* work across cores; a
  blocked-on-network task doesn't benefit from a lock-free deque, use a
  bounded goroutine pool or semaphore instead.
- **Work that must be processed in order** — `Run()`'s results arrive as
  leaves finish, not in submission order.

## How it works

Each worker pops its own deque LIFO (cache-friendly, and it means a
worker keeps working on the subtree it just spawned rather than jumping
around). When a worker's deque empties, it steals *half* of a random
victim's deque from the top (FIFO), amortizing the cost of a steal over
many items instead of stealing one item at a time. Workers spin briefly
on a failed steal, then park until woken by a `PushBottom`/spawn
elsewhere in the pool.

## Performance

Full suite (`go test ./...`) passes, including the concurrent
owner/thief/parking tests. Benchmarks below are from
`go test -bench=. -benchmem`, run on:

```
CPU:    11th Gen Intel(R) Core(TM) i5-1135G7 @ 2.40GHz
Cores:  4 physical cores, 8 threads (GOMAXPROCS=8)
GOOS/GOARCH: linux/amd64
```

### Deque primitives (`LFdeque`, uncontended and contended)

```
PushBottom              46.14 ns/op    0 B/op   0 allocs/op
PopBottom               31.31 ns/op    0 B/op   0 allocs/op
Steal                   25.61 ns/op    0 B/op   0 allocs/op
StealHalf (from 1024)  6224    ns/op  682 B/op   0 allocs/op   (~1000 elements/steal)

ConcurrentSteal, 1024 pre-filled, N thieves racing for them concurrently:
  thieves=1    19.63 ns/op
  thieves=2    19.65 ns/op
  thieves=4    19.66 ns/op
  thieves=8    19.63 ns/op
  thieves=16   19.74 ns/op
```

The concurrent-steal number is the one worth noting: per-steal cost is
**flat from 1 to 16 concurrent thieves** (19.6–19.7 ns/op throughout).
That's the lock-free deque doing its job — thieves aren't queueing up
behind each other or degrading under contention, each `Steal` call costs
the same regardless of how many other goroutines are hammering the same
deque at once.

### Full workload: threshold (leaf granularity) is the dominant knob

Counting primes below 200,000, pool size 8, sweeping leaf-size threshold:

```
threshold=1        94,972,866 ns/op  14,508,950 B/op  602,791 allocs/op
threshold=10       20,260,268 ns/op   2,392,607 B/op   99,196 allocs/op
threshold=50        4,795,156 ns/op     309,412 B/op   12,630 allocs/op
threshold=200       3,287,177 ns/op      83,704 B/op    3,260 allocs/op
threshold=1,000     3,043,703 ns/op      26,275 B/op      875 allocs/op   ← fastest
threshold=5,000     3,134,107 ns/op      11,624 B/op      266 allocs/op
threshold=200,000  12,998,140 ns/op       6,656 B/op       57 allocs/op   (1 leaf, no stealing)
sequential baseline 12,199,920 ns/op          0 B/op        0 allocs/op
```

Threshold=1 is **~31x slower** than the threshold=1,000 sweet spot here,
almost entirely from spawn/steal/CAS bookkeeping (600k+ allocations vs.
875) rather than real work — bisecting to single numbers generates far
more tree nodes than the leaf work justifies. At the other extreme,
threshold=200,000 forces a single leaf with zero parallelism, landing
right back near the sequential baseline. The best threshold sits where
leaves are cheap enough to steal in useful chunks but not so fine that
overhead swamps the work — sweep it for your own workload, this number
won't transfer directly.

### Pool size: speedup tracks physical core count, then plateaus

Same workload, threshold fixed at 500:

```
workers=1   12,503,690 ns/op   (1.0x — pool overhead roughly cancels out vs. sequential)
workers=2    6,778,972 ns/op   (1.8x)
workers=4    4,142,564 ns/op   (2.9x)
workers=8    3,100,820 ns/op   (3.9x)
workers=16   3,051,032 ns/op   (4.0x — no further gain)
workers=32   3,145,615 ns/op   (3.9x — slightly worse: oversubscription)
```

Speedup climbs cleanly through the physical core count (4) and continues
a bit further into hyperthreading territory, then **flattens right around
8 workers** — this CPU's thread count — and going well past that
(32 workers) costs a little rather than gaining anything, from scheduling
more goroutines than there's parallelism to run them. Matching pool size
to `runtime.NumCPU()` (or close to it) is the right default; there's
rarely a reason to go far beyond your thread count.

### Threshold × pool size interact

`PoolSizeXThreshold` makes the interaction explicit: at a too-fine
threshold (20), more workers barely help (17.4ms → 11.5ms → 12.6ms
going 1 → 4 → 16 workers, actually *regressing* at 16) because the
bottleneck is per-node overhead, not available parallelism. At a
reasonable threshold (500), the same pool-size increase scales cleanly
(12.6ms → 4.1ms → 3.1ms). Tuning one without the other leaves
performance on the table either way.

### Pool size, deque capacity, result buffer — mostly a memory knob past a point

Deque capacity and result-buffer size had a much smaller effect than
threshold or pool size on this workload — a few hundred µs across the
whole sweep — but memory scales directly with whatever you ask for
(e.g. `InitialWorkerCap=512` uses ~107 KB/op vs. ~43 KB/op at the
default), so oversizing them costs memory for little to no speed benefit
once they're past the point of avoiding resize churn.

`BenchmarkCountPrimes_PoolSize`, `_InitialWorkerCap`, `_ResultBuffSize`,
`_Threshold`, and `_PoolSizeXThreshold` in `primecount_bench_test.go`
sweep all of the above; rerun them on your target hardware and workload
before picking production values — these numbers are a starting point,
not a guarantee.

## Testing

```
go test ./...            # normal run
go test -race ./...      # may intermittently report the known race below;
                          # any other race, or a wrong count/value, is a real bug
go test -bench=. ./...   # benchmarks sweep pool size, deque cap, result
                          # buffer size, leaf threshold, and range size
```

## Known limitation: benign race under `-race`

Running the tests with `-race` will occasionally report a race between
`LFdeque.PushBottom`'s array write and `LFdeque.Steal`/`StealHalf`'s array
read (`deque.go`). This is **expected** and does not affect correctness.

`Steal` reads the array slot *before* CASing `top` to claim it — matching
the Chase-Lev paper. If the CAS fails (a thief lost the race), the value
it just read is discarded via `ok == false`, but the read itself already
happened, unsynchronized, against whatever the owner does next. So a
losing thief's read is a genuine data race on paper — the value can even
be torn for multi-word `T` — but since it's always thrown away, no caller
ever observes it. `-race` is correctly flagging an unsynchronized access,
not producing a false positive; it's just one that provably can't corrupt
a result.

Two real fixes exist if you're adapting this for production, neither
implemented here on purpose: **boxing** each element as
`atomic.Pointer[T]` (real atomic access, costs one allocation per push),
or **epoch/hazard-pointer reclamation** (what `crossbeam-deque` and
`ForkJoinPool` do — no boxing, more machinery).

`TestCountPrimesParallel_MatchesSequential` and
`TestCountPrimesParallel_Repeated` are the tests most likely to trigger
it, by design — they drive real concurrent push/steal traffic through a
struct-typed `T`.
