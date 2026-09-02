# workstealpool

A lock-free work-stealing pool for recursive divide-and-conquer workloads
in Go (Chase-Lev deque, Lê/Pop/Cohen/Nardelli, PPoPP 2013). Each worker
owns a deque, runs its own work LIFO, and steals half of a random
victim's deque (FIFO) when it runs dry. So an unevenly-shaped task tree
still balances itself across workers at runtime.

## Install

```bash
go get github.com/PAKIWASI/workstealpool
```

## API

```go
type Task[T, R any] func(ctx context.Context, workerId int, item T, res chan<- R, spawn func(...T)) error

func NewWorkerPool[T, R any](ctx context.Context, poolSize, initialWorkerCap, resultBuffSize int, execute Task[T, R]) *WorkerPool[T, R]
func (p *WorkerPool[T, R]) Submit(item T)
func (p *WorkerPool[T, R]) SubmitN(items ...T)
func (p *WorkerPool[T, R]) Run() <-chan R
func (p *WorkerPool[T, R]) Wait() error
```

- **`NewWorkerPool`** builds `poolSize` workers, each with an `LFdeque[T]`
  of initial capacity `initialWorkerCap`, and a results channel buffered
  to `resultBuffSize`. Workers don't start until `Run`.
- **`Submit`** seeds the pool with an initial item onto worker 0's deque.
  **`SubmitN`** seeds multiple initial items. Call before `Run`.
- **`Run`** starts every worker and returns the results channel. It
  closes once no work remains anywhere, or a task returns a fatal error.
- **`Wait`** blocks until every worker has exited and returns the first
  fatal error (nil on success). Safe to call while draining `Run()`'s
  channel concurrently.

### `Task`

Every call to your `Task` is one node in the recursion tree:

- **Leaf**: do the work and emit the outcome onto `res`: `res <- result; return nil`.
- **Internal node**: call `spawn(child...)` one or more times to enqueue child tasks: `spawn(child1, child2); return nil`.
- **Fatal error**: `return err`. Aborts the whole pool; `Wait()` reports it. Reserve this for real failures, not per-item conditions (e.g. a permission-denied file mid-walk) — fold those into `R` instead and emit them as a normal leaf result.

**`workerID`** is the index (`0..poolSize-1`) of the worker currently
running this call. It identifies which worker's local state to use for
task-local scratch space (a scratch buffer, a symlink-cycle stack,
anything you don't want shared/synchronized across workers). Index your
own `[]WorkerState` (sized `poolSize`, created alongside the pool) with
it. It is **not** a call ID: the same `workerID` runs many `Task` calls
over the pool's lifetime, sequentially, so state you key by it persists
and must be treated as reused, not per-call.

**`spawn`** must be called synchronously, from inside the `Task` call
itself — it pushes onto the *calling worker's own* deque. Don't stash it
or call it from another goroutine.

**`ctx`** is cancelled once the pool is done (all work finished, or a
fatal error). Long-running leaves should check it if they want to be
interruptible.

## Example

```go
type primeRange struct{ Lo, Hi int }

func countPrimesTask(threshold int) Task[primeRange, int] {
    return func(ctx context.Context, workerID int, item primeRange, res chan<- int, spawn func(...primeRange)) error {
        width := item.Hi - item.Lo
        if width <= threshold {
            res <- countPrimesSequential(item.Lo, item.Hi)
            return nil
        }
        mid := item.Lo + width/2
        spawn(primeRange{item.Lo, mid}, primeRange{mid, item.Hi})
        return nil
    }
}

pool := NewWorkerPool[primeRange, int](ctx, poolSize, initialCap, resultBuf, countPrimesTask(threshold))
pool.Submit(primeRange{lo, hi})

total := 0
for count := range pool.Run() {
    total += count
}
if err := pool.Wait(); err != nil {
    // handle error
}
```

See `primecount.go` for the full worked example.

## When this is (and isn't) a good fit

Good fit: quicksort/mergesort, tree/graph search, recursive numeric
subdivision, anything where you don't know the shape of the work
upfront and it can spawn more of itself. Bad fit: uniformly-sized batches
(nothing to steal) or I/O-bound work (stealing balances CPU, not
blocking calls).

## Testing

```
go test ./...
go test -race ./...   # occasionally flags a known-benign race in
                       # LFdeque.Steal vs PushBottom — see deque.go
go test -bench=. ./...
```

## Known limitation: benign race under `-race`

Running the tests with `-race` will occasionally report a race between
`LFdeque.PushBottom`'s array write and `LFdeque.Steal`/`StealHalf`'s array
read (`deque.go`). This is **expected** and does not affect correctness.

`Steal` reads the array slot *before* CASing `top` to claim it, matching
the Chase-Lev paper. If the CAS fails (a thief lost the race), the value
it just read is discarded via `ok == false`, but the read itself already
happened, unsynchronized, against whatever the owner does next. So a
losing thief's read is a genuine data race on paper, the value can even
be torn for multi-word `T`, but since it's always thrown away, no caller
ever observes it. `-race` is correctly flagging an unsynchronized access,
not producing a false positive; it's just one that provably can't corrupt
a result.

Two real fixes exist if you're adapting this for production, neither
implemented here on purpose: **boxing** each element as
`atomic.Pointer[T]` (real atomic access, costs one allocation per push),
or **epoch/hazard-pointer reclamation** (will attempt this).

`TestCountPrimesParallel_MatchesSequential` and
`TestCountPrimesParallel_Repeated` are the tests most likely to trigger
it, by design. They drive real concurrent push/steal traffic through a
struct-typed `T`.
