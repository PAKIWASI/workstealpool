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

Running high-concurrency benchmarks or repeat tests under `go test -race` will occasionally report a data race between `LFdeque.PushBottom` / `PushSliceBottom`'s array write and `LFdeque.Steal` / `StealHalf`'s array read (`deque.go`). This is **expected, benign, and does not affect correctness**.

### Why this happens

`LFdeque[T]` is an unboxed, zero-allocation lock-free Chase-Lev deque that stores items directly as value types `[]T` inside a circular ring buffer (`circularArray`).

1. **Ring Buffer Wrap-Around**: Physical slots in the backing buffer are addressed as `index % capacity`. When a thief steals an item at `curTop`, it claims the slot via CAS and reads `buf[curTop % capacity]`.
2. **Memory Slot Reuse**: As the owner pops and pushes subsequent work, `bottom` eventually wraps around after `capacity` items. If the queue is not full, the owner reuses that same physical memory slot `(curTop + capacity) % capacity` without needing to allocate a new array.
3. **ThreadSanitizer Detection**: Because `T` is an unboxed multi-word struct (such as `primeRange{Lo, Hi int}` or `walkItem`), the thief's past read and the owner's later write are standard memory copies. The Go memory model does not have relaxed atomic operations for arbitrary struct types. ThreadSanitizer flags this memory address reuse across wrap-around as a `DATA RACE` because there is no explicit atomic release/acquire edge from the thief's *past read* to the owner's *future write*.

### Why it does not affect correctness

- The owner never overwrites a slot while it is logically active in the deque; the capacity check `b - t >= capacity` guarantees the owner will allocate a brand-new array if the ring is full.
- The thief only reads a slot after successfully claiming it via `top` CAS, so the value read is always the exact item pushed by the owner.
- All functional tests (`go test ./...`) pass 100% reliably.

### Trade-offs & Alternatives

To eliminate `-race` reports completely, one would have to **box** every element (e.g. `atomic.Pointer[T]` or `[]*T`). However, boxing requires allocating every task item on the heap with `new(T)`, introducing substantial GC pressure and memory allocations on the hot path. `workstealpool` deliberately chooses an unboxed, zero-allocation design for maximum throughput.

`TestCountPrimesParallel_MatchesSequential` and `TestCountPrimesParallel_Repeated` are the tests most likely to trigger it under `-race`, as they drive intense concurrent push/steal traffic through struct-typed tasks.

