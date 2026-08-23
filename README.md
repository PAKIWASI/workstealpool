# worksteal

A lock-free work-stealing scheduler for recursive divide-and-conquer workloads in Go.

- `deque.go` — `LFdeque[T]`, a growable, array-based Chase-Lev work-stealing
  deque (Lê, Pop, Cohen & Nardelli, PPoPP 2013). One owner goroutine pushes
  and pops from the bottom; any number of thief goroutines steal from the top.
- `work_pool.go` — `WorkerPool[T, R]`, a pool of workers, each owning one
  `LFdeque[T]`. Workers execute local work LIFO and steal from other workers'
  deques FIFO (in half-batches) when they run dry.
- `primecount.go` — a divide-and-conquer prime-counting workload used to
  exercise the pool end-to-end, and as the correctness/benchmark harness for
  the rest of the package.

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

## Known limitation: benign data race under `-race`

Running the test suite with `-race` will occasionally (not every run — it's
timing-dependent) report a data race between `LFdeque.PushBottom`
(`deque.go`, the `a.put` write) and `LFdeque.Steal` / `StealHalf`
(`deque.go`, the `a.get` read). This is **expected** and does not indicate
an incorrect result. See `TestCountPrimesParallel_MatchesSequential` and
`TestCountPrimesParallel_Repeated` in `primecount_test.go`, which are the
tests most likely to reproduce it (they drive real concurrent
push/steal traffic through `WorkerPool` on a struct-typed `T`).

### Why it happens

`Steal()` reads the value at its snapshot of `top` *before* attempting
`CompareAndSwap(top, top+1)` — intentional, matching the Chase-Lev paper:
grab the value first, then verify you won the claim.

Split by outcome:

- **The thief's CAS succeeds** (it won the slot). Its read happened before
  its own CAS store, in program order. The owner can only legally reuse
  that slot after its own `top.Load()` observes a value ≥ `t+1` — and
  because `sync/atomic` operations form a single total order, that load
  *synchronizes-with* this thief's store. Chaining it: thief's read →
  (program order) → thief's CAS store → (synchronizes-with) → owner's
  later `top.Load()` → (program order) → owner's write. That's a full
  happens-before edge. **A winning thief's read is already properly
  ordered relative to any later reuse of that slot — this case is not
  racy.**

- **The thief's CAS fails** (someone else already advanced `top` past
  `t` first). The read still happened, but this thief never performs a
  store of its own — there's nothing for the owner's later load to
  synchronize *with* on this thief's side. Its read is completely
  unordered relative to whatever the owner does afterward. **This is the
  actual race**: a losing thief's array read, not the value it returns
  (which is always correctly discarded via `ok == false`), but the bare
  act of reading a slot the owner may already be free to reuse and is
  concurrently overwriting.

So the hazard is specifically: *a thief that is about to lose the race for
`top` reads the array before finding that out.*

### Why the result is still always correct

Regardless of which case above applies, whenever the CAS fails the read
value is discarded and never returned to a caller — that's the existing
`ok == false` path. No caller ever observes data from a reused slot.

### What isn't fully guaranteed

Discarding the *value* doesn't make the *read itself* well-defined. Per
the above, a losing thief's `a.get(t)` is an unsynchronized concurrent
access against the owner's `a.put(b, v)` on the same slot, using plain,
non-atomic slice element access. Under Go's memory model that is
technically a data race regardless of what happens to the result — and for
a multi-word `T` (e.g. `primeRange{Lo, Hi int}`), a losing thief could in
principle observe a torn value (part old, part new) during that read. That
torn value is still thrown away, so no caller is affected, but it's why
`-race` correctly flags this rather than it being a detector false
positive.

### Why re-ordering `Steal` (e.g. CAS-then-read, or CAS-read-CAS) doesn't fix it

Any fix built only out of additional loads/CASes on `top` can only
synchronize `top` itself, not the array memory being read. Moving the CAS
before the read removes the losing thief's read entirely (good), but now
exposes the *winning* thief instead: its read happens after its own
successful CAS store rather than before, so nothing requires the owner's
write to wait until that thief has actually finished reading — the owner
is free to reuse the slot (after the array wraps around again) once it
next observes `top` having advanced, without waiting on the winner's read
to complete. A second CAS/load on `top` after the read doesn't add
anything either, since nothing else can contend for `top` once a thief has
already won that index — such a check would trivially always pass and
says nothing about whether the array slot itself was touched. Making the
array read provably safe requires synchronization tied to the array memory
itself, not more operations on `top` — hence the two real fixes below.

### Fixing it properly (not done here, on purpose)

- **Box elements**: store `atomic.Pointer[T]` per slot instead of `T`
  directly, so `get`/`put` become real atomic operations. Removes the race
  entirely; costs one heap allocation per pushed element.
- **Epoch/hazard-pointer reclamation**: thieves announce which slot/array
  generation they're touching before reading it; the owner defers reusing a
  slot until no thief has it announced. This is what production
  work-stealing deques (e.g. Rust's `crossbeam-deque`, Java's
  `ForkJoinPool`) do. Removes the race without per-element boxing, at the
  cost of meaningfully more machinery.

Neither is implemented here: the allocation cost of boxing defeats a chunk
of the point of this package, and hazard pointers are a lot of complexity
for a learning/benchmarking deque whose results are already provably
correct. If you're adapting this code for production use, pick one of the
above.

### Practical guidance

- `go test ./...` (no `-race`) is unaffected and is the normal way to run
  the suite.
- `go test -race ./...` may intermittently report this specific race
  (`PushBottom` write vs `Steal`/`StealHalf` read in `deque.go`) on the
  `TestCountPrimesParallel_*` tests. That failure is expected and does not
  indicate a regression — it's about the memory model, not the result. Any
  *other* race, or any test asserting a wrong count/value, is a real bug.
