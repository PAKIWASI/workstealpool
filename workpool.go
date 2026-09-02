// Package workstealpool implements concurrent work stealing for worker pools.
//
// Work-stealing pools exist for a specific shape of problem: recursive divide-and-conquer.
// The classic example is parallel quicksort or a parallel tree walk.
// You don't know the full list of work upfront. Each piece of work, when you look at it, discovers more work.
//
// A worker pool consists of multiple worker goroutines. Each worker owns a
// lock-free deque. Workers execute their own work from the bottom of the
// deque and steal work from the top of other workers' deques when they run out of local work.
package workstealpool

import (
	"context"
	"math/rand/v2"
	"runtime"
	"sync/atomic"

	"golang.org/x/sync/errgroup"
)

const maxStealAttempts = 50

// Worker owns a local work-stealing deque.
//
// The worker's normal path is to pop work from the bottom of its deque and
// push newly spawned work onto the bottom. This keeps the common path local
// to the worker.
//
// Worker does not know about other workers. The WorkerPool coordinates
// stealing between workers.
type Worker[T any] struct {
	// this has to be ptr as you can't copy the internal atomic ints
	deque *LFdeque[T]
	// per worker scratch space, passed to StealHalf
	scratch []T
}

func newWorker[T any](capacity int) Worker[T] {
	return Worker[T]{
		deque:   NewLFdeque[T](capacity),
		scratch: make([]T, capacity/2),
	}
}

// Task is the unit of work a WorkerPool executes.
//
// ctx should be checked by long-running tasks that want to be interruptible.
// spawn schedules child items of work onto the calling worker's local deque.
// It must only be called synchronously, from within this Task invocation.
//
// res is the write-only results channel where leaf results can be emitted.
//
// Returning a non-nil error aborts the pool and records it as the terminal error.
// T is the input type and R is the result type.
type Task[T, R any] func(ctx context.Context, workerId int, item T, res chan<- R, spawn func(...T)) error

// WorkerPool manages a collection of workers and schedules work between them.
//
// The pool does not care what T represents. It only moves T between worker
// deques. execute defines how a worker executes a T.
//
// R is the result type expected from each execute call of the worker
type WorkerPool[T, R any] struct {
	workers []Worker[T]
	execute Task[T, R]

	// every parked worker wakes up when this channel is closed
	wakeup atomic.Pointer[chan struct{}]
	// how many workers are currently parked
	parked atomic.Int32

	ctx    context.Context
	cancel context.CancelFunc

	// pending counts items that have been submitted or spawned but not yet
	// finished executing. It's what tells the pool "there is no more work
	// anywhere". The task that decrements it to zero calls cancel itself,
	// so there's no separate done-channel or monitor goroutine watching it
	pending atomic.Int64

	// eg owns the fixed set of worker goroutines. The first error wins and cancels,
	// and eg.Wait() tells us when every worker has actually returned
	// which is the only safe moment to close results, since a worker can't be mid-send after that
	eg *errgroup.Group

	results chan R
}

// NewWorkerPool creates a pool of poolSize workers, each with its own deque
// of initial capacity initialWorkerCap.
//
// execute defines the work each worker performs for a given item. See Task
// for the contract around ctx, spawn, and error handling.
//
// The pool does not start running until Submit is called and workers begin
// pulling from their deques; there is no separate "Start" step, workers
// run as soon as they're constructed, watching ctx and their deques.
func NewWorkerPool[T, R any](
	ctx context.Context,
	poolSize, initialWorkerCap, resultBuffSize int,
	execute Task[T, R],
) *WorkerPool[T, R] {
	// derive a cancellable context from the user's
	ctx, cancel := context.WithCancel(ctx)

	workers := make([]Worker[T], poolSize)
	for i := range poolSize {
		workers[i] = newWorker[T](initialWorkerCap)
	}

	p := &WorkerPool[T, R]{
		workers: workers,
		execute: execute,
		ctx:     ctx,
		cancel:  cancel,
		results: make(chan R, resultBuffSize),
	}

	ch := make(chan struct{})
	p.wakeup.Store(&ch)

	return p
}

// Submit adds initial work to the pool. Call before Run.
// Submit does not itself start any workers.
func (p *WorkerPool[T, R]) Submit(item T) {
	p.pending.Add(1)
	p.workers[0].deque.PushBottom(item)
}

// SubmitN adds multiple initial work items to the pool. Call before Run.
func (p *WorkerPool[T, R]) SubmitN(items ...T) {
	n := len(items)
	if n == 0 {
		p.cancel()
		return
	}
	if n == 1 {
		p.pending.Add(1)
		p.workers[0].deque.PushBottom(items[0])
		return
	}
	p.pending.Add(int64(n))
	p.workers[0].deque.PushSliceBottom(items)
}

// Run: Result channel generator. Starts all workers and returns the results
// channel. The channel closes once every worker has exited,
// either because there's no work left anywhere or because a task returned an error.
//
// Call Wait afterward (or concurrently, while draining results in another
// goroutine) to get the terminal error, if any.
func (p *WorkerPool[T, R]) Run() <-chan R {
	p.eg, p.ctx = errgroup.WithContext(p.ctx)

	// if any worker returns a non-nil error, errgroup cancels this new p.ctx
	// automatically and remembers that error as the one Wait() will report.
	for i := range p.workers {
		// launch all workers as errgroup goroutines
		p.eg.Go(func() error {
			return p.runWorker(i)
		})
	}

	go func() {
		p.eg.Wait()      // blocks until all worker goroutines return
		close(p.results) // only then can we close the results channel
	}()

	return p.results
}

// Wait blocks until every worker has exited and returns the first error
// encountered (nil on normal completion). Safe to call while another
// goroutine drains the results channel returned by Run,
// since results only closes once workers have exited too.
func (p *WorkerPool[T, R]) Wait() error {
	return p.eg.Wait() // errgroup.Wait is safe to call more than once
}

// runWorker implements the worker loop: get LIFO work from its own deque.
// if you run out, steal half from another worker, FIFO.
func (p *WorkerPool[T, R]) runWorker(idx int) error {
	w := p.workers[idx]

	spawn := func(child ...T) {
		n := len(child)
		if n == 0 {
			return
		}
		if n == 1 {
			p.pending.Add(1) // before push: must be visible before any thief can see the child
			w.deque.PushBottom(child[0])
		} else {
			p.pending.Add(int64(n))
			w.deque.PushSliceBottom(child)
		}
		p.broadcastWakeup()
	}

	spins := 0
	for {
		select {
		case <-p.ctx.Done():
			return nil // shutdown, not this worker's own failure
		default:
		}

		item, ok := w.deque.PopBottom()
		if ok {
			spins = 0
			if err := p.runTask(spawn, idx, item); err != nil {
				return err // some err occurred while running the current task, abort
			}
			continue // task done, continue with the loop
		}

		if p.StealHalf(idx) {
			spins = 0
			continue // steal succeeded, continue with the loop
		}

		// steal failed, spin block for some loops then park

		spins++
		if spins < maxStealAttempts {
			// put this goroutine at the back of the queue but don't suspend it
			runtime.Gosched()
			continue // still within budget
		}

		// Exhausted the spin budget: actually block until woken.
		p.parkUntilWork()
		spins = 0
	}
}

// parkUntilWork blocks the worker with id `idx` and adds it to a waiting list.
// whenever more work is added by any worker, all workers on the list are awakened
func (p *WorkerPool[T, R]) parkUntilWork() {
	p.parked.Add(1)
	defer p.parked.Add(-1)
	ch := *p.wakeup.Load()
	select {
	case <-p.ctx.Done():
	case <-ch:
	}
}

func (p *WorkerPool[T, R]) broadcastWakeup() {
	if p.parked.Load() == 0 {
		return
	}
	newCh := make(chan struct{})
	old := p.wakeup.Swap(&newCh)
	close(*old)
}

// runTask implements the actual task execution of a worker.
func (p *WorkerPool[T, R]) runTask(spawn func(...T), idx int, item T) error {

	// Task func takes the results chan as write only channel. long running tasks should
	// also check ctx.Done to stop. short tasks (few ms) don't need to do this as each worker's
	// goroutine already checks for Done each loop before calling runTask
	err := p.execute(p.ctx, idx, item, p.results, spawn)
	if err != nil {
		return err // errgroup records it and cancels egCtx for every worker
	}

	if p.pending.Add(-1) == 0 {
		p.cancel() // last piece of work finished anywhere in the pool
	}
	return nil
}

// StealHalf attempts to steal work for the given worker from a randomly chosen
// victim among the other workers in the pool. It tries up to len(workers)-1
// distinct victims before giving up.
func (p *WorkerPool[T, R]) StealHalf(thiefIdx int) (ok bool) {
	thief := p.workers[thiefIdx]
	if n := len(p.workers); n > 1 {
		// Random start index, then scan forward so we don't retry the same
		// victim twice and don't bias toward low-index workers.
		start := rand.IntN(n)

		for i := range n {
			idx := (start + i) % n
			if idx == thiefIdx {
				continue
			}
			v, okk := p.workers[idx].deque.StealHalf(thief.scratch)
			if okk {
				thief.deque.PushSliceBottom(v)
				p.broadcastWakeup()
				return true
			}
		}
	}
	return false
}
