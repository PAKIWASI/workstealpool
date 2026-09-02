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
			got, err = countPrimesParallel(context.Background(), 0, 3000, cfg)
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

func TestWorkerPool_TaskError_AbortsAndReports(t *testing.T) {
	wantErr := context.DeadlineExceeded

	task := func(ctx context.Context, workerID int, item int, res chan<- int, spawn func(...int)) error {
		if item == 3 {
			return wantErr
		}
		if item < 10 {
			spawn(item + 1)
		}
		res <- item
		return nil
	}

	p := NewWorkerPool[int, int](context.Background(), 4, 8, 4, task)
	p.Submit(1)

	var results []int
	for r := range p.Run() {
		results = append(results, r)
	}

	err := p.Wait()
	if err == nil {
		t.Fatal("Wait() = nil, want error")
	}
	if err != wantErr {
		t.Fatalf("Wait() = %v, want %v", err, wantErr)
	}
}

func TestWorkerPool_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	started := make(chan struct{})
	task := func(taskCtx context.Context, workerID int, item int, res chan<- int, spawn func(...int)) error {
		select {
		case <-started:
		default:
			close(started)
		}
		// Spawn infinite work to keep pool busy until cancelled
		select {
		case <-taskCtx.Done():
			return nil
		case <-time.After(10 * time.Millisecond):
			spawn(item+1, item+2)
		}
		return nil
	}

	p := NewWorkerPool[int, int](ctx, 4, 8, 4, task)
	p.Submit(1)

	done := make(chan struct{})
	go func() {
		for range p.Run() {
		}
		close(done)
	}()

	<-started
	cancel() // Cancel the parent context while workers are active

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("pool did not shut down within 3s after context cancellation")
	}

	if err := p.Wait(); err != nil {
		t.Fatalf("Wait() = %v, want nil on clean context cancellation", err)
	}
}

func TestWorkerPool_SubmitN(t *testing.T) {
	t.Run("zero_items", func(t *testing.T) {
		task := func(ctx context.Context, workerID int, item int, res chan<- int, spawn func(...int)) error {
			res <- item
			return nil
		}
		p := NewWorkerPool[int, int](context.Background(), 2, 4, 4, task)
		p.SubmitN() // 0 items

		var results []int
		for r := range p.Run() {
			results = append(results, r)
		}
		if err := p.Wait(); err != nil {
			t.Fatalf("Wait() = %v", err)
		}
		if len(results) != 0 {
			t.Fatalf("got %d results, want 0", len(results))
		}
	})

	t.Run("one_item", func(t *testing.T) {
		task := func(ctx context.Context, workerID int, item int, res chan<- int, spawn func(...int)) error {
			res <- item * 2
			return nil
		}
		p := NewWorkerPool[int, int](context.Background(), 2, 4, 4, task)
		p.SubmitN(21)

		var results []int
		for r := range p.Run() {
			results = append(results, r)
		}
		if err := p.Wait(); err != nil {
			t.Fatalf("Wait() = %v", err)
		}
		if len(results) != 1 || results[0] != 42 {
			t.Fatalf("results = %v, want [42]", results)
		}
	})

	t.Run("multiple_items", func(t *testing.T) {
		task := func(ctx context.Context, workerID int, item int, res chan<- int, spawn func(...int)) error {
			res <- item
			return nil
		}
		p := NewWorkerPool[int, int](context.Background(), 4, 8, 16, task)
		p.SubmitN(1, 2, 3, 4, 5)

		sum := 0
		count := 0
		for r := range p.Run() {
			sum += r
			count++
		}
		if err := p.Wait(); err != nil {
			t.Fatalf("Wait() = %v", err)
		}
		if count != 5 || sum != 15 {
			t.Fatalf("count=%d sum=%d, want count=5 sum=15", count, sum)
		}
	})
}

func TestWorkerPool_BatchSpawn(t *testing.T) {
	task := func(ctx context.Context, workerID int, item int, res chan<- int, spawn func(...int)) error {
		if item == 0 {
			// Spawn with 0 items (no-op), and a batch of 4 items
			spawn()
			spawn(1, 2, 3, 4)
			return nil
		}
		res <- item
		return nil
	}

	p := NewWorkerPool[int, int](context.Background(), 4, 8, 16, task)
	p.Submit(0)

	sum := 0
	count := 0
	for r := range p.Run() {
		sum += r
		count++
	}
	if err := p.Wait(); err != nil {
		t.Fatalf("Wait() = %v", err)
	}
	if count != 4 || sum != 10 {
		t.Fatalf("count=%d sum=%d, want count=4 sum=10", count, sum)
	}
}

func TestWorkerPool_UnbufferedResultChannel(t *testing.T) {
	task := func(ctx context.Context, workerID int, item int, res chan<- int, spawn func(...int)) error {
		if item < 20 {
			spawn(item + 1)
		}
		res <- item
		return nil
	}

	// ResultBuffSize = 0 (unbuffered channel)
	p := NewWorkerPool[int, int](context.Background(), 4, 8, 0, task)
	p.Submit(1)

	count := 0
	for range p.Run() {
		count++
	}
	if err := p.Wait(); err != nil {
		t.Fatalf("Wait() = %v", err)
	}
	if count != 20 {
		t.Fatalf("count = %d, want 20", count)
	}
}

