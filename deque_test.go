package workstealpool

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
)

func TestLFdeque_BasicLIFO(t *testing.T) {
	d := NewLFdeque[int](8)

	for i := range 5 {
		d.PushBottom(i)
	}

	if got := d.Len(); got != 5 {
		t.Fatalf("Len() = %d, want 5", got)
	}

	for want := 4; want >= 0; want-- {
		got, ok := d.PopBottom()
		if !ok {
			t.Fatalf("PopBottom() failed, want %d", want)
		}
		if got != want {
			t.Fatalf("PopBottom() = %d, want %d", got, want)
		}
	}

	if _, ok := d.PopBottom(); ok {
		t.Fatal("PopBottom() succeeded on empty deque")
	}

	if got := d.Len(); got != 0 {
		t.Fatalf("Len() = %d after draining, want 0", got)
	}
}

func TestLFdeque_StealFIFO(t *testing.T) {
	d := NewLFdeque[int](8)

	for i := range 8 {
		d.PushBottom(i)
	}

	for want := range 8 {
		got, ok := d.Steal()
		if !ok {
			t.Fatalf("Steal() failed, want %d", want)
		}
		if got != want {
			t.Fatalf("Steal() = %d, want %d", got, want)
		}
	}

	if _, ok := d.Steal(); ok {
		t.Fatal("Steal() succeeded on empty deque")
	}
}

func TestLFdeque_OwnerAndThiefEnds(t *testing.T) {
	d := NewLFdeque[int](8)

	for i := range 10 {
		d.PushBottom(i)
	}

	got, ok := d.Steal()
	if !ok || got != 0 {
		t.Fatalf("first Steal() = %d, %v, want 0, true", got, ok)
	}

	got, ok = d.PopBottom()
	if !ok || got != 9 {
		t.Fatalf("first PopBottom() = %d, %v, want 9, true", got, ok)
	}

	got, ok = d.Steal()
	if !ok || got != 1 {
		t.Fatalf("second Steal() = %d, %v, want 1, true", got, ok)
	}

	got, ok = d.PopBottom()
	if !ok || got != 8 {
		t.Fatalf("second PopBottom() = %d, %v, want 8, true", got, ok)
	}
}

func TestLFdeque_Resize(t *testing.T) {
	d := NewLFdeque[int](2)

	const n = 10_000

	for i := range n {
		d.PushBottom(i)
	}

	if got := d.Len(); got != n {
		t.Fatalf("Len() = %d, want %d", got, n)
	}

	for want := n - 1; want >= 0; want-- {
		got, ok := d.PopBottom()
		if !ok {
			t.Fatalf("PopBottom() failed at %d", want)
		}
		if got != want {
			t.Fatalf("PopBottom() = %d, want %d", got, want)
		}
	}
}

func TestLFdeque_StealHalfExactSizes(t *testing.T) {
	tests := []struct {
		size int
		want int
	}{
		{0, 0},
		{1, 0},
		{2, 1},
		{3, 1},
		{4, 2},
		{5, 2},
		{6, 3},
		{7, 3},
		{8, 4},
		{9, 4},
		{10, 5},
		{100, 50},
	}

	for _, tc := range tests {
		t.Run(string(rune(tc.size)), func(t *testing.T) {
			d := NewLFdeque[int](8)

			for i := 0; i < tc.size; i++ {
				d.PushBottom(i)
			}

			got, ok := d.StealHalf()
			read := len(got)

			if tc.want == 0 {
				if ok || read != 0 || got != nil {
					t.Fatalf(
						"StealHalf() = (%v, %d, %v), want (nil, 0, false)",
						got, read, ok,
					)
				}
				return
			}

			if !ok {
				t.Fatal("StealHalf() failed")
			}

			if read != tc.want {
				t.Fatalf("read = %d, want %d", read, tc.want)
			}

			if len(got) != tc.want {
				t.Fatalf("len(result) = %d, want %d", len(got), tc.want)
			}

			for i, v := range got {
				if v != i {
					t.Fatalf("result[%d] = %d, want %d", i, v, i)
				}
			}

			if d.Len() != int64(tc.size-tc.want) {
				t.Fatalf(
					"Len() after StealHalf = %d, want %d",
					d.Len(),
					tc.size-tc.want,
				)
			}
		})
	}
}

func TestLFdeque_StealHalfMultipleBatches(t *testing.T) {
	d := NewLFdeque[int](8)

	const n = 1024

	for i := range n {
		d.PushBottom(i)
	}

	var got []int

	for {
		batch, ok := d.StealHalf()
		read := len(batch)
		if !ok {
			break
		}

		if read != len(batch) {
			t.Fatalf("read = %d, len(batch) = %d", read, len(batch))
		}

		got = append(got, batch...)
	}

	if len(got) != n-1 {
		t.Fatalf(
			"stole %d items, want %d",
			len(got),
			n-1,
		)
	}

	// One item remains because StealHalf refuses to steal when size <= 1.
	v, ok := d.Steal()
	if !ok {
		t.Fatal("final Steal() failed")
	}

	if v < 0 || v >= n {
		t.Fatalf("invalid final value %d", v)
	}
}

func TestLFdeque_StealHalfConcurrentThieves(t *testing.T) {
	const (
		initial = 100_000
		thieves = 8
	)

	d := NewLFdeque[int](8)

	for i := range initial {
		d.PushBottom(i)
	}

	var delivered atomic.Int64
	var duplicates atomic.Int64

	seen := make([]atomic.Bool, initial)

	var wg sync.WaitGroup

	for range thieves {

		wg.Go(func() {

			for {
				values, ok := d.StealHalf()
				if !ok {
					return
				}

				for _, v := range values {
					if v < 0 || v >= initial {
						t.Errorf("invalid value %d", v)
						return
					}

					if seen[v].Swap(true) {
						duplicates.Add(1)
					}

					delivered.Add(1)
				}
			}
		})
	}

	wg.Wait()

	// StealHalf leaves one item behind.
	v, ok := d.Steal()
	if ok {
		if seen[v].Swap(true) {
			duplicates.Add(1)
		}
		delivered.Add(1)
	}

	if got := delivered.Load(); got != initial {
		t.Fatalf("delivered %d items, want %d", got, initial)
	}

	if got := duplicates.Load(); got != 0 {
		t.Fatalf("detected %d duplicate deliveries", got)
	}
}

func TestLFdeque_ConcurrentOwnerAndThieves(t *testing.T) {
	const (
		initial = 20_000
		thieves = 4
	)

	d := NewLFdeque[int](8)

	for i := range initial {
		d.PushBottom(i)
	}

	var nextID atomic.Int64
	nextID.Store(initial)

	seen := make([]atomic.Bool, initial+initial)

	var pushed atomic.Int64
	var delivered atomic.Int64
	var duplicates atomic.Int64

	pushed.Store(initial)

	record := func(v int) {
		if v < 0 || v >= len(seen) {
			t.Errorf("invalid value %d", v)
			return
		}

		if seen[v].Swap(true) {
			duplicates.Add(1)
		}

		delivered.Add(1)
	}

	var wg sync.WaitGroup

	for range thieves {

		wg.Go(func() {

			for {
				values, ok := d.StealHalf()
				if !ok {
					if d.Len() == 0 {
						return
					}

					runtime.Gosched()
					continue
				}

				for _, v := range values {
					record(v)
				}
			}
		})
	}

	wg.Go(func() {

		pops := 0

		for {
			v, ok := d.PopBottom()

			if !ok {
				if d.Len() == 0 {
					return
				}

				runtime.Gosched()
				continue
			}

			record(v)
			pops++

			if pops%4 == 0 {
				id := int(nextID.Add(1) - 1)

				d.PushBottom(id)
				pushed.Add(1)
			}
		}
	})

	wg.Wait()

	if got := duplicates.Load(); got != 0 {
		t.Fatalf("duplicate deliveries = %d", got)
	}

	if got := delivered.Load(); got != pushed.Load() {
		t.Fatalf(
			"delivered = %d, pushed = %d",
			got,
			pushed.Load(),
		)
	}
}

func TestLFdeque_WorkloadRatios(t *testing.T) {
	tests := []struct {
		name    string
		thieves int
	}{
		{"owner_only", 0},
		{"one_thief", 1},
		{"two_thieves", 2},
		{"four_thieves", 4},
		{"eight_thieves", 8},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			const initial = 10_000

			d := NewLFdeque[int](8)

			for i := range initial {
				d.PushBottom(i)
			}

			var owner atomic.Int64
			var stolen atomic.Int64

			var wg sync.WaitGroup

			for i := 0; i < tc.thieves; i++ {

				wg.Go(func() {

					for {
						values, ok := d.StealHalf()
						if !ok {
							if d.Len() == 0 {
								return
							}

							runtime.Gosched()
							continue
						}

						stolen.Add(int64(len(values)))
					}
				})
			}

			wg.Go(func() {

				for {
					_, ok := d.PopBottom()
					if !ok {
						if d.Len() == 0 {
							return
						}

						runtime.Gosched()
						continue
					}

					owner.Add(1)
				}
			})

			wg.Wait()

			t.Logf(
				"owner=%d stolen=%d total=%d",
				owner.Load(),
				stolen.Load(),
				owner.Load()+stolen.Load(),
			)
		})
	}
}

// TestLFdeque_StealHalf_NoOverlapUnderOwnerContention is a regression test
// for a bug in an earlier StealHalf implementation: it computed
// half = (bottom-top)/2 from a non-atomic snapshot of top/bottom, then
// tried to claim the whole range with a single CompareAndSwap on top
// alone. That's unsound, because PopBottom's fast path (size > 0) commits
// bottom decrements without ever touching top, so a thief that reads a
// wide, slightly-stale gap and stalls before its CAS can have that CAS
// succeed - top never moved - even though the owner has, in the
// meantime, already popped and delivered several of the indices inside
// the claimed range through the untouched fast path. Both sides then
// deliver the same element.
//
// This races an aggressively draining owner against a single StealHalf
// call, many times over varied interleavings, and fails if any value is
// ever delivered to both sides.
func TestLFdeque_StealHalf_NoOverlapUnderOwnerContention(t *testing.T) {
	const (
		trials = 500
		n      = 200
	)

	for trial := range trials {
		d := NewLFdeque[int](64)
		for i := range n {
			d.PushBottom(i)
		}

		seen := make([]atomic.Bool, n)
		var dup atomic.Int64

		var wg sync.WaitGroup
		wg.Go(func() {
			vals, ok := d.StealHalf()
			if !ok {
				return
			}
			for _, v := range vals {
				if seen[v].Swap(true) {
					dup.Add(1)
				}
			}
		})

		// Vary the interleaving a little across trials instead of
		// always racing the same way.
		if trial%3 == 0 {
			runtime.Gosched()
		}

		for range n {
			v, ok := d.PopBottom()
			if !ok {
				break
			}
			if seen[v].Swap(true) {
				dup.Add(1)
			}
		}

		wg.Wait()

		if got := dup.Load(); got > 0 {
			t.Fatalf("trial %d: %d value(s) delivered to both owner and thief", trial, got)
		}
	}
}

func TestLFdeque_PushSliceBottom(t *testing.T) {
	tests := []struct {
		name string // description of this test case
		// Named input parameters for target function.
		v int
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// TODO: construct the receiver type.
			//var d LFdeque[int]
			//d.PushSliceBottom()
		})
	}
}
