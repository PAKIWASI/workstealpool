package workstealpool

import (
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
)

func BenchmarkLFdeque_PushBottom(b *testing.B) {
	d := NewLFdeque[int](1024)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		d.PushBottom(i)
		d.PopBottom()
	}
}

func BenchmarkLFdeque_PopBottom(b *testing.B) {
	d := NewLFdeque[int](1024)

	for i := 0; i < b.N; i++ {
		d.PushBottom(i)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		d.PushBottom(i)
		d.PopBottom()
	}
}

func BenchmarkLFdeque_Steal(b *testing.B) {
	d := NewLFdeque[int](1024)

	for i := 0; i < b.N+1; i++ {
		d.PushBottom(i)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _ = d.Steal()
		d.PushBottom(i)
	}
}

func BenchmarkLFdeque_StealHalf(b *testing.B) {
	d := NewLFdeque[int](1024)
	scratch := make([]int, 512)

	for i := range 1024 {
		d.PushBottom(i)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		values, ok := d.StealHalf(scratch)

		if !ok {
			for j := range 1024 {
				d.PushBottom(j)
			}
			continue
		}

		for _, v := range values {
			_ = v
		}
	}
}

func BenchmarkLFdeque_ConcurrentSteal(b *testing.B) {
	for _, thieves := range []int{1, 2, 4, 8, 16} {
		b.Run("thieves="+strconv.Itoa(thieves), func(b *testing.B) {
			d := NewLFdeque[int](1024)
			for i := 0; i < b.N+1024; i++ {
				d.PushBottom(i)
			}

			var wg sync.WaitGroup
			b.ResetTimer()

			for range thieves {
				wg.Go(func() {
					for {
						if _, ok := d.Steal(); !ok {
							return
						}
					}
				})
			}

			wg.Wait()
		})
	}
}

func BenchmarkLFdeque_WorkStealing(b *testing.B) {
	for _, thieves := range []int{1, 2, 4, 8, 16} {
		b.Run("thieves="+strconv.Itoa(thieves), func(b *testing.B) {
			d := NewLFdeque[int](8)
			scratch := make([]int, 4)
			for i := 0; i < b.N; i++ {
				d.PushBottom(i)
			}

			var wg sync.WaitGroup
			var stolen atomic.Int64
			done := make(chan struct{})

			b.ResetTimer()

			wg.Go(func() {
				for {
					select {
					case <-done:
						return
					default:
					}
					if _, ok := d.PopBottom(); !ok {
						return
					}
				}
			})

			for range thieves {
				wg.Go(func() {
					for stolen.Load() < int64(b.N) {
						values, ok := d.StealHalf(scratch)
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

			wg.Wait()
			close(done)
		})
	}
}

func BenchmarkLFdeque_StealVsHalf(b *testing.B) {
	for _, size := range []int{16, 64, 256, 1024, 4096} {
		b.Run("Steal/"+strconv.Itoa(size), func(b *testing.B) {
			d := NewLFdeque[int](size)

			for i := range size {
				d.PushBottom(i)
			}

			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				_, _ = d.Steal()

				d.PushBottom(i)
			}
		})

		b.Run("StealHalf/"+strconv.Itoa(size), func(b *testing.B) {
			d := NewLFdeque[int](size)
			scratch := make([]int, size/2)

			for i := range size {
				d.PushBottom(i)
			}

			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				values, ok := d.StealHalf(scratch)

				if !ok {
					d.PushBottom(i)
					continue
				}

				_ = values
			}
		})
	}
}
