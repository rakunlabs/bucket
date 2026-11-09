package bucket_test

import (
	"context"
	"fmt"
	"runtime"
	"testing"

	"github.com/rakunlabs/bucket"
)

func noop(context.Context, []int) error { return nil }

func noopIndex(context.Context, int, []int) error { return nil }

func BenchmarkProcess(b *testing.B) {
	cases := []struct {
		items, process, maxSize int
	}{
		{items: 1_000, process: 1},
		{items: 1_000, process: 1, maxSize: 1},
		{items: 100_000, process: 1, maxSize: 100},
		{items: 100_000, process: runtime.GOMAXPROCS(0)},
		{items: 100_000, process: runtime.GOMAXPROCS(0), maxSize: 100},
		{items: 100_000, process: runtime.GOMAXPROCS(0), maxSize: 1},
	}

	for _, c := range cases {
		data := make([]int, c.items)
		base := bucket.New[int](bucket.WithProcessCount(c.process), bucket.WithMaxSize(c.maxSize))

		for name, bk := range map[string]*bucket.Bucket[int]{
			"plain": base.CallBack(noop),
			"index": base.CallBack(noopIndex),
		} {
			b.Run(fmt.Sprintf("%s/items=%d/process=%d/max=%d", name, c.items, c.process, c.maxSize), func(b *testing.B) {
				b.ReportAllocs()

				for b.Loop() {
					if err := bk.Process(context.Background(), data); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

func BenchmarkCallBack(b *testing.B) {
	base := bucket.New[int]()

	b.Run("plain", func(b *testing.B) {
		b.ReportAllocs()

		for b.Loop() {
			_ = base.CallBack(noop)
		}
	})

	b.Run("index", func(b *testing.B) {
		b.ReportAllocs()

		for b.Loop() {
			_ = base.CallBack(noopIndex)
		}
	})
}
