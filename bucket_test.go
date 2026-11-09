package bucket_test

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"math"
	"slices"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/rakunlabs/bucket"
)

func TestBucket(t *testing.T) {
	t.Run("one process count", func(t *testing.T) {
		v := 0

		b := bucket.New[int]()
		b = b.CallBack(func(ctx context.Context, t []int) error {
			for _, i := range t {
				v += i
			}

			return nil
		})

		if err := b.Process(context.Background(), []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}); err != nil {
			t.Fatal(err)
		}

		if v != 55 {
			t.Errorf("got %d, want 55", v)
		}
	})

	t.Run("nil data", func(t *testing.T) {
		v := 0
		b := bucket.New[int]().CallBack(func(ctx context.Context, t []int) error {
			for _, i := range t {
				v += i
			}

			return nil
		})

		if err := b.Process(context.Background(), nil); err != nil {
			t.Fatal(err)
		}

		if v != 0 {
			t.Errorf("got %d, want 0", v)
		}
	})

	t.Run("return error multi", func(t *testing.T) {
		v := int64(0)
		b := bucket.New[int](bucket.WithProcessCount(10)).CallBack(func(ctx context.Context, t []int) error {
			for _, i := range t {
				if i == 3 {
					return fmt.Errorf("some error")
				}

				atomic.AddInt64(&v, int64(i))
			}

			return nil
		})

		if err := b.Process(context.Background(), []int{1, 2, 3, 4, 5}); err == nil {
			t.Fatal("want error, got nil")
		} else {
			if err.Error() != "some error" {
				t.Errorf("got %s, want some error", err.Error())
			}
		}
	})

	t.Run("return error", func(t *testing.T) {
		v := 0
		b := bucket.New[int]().CallBack(func(ctx context.Context, t []int) error {
			for _, i := range t {
				if i == 3 {
					return fmt.Errorf("some error")
				}

				v += i
			}

			return nil
		})

		if err := b.Process(context.Background(), []int{1, 2, 3, 4, 5}); err == nil {
			t.Fatal("want error, got nil")
		} else {
			if err.Error() != "some error" {
				t.Errorf("got %s, want some error", err.Error())
			}
		}
	})

	t.Run("multi", func(t *testing.T) {
		v := int64(0)
		b := bucket.New[int](bucket.WithProcessCount(4), bucket.WithMinSize(20), bucket.WithMaxSize(100)).CallBack(func(ctx context.Context, t []int) error {
			for _, i := range t {
				atomic.AddInt64(&v, int64(i))
			}

			return nil
		})

		data := make([]int, 9453)
		total := 0
		for i := 0; i < len(data); i++ {
			data[i] = i + 1
			total += i + 1
		}

		if err := b.Process(context.Background(), data); err != nil {
			t.Fatal(err)
		}

		if v != int64(total) {
			t.Errorf("got %d, want %d", v, total)
		}
	})

	t.Run("nil callback", func(t *testing.T) {
		err := bucket.New[int]().Process(context.Background(), []int{1, 2, 3})
		if !errors.Is(err, bucket.ErrNoCallback) {
			t.Fatalf("got %v, want %v", err, bucket.ErrNoCallback)
		}
	})

	t.Run("callback does not modify original", func(t *testing.T) {
		b := bucket.New[int]()
		_ = b.CallBack(func(context.Context, []int) error { return nil })

		if err := b.Process(context.Background(), []int{1}); !errors.Is(err, bucket.ErrNoCallback) {
			t.Fatalf("got %v, want %v", err, bucket.ErrNoCallback)
		}
	})

	t.Run("index results without lock", func(t *testing.T) {
		data := make([]int, 1000)
		for i := range data {
			data[i] = i
		}

		results := make([]int, len(data))

		b := bucket.New[int](bucket.WithProcessCount(8), bucket.WithMaxSize(7)).CallBack(func(_ context.Context, index int, chunk []int) error {
			for j, v := range chunk {
				results[index+j] = v * 2
			}

			return nil
		})

		if err := b.Process(context.Background(), data); err != nil {
			t.Fatal(err)
		}

		for i, v := range results {
			if v != i*2 {
				t.Fatalf("results[%d] = %d, want %d", i, v, i*2)
			}
		}
	})

	t.Run("nil index callback", func(t *testing.T) {
		var fn func(context.Context, int, []int) error

		if err := bucket.New[int]().CallBack(fn).Process(context.Background(), []int{1}); !errors.Is(err, bucket.ErrNoCallback) {
			t.Fatalf("got %v, want %v", err, bucket.ErrNoCallback)
		}
	})

	t.Run("nil plain callback", func(t *testing.T) {
		var fn func(context.Context, []int) error

		if err := bucket.New[int]().CallBack(fn).Process(context.Background(), []int{1}); !errors.Is(err, bucket.ErrNoCallback) {
			t.Fatalf("got %v, want %v", err, bucket.ErrNoCallback)
		}
	})

	t.Run("index and chunks", func(t *testing.T) {
		for _, processCount := range []int{1, 3} {
			var mu sync.Mutex
			got := map[int][]int{}

			b := bucket.New[int](bucket.WithProcessCount(processCount), bucket.WithMaxSize(4)).CallBack(func(_ context.Context, index int, data []int) error {
				mu.Lock()
				defer mu.Unlock()

				got[index] = slices.Clone(data)

				return nil
			})

			if err := b.Process(context.Background(), []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}); err != nil {
				t.Fatal(err)
			}

			want := map[int][]int{0: {0, 1, 2, 3}, 4: {4, 5, 6, 7}, 8: {8, 9}}
			if !maps.EqualFunc(got, want, slices.Equal[[]int]) {
				t.Errorf("processCount %d: got %v, want %v", processCount, got, want)
			}
		}
	})

	t.Run("cancelled context", func(t *testing.T) {
		for _, processCount := range []int{1, 4} {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()

			var calls atomic.Int64
			b := bucket.New[int](bucket.WithProcessCount(processCount), bucket.WithMaxSize(1)).CallBack(func(context.Context, []int) error {
				calls.Add(1)

				return nil
			})

			if err := b.Process(ctx, []int{1, 2, 3, 4, 5, 6, 7, 8}); !errors.Is(err, context.Canceled) {
				t.Fatalf("processCount %d: got %v, want %v", processCount, err, context.Canceled)
			}

			if c := calls.Load(); c != 0 {
				t.Errorf("processCount %d: callback called %d times, want 0", processCount, c)
			}
		}
	})

	t.Run("append does not overwrite next bucket", func(t *testing.T) {
		for _, processCount := range []int{1, 2} {
			data := []int{1, 2, 3, 4}
			b := bucket.New[int](bucket.WithProcessCount(processCount), bucket.WithMaxSize(2)).CallBack(func(_ context.Context, c []int) error {
				if cap(c) != len(c) {
					return fmt.Errorf("cap %d, len %d", cap(c), len(c))
				}

				_ = append(c, 99)

				return nil
			})

			if err := b.Process(context.Background(), data); err != nil {
				t.Fatal(err)
			}

			if !slices.Equal(data, []int{1, 2, 3, 4}) {
				t.Errorf("processCount %d: data modified %v", processCount, data)
			}
		}
	})

	t.Run("huge process count", func(t *testing.T) {
		var v atomic.Int64
		b := bucket.New[int](bucket.WithProcessCount(math.MaxInt)).CallBack(func(_ context.Context, c []int) error {
			for _, i := range c {
				v.Add(int64(i))
			}

			return nil
		})

		if err := b.Process(context.Background(), []int{1, 2, 3}); err != nil {
			t.Fatal(err)
		}

		if v.Load() != 6 {
			t.Errorf("got %d, want 6", v.Load())
		}
	})

	t.Run("stop after error", func(t *testing.T) {
		var calls atomic.Int64
		errStop := errors.New("stop")

		b := bucket.New[int](bucket.WithProcessCount(2), bucket.WithMaxSize(1)).CallBack(func(context.Context, []int) error {
			calls.Add(1)

			return errStop
		})

		data := make([]int, 1000)
		if err := b.Process(context.Background(), data); !errors.Is(err, errStop) {
			t.Fatalf("got %v, want %v", err, errStop)
		}

		if c := calls.Load(); c >= int64(len(data)) {
			t.Errorf("callback called %d times, expected early stop", c)
		}
	})
}
