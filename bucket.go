// Package bucket splits a slice into buckets and calls a function for each
// bucket, sequentially or concurrently with a limited number of goroutines.
package bucket

import (
	"context"
	"errors"
	"sync/atomic"

	"golang.org/x/sync/errgroup"
)

// ErrNoCallback is returned by Process when no callback is set.
var ErrNoCallback = errors.New("bucket: callback is not set")

// Bucket holds the bucketing configuration and the callback.
// It is immutable after creation and safe for concurrent use.
type Bucket[T any] struct {
	// callback is the function that will be called for each bucket with its begin index.
	callback func(context.Context, int, []T) error
	// size return bucket size with the given total item.
	size         func(int) int
	processCount int
}

// New creates a new Bucket with the given options.
// Set the callback with CallBack before calling Process.
func New[T any](opts ...Option) *Bucket[T] {
	o := apply(opts)

	return &Bucket[T]{
		size:         bucketSizer(o),
		processCount: o.ProcessCount,
	}
}

// Callback is the accepted callback signatures of CallBack.
//   - func(ctx, data) for processing only the bucket data.
//   - func(ctx, index, data) also receives the begin index of the bucket in the original data.
type Callback[T any] interface {
	func(context.Context, []T) error | func(context.Context, int, []T) error
}

// CallBack returns a copy of the bucket with the given callback.
// The original bucket is not modified.
func (b Bucket[T]) CallBack[F Callback[T]](callback F) *Bucket[T] {
	b.callback = toIndexCallback[T](callback)

	return &b
}

func toIndexCallback[T any, F Callback[T]](callback F) func(context.Context, int, []T) error {
	switch f := any(callback).(type) {
	case func(context.Context, int, []T) error:
		return f
	case func(context.Context, []T) error:
		if f == nil {
			return nil
		}

		return func(ctx context.Context, _ int, data []T) error {
			return f(ctx, data)
		}
	}

	// unreachable, F is limited to the Callback type set
	return nil
}

// Process will process the data concurrently based on the ProcessCount.
//   - If the data is empty, it will return nil.
//   - If the callback is not set, it will return ErrNoCallback.
//   - If the ProcessCount is 1, it will process the data sequentially.
//   - If the ProcessCount is more than 1, it will process the data concurrently.
//   - The bucket size will be calculated by len(data) / ProcessCount, if not divisible, it will be rounded up. 10/3 -> 4 that means 4,4,2 buckets.
//   - If the bucket size is less than MinSize, it will be set to MinSize.
//   - If the bucket size is more than MaxSize, it will be set to MaxSize.
//   - Each bucket is a sub-slice of data with capped capacity, appending to it does not affect other buckets.
//   - The function will return an error if any of the bucket processing returns an error,
//     remaining buckets are not started after the first error or context cancellation.
func (b *Bucket[T]) Process(ctx context.Context, data []T) error {
	if len(data) == 0 {
		return nil
	}

	if b.callback == nil {
		return ErrNoCallback
	}

	bucketSize := b.size(len(data))

	if b.processCount == 1 || len(data) <= bucketSize {
		for i := 0; i < len(data); i += bucketSize {
			if err := ctx.Err(); err != nil {
				return err
			}

			if err := b.callback(ctx, i, chunk(data, i, bucketSize)); err != nil {
				return err
			}
		}

		return nil
	}

	bucketCount := (len(data) + bucketSize - 1) / bucketSize
	workers := min(b.processCount, bucketCount)

	g, gCtx := errgroup.WithContext(ctx)

	// fixed workers pull bucket numbers instead of a goroutine per bucket
	var next atomic.Int64

	for range workers {
		g.Go(func() error {
			for {
				// stop taking new buckets after an error or cancellation
				if gCtx.Err() != nil {
					return nil
				}

				n := int(next.Add(1) - 1)
				if n >= bucketCount {
					return nil
				}

				i := n * bucketSize
				if err := b.callback(gCtx, i, chunk(data, i, bucketSize)); err != nil {
					return err
				}
			}
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}

	// parent context cancelled before all buckets were processed
	return ctx.Err()
}

// chunk returns data[i:i+size] with capacity limited to its length.
func chunk[T any](data []T, i, size int) []T {
	end := i + min(size, len(data)-i)

	return data[i:end:end]
}
