# bucket 🪣

[![License](https://img.shields.io/github/license/rakunlabs/bucket?color=red&style=flat-square)](https://raw.githubusercontent.com/rakunlabs/bucket/main/LICENSE)
[![Coverage](https://img.shields.io/sonar/coverage/rakunlabs_bucket?logo=sonarcloud&server=https%3A%2F%2Fsonarcloud.io&style=flat-square)](https://sonarcloud.io/summary/overall?id=rakunlabs_bucket)
[![GitHub Workflow Status](https://img.shields.io/github/actions/workflow/status/rakunlabs/bucket/test.yml?branch=main&logo=github&style=flat-square&label=ci)](https://github.com/rakunlabs/bucket/actions)
[![Go PKG](https://raw.githubusercontent.com/rakunlabs/.github/main/assets/badges/gopkg.svg)](https://pkg.go.dev/github.com/rakunlabs/bucket)

Bucket calls a function with a limited number of goroutines and data count.  
Size of each bucket is calculated by `len(data) / processCount` with minimum and maximum size configurations.

```sh
go get github.com/rakunlabs/bucket
```

> This package is based on [golang.org/x/sync/errgroup](https://pkg.go.dev/golang.org/x/sync/errgroup).

## Usage

Bucket accepts a function with signature `func(context.Context, []T) error`  
or `func(context.Context, int, []T) error` to also get the begin index of the bucket in the original data.

```go
processBucket := bucket.New[int](
    bucket.WithProcessCount(4),
    bucket.WithMinSize(2),
    bucket.WithMaxSize(100),
)

processBucket = processBucket.CallBack(func(ctx context.Context, data []int) error {
    fmt.Println("process", data)
    return nil
})

// 10 items -> 10/4 -> 3 items per bucket, 3,3,3,1 will be processed in 4 goroutines
data := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}

// processBucket returns error of process function
if err := processBucket.Process(context.Background(), data); err != nil {
    return err
}
```

Use the index signature to write results without a lock, each bucket only touches its own range:

```go
results := make([]int, len(data))

processBucket = processBucket.CallBack(func(ctx context.Context, index int, chunk []int) error {
    for i, v := range chunk {
        results[index+i] = v * v
    }

    return nil
})
```

`Process` returns `bucket.ErrNoCallback` if no callback is set and stops scheduling new buckets after the first error or context cancellation.
