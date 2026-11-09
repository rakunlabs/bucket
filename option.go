package bucket

type option struct {
	ProcessCount int
	MaxSize      int
	MinSize      int
}

type Option func(o *option)

// apply builds the option from opts and normalizes invalid values.
func apply(opts []Option) option {
	var o option
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}

	if o.MinSize <= 0 {
		o.MinSize = 1
	}

	if o.ProcessCount <= 0 {
		o.ProcessCount = 1
	}

	return o
}

// WithProcessCount is an option to divide the data into several parts and process them concurrently.
//   - Sets the maximum number of processes that will be run concurrently.
//   - Default is 1, which means the data will be processed sequentially.
//   - If ProcessCount is less than or equal to 0, it will be set to 1.
func WithProcessCount(processCount int) Option {
	return func(o *option) {
		o.ProcessCount = processCount
	}
}

// WithMaxSize is an option to limit the maximum size of the bucket.
//   - Bucket size automatically calculated by len(data) / ProcessCount.
//   - Default no limit which is 0 or less than 0.
func WithMaxSize(maxSize int) Option {
	return func(o *option) {
		o.MaxSize = maxSize
	}
}

// WithMinSize is an option to set the minimum size of the bucket.
//   - If the bucket size is less than MinSize, it will be set to MinSize.
//   - MinSize takes precedence over MaxSize when MinSize > MaxSize.
//   - Default is 1, values less than or equal to 0 will be set to 1.
func WithMinSize(minSize int) Option {
	return func(o *option) {
		o.MinSize = minSize
	}
}
