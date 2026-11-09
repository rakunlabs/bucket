package bucket

// bucketSizer returns a function that calculates bucket size for the given total item.
// Option must be normalized with apply.
func bucketSizer(s option) func(totalItem int) int {
	return func(totalItem int) int {
		// ceil division without overflow
		bucketSize := totalItem / s.ProcessCount
		if totalItem%s.ProcessCount != 0 {
			bucketSize++
		}

		if s.MaxSize > 0 {
			bucketSize = min(bucketSize, s.MaxSize)
		}

		return max(bucketSize, s.MinSize)
	}
}
