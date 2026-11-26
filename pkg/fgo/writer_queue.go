package fgo

// popWriterBatch transfers the first batch out of a nonempty bucket queue.
// Clear the consumed pointer even when other batches share the backing array.
func popWriterBatch[T any](pending map[int32][]*T, bucket int32) *T {
	queue := pending[bucket]
	batch := queue[0]
	queue[0] = nil
	if len(queue) == 1 {
		delete(pending, bucket)
	} else {
		pending[bucket] = queue[1:]
	}
	return batch
}
