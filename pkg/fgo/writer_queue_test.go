package fgo

import "testing"

func TestWriterQueuesReleaseConsumedBatchSlots(t *testing.T) {
	t.Run("append", func(t *testing.T) {
		checkWriterQueueDrain(t, &bucketBatch{bytes: 1 << 20}, &bucketBatch{bytes: 2 << 20})
	})
	t.Run("upsert", func(t *testing.T) {
		checkWriterQueueDrain(t, &kvPendingBatch{bytes: 1 << 20}, &kvPendingBatch{bytes: 2 << 20})
	})
}

func checkWriterQueueDrain[T any](t *testing.T, first, second *T) {
	t.Helper()
	const bucket = 1
	queue := make([]*T, 2, 8)
	queue[0], queue[1] = first, second
	pending := map[int32][]*T{bucket: queue, 2: {first}}
	if got := popWriterBatch(pending, bucket); got != first || len(pending[bucket]) != 1 ||
		queue[0] != nil || pending[bucket][0] != second {
		t.Fatal("partial drain retained the first batch or changed queue order")
	}
	if got := popWriterBatch(pending, bucket); got != second || queue[1] != nil {
		t.Fatal("full drain retained the last batch or changed queue order")
	}
	if _, exists := pending[bucket]; exists || len(pending[2]) != 1 {
		t.Fatalf("bucket map after drain = %v", pending)
	}
}
