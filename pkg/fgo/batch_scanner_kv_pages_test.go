package fgo

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestBatchScannerSplitsKVPagesUntilExhausted(t *testing.T) {
	table := upsertWriterTable()
	backend := &fakeKVSessionBackend{batches: []kvSessionBatch{
		{scannerID: []byte("scan"), hasMore: true, records: encodedValueBatch(t, table,
			Row{int32(1), "one", int64(10)}, Row{int32(2), "two", int64(20)}, Row{int32(3), "three", int64(30)})},
		{scannerID: []byte("scan"), records: encodedValueBatch(t, table,
			Row{int32(4), "four", int64(40)}, Row{int32(5), "five", int64(50)}, Row{int32(6), "six", int64(60)})},
	}}
	scanner, err := newBatchScanner(t.Context(), backend, nil, table, testTableBucket(table), WithBatchLimit(2))
	if err != nil {
		t.Fatal(err)
	}
	defer scanner.Close()

	want := [][]int32{{1, 2}, {3}, {4, 5}, {6}}
	for i, ids := range want {
		result, err := scanner.Poll(t.Context())
		got := make([]int32, 0, len(result.Rows))
		for _, row := range result.Rows {
			got = append(got, row[0].(int32))
		}
		done := i == len(want)-1
		if err != nil || !reflect.DeepEqual(got, ids) || result.Done != done || scanner.Done() != done {
			t.Fatalf("poll %d: ids=%v done=%v err=%v; want %v done=%v", i, got, result.Done, err, ids, done)
		}
		if len(backend.calls) != i/2+1 {
			t.Fatalf("poll %d sent an RPC before draining the previous server page", i)
		}
		if i == 0 {
			// The caller can append to its row slice without overwriting an
			// undelivered entry in the backing server page.
			result.Rows = append(result.Rows, Row{int32(999)})
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			if _, err := scanner.Poll(ctx); !errors.Is(err, context.Canceled) {
				t.Fatalf("canceled poll: %v", err)
			}
		}
	}
	if backend.calls[0].sequence != 0 || backend.calls[1].sequence != 1 ||
		string(backend.calls[1].scannerID) != "scan" {
		t.Fatalf("unexpected KV scan sequence: %+v", backend.calls)
	}
	again, err := scanner.Poll(t.Context())
	if err != nil || !again.Done || len(again.Rows) != 0 || len(backend.calls) != 2 {
		t.Fatalf("poll after completion: %+v, %v (calls=%d)", again, err, len(backend.calls))
	}
}

func TestBatchScannerClosesWithBufferedKVRows(t *testing.T) {
	for _, tc := range []struct {
		name      string
		hasMore   bool
		wantCalls int
	}{
		{"active session", true, 2},
		{"server already closed", false, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			table := upsertWriterTable()
			backend := &fakeKVSessionBackend{batches: []kvSessionBatch{{
				scannerID: []byte("scan"), hasMore: tc.hasMore, records: encodedValueBatch(t, table,
					Row{int32(1), "one", int64(10)}, Row{int32(2), "two", int64(20)}),
			}}}
			scanner, err := newBatchScanner(t.Context(), backend, nil, table, testTableBucket(table),
				WithBatchLimit(1), WithBatchProjection("name"))
			if err != nil {
				t.Fatal(err)
			}
			result, err := scanner.Poll(t.Context())
			if err != nil || result.Done || !reflect.DeepEqual(result.Rows, []Row{{"one"}}) {
				t.Fatalf("projected first page: %+v %v", result, err)
			}
			if err := scanner.Close(); err != nil {
				t.Fatal(err)
			}
			if scanner.pendingKV != nil || len(backend.calls) != tc.wantCalls {
				t.Fatalf("close retained rows or made an unexpected request: pending=%v calls=%+v", scanner.pendingKV, backend.calls)
			}
			if tc.hasMore && (!backend.calls[1].closeScanner || backend.calls[1].sequence != 1 || string(backend.calls[1].scannerID) != "scan") {
				t.Fatalf("unexpected close request: %+v", backend.calls[1])
			}
		})
	}
}

func TestBatchScannerRejectsNonpositiveCustomLimit(t *testing.T) {
	table := upsertWriterTable()
	option := func(c *BatchScannerConfig) error {
		c.Limit = 0
		return nil
	}
	if _, err := newBatchScanner(t.Context(), &fakeKVSessionBackend{}, nil, table, testTableBucket(table), option); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("open with zero limit: %v", err)
	}
}
