package fgo

import (
	"context"
	"errors"
	"testing"
)

type progressLogScannerBackend struct {
	*fakeLogScannerBackend
	beforeFetch func(context.Context, logFetchRequest) error
}

func (b *progressLogScannerBackend) fetch(ctx context.Context, input logFetchRequest) (scannerFetch, error) {
	if b.beforeFetch != nil {
		if err := b.beforeFetch(ctx, input); err != nil {
			return scannerFetch{}, err
		}
	}
	return b.fakeLogScannerBackend.fetch(ctx, input)
}

func TestLogScannerInterruptedPollPreservesProgress(t *testing.T) {
	for _, interruption := range []string{"cancel", "wakeup"} {
		for _, format := range []string{"rows", "arrow", "mixed"} {
			for _, bound := range []string{"unbounded", "row limit", "stopping offsets"} {
				t.Run(interruption+"/"+format+"/"+bound, func(t *testing.T) {
					testInterruptedLogPollProgress(t, interruption, format, bound)
				})
			}
		}
	}
}

func testInterruptedLogPollProgress(t *testing.T, interruption, format, bound string) {
	t.Helper()
	table := appendWriterTable()
	table.BucketCount = 2
	backend := &progressLogScannerBackend{fakeLogScannerBackend: scannerBackend(0, 1)}
	backend.fetches[0] = scannerFetch{records: progressLogRecords(t, table.Schema, format), highWatermark: 2}
	backend.fetches[1] = scannerFetch{records: encodedRows(t, table.Schema, 0, 3), highWatermark: 1}
	scanner, err := newLogScanner(context.Background(), backend, table, AtOffset(0), progressLogBounds(bound)...)
	if err != nil {
		t.Fatal(err)
	}
	defer scanner.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wantErr := context.Canceled
	if interruption == "wakeup" {
		wantErr = ErrWakeup
	}
	backend.beforeFetch = func(fetchCtx context.Context, input logFetchRequest) error {
		if input.bucket != 1 {
			return nil
		}
		if interruption == "cancel" {
			cancel()
		} else {
			scanner.Wakeup()
		}
		return fetchCtx.Err()
	}
	result, err := scanner.Poll(ctx)
	defer result.Release()
	if !errors.Is(err, wantErr) || len(result.Records) != 0 || len(result.ArrowBatches) != 0 {
		t.Fatalf("interrupted Poll = %#v, %v, want empty result and %v", result, err, wantErr)
	}
	if scanner.offset[0] != 0 || scanner.offset[1] != 0 || scanner.delivered != 0 || scanner.Done() {
		t.Fatalf("discarded poll advanced state: offsets=%v delivered=%d done=%v", scanner.offset, scanner.delivered, scanner.Done())
	}
	backend.beforeFetch = nil
	retry, err := scanner.Poll(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer retry.Release()
	if got := scanResultRows(retry.Records, retry.ArrowBatches); got != 3 || len(retry.BucketErrors) != 0 {
		t.Fatalf("retry returned %d rows and errors %v, want all three rows", got, retry.BucketErrors)
	}
	if retry.Done != (bound != "unbounded") || scanner.delivered != 3 || scanner.offset[0] != 2 || scanner.offset[1] != 1 {
		t.Fatalf("retry state: offsets=%v delivered=%d done=%v", scanner.offset, scanner.delivered, retry.Done)
	}
}

func progressLogBounds(bound string) []LogScannerOption {
	switch bound {
	case "row limit":
		return []LogScannerOption{WithScanRowLimit(3)}
	case "stopping offsets":
		return []LogScannerOption{WithScanStoppingOffsets(map[int32]int64{0: 2, 1: 1})}
	default:
		return nil
	}
}

func progressLogRecords(t *testing.T, schema Schema, format string) []byte {
	t.Helper()
	switch format {
	case "arrow":
		return encodedArrowRows(t, schema, 0, 1, 2)
	case "mixed":
		return append(encodedRows(t, schema, 0, 1), encodedArrowRows(t, schema, 1, 2)...)
	default:
		return encodedRows(t, schema, 0, 1, 2)
	}
}

func TestLogScannerInterruptAfterSuccessfulFinalFetch(t *testing.T) {
	for _, interruption := range []string{"cancel", "wakeup", "close"} {
		t.Run(interruption, func(t *testing.T) {
			backend := &progressLogScannerBackend{fakeLogScannerBackend: scannerBackend(0)}
			backend.fetches[0] = scannerFetch{records: encodedRows(t, appendWriterTable().Schema, 0, 1)}
			scanner, err := newLogScanner(context.Background(), backend, appendWriterTable(), AtOffset(0), WithScanRowLimit(1))
			if err != nil {
				t.Fatal(err)
			}
			defer scanner.Close()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			wantErr := context.Canceled
			backend.beforeFetch = func(context.Context, logFetchRequest) error {
				switch interruption {
				case "cancel":
					cancel()
				case "wakeup":
					scanner.Wakeup()
				case "close":
					_ = scanner.Close()
				}
				return nil
			}
			switch interruption {
			case "wakeup":
				wantErr = ErrWakeup
			case "close":
				wantErr = ErrClosed
			}
			result, err := scanner.Poll(ctx)
			defer result.Release()
			if !errors.Is(err, wantErr) || len(result.Records) != 0 || scanner.delivered != 0 {
				t.Fatalf("Poll = %#v, %v, delivered=%d, want %v without progress", result, err, scanner.delivered, wantErr)
			}
		})
	}
}

func TestLogScannerPollPreservesSubscriptionChanges(t *testing.T) {
	for _, change := range []string{"reset same offset", "reset different offset", "unsubscribe"} {
		for _, interrupted := range []bool{false, true} {
			name := change + "/success"
			if interrupted {
				name = change + "/cancel"
			}
			t.Run(name, func(t *testing.T) {
				table := appendWriterTable()
				table.BucketCount = 2
				backend := &progressLogScannerBackend{fakeLogScannerBackend: scannerBackend(0, 1)}
				backend.fetches[0] = scannerFetch{records: encodedRows(t, table.Schema, 0, 1)}
				scanner, err := newLogScanner(context.Background(), backend, table, AtOffset(0))
				if err != nil {
					t.Fatal(err)
				}
				defer scanner.Close()
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				wantOffset := int64(0)
				backend.beforeFetch = func(fetchCtx context.Context, input logFetchRequest) error {
					if input.bucket != 1 {
						return nil
					}
					if change == "unsubscribe" {
						scanner.Unsubscribe(0)
					} else {
						if change == "reset different offset" {
							wantOffset = 5
						}
						if err := scanner.Subscribe(context.Background(), 0, AtOffset(wantOffset)); err != nil {
							t.Fatal(err)
						}
					}
					if interrupted {
						cancel()
						return fetchCtx.Err()
					}
					return nil
				}
				result, err := scanner.Poll(ctx)
				defer result.Release()
				if interrupted && !errors.Is(err, context.Canceled) || !interrupted && err != nil {
					t.Fatalf("Poll error = %v", err)
				}
				offset, subscribed := scanner.offset[0]
				if change == "unsubscribe" {
					if subscribed {
						t.Fatal("Poll restored an unsubscribed bucket")
					}
				} else if !subscribed || offset != wantOffset {
					t.Fatalf("Poll overwrote subscription: offset=%d subscribed=%v, want %d", offset, subscribed, wantOffset)
				}
			})
		}
	}
}
