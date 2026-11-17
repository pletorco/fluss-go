//go:build integration

package integration_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/pletorco/fluss-go/pkg/fgo"
)

func testInterruptedLogPoll(t *testing.T, address string, path fgo.TablePath) {
	t.Helper()
	for _, interruption := range []string{"cancel", "wakeup"} {
		t.Run(interruption, func(t *testing.T) {
			var interrupt func()
			client := openClient(t, []string{address}, fgo.WithMetricsObserver(fgo.MetricsObserverFunc(func(event fgo.MetricEvent) {
				if event.Kind == fgo.MetricScannerFetch && event.Records > 0 && interrupt != nil {
					trigger := interrupt
					interrupt = nil
					trigger()
				}
			})))
			defer client.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			table, err := client.GetTable(ctx, path)
			if err != nil {
				t.Fatal(err)
			}
			scanner, err := client.NewLogScanner(ctx, table, fgo.Earliest(), fgo.WithScanRowLimit(3),
				fgo.WithLogFetchLimits(1<<20, 1<<20, 1, 100*time.Millisecond))
			if err != nil {
				t.Fatal(err)
			}
			defer scanner.Close()
			pollCtx, cancelPoll := context.WithCancel(ctx)
			defer cancelPoll()
			interrupt = cancelPoll
			wantErr := context.Canceled
			if interruption == "wakeup" {
				interrupt = scanner.Wakeup
				wantErr = fgo.ErrWakeup
			}
			result, err := scanner.Poll(pollCtx)
			defer result.Release()
			if !errors.Is(err, wantErr) || len(result.Records) != 0 || len(result.ArrowBatches) != 0 || scanner.Done() {
				t.Fatalf("interrupted Poll = %#v, %v, done=%v; want %v without consumed rows", result, err, scanner.Done(), wantErr)
			}
			var rows int64
			for !scanner.Done() && ctx.Err() == nil {
				result, err := scanner.Poll(ctx)
				if err != nil {
					t.Fatal(err)
				}
				rows += int64(len(result.Records))
				for _, batch := range result.ArrowBatches {
					rows += batch.Batch.Record.NumRows()
				}
				result.Release()
			}
			if rows != 3 {
				t.Fatalf("retry delivered %d rows, want all 3 before the row limit; context=%v", rows, ctx.Err())
			}
		})
	}
}
