package fgo

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"
)

func TestQueuedScannerPollDeadline(t *testing.T) {
	t.Run("log", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			backend := scannerBackend(0)
			backend.entered = make(chan struct{})
			backend.block = make(chan struct{})
			scanner, err := newLogScanner(context.Background(), backend, appendWriterTable(), AtOffset(0))
			if err != nil {
				t.Fatal(err)
			}
			defer scanner.Close()
			assertQueuedPollDeadline(t, backend.entered, func(ctx context.Context) error {
				_, err := scanner.Poll(ctx)
				return err
			})
		})
	})
	t.Run("batch", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			reader := &fakeSnapshotBatchReader{block: true, entered: make(chan struct{})}
			table := upsertWriterTable()
			scanner, err := newBatchScanner(context.Background(), nil, reader, table, testTableBucket(table))
			if err != nil {
				t.Fatal(err)
			}
			defer scanner.Close()
			assertQueuedPollDeadline(t, reader.entered, func(ctx context.Context) error {
				_, err := scanner.Poll(ctx)
				return err
			})
		})
	})
}

func assertQueuedPollDeadline(t *testing.T, entered <-chan struct{}, poll func(context.Context) error) {
	t.Helper()
	active, cancelActive := context.WithCancel(context.Background())
	defer cancelActive()
	first := make(chan error, 1)
	go func() { first <- poll(active) }()
	<-entered
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := poll(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("queued poll = %v, want deadline exceeded", err)
	}
	select {
	case err := <-first:
		t.Fatalf("queued deadline interrupted the active poll: %v", err)
	default:
	}
	cancelActive()
	if err := <-first; !errors.Is(err, context.Canceled) {
		t.Fatalf("active poll = %v, want canceled", err)
	}
}

func TestQueuedPollWakesWhenScannerCloses(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		life, cancel := context.WithCancel(context.Background())
		defer cancel()
		gate := make(chan struct{}, 1)
		gate <- struct{}{}
		result := make(chan error, 1)
		go func() { result <- acquirePoll(context.Background(), life, gate) }()
		synctest.Wait()
		cancel()
		if err := <-result; !errors.Is(err, ErrClosed) {
			t.Fatalf("queued poll = %v, want ErrClosed", err)
		}
	})
}
