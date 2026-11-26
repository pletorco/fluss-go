package fgo

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/pletorco/fluss-go/pkg/fmsg"
)

const kvScanRequestTimeout = 30 * time.Second

// The worker owns the result until it closes done. Only the pollGate owner
// starts or consumes a call; caller cancellation does not discard it.
type kvScanCall struct {
	done  chan struct{}
	batch kvSessionBatch
	err   error
}

func (s *BatchScanner) pollKVSession(ctx context.Context, backend kvSessionScanBackend) (BatchResult, error) {
	if err := ctx.Err(); err != nil {
		return BatchResult{}, err
	}
	if s.pendingKV == nil {
		if err := s.awaitKVPage(ctx, backend); err != nil {
			return BatchResult{}, err
		}
	}
	batch := s.pendingKV
	var rows []Row
	if len(batch.records) != 0 {
		var err error
		rows, err = decodeValueRecordBatchWithResolver(ctx, s.resolver, s.table, batch.records, s.valueEncoding)
		if err != nil {
			return BatchResult{}, err
		}
	}
	if err := ctx.Err(); err != nil {
		return BatchResult{}, err
	}
	result := BatchResult{Rows: s.projectRows(rows), Done: !batch.hasMore}
	s.pendingKV = nil
	if result.Done {
		s.markDone()
	}
	return result, nil
}

func (s *BatchScanner) awaitKVPage(ctx context.Context, backend kvSessionScanBackend) error {
	if s.kvErr != nil {
		return s.kvErr
	}
	if s.kvCall == nil {
		if err := s.life.Err(); err != nil {
			return err
		}
		s.startKVCall(ctx, backend)
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.kvCall.done:
		if err := ctx.Err(); err != nil {
			return err
		}
		s.acceptKVReply()
		return s.kvErr
	}
}

func (s *BatchScanner) startKVCall(ctx context.Context, backend kvSessionScanBackend) {
	life, cancelLife := contextForLifetime(context.WithoutCancel(ctx), s.life)
	requestCtx, cancelRequest := context.WithTimeout(life, kvScanRequestTimeout)
	call := &kvScanCall{done: make(chan struct{})}
	s.kvCall = call
	scannerID, sequence := s.scannerID, s.sequence // Stable until the result is accepted.
	go func() {
		defer close(call.done)
		defer cancelLife()
		defer cancelRequest()
		call.batch, call.err = backend.scanKV(
			requestCtx, s.bucket, int64(s.config.Limit), s.config.BatchSizeBytes, scannerID, sequence, false,
		)
	}()
}

// acceptKVReply records server progress, not delivery. A valid response is
// retained until decoded and delivered, even if it ended the server session.
func (s *BatchScanner) acceptKVReply() {
	call := s.kvCall
	s.kvCall = nil
	err := call.err
	if err == nil {
		err = s.validateKVReply(&call.batch)
	}
	if err != nil {
		// The server may already have advanced the cursor. Neither retrying
		// this sequence nor opening a different snapshot can recover it.
		s.kvErr = fmt.Errorf("fgo: KV scan session stopped at sequence %d: %w", s.sequence, err)
		return
	}
	if len(call.batch.scannerID) != 0 {
		s.scannerID = append(s.scannerID[:0], call.batch.scannerID...)
		s.sequence++
	}
	s.pendingKV = &call.batch
}

func (s *BatchScanner) validateKVReply(batch *kvSessionBatch) error {
	if len(batch.scannerID) == 0 {
		// Fluss 1.0 does not register a session for an empty bucket.
		if batch.hasMore || len(batch.records) != 0 || len(s.scannerID) != 0 {
			return fmt.Errorf("%w: KV scan response omitted scanner ID", ErrValidation)
		}
	} else if len(s.scannerID) != 0 && !bytes.Equal(s.scannerID, batch.scannerID) {
		return fmt.Errorf("%w: KV scan response changed scanner ID", ErrValidation)
	}
	return nil
}

func (s *BatchScanner) closeKVSession(backend kvSessionScanBackend) error {
	s.pollGate <- struct{}{}
	defer func() { <-s.pollGate }()
	if s.kvCall != nil {
		// Close has canceled the request. If a response won the race, use it
		// to learn the session ID or avoid closing an already terminal page.
		<-s.kvCall.done
		s.acceptKVReply()
	}
	closeSession := len(s.scannerID) != 0 && !s.done && (s.pendingKV == nil || s.pendingKV.hasMore)
	s.pendingKV = nil
	if !closeSession {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := backend.scanKV(
		ctx, s.bucket, int64(s.config.Limit), s.config.BatchSizeBytes, s.scannerID, s.sequence, true,
	)
	var server *ServerError
	if errors.As(err, &server) && (server.Code == fmsg.ErrorCodeScannerExpired || server.Code == fmsg.ErrorCodeUnknownScannerId) {
		return nil // The server no longer has a session to release.
	}
	return err
}
