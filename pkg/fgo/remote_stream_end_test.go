package fgo

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
)

type remoteTestReader struct {
	RemoteFileReaderFunc
	open func(RemoteFileRequest) io.ReadCloser
}

func (r remoteTestReader) OpenRemoteFile(_ context.Context, req RemoteFileRequest) (io.ReadCloser, error) {
	return r.open(req), nil
}

type rangeReadStep struct {
	data string
	err  error
}

type scriptedRangeRead struct {
	steps      []rangeReadStep
	calls      int
	closes     int
	closeErr   error
	beforeRead func(int)
}

func (r *scriptedRangeRead) Read(dst []byte) (int, error) {
	r.calls++
	if r.beforeRead != nil {
		r.beforeRead(r.calls)
	}
	if len(r.steps) == 0 {
		return 0, nil
	}
	step := r.steps[0]
	r.steps = r.steps[1:]
	return copy(dst, step.data), step.err
}

func (r *scriptedRangeRead) Close() error {
	r.closes++
	return r.closeErr
}

func TestRemoteRangeRequiresEOFAndPreservesFinalError(t *testing.T) {
	integrity := errors.New("range integrity failure")
	closeErr := errors.New("range close failure")
	for _, tc := range []struct {
		name     string
		steps    []rangeReadStep
		calls    int
		want     error
		closeErr error
		cancelAt int
	}{
		{name: "bytes and EOF", steps: []rangeReadStep{{"abc", io.EOF}}, calls: 1},
		{name: "progress after empty reads", steps: []rangeReadStep{{}, {"a", nil}, {}, {"bc", nil}, {}, {"", io.EOF}}, calls: 6},
		{name: "bytes and error", steps: []rangeReadStep{{"abc", integrity}}, calls: 1, want: integrity, closeErr: closeErr},
		{name: "EOF joined with error", steps: []rangeReadStep{{"abc", errors.Join(io.EOF, integrity)}}, calls: 1, want: integrity},
		{name: "short", steps: []rangeReadStep{{"ab", io.EOF}}, calls: 1, want: io.ErrUnexpectedEOF},
		{name: "oversized after empty read", steps: []rangeReadStep{{"abc", nil}, {}, {"x", io.EOF}}, calls: 3, want: ErrValidation},
		{name: "extra byte and error", steps: []rangeReadStep{{"abc", nil}, {"x", integrity}}, calls: 2, want: integrity},
		{name: "stalled in body", calls: 100, want: io.ErrNoProgress},
		{name: "stalled at EOF probe", steps: []rangeReadStep{{"abc", nil}}, calls: 101, want: io.ErrNoProgress},
		{name: "close failed", steps: []rangeReadStep{{"abc", io.EOF}}, calls: 1, closeErr: closeErr, want: closeErr},
		{name: "cancel at probe", steps: []rangeReadStep{{"abc", nil}}, calls: 2, cancelAt: 2, want: context.Canceled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			body := &scriptedRangeRead{steps: tc.steps, closeErr: tc.closeErr, beforeRead: func(call int) {
				if call == tc.cancelAt {
					cancel()
				}
			}}
			settings := remoteFileSettings{
				reader: remoteTestReader{open: func(req RemoteFileRequest) io.ReadCloser {
					if req.Offset != 2 || req.Length != 3 || req.ExpectedSize != 5 {
						t.Fatalf("range request = %+v", req)
					}
					return body
				}},
				config: RemoteFileReadConfig{MaxFileBytes: 5, MaxAttempts: 1},
			}
			dst := []byte("xxx")
			err := readRemoteFileIntoWithRetry(ctx, settings, RemoteFileRequest{ExpectedSize: 5, Offset: 2}, dst, nil)
			if !errors.Is(err, tc.want) || body.calls != tc.calls || body.closes != 1 {
				t.Fatalf("read error=%v calls=%d closes=%d", err, body.calls, body.closes)
			}
			if err == nil && string(dst) != "abc" || err != nil && !bytes.Equal(dst, []byte{0, 0, 0}) {
				t.Fatalf("destination after read = %q", dst)
			}
		})
	}
}

func TestRemoteRangeRetryAfterErrorWithFinalBytes(t *testing.T) {
	dst := make([]byte, 3)
	first := &scriptedRangeRead{steps: []rangeReadStep{{"bad", io.ErrUnexpectedEOF}}}
	second := &scriptedRangeRead{steps: []rangeReadStep{{"new", io.EOF}}}
	calls := 0
	settings := remoteFileSettings{
		reader: remoteTestReader{open: func(RemoteFileRequest) io.ReadCloser {
			calls++
			if calls == 1 {
				return first
			}
			if first.closes != 1 || !bytes.Equal(dst, []byte{0, 0, 0}) {
				t.Fatalf("before retry: closed=%d dst=%q", first.closes, dst)
			}
			return second
		}},
		config: RemoteFileReadConfig{MaxAttempts: 2, MaxFileBytes: 3},
	}
	err := readRemoteFileIntoWithRetry(t.Context(), settings, RemoteFileRequest{ExpectedSize: 3}, dst, nil)
	if err != nil || string(dst) != "new" || calls != 2 || second.closes != 1 {
		t.Fatalf("retry: err=%v data=%q opens=%d closes=%d", err, dst, calls, second.closes)
	}
}
