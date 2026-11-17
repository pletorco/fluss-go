package transport

import (
	"context"
	"errors"
	"io"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type admissionProbeConn struct {
	net.Conn
	entered   chan struct{}
	hold      <-chan struct{}
	first     sync.Once
	writes    atomic.Int32
	deadlines atomic.Int32
}

func (c *admissionProbeConn) Write(frame []byte) (int, error) {
	c.writes.Add(1)
	c.first.Do(func() {
		close(c.entered)
		if c.hold != nil {
			<-c.hold
		}
	})
	return c.Conn.Write(frame)
}

func (c *admissionProbeConn) SetWriteDeadline(deadline time.Time) error {
	c.deadlines.Add(1)
	return c.Conn.SetWriteDeadline(deadline)
}

func waitForPendingRequests(t *testing.T, c *Connection, count int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for {
		c.mu.Lock()
		pending := len(c.pending)
		c.mu.Unlock()
		if pending == count {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("pending requests = %d, want %d", pending, count)
		default:
			runtime.Gosched()
		}
	}
}

func awaitAdmissionResult(t *testing.T, completed <-chan error) error {
	t.Helper()
	select {
	case err := <-completed:
		return err
	case <-time.After(time.Second):
		t.Fatal("request did not return independently of the blocked writer")
		return nil
	}
}

func startAdmissionRequest(t *testing.T, c *Connection, ctx context.Context) <-chan error {
	t.Helper()
	request := apiVersionsRequest(t)
	completed := make(chan error, 1)
	go func() {
		_, err := c.Request(ctx, request)
		completed <- err
	}()
	return completed
}

func TestQueuedWriteCancellationKeepsActiveRequestUsable(t *testing.T) {
	for _, mode := range []string{"cancel", "deadline"} {
		t.Run(mode, func(t *testing.T) {
			client, server := net.Pipe()
			t.Cleanup(func() { _ = server.Close() })
			probe := &admissionProbeConn{Conn: client, entered: make(chan struct{})}
			connection, err := New(probe, Config{MaxInFlight: 2})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = connection.Close() })
			active := startAdmissionRequest(t, connection, context.Background())
			<-probe.entered
			ctx, cancel := context.WithCancel(context.Background())
			want := context.Canceled
			if mode == "deadline" {
				cancel()
				ctx, cancel = context.WithTimeout(context.Background(), 20*time.Millisecond)
				want = context.DeadlineExceeded
			}
			defer cancel()
			queued := startAdmissionRequest(t, connection, ctx)
			// Registration proves this request passed in-flight admission while the
			// first write still owns the socket. No peer read or sleep is needed.
			waitForPendingRequests(t, connection, 2)
			if mode == "cancel" {
				cancel()
			}
			if err := awaitAdmissionResult(t, queued); !errors.Is(err, want) || errors.Is(err, ErrClosed) {
				t.Fatalf("queued request = %v, want only %v", err, want)
			}
			waitForPendingRequests(t, connection, 1)
			if len(connection.sem) != 1 {
				t.Fatalf("in-flight slots = %d, want only active request", len(connection.sem))
			}
			if probe.writes.Load() != 1 || probe.deadlines.Load() != 0 {
				t.Fatalf("queued cancellation touched active socket: writes=%d deadlines=%d",
					probe.writes.Load(), probe.deadlines.Load())
			}
			select {
			case err := <-active:
				t.Fatalf("queued cancellation interrupted active request: %v", err)
			default:
			}
			served := make(chan struct{})
			go func() {
				defer close(served)
				for range 2 {
					id := readRequestID(t, server)
					// ApiVersionsResponse has an empty valid protobuf body.
					writeResponse(t, server, 0, id, nil)
				}
			}()
			if err := awaitAdmissionResult(t, active); err != nil {
				t.Fatalf("active request after queued cancellation = %v", err)
			}
			later := startAdmissionRequest(t, connection, context.Background())
			if err := awaitAdmissionResult(t, later); err != nil {
				t.Fatalf("later request = %v", err)
			}
			<-served
			waitForPendingRequests(t, connection, 0)
			if len(connection.sem) != 0 || probe.writes.Load() != 2 {
				t.Fatalf("after recovery: in-flight=%d writes=%d", len(connection.sem), probe.writes.Load())
			}
		})
	}
}

func TestCloseReleasesQueuedWriteBeforeActiveWriteReturns(t *testing.T) {
	client, server := net.Pipe()
	defer server.Close()
	release := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	probe := &admissionProbeConn{Conn: client, entered: make(chan struct{}), hold: release}
	connection, err := New(probe, Config{MaxInFlight: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	active := startAdmissionRequest(t, connection, context.Background())
	<-probe.entered
	queued := startAdmissionRequest(t, connection, context.Background())
	waitForPendingRequests(t, connection, 2)
	_ = connection.Close()
	if err := awaitAdmissionResult(t, queued); !errors.Is(err, ErrClosed) {
		t.Fatalf("queued request after Close = %v", err)
	}
	if probe.writes.Load() != 1 || probe.deadlines.Load() != 0 {
		t.Fatal("closed queued request reached socket write/deadline handling")
	}
	waitForPendingRequests(t, connection, 0)
	if len(connection.sem) != 1 {
		t.Fatalf("queued request leaked its in-flight slot: %d", len(connection.sem))
	}
	releaseOnce.Do(func() { close(release) })
	if err := awaitAdmissionResult(t, active); !errors.Is(err, ErrClosed) {
		t.Fatalf("active request after Close = %v", err)
	}
	if len(connection.sem) != 0 {
		t.Fatal("active request leaked its in-flight slot")
	}
}

func TestPartialFrameCancellationClosesQueuedRequest(t *testing.T) {
	client, server := net.Pipe()
	defer server.Close()
	probe := &admissionProbeConn{Conn: client, entered: make(chan struct{})}
	connection, err := New(probe, Config{MaxInFlight: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	active := startAdmissionRequest(t, connection, ctx)
	<-probe.entered
	// Consume only part of the frame, leaving the active Write blocked with
	// bytes already sent. Cancellation must still discard this connection.
	var partial [5]byte
	if _, err := io.ReadFull(server, partial[:]); err != nil {
		t.Fatal(err)
	}
	queued := startAdmissionRequest(t, connection, context.Background())
	waitForPendingRequests(t, connection, 2)
	cancel()
	if err := awaitAdmissionResult(t, active); !errors.Is(err, context.Canceled) || !errors.Is(err, ErrClosed) {
		t.Fatalf("partially written request = %v", err)
	}
	if err := awaitAdmissionResult(t, queued); !errors.Is(err, ErrClosed) || errors.Is(err, context.Canceled) {
		t.Fatalf("queued request after partial frame = %v", err)
	}
	waitForPendingRequests(t, connection, 0)
	if len(connection.sem) != 0 {
		t.Fatal("discarded connection leaked in-flight slots")
	}
}

func TestWriteAdmissionRechecksCancellationAndClosure(t *testing.T) {
	for _, mode := range []string{"cancel", "close"} {
		t.Run(mode, func(t *testing.T) {
			client, server := net.Pipe()
			defer server.Close()
			probe := &admissionProbeConn{Conn: client, entered: make(chan struct{})}
			connection, err := New(probe, Config{})
			if err != nil {
				t.Fatal(err)
			}
			defer connection.Close()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			want := context.Canceled
			if mode == "cancel" {
				cancel()
			} else {
				_ = connection.Close()
				want = ErrClosed
			}
			request := apiVersionsRequest(t)
			for range 100 {
				// The gate and context/done cases can both be ready. Neither
				// select outcome may allocate/send a frame or mutate deadlines.
				discard, err := connection.writeRequest(ctx, 1, request, nil)
				if discard || !errors.Is(err, want) {
					t.Fatalf("write admission = %t, %v", discard, err)
				}
			}
			if probe.writes.Load() != 0 || probe.deadlines.Load() != 0 {
				t.Fatal("canceled/closed admission reached the socket")
			}
		})
	}
}
