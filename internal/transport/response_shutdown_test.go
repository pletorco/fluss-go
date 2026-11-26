package transport

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/pletorco/fluss-go/pkg/fmsg"
	"google.golang.org/protobuf/proto"
)

type deadlineCleanupGate struct {
	net.Conn
	entered, release chan struct{}
	ignoreCloseError bool
}

func (c deadlineCleanupGate) SetWriteDeadline(deadline time.Time) error {
	if deadline.IsZero() {
		close(c.entered)
		<-c.release
		if c.ignoreCloseError {
			return nil
		}
	}
	return c.Conn.SetWriteDeadline(deadline)
}

func TestCompletedResponseSurvivesConnectionFailure(t *testing.T) {
	for _, cleanup := range []struct {
		name             string
		ignoreCloseError bool
	}{{"cleanup succeeds", true}, {"cleanup on closed socket fails", false}} {
		t.Run(cleanup.name, func(t *testing.T) {
			for _, kind := range []string{"success", "remote error", "no reply"} {
				t.Run(kind, func(t *testing.T) {
					assertReplyBeforeDisconnect(t, kind, cleanup.ignoreCloseError)
				})
			}
		})
	}
}

func assertReplyBeforeDisconnect(t *testing.T, kind string, ignoreCloseError bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	client, server := net.Pipe()
	defer server.Close()
	deadline, _ := ctx.Deadline()
	if err := server.SetDeadline(deadline); err != nil {
		t.Fatal(err)
	}
	gate := deadlineCleanupGate{Conn: client, entered: make(chan struct{}), release: make(chan struct{}), ignoreCloseError: ignoreCloseError}
	resume := sync.OnceFunc(func() { close(gate.release) })
	defer resume()
	c, err := New(gate, Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	type outcome struct {
		reply fmsg.Response
		err   error
	}
	request := apiVersionsRequest(t)
	done := make(chan outcome, 1)
	go func() {
		reply, err := c.Request(ctx, request)
		done <- outcome{reply, err}
	}()
	id := readRequestID(t, server)
	select {
	case <-gate.entered:
	case <-ctx.Done():
		t.Fatal("request never finished writing")
	}
	switch kind {
	case "success":
		body, err := proto.Marshal(&fmsg.ApiVersionsResponse{})
		if err != nil {
			t.Fatal(err)
		}
		writeResponse(t, server, 0, id, body)
	case "remote error":
		body, err := proto.Marshal(&fmsg.ErrorResponse{ErrorCode: proto.Int32(7), ErrorMessage: proto.String("denied")})
		if err != nil {
			t.Fatal(err)
		}
		writeResponse(t, server, 1, id, body)
	}
	_ = server.Close()
	select {
	case <-c.done:
	case <-ctx.Done():
		t.Fatal("reader did not process the disconnect")
	}
	resume()
	select {
	case got := <-done:
		switch kind {
		case "success":
			if got.err != nil || got.reply == nil || got.reply.APIKey() != fmsg.APIKeyApiVersions {
				t.Fatalf("request = %v, %v; want correlated reply", got.reply, got.err)
			}
		case "remote error":
			var remote *RemoteError
			if got.reply != nil || !errors.As(got.err, &remote) || remote.Code != 7 || remote.Message != "denied" {
				t.Fatalf("request = %v, %v; want the request's remote error", got.reply, got.err)
			}
		case "no reply":
			if got.reply != nil || !errors.Is(got.err, io.EOF) {
				t.Fatalf("request = %v, %v; want EOF", got.reply, got.err)
			}
		}
	case <-ctx.Done():
		t.Fatal("request did not terminate")
	}
}
