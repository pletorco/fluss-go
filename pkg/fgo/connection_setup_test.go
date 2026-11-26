package fgo

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/pletorco/fluss-go/pkg/fmsg"
	"google.golang.org/protobuf/proto"
)

type gatedConnectionAuth struct {
	*testAuthenticator
	entered chan struct{}
	release chan struct{}
	closes  atomic.Int32
}

func TestBootstrapAfterSilentSeed(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		silentClosed := make(chan struct{})
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		var dials atomic.Int32
		client, err := Open(ctx, WithBootstrapServers("silent", "healthy"), WithConnectTimeout(10*time.Millisecond),
			WithDialContext(func(_ context.Context, _, address string) (net.Conn, error) {
				dials.Add(1)
				client, server := net.Pipe()
				if address == "silent" {
					drainSetupPipe(server, silentClosed)
				} else {
					serveVersions(t, server, Coordinator)
				}
				return client, nil
			}))
		if err != nil || dials.Load() != 2 {
			t.Fatalf("bootstrap dials=%d, err=%v", dials.Load(), err)
		}
		defer client.Close()
		<-silentClosed
	})
}

func TestSharedConnectionAttemptDeadlineDoesNotFollowFirstWaiter(t *testing.T) {
	for _, stage := range []string{"TLS", "negotiation", "authentication"} {
		t.Run(stage, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				entered, closed := make(chan struct{}), make(chan struct{})
				var dials atomic.Int32
				cfg := config{connectTimeout: 20 * time.Millisecond}
				cfg.dialContext = func(_ context.Context, _, _ string) (net.Conn, error) {
					dials.Add(1)
					client, server := net.Pipe()
					if stage == "authentication" {
						serveSilentAuth(t, server, entered, closed)
					} else {
						drainSetupPipe(server, closed)
						close(entered)
					}
					return client, nil
				}
				if stage == "TLS" {
					cfg.tlsConfig = &tls.Config{ServerName: "seed"}
				}
				if stage == "authentication" {
					cfg.authFactory = SASLPlainAuthenticator("fixture", "fixture")
				}
				manager := newConnectionManager(cfg)
				defer manager.Close()
				firstCtx, cancel := context.WithTimeout(t.Context(), 5*time.Millisecond)
				defer cancel()
				first, second := make(chan error, 1), make(chan error, 1)
				start := time.Now()
				go func() { _, err := manager.get(firstCtx, "seed", Coordinator); first <- err }()
				<-entered
				go func() { _, err := manager.get(t.Context(), "seed", Coordinator); second <- err }()
				if err := <-first; !errors.Is(err, context.DeadlineExceeded) || time.Since(start) != 5*time.Millisecond {
					t.Fatalf("first waiter at %v: %v", time.Since(start), err)
				}
				if err := <-second; !errors.Is(err, context.DeadlineExceeded) || time.Since(start) != 20*time.Millisecond || dials.Load() != 1 {
					t.Fatalf("shared attempt at %v, dials=%d: %v", time.Since(start), dials.Load(), err)
				}
				<-closed
			})
		})
	}
}

func drainSetupPipe(server net.Conn, closed chan<- struct{}) {
	go func() {
		defer close(closed)
		defer server.Close()
		_, _ = io.Copy(io.Discard, server)
	}()
}

func serveSilentAuth(t *testing.T, server net.Conn, entered, closed chan<- struct{}) {
	t.Helper()
	go func() {
		defer close(closed)
		defer server.Close()
		id, _, _ := readTransportRequest(t, server)
		body, err := proto.Marshal(&fmsg.ApiVersionsResponse{
			ServerType: proto.Int32(int32(Coordinator)),
			ApiVersions: []*fmsg.PbApiVersion{
				apiVersion(fmsg.APIKeyApiVersions, 0, 0),
				apiVersion(fmsg.APIKeyAuthenticate, 0, 0),
			},
		})
		if err != nil {
			t.Error(err)
		}
		writeTransportResponse(t, server, id, body)
		_, _, _ = readTransportRequest(t, server)
		close(entered)
		_, _ = io.Copy(io.Discard, server)
	}()
}

func (a *gatedConnectionAuth) Complete() bool {
	if a.entered != nil {
		close(a.entered)
		<-a.release
	}
	return a.testAuthenticator.Complete()
}

func (a *gatedConnectionAuth) Close() error {
	a.closes.Add(1)
	return a.testAuthenticator.Close()
}

func TestConnectionManagerDisposesAbandonedSetup(t *testing.T) {
	for _, order := range []string{"before replacement", "after replacement", "after close"} {
		t.Run(order, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ready := make(chan struct{})
				close(ready)
				auth := []*gatedConnectionAuth{
					{entered: make(chan struct{}), release: make(chan struct{})},
					{},
				}
				for _, a := range auth {
					a.testAuthenticator = &testAuthenticator{protocol: "test", initial: true, tokens: [][]byte{{1}}, completeAfter: 1}
				}
				var dials, factories atomic.Int32
				manager := newConnectionManager(config{
					dialContext: func(_ context.Context, _, _ string) (net.Conn, error) {
						dials.Add(1)
						client, server := net.Pipe()
						serveAuthenticatedVersions(t, server, Coordinator, nil, ready)
						return client, nil
					},
					authFactory: func() (Authenticator, error) {
						return auth[factories.Add(1)-1], nil
					},
				})
				defer manager.Close()
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				first := make(chan error, 1)
				go func() { _, err := manager.get(ctx, "seed", Coordinator); first <- err }()
				<-auth[0].entered
				cancel()
				firstErr := <-first
				var replacement *Client
				var err error
				switch order {
				case "before replacement":
					close(auth[0].release)
					synctest.Wait()
					replacement, err = manager.get(t.Context(), "seed", Coordinator)
				case "after replacement":
					replacement, err = manager.get(t.Context(), "seed", Coordinator)
					close(auth[0].release)
				case "after close":
					err = manager.Close()
					close(auth[0].release)
				}
				synctest.Wait()
				if !errors.Is(firstErr, context.Canceled) || err != nil || auth[0].closes.Load() != 1 {
					t.Fatalf("first=%v operation=%v abandoned closes=%d", firstErr, err, auth[0].closes.Load())
				}
				if replacement != nil {
					cached, err := manager.get(t.Context(), "seed", Coordinator)
					if cached != replacement || err != nil || dials.Load() != 2 || auth[1].closes.Load() != 0 {
						t.Fatalf("cached replacement=%v error=%v dials=%d", cached == replacement, err, dials.Load())
					}
					_ = manager.Close()
					if auth[1].closes.Load() != 1 {
						t.Fatalf("replacement closes=%d", auth[1].closes.Load())
					}
				}
			})
		})
	}
}
