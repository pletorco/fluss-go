package fgo

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"testing/synctest"
)

type gatedConnectionAuth struct {
	*testAuthenticator
	entered chan struct{}
	release chan struct{}
	closes  atomic.Int32
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
