package fgo

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"

	"github.com/pletorco/fluss-go/pkg/fmsg"
	"google.golang.org/protobuf/proto"
)

func TestConnectionManagerBootstrapFallsBackToNextSeed(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	done := serveVersions(t, serverConn, Coordinator)
	var calls []string
	manager := newConnectionManager(config{
		seeds: []string{"down:9123", "up:9123"}, name: "test", version: "1",
		dialContext: func(_ context.Context, _, address string) (net.Conn, error) {
			calls = append(calls, address)
			if address == "down:9123" {
				return nil, errors.New("down")
			}
			return clientConn, nil
		},
	})
	client, err := manager.bootstrap(context.Background())
	if err != nil {
		t.Fatalf("bootstrap() error = %v", err)
	}
	if got, want := client.address, "up:9123"; got != want {
		t.Fatalf("client address = %q, want %q", got, want)
	}
	if got, want := len(calls), 2; got != want {
		t.Fatalf("dial calls = %d, want %d", got, want)
	}
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
	<-done
}

func TestConnectionManagerDeduplicatesConcurrentDials(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	done := serveVersions(t, serverConn, TabletServer)
	var mu sync.Mutex
	dials := 0
	manager := newConnectionManager(config{
		name: "test", version: "1",
		dialContext: func(context.Context, string, string) (net.Conn, error) {
			mu.Lock()
			dials++
			mu.Unlock()
			return clientConn, nil
		},
	})
	const callers = 16
	clients := make(chan *Client, callers)
	errs := make(chan error, callers)
	var wait sync.WaitGroup
	for range callers {
		wait.Add(1)
		go func() {
			defer wait.Done()
			client, err := manager.get(context.Background(), "tablet:9123", TabletServer)
			clients <- client
			errs <- err
		}()
	}
	wait.Wait()
	close(clients)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("get() error = %v", err)
		}
	}
	var first *Client
	for client := range clients {
		if first == nil {
			first = client
		} else if client != first {
			t.Fatal("get() returned duplicate connections")
		}
	}
	mu.Lock()
	gotDials := dials
	mu.Unlock()
	if got, want := gotDials, 1; got != want {
		t.Fatalf("dial calls = %d, want %d", got, want)
	}
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
	<-done
}

func TestConnectionManagerRejectsWrongServerRole(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	done := serveVersions(t, serverConn, TabletServer)
	manager := newConnectionManager(config{
		name: "test", version: "1",
		dialContext: func(context.Context, string, string) (net.Conn, error) { return clientConn, nil },
	})
	_, err := manager.get(context.Background(), "tablet:9123", Coordinator)
	if !errors.Is(err, ErrServerRole) {
		t.Fatalf("get() error = %v, want ErrServerRole", err)
	}
	<-done
}

func TestServerRole(t *testing.T) {
	for _, role := range []ServerRole{Coordinator, TabletServer} {
		got, err := serverRole(int32(role))
		if err != nil || got != role {
			t.Fatalf("serverRole(%d) = %v, %v", role, got, err)
		}
	}
	if _, err := serverRole(0); !errors.Is(err, ErrServerRole) {
		t.Fatalf("serverRole(0) error = %v", err)
	}
}

func serveVersions(t *testing.T, conn net.Conn, role ServerRole) <-chan struct{} {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer conn.Close()
		id, key, _ := readTransportRequest(t, conn)
		if key != fmsg.APIKeyApiVersions {
			t.Errorf("API key = %d, want API_VERSIONS", key)
			return
		}
		apiKey := int32(fmsg.APIKeyApiVersions)
		minimum, maximum, roleValue := int32(0), int32(0), int32(role)
		body, err := proto.Marshal(&fmsg.ApiVersionsResponse{
			ServerType: &roleValue,
			ApiVersions: []*fmsg.PbApiVersion{{
				ApiKey: &apiKey, MinVersion: &minimum, MaxVersion: &maximum,
			}},
		})
		if err != nil {
			t.Error(err)
			return
		}
		writeTransportResponse(t, conn, id, body)
		_, _ = io.Copy(io.Discard, conn)
	}()
	return done
}
