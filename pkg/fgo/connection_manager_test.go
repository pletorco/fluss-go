package fgo

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

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

func TestConnectionManagerAuthenticatesEveryConnectionBeforePublishingIt(t *testing.T) {
	firstClient, firstServer := net.Pipe()
	secondClient, secondServer := net.Pipe()
	firstSeen, firstRelease := make(chan struct{}), make(chan struct{})
	secondRelease := make(chan struct{})
	close(secondRelease)
	firstDone := serveAuthenticatedVersions(t, firstServer, TabletServer, firstSeen, firstRelease)
	secondDone := serveAuthenticatedVersions(t, secondServer, TabletServer, nil, secondRelease)

	connections := map[string]net.Conn{
		"tablet-1:9123": firstClient,
		"tablet-2:9123": secondClient,
	}
	var mu sync.Mutex
	var authenticators []*testAuthenticator
	manager := newConnectionManager(config{
		name: "test", version: "1",
		dialContext: func(_ context.Context, _, address string) (net.Conn, error) {
			return connections[address], nil
		},
		authFactory: func() (Authenticator, error) {
			mu.Lock()
			defer mu.Unlock()
			auth := &testAuthenticator{
				protocol: "test", initial: true, tokens: [][]byte{[]byte("connection-token")},
				completeAfter: 1,
			}
			authenticators = append(authenticators, auth)
			return auth, nil
		},
	})

	type openResult struct {
		client *Client
		err    error
	}
	result := make(chan openResult, 1)
	go func() {
		client, err := manager.getNode(context.Background(), Node{
			ID: 1, Address: "tablet-1:9123", Role: TabletServer,
		})
		result <- openResult{client: client, err: err}
	}()
	<-firstSeen
	select {
	case opened := <-result:
		t.Fatalf("connection published before authentication completed: %#v", opened)
	default:
	}
	close(firstRelease)
	opened := <-result
	if opened.err != nil || opened.client == nil {
		t.Fatalf("first authenticated connection = %#v", opened)
	}
	second, err := manager.getNode(context.Background(), Node{
		ID: 2, Address: "tablet-2:9123", Role: TabletServer,
	})
	if err != nil || second == nil || second == opened.client {
		t.Fatalf("second authenticated connection = %p, %v", second, err)
	}

	mu.Lock()
	if len(authenticators) != 2 || authenticators[0] == authenticators[1] {
		t.Fatalf("connection authenticators = %#v", authenticators)
	}
	mu.Unlock()
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	for index, auth := range authenticators {
		if !auth.closed {
			t.Fatalf("authenticator %d was not closed", index)
		}
	}
	mu.Unlock()
	<-firstDone
	<-secondDone
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

func TestConnectionManagerRejectsInvalidOrClosedRequests(t *testing.T) {
	manager := newConnectionManager(config{})
	if _, err := manager.getNode(context.Background(), Node{Role: Coordinator}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("empty address error = %v", err)
	}
	if _, err := manager.getNode(context.Background(), Node{Address: "server:9123", Role: UnknownServerRole}); !errors.Is(err, ErrServerRole) {
		t.Fatalf("unknown role error = %v", err)
	}
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.get(context.Background(), "server:9123", Coordinator); !errors.Is(err, ErrClosed) {
		t.Fatalf("get after Close error = %v", err)
	}
}

func TestConnectionManagerCancelsWaitingDial(t *testing.T) {
	manager := newConnectionManager(config{})
	key := connectionKey{id: 1, address: "tablet:9123", role: TabletServer}
	manager.flights[key] = &connectionFlight{done: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := manager.getNode(ctx, Node{ID: 1, Address: "tablet:9123", Role: TabletServer}); !errors.Is(err, context.Canceled) {
		t.Fatalf("waiting get error = %v, want context.Canceled", err)
	}
}

func TestWaitRetryHandlesImmediateDelayAndCancellation(t *testing.T) {
	if err := waitRetry(context.Background(), 0); err != nil {
		t.Fatalf("waitRetry(0) error = %v", err)
	}
	if err := waitRetry(context.Background(), time.Millisecond); err != nil {
		t.Fatalf("waitRetry(delay) error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := waitRetry(ctx, time.Hour); !errors.Is(err, context.Canceled) {
		t.Fatalf("waitRetry(canceled) error = %v", err)
	}
	if err := waitRetry(ctx, 0); !errors.Is(err, context.Canceled) {
		t.Fatalf("waitRetry(canceled, zero) error = %v", err)
	}
}

func TestConnectionManagerDialAndAuthenticatorFailures(t *testing.T) {
	dialFailure := newConnectionManager(config{
		dialContext: func(context.Context, string, string) (net.Conn, error) { return nil, errors.New("dial failed") },
	})
	if _, err := dialFailure.open(context.Background(), "server:9123", Coordinator); err == nil {
		t.Fatal("open() dial failure error = nil")
	}

	clientConn, serverConn := net.Pipe()
	done := serveVersions(t, serverConn, Coordinator)
	authFailure := newConnectionManager(config{
		name: "test", version: "1",
		dialContext: func(context.Context, string, string) (net.Conn, error) { return clientConn, nil },
		authFactory: func() (Authenticator, error) { return nil, errors.New("factory failed") },
	})
	if _, err := authFailure.get(context.Background(), "server:9123", Coordinator); !errors.Is(err, ErrAuthentication) {
		t.Fatalf("auth factory error = %v", err)
	}
	<-done
}

func TestClientRequestToUsesTabletConnection(t *testing.T) {
	coordinatorClient, coordinatorServer := net.Pipe()
	coordinatorDone := serveVersions(t, coordinatorServer, Coordinator)
	tabletClient, tabletServer := net.Pipe()
	tabletDone := serveVersionThenRequest(t, tabletServer, TabletServer, fmsg.APIKeyApiVersions)
	client, err := Open(context.Background(),
		WithSeedBrokers("coordinator:9123"),
		WithDialContext(func(_ context.Context, _, address string) (net.Conn, error) {
			switch address {
			case "coordinator:9123":
				return coordinatorClient, nil
			case "tablet:9123":
				return tabletClient, nil
			default:
				return nil, fmt.Errorf("unexpected address %s", address)
			}
		}),
	)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	request, err := apiVersionsRequest()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.RequestTo(context.Background(), Node{ID: 7, Address: "tablet:9123", Role: TabletServer}, request); err != nil {
		t.Fatalf("RequestTo() error = %v", err)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	<-coordinatorDone
	<-tabletDone
}

func TestClientRequestBucketRefreshesStaleLeader(t *testing.T) {
	firstClient, firstServer := net.Pipe()
	firstDone := serveVersionThenRemoteError(t, firstServer, TabletServer, fmsg.ErrorCodeNotLeaderOrFollower)
	secondClient, secondServer := net.Pipe()
	secondDone := serveVersionThenRequest(t, secondServer, TabletServer, fmsg.APIKeyApiVersions)
	dials := map[string]int{}
	manager := newConnectionManager(config{
		name: "test", version: "1",
		dialContext: func(_ context.Context, _, address string) (net.Conn, error) {
			dials[address]++
			switch address {
			case "tablet-1:9123":
				return firstClient, nil
			case "tablet-2:9123":
				return secondClient, nil
			default:
				return nil, fmt.Errorf("unexpected address %s", address)
			}
		},
	})
	path := PhysicalTablePath{TablePath: TablePath{Database: "db", Table: "events"}, Partition: "day=2026-07-30"}
	leader := 0
	router := NewRouter(Node{}, func(context.Context, TablePath) (TableMetadata, error) {
		return TableMetadata{Path: path.TablePath}, nil
	}).WithPhysicalMetadataFetcher(func(context.Context, PhysicalTablePath) (PartitionMetadata, error) {
		leader++
		return PartitionMetadata{Path: path, Buckets: map[int32]Node{0: {ID: int32(leader), Address: fmt.Sprintf("tablet-%d:9123", leader), Role: TabletServer}}}, nil
	})
	client := &Client{manager: manager, router: router}
	request, err := apiVersionsRequest()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.RequestBucket(context.Background(), path, 0, request); err != nil {
		t.Fatalf("RequestBucket() error = %v", err)
	}
	if dials["tablet-1:9123"] != 1 || dials["tablet-2:9123"] != 1 {
		t.Fatalf("dials = %#v", dials)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	<-firstDone
	<-secondDone
}

func TestConnectionManagerRedialsDisconnectedServer(t *testing.T) {
	firstClient, firstServer := net.Pipe()
	disconnect := make(chan struct{})
	firstDone := serveThenDisconnect(t, firstServer, TabletServer, disconnect)
	secondClient, secondServer := net.Pipe()
	secondDone := serveVersionThenRequest(t, secondServer, TabletServer, fmsg.APIKeyApiVersions)
	dials := 0
	manager := newConnectionManager(config{
		name: "test", version: "1",
		dialContext: func(context.Context, string, string) (net.Conn, error) {
			dials++
			if dials == 1 {
				return firstClient, nil
			}
			return secondClient, nil
		},
	})
	node := Node{ID: 7, Address: "tablet:9123", Role: TabletServer}
	if _, err := manager.getNode(context.Background(), node); err != nil {
		t.Fatalf("getNode() error = %v", err)
	}
	close(disconnect)
	<-firstDone
	request, _ := apiVersionsRequest()
	if _, err := manager.request(context.Background(), node, request); err == nil {
		t.Fatal("request to disconnected server error = nil")
	}
	request, _ = apiVersionsRequest()
	if _, err := manager.request(context.Background(), node, request); err != nil {
		t.Fatalf("request after redial error = %v", err)
	}
	if got, want := dials, 2; got != want {
		t.Fatalf("dial calls = %d, want %d", got, want)
	}
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
	<-secondDone
}

func TestConnectionManagerRetriesOnlySafeRequests(t *testing.T) {
	firstClient, firstServer := net.Pipe()
	firstDone := serveVersionThenRemoteError(t, firstServer, TabletServer, fmsg.ErrorCodeNotLeaderOrFollower)
	secondClient, secondServer := net.Pipe()
	secondDone := serveVersionThenRequest(t, secondServer, TabletServer, fmsg.APIKeyApiVersions)
	dials := 0
	metrics := &metricRecorder{}
	manager := newConnectionManager(config{
		name: "test", version: "1", retry: RetryPolicy{MaxAttempts: 2, Backoff: func(int) time.Duration { return 0 }},
		observer: metrics,
		dialContext: func(context.Context, string, string) (net.Conn, error) {
			dials++
			if dials == 1 {
				return firstClient, nil
			}
			return secondClient, nil
		},
	})
	node := Node{ID: 7, Address: "tablet:9123", Role: TabletServer}
	request, _ := apiVersionsRequest()
	if _, err := manager.request(context.Background(), node, request); err != nil {
		t.Fatalf("retry request error = %v", err)
	}
	if got, want := dials, 2; got != want {
		t.Fatalf("dial calls = %d, want %d", got, want)
	}
	if event, ok := metrics.find(MetricRetry, MetricOperationRPC); !ok ||
		event.APIKey != fmsg.APIKeyApiVersions || event.Attempt != 2 || !event.Failed {
		t.Fatalf("retry metric = %#v, found=%v", event, ok)
	}
	if event, ok := metrics.find(MetricRemoteIO, MetricOperationDial); !ok ||
		event.ServerRole != TabletServer || event.Failed {
		t.Fatalf("dial metric = %#v, found=%v", event, ok)
	}
	if safeToRetry(fmsg.APIKeyCreateDatabase) {
		t.Fatal("create database must not be retried automatically")
	}
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
	<-firstDone
	<-secondDone
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
	if got, want := UnknownServerRole.String(), "unknown"; got != want {
		t.Fatalf("unknown role string = %q, want %q", got, want)
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
		body, err := versionResponse(role)
		if err != nil {
			t.Error(err)
			return
		}
		writeTransportResponse(t, conn, id, body)
		_, _ = io.Copy(io.Discard, conn)
	}()
	return done
}

func serveVersionThenRequest(t *testing.T, conn net.Conn, role ServerRole, expected fmsg.APIKey) <-chan struct{} {
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
		body, err := versionResponse(role)
		if err != nil {
			t.Error(err)
			return
		}
		writeTransportResponse(t, conn, id, body)
		id, key, _ = readTransportRequest(t, conn)
		if key != expected {
			t.Errorf("API key = %d, want %d", key, expected)
			return
		}
		if expected == fmsg.APIKeyApiVersions {
			body, err = versionResponse(role)
			if err != nil {
				t.Error(err)
				return
			}
		}
		writeTransportResponse(t, conn, id, body)
		_, _ = io.Copy(io.Discard, conn)
	}()
	return done
}

func serveAuthenticatedVersions(
	t *testing.T,
	conn net.Conn,
	role ServerRole,
	authenticationSeen chan<- struct{},
	release <-chan struct{},
) <-chan struct{} {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer conn.Close()
		id, key, _ := readTransportRequest(t, conn)
		if key != fmsg.APIKeyApiVersions {
			t.Errorf("first API key = %d, want API_VERSIONS", key)
			return
		}
		body, err := proto.Marshal(&fmsg.ApiVersionsResponse{
			ServerType: proto.Int32(int32(role)),
			ApiVersions: []*fmsg.PbApiVersion{
				apiVersion(fmsg.APIKeyApiVersions, 0, 0),
				apiVersion(fmsg.APIKeyAuthenticate, 0, 0),
			},
		})
		if err != nil {
			t.Error(err)
			return
		}
		writeTransportResponse(t, conn, id, body)
		id, key, _ = readTransportRequest(t, conn)
		if key != fmsg.APIKeyAuthenticate {
			t.Errorf("second API key = %d, want AUTHENTICATE", key)
			return
		}
		if authenticationSeen != nil {
			close(authenticationSeen)
		}
		<-release
		writeTransportResponse(t, conn, id, nil)
		_, _ = io.Copy(io.Discard, conn)
	}()
	return done
}

func serveThenDisconnect(t *testing.T, conn net.Conn, role ServerRole, disconnect <-chan struct{}) <-chan struct{} {
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
		body, err := versionResponse(role)
		if err != nil {
			t.Error(err)
			return
		}
		writeTransportResponse(t, conn, id, body)
		<-disconnect
	}()
	return done
}

func serveVersionThenRemoteError(t *testing.T, conn net.Conn, role ServerRole, code fmsg.ErrorCode) <-chan struct{} {
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
		body, err := versionResponse(role)
		if err != nil {
			t.Error(err)
			return
		}
		writeTransportResponse(t, conn, id, body)
		id, _, _ = readTransportRequest(t, conn)
		codeValue := int32(code)
		body, err = proto.Marshal(&fmsg.ErrorResponse{ErrorCode: &codeValue})
		if err != nil {
			t.Error(err)
			return
		}
		writeTransportError(t, conn, id, body)
		_, _ = io.Copy(io.Discard, conn)
	}()
	return done
}

func versionResponse(role ServerRole) ([]byte, error) {
	apiKey := int32(fmsg.APIKeyApiVersions)
	minimum, maximum, roleValue := int32(0), int32(0), int32(role)
	return proto.Marshal(&fmsg.ApiVersionsResponse{
		ServerType: &roleValue,
		ApiVersions: []*fmsg.PbApiVersion{{
			ApiKey: &apiKey, MinVersion: &minimum, MaxVersion: &maximum,
		}},
	})
}

func apiVersionsRequest() (*fmsg.MessageRequest, error) {
	request, err := fmsg.NewRequest(fmsg.APIKeyApiVersions, 0)
	if err != nil {
		return nil, err
	}
	message := request.Message().(*fmsg.ApiVersionsRequest)
	message.ClientSoftwareName = proto.String("test")
	message.ClientSoftwareVersion = proto.String("1")
	return request, nil
}

func writeTransportError(t *testing.T, conn net.Conn, id int32, body []byte) {
	t.Helper()
	frame := make([]byte, 9+len(body))
	binary.BigEndian.PutUint32(frame, uint32(5+len(body)))
	frame[4] = 1
	binary.BigEndian.PutUint32(frame[5:], uint32(id))
	copy(frame[9:], body)
	if _, err := conn.Write(frame); err != nil {
		t.Error(err)
	}
}
