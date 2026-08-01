package fgo

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pletorco/fluss-go/internal/transport"
	"github.com/pletorco/fluss-go/pkg/fmsg"
	"google.golang.org/protobuf/proto"
)

type requesterFunc func(context.Context, fmsg.Request) (fmsg.Response, error)

func (f requesterFunc) Request(ctx context.Context, request fmsg.Request) (fmsg.Response, error) {
	return f(ctx, request)
}

func TestOpenRejectsInvalidConfigBeforeDial(t *testing.T) {
	called := false
	_, err := Open(context.Background(), WithDialContext(func(context.Context, string, string) (net.Conn, error) {
		called = true
		return nil, nil
	}))
	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("Open() error = %v, want ErrInvalidConfig", err)
	}
	if called {
		t.Fatal("Open() dialed invalid configuration")
	}
}

func TestOpenRejectsNilOption(t *testing.T) {
	if _, err := Open(context.Background(), nil); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("Open(nil option) error = %v, want ErrInvalidConfig", err)
	}
}

func TestClientNegotiatesAndAppliesVersion(t *testing.T) {
	var seenVersion int16
	requester := requesterFunc(func(_ context.Context, request fmsg.Request) (fmsg.Response, error) {
		if request.APIKey() == fmsg.APIKeyApiVersions {
			message := request.(*fmsg.MessageRequest).Message().(*fmsg.ApiVersionsRequest)
			if got, want := message.GetClientSoftwareName()+":"+message.GetClientSoftwareVersion(), "test-client:1.0"; got != want {
				t.Fatalf("client identity = %q, want %q", got, want)
			}
			response, _ := fmsg.NewResponse(fmsg.APIKeyApiVersions, 0)
			responseMessage := response.Message().(*fmsg.ApiVersionsResponse)
			responseMessage.ApiVersions = []*fmsg.PbApiVersion{
				apiVersion(fmsg.APIKeyApiVersions, 0, 0),
				apiVersion(fmsg.APIKeyLookup, 0, 1),
			}
			return response, nil
		}
		seenVersion = request.Version()
		response, _ := fmsg.NewResponse(request.APIKey(), request.Version())
		return response, nil
	})
	client := newClient(requester, nil)
	if err := client.negotiate(context.Background(), "test-client", "1.0"); err != nil {
		t.Fatalf("negotiate() error = %v", err)
	}
	request, err := fmsg.NewRequest(fmsg.APIKeyLookup, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Request(context.Background(), request); err != nil {
		t.Fatalf("Request() error = %v", err)
	}
	if got, want := seenVersion, int16(1); got != want {
		t.Fatalf("request version = %d, want %d", got, want)
	}
}

func TestClientCloseIsIdempotent(t *testing.T) {
	calls := 0
	client := newClient(requesterFunc(func(context.Context, fmsg.Request) (fmsg.Response, error) {
		return nil, nil
	}), func() error {
		calls++
		return nil
	})
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	if got, want := calls, 1; got != want {
		t.Fatalf("close calls = %d, want %d", got, want)
	}
	request, _ := fmsg.NewRequest(fmsg.APIKeyApiVersions, 0)
	if _, err := client.Request(context.Background(), request); !errors.Is(err, ErrClosed) {
		t.Fatalf("Request after Close error = %v, want ErrClosed", err)
	}
}

func TestClientCloseIsConcurrentSafe(t *testing.T) {
	closed := make(chan struct{}, 16)
	client := newClient(nil, func() error {
		closed <- struct{}{}
		return nil
	})
	var wait sync.WaitGroup
	errs := make(chan error, 16)
	for range 16 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			errs <- client.Close()
		}()
	}
	wait.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if len(closed) != 1 {
		t.Fatalf("transport close calls = %d, want 1", len(closed))
	}
	request, _ := fmsg.NewRequest(fmsg.APIKeyApiVersions, 0)
	if _, err := client.Request(context.Background(), request); !errors.Is(err, ErrClosed) {
		t.Fatalf("Request() after concurrent Close error = %v", err)
	}
}

func TestClientCloseRejectsPublicOperationsAndResourceConstruction(t *testing.T) {
	providerCalls := 0
	client := newClient(nil, nil)
	client.snapshotProvider = SnapshotBatchProviderFunc(func(
		context.Context,
		SnapshotBatchRequest,
	) (SnapshotBatchReader, error) {
		providerCalls++
		return &fakeSnapshotBatchReader{}, nil
	})
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	request, _ := fmsg.NewRequest(fmsg.APIKeyApiVersions, 0)
	path := PhysicalTablePath{TablePath: TablePath{Database: "db", Table: "events"}}
	table := kvWriterTable()
	bucket := testTableBucket(table)
	checks := map[string]func() error{
		"request": func() error {
			_, err := client.Request(context.Background(), request)
			return err
		},
		"request to": func() error {
			_, err := client.RequestTo(context.Background(), Node{Address: "server:9123", Role: Coordinator}, request)
			return err
		},
		"request coordinator": func() error {
			_, err := client.RequestCoordinator(context.Background(), request)
			return err
		},
		"request bucket": func() error {
			_, err := client.RequestBucket(context.Background(), path, 0, request)
			return err
		},
		"open table": func() error {
			_, err := client.OpenTable(context.Background(), path.TablePath)
			return err
		},
		"resolve buckets": func() error {
			_, err := client.ResolveTableBuckets(context.Background(), path)
			return err
		},
		"log writer": func() error {
			_, err := client.NewLogWriter(context.Background(), logWriterTable())
			return err
		},
		"KV writer": func() error {
			_, err := client.NewKVWriter(context.Background(), table)
			return err
		},
		"lookup": func() error {
			_, err := client.NewLookupClient(context.Background(), table)
			return err
		},
		"log scanner": func() error {
			_, err := client.NewLogScanner(context.Background(), logWriterTable(), AtOffset(0))
			return err
		},
		"batch scanner": func() error {
			_, err := client.NewBatchScanner(context.Background(), table, bucket)
			return err
		},
		"snapshot scanner": func() error {
			_, err := client.NewSnapshotBatchScanner(context.Background(), table, bucket, 1)
			return err
		},
	}
	for name, check := range checks {
		t.Run(name, func(t *testing.T) {
			if err := check(); !errors.Is(err, ErrClosed) {
				t.Fatalf("error = %v, want ErrClosed", err)
			}
		})
	}
	if providerCalls != 0 {
		t.Fatalf("snapshot provider calls after Close = %d", providerCalls)
	}
}

func TestClientOptions(t *testing.T) {
	var cfg config
	if err := WithSeedBrokers("a:1", "b:2")(&cfg); err != nil || len(cfg.seeds) != 2 {
		t.Fatalf("WithSeedBrokers() = %#v, %v", cfg, err)
	}
	if err := WithClientIdentity("client", "1")(&cfg); err != nil || cfg.name != "client" {
		t.Fatalf("WithClientIdentity() = %#v, %v", cfg, err)
	}
	if err := WithDialTimeout(time.Second)(&cfg); err != nil || cfg.timeout != time.Second {
		t.Fatalf("WithDialTimeout() = %#v, %v", cfg, err)
	}
	if err := WithTLSConfig(&tls.Config{ServerName: "example.test"})(&cfg); err != nil || cfg.tlsConfig.ServerName != "example.test" {
		t.Fatalf("WithTLSConfig() = %#v, %v", cfg, err)
	}
	if err := WithTransportLimits(transport.Config{MaxFrameSize: 5})(&cfg); err != nil {
		t.Fatal(err)
	}
	if err := WithAuthenticator(func() (Authenticator, error) {
		return &testAuthenticator{protocol: "test", completeAfter: 1}, nil
	})(&cfg); err != nil || cfg.authFactory == nil {
		t.Fatalf("WithAuthenticator() = %#v, %v", cfg, err)
	}
	if err := WithRetryPolicy(RetryPolicy{MaxAttempts: 2})(&cfg); err != nil ||
		cfg.retry.MaxAttempts != 2 || cfg.retry.Backoff(1) != 0 {
		t.Fatalf("WithRetryPolicy() = %#v, %v", cfg.retry, err)
	}
	observer := MetricsObserverFunc(func(MetricEvent) {})
	if err := WithMetricsObserver(observer)(&cfg); err != nil || cfg.observer == nil {
		t.Fatalf("WithMetricsObserver() = %#v, %v", cfg.observer, err)
	}
}

func TestClientOptionsRejectInvalidValues(t *testing.T) {
	var cfg config
	for _, option := range []Option{
		WithSeedBrokers(),
		WithClientIdentity("", ""),
		WithDialContext(nil),
		WithTLSConfig(nil),
		WithAuthenticator(nil),
		WithDialTimeout(0),
		WithTransportLimits(transport.Config{MaxFrameSize: 1}),
		WithTransportLimits(transport.Config{MaxInFlight: -1}),
		WithRetryPolicy(RetryPolicy{}),
		WithMetricsObserver(nil),
	} {
		if err := option(&cfg); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("option error = %v, want ErrInvalidConfig", err)
		}
	}
}

func TestClientRequestErrors(t *testing.T) {
	client := newClient(requesterFunc(func(context.Context, fmsg.Request) (fmsg.Response, error) {
		return nil, errors.New("unreachable")
	}), nil)
	request, _ := fmsg.NewRequest(fmsg.APIKeyLookup, 0)
	if _, err := client.Request(context.Background(), request); !errors.Is(err, ErrUnsupportedAPI) {
		t.Fatalf("Request unsupported error = %v", err)
	}
	client.versions[fmsg.APIKeyLookup] = 1
	if _, err := client.Request(context.Background(), request); err == nil {
		t.Fatal("Request error = nil")
	}
	if client.Requester() != client {
		t.Fatal("Requester() did not return client")
	}
	if _, err := client.Request(context.Background(), nil); !errors.Is(err, fmsg.ErrInvalidArgument) {
		t.Fatalf("Request(nil) error = %v, want ErrInvalidArgument", err)
	}
	if _, err := client.RequestTo(
		context.Background(), Node{Address: "tablet:9123", Role: TabletServer}, request,
	); !errors.Is(err, ErrClosed) {
		t.Fatalf("RequestTo() unmanaged error = %v, want ErrClosed", err)
	}
}

func TestClientRequestUsesManagedConnection(t *testing.T) {
	node := Node{ID: 2, Address: "tablet:9123", Role: TabletServer}
	tablet := newClient(requesterFunc(func(_ context.Context, request fmsg.Request) (fmsg.Response, error) {
		return fmsg.NewResponse(request.APIKey(), request.Version())
	}), nil)
	tablet.versions[fmsg.APIKeyApiVersions] = 0
	manager := newConnectionManager(config{})
	manager.clients[connectionKey{id: node.ID, address: node.Address, role: node.Role}] = tablet
	client := &Client{
		manager: manager, serverID: node.ID, address: node.Address, role: node.Role,
	}
	request := apiVersionsRequestForClient(t)
	if _, err := client.Request(context.Background(), request); err != nil {
		t.Fatalf("Request() error = %v", err)
	}
	if _, err := client.Request(context.Background(), nil); !errors.Is(err, fmsg.ErrInvalidArgument) {
		t.Fatalf("managed Request(nil) error = %v", err)
	}
	if _, err := client.RequestTo(context.Background(), node, nil); !errors.Is(err, fmsg.ErrInvalidArgument) {
		t.Fatalf("managed RequestTo(nil) error = %v", err)
	}
}

func apiVersionsRequestForClient(t *testing.T) *fmsg.MessageRequest {
	t.Helper()
	request, err := fmsg.NewRequest(fmsg.APIKeyApiVersions, 0)
	if err != nil {
		t.Fatal(err)
	}
	message := request.Message().(*fmsg.ApiVersionsRequest)
	message.ClientSoftwareName = proto.String("test")
	message.ClientSoftwareVersion = proto.String("test")
	return request
}

func TestAuthenticateDoesNotExposeToken(t *testing.T) {
	token := []byte("top-secret")
	requests := 0
	requester := requesterFunc(func(_ context.Context, request fmsg.Request) (fmsg.Response, error) {
		requests++
		message := request.(*fmsg.MessageRequest).Message().(*fmsg.AuthenticateRequest)
		if message.GetProtocol() != "test" || string(message.GetToken()) != string(token) {
			t.Fatalf("authenticate request = protocol %q token %q", message.GetProtocol(), message.GetToken())
		}
		response, _ := fmsg.NewResponse(request.APIKey(), request.Version())
		return response, nil
	})
	client := newClient(requester, nil)
	client.versions[fmsg.APIKeyAuthenticate] = 0
	err := client.authenticate(context.Background(), &testAuthenticator{protocol: "test", initial: true, tokens: [][]byte{token}, completeAfter: 1})
	if err != nil {
		t.Fatalf("authenticate() error = %v", err)
	}
	if requests != 1 {
		t.Fatalf("authenticate requests = %d, want 1", requests)
	}
}

func TestAuthenticateAcceptsFinalChallengeAfterClientCompletion(t *testing.T) {
	for _, challenge := range [][]byte{{}, []byte("server-final-data")} {
		requests := 0
		client := newClient(requesterFunc(func(_ context.Context, request fmsg.Request) (fmsg.Response, error) {
			requests++
			response, _ := fmsg.NewResponse(request.APIKey(), request.Version())
			response.Message().(*fmsg.AuthenticateResponse).Challenge = challenge
			return response, nil
		}), nil)
		client.versions[fmsg.APIKeyAuthenticate] = 0
		auth := &testAuthenticator{
			protocol: "PLAIN", initial: true,
			tokens: [][]byte{[]byte("\x00alice\x00secret")}, completeAfter: 1,
		}
		if err := client.authenticate(context.Background(), auth); err != nil {
			t.Fatalf("challenge %q: authenticate() error = %v", challenge, err)
		}
		if requests != 1 || auth.calls != 1 {
			t.Fatalf("challenge %q: requests = %d, authenticator calls = %d", challenge, requests, auth.calls)
		}
	}
}

func TestAuthenticateCompletesMultiStepExchange(t *testing.T) {
	var got [][]byte
	requester := requesterFunc(func(_ context.Context, request fmsg.Request) (fmsg.Response, error) {
		got = append(got, append([]byte(nil), request.(*fmsg.MessageRequest).Message().(*fmsg.AuthenticateRequest).GetToken()...))
		response, _ := fmsg.NewResponse(request.APIKey(), request.Version())
		if len(got) < 3 {
			response.Message().(*fmsg.AuthenticateResponse).Challenge = []byte("challenge-" + string(rune('0'+len(got))))
		}
		return response, nil
	})
	client := newClient(requester, nil)
	client.versions[fmsg.APIKeyAuthenticate] = 0
	auth := &testAuthenticator{
		protocol: "test", initial: true,
		tokens:        [][]byte{[]byte("initial"), []byte("reply-1"), []byte("reply-2")},
		completeAfter: 3,
	}
	if err := client.authenticate(context.Background(), auth); err != nil {
		t.Fatalf("authenticate() error = %v", err)
	}
	if got, want := string(got[0])+":"+string(got[1])+":"+string(got[2]), "initial:reply-1:reply-2"; got != want {
		t.Fatalf("tokens = %q, want %q", got, want)
	}
}

func TestAuthenticateWaitsForServerChallengeWhenNeeded(t *testing.T) {
	requests := 0
	requester := requesterFunc(func(_ context.Context, request fmsg.Request) (fmsg.Response, error) {
		requests++
		response, _ := fmsg.NewResponse(request.APIKey(), request.Version())
		if requests == 1 {
			if got := request.(*fmsg.MessageRequest).Message().(*fmsg.AuthenticateRequest).GetToken(); len(got) != 0 {
				t.Fatalf("initial token = %q, want empty", got)
			}
			response.Message().(*fmsg.AuthenticateResponse).Challenge = []byte("challenge")
		}
		return response, nil
	})
	client := newClient(requester, nil)
	client.versions[fmsg.APIKeyAuthenticate] = 0
	auth := &testAuthenticator{protocol: "test", initial: false, tokens: [][]byte{[]byte("response")}, completeAfter: 1}
	if err := client.authenticate(context.Background(), auth); err != nil {
		t.Fatalf("authenticate() error = %v", err)
	}
	if requests != 2 {
		t.Fatalf("authenticate requests = %d, want 2", requests)
	}
}

func TestAuthenticateErrors(t *testing.T) {
	unsupported := newClient(requesterFunc(func(context.Context, fmsg.Request) (fmsg.Response, error) {
		return nil, nil
	}), nil)
	if err := unsupported.authenticate(context.Background(), &testAuthenticator{protocol: "test", initial: true, tokens: [][]byte{[]byte("token")}, completeAfter: 1}); !errors.Is(err, ErrUnsupportedAPI) {
		t.Fatalf("unsupported error = %v", err)
	}
	client := newClient(requesterFunc(func(context.Context, fmsg.Request) (fmsg.Response, error) {
		return fmsg.NewResponse(fmsg.APIKeyLookup, 0)
	}), nil)
	client.versions[fmsg.APIKeyAuthenticate] = 0
	if err := client.authenticate(context.Background(), &testAuthenticator{protocol: "test", initial: true, tokens: [][]byte{[]byte("token")}, completeAfter: 1}); !errors.Is(err, ErrAuthentication) {
		t.Fatalf("unexpected response error = %v", err)
	}
	client.requester = requesterFunc(func(context.Context, fmsg.Request) (fmsg.Response, error) {
		return fmsg.NewResponse(fmsg.APIKeyAuthenticate, 0)
	})
	if err := client.authenticate(context.Background(), &testAuthenticator{protocol: "test", initial: true, tokens: [][]byte{[]byte("token")}}); !errors.Is(err, ErrAuthentication) {
		t.Fatalf("incomplete error = %v", err)
	}
	if err := client.authenticate(context.Background(), &testAuthenticator{protocol: "test", initial: true, authenticateErr: context.Canceled}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled error = %v", err)
	}
}

func TestAuthenticateClassifiesServerFailures(t *testing.T) {
	for _, test := range []struct {
		name      string
		code      fmsg.ErrorCode
		retriable bool
	}{
		{name: "permanent", code: fmsg.ErrorCodeAuthenticateException},
		{name: "retriable", code: fmsg.ErrorCodeRetriableAuthenticateException, retriable: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			client := newClient(requesterFunc(func(context.Context, fmsg.Request) (fmsg.Response, error) {
				return nil, &transport.RemoteError{Code: int32(test.code), Message: "safe server message"}
			}), nil)
			client.address = "coordinator:9123"
			client.versions[fmsg.APIKeyAuthenticate] = 0
			err := client.authenticate(context.Background(), &testAuthenticator{
				protocol: "test", initial: true, tokens: [][]byte{[]byte("secret")}, completeAfter: 1,
			})
			var authErr *AuthenticationError
			var serverErr *ServerError
			if !errors.As(err, &authErr) || authErr.Retriable != test.retriable ||
				!errors.As(err, &serverErr) || serverErr.Code != test.code ||
				strings.Contains(err.Error(), "secret") {
				t.Fatalf("authenticate error = %#v, server = %#v", authErr, serverErr)
			}
		})
	}
}

func TestOpenNegotiatesOverTransport(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer serverConn.Close()
	go func() {
		id := readTransportRequestID(t, serverConn)
		key := int32(fmsg.APIKeyApiVersions)
		minimum := int32(0)
		maximum := int32(0)
		role := int32(Coordinator)
		body, err := proto.Marshal(&fmsg.ApiVersionsResponse{ServerType: &role, ApiVersions: []*fmsg.PbApiVersion{{
			ApiKey: &key, MinVersion: &minimum, MaxVersion: &maximum,
		}}})
		if err != nil {
			t.Error(err)
			return
		}
		writeTransportResponse(t, serverConn, id, body)
	}()
	client, err := Open(context.Background(),
		WithSeedBrokers("seed:9123"),
		WithClientIdentity("coverage-test", "1.0"),
		WithDialContext(func(context.Context, string, string) (net.Conn, error) { return clientConn, nil }),
	)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestOpenAuthenticatesWithPlain(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer serverConn.Close()
	var auth *testAuthenticator
	go func() {
		id, key, _ := readTransportRequest(t, serverConn)
		if key != fmsg.APIKeyApiVersions {
			t.Errorf("first API key = %d, want API_VERSIONS", key)
			return
		}
		apiKey := int32(fmsg.APIKeyApiVersions)
		minimum, maximum := int32(0), int32(0)
		authenticateKey := int32(fmsg.APIKeyAuthenticate)
		role := int32(Coordinator)
		body, err := proto.Marshal(&fmsg.ApiVersionsResponse{ServerType: &role, ApiVersions: []*fmsg.PbApiVersion{
			{ApiKey: &apiKey, MinVersion: &minimum, MaxVersion: &maximum},
			{ApiKey: &authenticateKey, MinVersion: &minimum, MaxVersion: &maximum},
		}})
		if err != nil {
			t.Error(err)
			return
		}
		writeTransportResponse(t, serverConn, id, body)

		id, key, body = readTransportRequest(t, serverConn)
		if key != fmsg.APIKeyAuthenticate {
			t.Errorf("second API key = %d, want AUTHENTICATE", key)
			return
		}
		request := &fmsg.AuthenticateRequest{}
		if err := proto.Unmarshal(body, request); err != nil {
			t.Error(err)
			return
		}
		if got, want := request.GetProtocol()+":"+string(request.GetToken()), "PLAIN:\x00alice\x00secret"; got != want {
			t.Errorf("authenticate request = %q, want %q", got, want)
		}
		writeTransportResponse(t, serverConn, id, nil)
	}()
	client, err := Open(context.Background(),
		WithSeedBrokers("seed:9123"),
		WithDialContext(func(context.Context, string, string) (net.Conn, error) { return clientConn, nil }),
		WithAuthenticator(func() (Authenticator, error) {
			auth = &testAuthenticator{protocol: "PLAIN", initial: true, tokens: [][]byte{[]byte("\x00alice\x00secret")}, completeAfter: 1}
			return auth, nil
		}),
	)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	if auth == nil || !auth.closed {
		t.Fatal("Open() did not close the connection authenticator")
	}
}

func TestOpenDialFailure(t *testing.T) {
	_, err := Open(context.Background(), WithSeedBrokers("seed:9123"), WithDialContext(func(context.Context, string, string) (net.Conn, error) {
		return nil, errors.New("dial failure")
	}))
	if err == nil {
		t.Fatal("Open() error = nil")
	}
}

func TestNegotiationAndAuthenticationErrors(t *testing.T) {
	unsupported := newClient(requesterFunc(func(context.Context, fmsg.Request) (fmsg.Response, error) {
		return fmsg.NewResponse(fmsg.APIKeyApiVersions, 0)
	}), nil)
	if err := unsupported.negotiate(context.Background(), "test", "1"); !errors.Is(err, ErrUnsupportedAPI) {
		t.Fatalf("negotiate unsupported error = %v", err)
	}
	unexpected := newClient(requesterFunc(func(context.Context, fmsg.Request) (fmsg.Response, error) {
		return fmsg.NewResponse(fmsg.APIKeyLookup, 0)
	}), nil)
	if err := unexpected.negotiate(context.Background(), "test", "1"); err == nil {
		t.Fatal("negotiate unexpected response error = nil")
	}
	client := newClient(requesterFunc(func(context.Context, fmsg.Request) (fmsg.Response, error) {
		response, _ := fmsg.NewResponse(fmsg.APIKeyAuthenticate, 0)
		return response, nil
	}), nil)
	client.versions[fmsg.APIKeyAuthenticate] = 0
	if err := client.authenticate(context.Background(), &testAuthenticator{protocol: "test", initial: true, authenticateErr: errors.New("authentication failure")}); err == nil {
		t.Fatal("authenticate callback error = nil")
	}
	if min(2, 1) != 1 {
		t.Fatal("min() returned the wrong right value")
	}
	if min(1, 2) != 1 {
		t.Fatal("min() returned the wrong left value")
	}
}

func TestAuthenticationResponseTokenFailures(t *testing.T) {
	authFailure := &testAuthenticator{authenticateErr: errTestAuthentication}
	if _, _, err := authenticationResponseToken(context.Background(), authFailure, []byte("challenge")); !errors.Is(err, errTestAuthentication) {
		t.Fatalf("authenticationResponseToken() error = %v", err)
	}

	complete := &testAuthenticator{tokens: [][]byte{nil}, completeAfter: 1}
	if token, done, err := authenticationResponseToken(context.Background(), complete, nil); err != nil || token != nil || !done {
		t.Fatalf("completed response = %q, %t, %v", token, done, err)
	}

	incomplete := &testAuthenticator{}
	if _, _, err := authenticationResponseToken(context.Background(), incomplete, nil); !errors.Is(err, ErrAuthentication) {
		t.Fatalf("missing response error = %v, want ErrAuthentication", err)
	}
	if err := authenticationError(nil, true); err != nil {
		t.Fatalf("authenticationError(nil) = %v", err)
	}
}

func TestCloseAllPreservesFirstFailureAndRunsEveryCloser(t *testing.T) {
	first := errors.New("first close")
	second := errors.New("second close")
	calls := 0
	close := closeAll(
		nil,
		func() error { calls++; return first },
		func() error { calls++; return second },
		func() error { calls++; return nil },
	)
	if err := close(); !errors.Is(err, first) {
		t.Fatalf("closeAll() error = %v, want first error", err)
	}
	if calls != 3 {
		t.Fatalf("close calls = %d, want 3", calls)
	}
}

var errTestAuthentication = errors.New("test authentication failure")

type testAuthenticator struct {
	protocol        string
	initial         bool
	tokens          [][]byte
	completeAfter   int
	authenticateErr error
	calls           int
	closed          bool
}

func (a *testAuthenticator) Protocol() string         { return a.protocol }
func (a *testAuthenticator) HasInitialResponse() bool { return a.initial }
func (a *testAuthenticator) Complete() bool           { return a.completeAfter > 0 && a.calls >= a.completeAfter }
func (a *testAuthenticator) Close() error             { a.closed = true; return nil }

func (a *testAuthenticator) Authenticate(_ context.Context, _ []byte) ([]byte, error) {
	if a.authenticateErr != nil {
		return nil, a.authenticateErr
	}
	if a.calls >= len(a.tokens) {
		return nil, nil
	}
	token := append([]byte(nil), a.tokens[a.calls]...)
	a.calls++
	return token, nil
}

func apiVersion(key fmsg.APIKey, minVersion, maxVersion int32) *fmsg.PbApiVersion {
	keyValue := int32(key)
	return &fmsg.PbApiVersion{ApiKey: &keyValue, MinVersion: &minVersion, MaxVersion: &maxVersion}
}

func readTransportRequestID(t *testing.T, conn net.Conn) int32 {
	id, _, _ := readTransportRequest(t, conn)
	return id
}

func readTransportRequest(t *testing.T, conn net.Conn) (int32, fmsg.APIKey, []byte) {
	t.Helper()
	var size [4]byte
	if _, err := io.ReadFull(conn, size[:]); err != nil {
		t.Error(err)
		return 0, 0, nil
	}
	frame := make([]byte, binary.BigEndian.Uint32(size[:]))
	if _, err := io.ReadFull(conn, frame); err != nil {
		t.Error(err)
		return 0, 0, nil
	}
	if len(frame) < 8 {
		t.Errorf("request frame is too short: %d", len(frame))
		return 0, 0, nil
	}
	return int32(binary.BigEndian.Uint32(frame[4:8])), fmsg.APIKey(binary.BigEndian.Uint16(frame[:2])), append([]byte(nil), frame[8:]...)
}

func writeTransportResponse(t *testing.T, conn net.Conn, id int32, body []byte) {
	t.Helper()
	frame := make([]byte, 9+len(body))
	binary.BigEndian.PutUint32(frame, uint32(5+len(body)))
	binary.BigEndian.PutUint32(frame[5:], uint32(id))
	copy(frame[9:], body)
	if _, err := conn.Write(frame); err != nil {
		t.Error(err)
	}
}
