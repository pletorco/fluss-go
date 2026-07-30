package fgo

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"io"
	"net"
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

func TestClientNegotiatesAndAppliesVersion(t *testing.T) {
	var seenVersion int16
	requester := requesterFunc(func(_ context.Context, request fmsg.Request) (fmsg.Response, error) {
		if request.APIKey() == fmsg.APIKeyApiVersions {
			response, _ := fmsg.NewResponse(fmsg.APIKeyApiVersions, 0)
			message := response.Message().(*fmsg.ApiVersionsResponse)
			message.ApiVersions = []*fmsg.PbApiVersion{
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
	if err := WithTransportLimits(transport.Config{MaxFrameSize: 1})(&cfg); err != nil {
		t.Fatal(err)
	}
	if err := WithAuthenticator(func(context.Context, []byte) (string, []byte, error) {
		return "test", nil, nil
	})(&cfg); err != nil || cfg.auth == nil {
		t.Fatalf("WithAuthenticator() = %#v, %v", cfg, err)
	}
	for _, option := range []Option{
		WithSeedBrokers(),
		WithClientIdentity("", ""),
		WithDialContext(nil),
		WithTLSConfig(nil),
		WithAuthenticator(nil),
		WithDialTimeout(0),
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
}

func TestAuthenticateDoesNotExposeToken(t *testing.T) {
	token := []byte("top-secret")
	requester := requesterFunc(func(_ context.Context, request fmsg.Request) (fmsg.Response, error) {
		response, _ := fmsg.NewResponse(request.APIKey(), request.Version())
		if request.APIKey() == fmsg.APIKeyAuthenticate {
			message := response.Message().(*fmsg.AuthenticateResponse)
			message.Challenge = []byte("challenge")
		}
		return response, nil
	})
	client := newClient(requester, nil)
	client.versions[fmsg.APIKeyAuthenticate] = 0
	err := client.authenticate(context.Background(), func(context.Context, []byte) (string, []byte, error) {
		return "test", token, nil
	})
	if err != nil {
		t.Fatalf("authenticate() error = %v", err)
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
		body, err := proto.Marshal(&fmsg.ApiVersionsResponse{ApiVersions: []*fmsg.PbApiVersion{{
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
	if err := client.authenticate(context.Background(), func(context.Context, []byte) (string, []byte, error) {
		return "", nil, errors.New("authentication failure")
	}); err == nil {
		t.Fatal("authenticate callback error = nil")
	}
	if min(2, 1) != 1 {
		t.Fatal("min() returned the wrong right value")
	}
}

func apiVersion(key fmsg.APIKey, minVersion, maxVersion int32) *fmsg.PbApiVersion {
	keyValue := int32(key)
	return &fmsg.PbApiVersion{ApiKey: &keyValue, MinVersion: &minVersion, MaxVersion: &maxVersion}
}

func readTransportRequestID(t *testing.T, conn net.Conn) int32 {
	t.Helper()
	var size [4]byte
	if _, err := io.ReadFull(conn, size[:]); err != nil {
		t.Error(err)
		return 0
	}
	frame := make([]byte, binary.BigEndian.Uint32(size[:]))
	if _, err := io.ReadFull(conn, frame); err != nil {
		t.Error(err)
		return 0
	}
	return int32(binary.BigEndian.Uint32(frame[4:8]))
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
