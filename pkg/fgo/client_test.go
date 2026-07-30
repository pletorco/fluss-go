package fgo

import (
	"context"
	"errors"
	"net"
	"testing"

	"github.com/pletorco/fluss-go/pkg/fmsg"
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

func apiVersion(key fmsg.APIKey, minVersion, maxVersion int32) *fmsg.PbApiVersion {
	keyValue := int32(key)
	return &fmsg.PbApiVersion{ApiKey: &keyValue, MinVersion: &minVersion, MaxVersion: &maxVersion}
}
