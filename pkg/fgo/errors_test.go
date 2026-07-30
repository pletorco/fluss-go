package fgo

import (
	"errors"
	"testing"

	"github.com/pletorco/fluss-go/internal/transport"
	"github.com/pletorco/fluss-go/pkg/fmsg"
)

func TestServerErrorMapsPinnedErrorRegistry(t *testing.T) {
	for _, metadata := range fmsg.ErrorCodes() {
		err := serverError(&transport.RemoteError{Code: int32(metadata.Code), Message: "server message"}, fmsg.APIKeyLookup, "tablet:9123")
		var server *ServerError
		if !errors.As(err, &server) {
			t.Fatalf("code %d error = %T, want ServerError", metadata.Code, err)
		}
		if got, want := server.Code, metadata.Code; got != want {
			t.Fatalf("code = %d, want %d", got, want)
		}
		if got, want := server.Name, metadata.Name; got != want {
			t.Fatalf("name = %q, want %q", got, want)
		}
		if !errors.Is(server, ErrServerFailure) {
			t.Fatalf("code %d does not match ErrServerFailure", metadata.Code)
		}
	}
}

func TestServerErrorCategoriesAndRetryability(t *testing.T) {
	tests := []struct {
		code      fmsg.ErrorCode
		category  error
		retriable bool
	}{
		{fmsg.ErrorCodeAuthorizationException, ErrAuthorization, false},
		{fmsg.ErrorCodeAuthenticateException, ErrAuthentication, false},
		{fmsg.ErrorCodeRetriableAuthenticateException, ErrAuthentication, true},
		{fmsg.ErrorCodeNotLeaderOrFollower, ErrMetadata, true},
		{fmsg.ErrorCodeRequestTimeOut, ErrTimeout, true},
		{fmsg.ErrorCodeStorageException, ErrStorage, false},
		{fmsg.ErrorCodeOutOfOrderSequenceException, ErrSequence, false},
		{fmsg.ErrorCodeRecordTooLargeException, ErrRecord, false},
		{fmsg.ErrorCodeInvalidConfigException, ErrValidation, false},
	}
	for _, test := range tests {
		err := serverError(&transport.RemoteError{Code: int32(test.code)}, fmsg.APIKeyLookup, "tablet:9123")
		var server *ServerError
		if !errors.As(err, &server) || !errors.Is(err, test.category) || server.Retriable != test.retriable {
			t.Fatalf("code %d error = %#v, category %v, retriable %t", test.code, err, test.category, test.retriable)
		}
	}
}

func TestPartitionNotExistsMapsToUnknownPartition(t *testing.T) {
	err := ResponseError(
		int32(fmsg.ErrorCodePartitionNotExists),
		"partition is absent",
		fmsg.APIKeyGetMetadata,
	)
	if !errors.Is(err, ErrUnknownPartition) {
		t.Fatalf("error = %v, want ErrUnknownPartition", err)
	}
	if err := ResponseError(int32(fmsg.ErrorCodeNone), "", fmsg.APIKeyGetMetadata); err != nil {
		t.Fatalf("ResponseError(NONE) = %v", err)
	}
}

func TestServerErrorKeepsUnknownCodesInspectable(t *testing.T) {
	err := serverError(&transport.RemoteError{Code: 999, Message: "future"}, fmsg.APIKeyLookup, "tablet:9123")
	var server *ServerError
	if !errors.As(err, &server) {
		t.Fatalf("error = %T, want ServerError", err)
	}
	if server.Name != "UNKNOWN_FUTURE_ERROR" || server.Retriable {
		t.Fatalf("unknown error = %#v", server)
	}
	if !errors.Is(server, ErrValidation) {
		t.Fatalf("unknown error category = %v", server)
	}
	if got := server.Error(); got == "" {
		t.Fatal("unknown error text is empty")
	}
}

func TestServerErrorFormattingAndPassthrough(t *testing.T) {
	plain := errors.New("local failure")
	if got := serverError(plain, fmsg.APIKeyLookup, "tablet:9123"); got != plain {
		t.Fatalf("non-remote error = %v, want original", got)
	}
	server := &ServerError{Name: "NONE"}
	if got, want := server.Error(), "fgo: server failure: NONE"; got != want {
		t.Fatalf("empty message error = %q, want %q", got, want)
	}
	if got, want := ((*ServerError)(nil)).Error(), ErrServerFailure.Error(); got != want {
		t.Fatalf("nil error = %q, want %q", got, want)
	}
}
