package fmsg

import (
	"errors"
	"testing"

	"google.golang.org/protobuf/proto"
)

func TestRequestRoundTrip(t *testing.T) {
	request, err := NewRequest(APIKeyApiVersions, 0)
	if err != nil {
		t.Fatalf("NewRequest() error = %v", err)
	}
	message, ok := request.Message().(*ApiVersionsRequest)
	if !ok {
		t.Fatalf("request message type = %T", request.Message())
	}
	message.ClientSoftwareName = proto.String("fluss-go")
	message.ClientSoftwareVersion = proto.String("0.1.0")
	body, err := request.Marshal()
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	if len(body) == 0 {
		t.Fatal("Marshal() returned an empty body")
	}
	response := request.NewResponse()
	if got, want := response.APIKey(), APIKeyApiVersions; got != want {
		t.Fatalf("response API key = %d, want %d", got, want)
	}
	responseBody, err := proto.Marshal(&ApiVersionsResponse{})
	if err != nil {
		t.Fatalf("marshal response error = %v", err)
	}
	if err := response.Unmarshal(responseBody); err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}
}

func TestRequestValidation(t *testing.T) {
	_, err := NewRequest(APIKeyPutKv, 2)
	if !errors.Is(err, ErrUnsupportedVersion) {
		t.Fatalf("NewRequest(PUT_KV, 2) error = %v, want ErrUnsupportedVersion", err)
	}
	_, err = NewRequest(APIKeyUpdateMetadata, 0)
	if !errors.Is(err, ErrPrivateAPI) {
		t.Fatalf("NewRequest(UPDATE_METADATA, 0) error = %v, want ErrPrivateAPI", err)
	}
	_, err = NewRequest(APIKey(999), 0)
	if !errors.Is(err, ErrUnknownAPIKey) {
		t.Fatalf("NewRequest(999, 0) error = %v, want ErrUnknownAPIKey", err)
	}
}

func TestResponseRejectsMalformedPayload(t *testing.T) {
	response, err := NewResponse(APIKeyLookup, 1)
	if err != nil {
		t.Fatalf("NewResponse() error = %v", err)
	}
	if err := response.Unmarshal([]byte{0xff}); !errors.Is(err, ErrMalformedMessage) {
		t.Fatalf("Unmarshal() error = %v, want ErrMalformedMessage", err)
	}
}

func FuzzResponseUnmarshal(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{0xff})
	f.Add([]byte{0x08, 0x01})
	f.Fuzz(func(t *testing.T, body []byte) {
		response, err := NewResponse(APIKeyLookup, 1)
		if err != nil {
			t.Fatalf("NewResponse() error = %v", err)
		}
		_ = response.Unmarshal(body)
	})
}
