package fmsg

import (
	"bytes"
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

func TestProtocolGoldenBytes(t *testing.T) {
	tests := []struct {
		name string
		key  APIKey
		set  func(proto.Message)
		want []byte
	}{
		{
			name: "api versions request",
			key:  APIKeyApiVersions,
			set: func(message proto.Message) {
				request := message.(*ApiVersionsRequest)
				request.ClientSoftwareName = proto.String("test")
				request.ClientSoftwareVersion = proto.String("1.0")
			},
			want: []byte{0x0a, 0x04, 't', 'e', 's', 't', 0x12, 0x03, '1', '.', '0'},
		},
		{
			name: "lookup request",
			key:  APIKeyLookup,
			set: func(message proto.Message) {
				message.(*LookupRequest).TableId = proto.Int64(42)
			},
			want: []byte{0x08, 0x2a},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request, err := NewRequest(test.key, 0)
			if err != nil {
				t.Fatal(err)
			}
			test.set(request.Message())
			got, err := request.Marshal()
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, test.want) {
				t.Fatalf("wire bytes = %x, want %x", got, test.want)
			}
		})
	}

	errorCode := int32(ErrorCodeNotLeaderOrFollower)
	errorBody, err := proto.Marshal(&ErrorResponse{ErrorCode: &errorCode, ErrorMessage: proto.String("leader")})
	if err != nil {
		t.Fatal(err)
	}
	if want := []byte{0x08, 0x0c, 0x12, 0x06, 'l', 'e', 'a', 'd', 'e', 'r'}; !bytes.Equal(errorBody, want) {
		t.Fatalf("error wire bytes = %x, want %x", errorBody, want)
	}
}

func TestErrorCodeRegistry(t *testing.T) {
	errors := ErrorCodes()
	if got, want := len(errors), 65; got != want {
		t.Fatalf("ErrorCodes() length = %d, want %d", got, want)
	}
	for _, entry := range errors {
		got, ok := LookupErrorCode(int32(entry.Code))
		if !ok || got != entry {
			t.Fatalf("LookupErrorCode(%d) = %#v, %t", entry.Code, got, ok)
		}
	}
	if _, ok := LookupErrorCode(64); ok {
		t.Fatal("LookupErrorCode(64) unexpectedly succeeded")
	}
}

func TestResponseRetainsUnknownFields(t *testing.T) {
	response, err := NewResponse(APIKeyApiVersions, 0)
	if err != nil {
		t.Fatal(err)
	}
	body := []byte{0x48, 0x01}
	if err := response.Unmarshal(body); err != nil {
		t.Fatal(err)
	}
	if got := response.Message().ProtoReflect().GetUnknown(); !bytes.Equal(got, body) {
		t.Fatalf("unknown fields = %x, want %x", got, body)
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

func TestProtocolNilAndUnknownInputs(t *testing.T) {
	var request *MessageRequest
	if _, err := request.Marshal(); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("nil Marshal error = %v", err)
	}
	if err := request.SetVersion(0); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("nil SetVersion error = %v", err)
	}
	var response *MessageResponse
	if err := response.Unmarshal(nil); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("nil Unmarshal error = %v", err)
	}
	if _, err := NewResponse(APIKey(999), 0); !errors.Is(err, ErrUnknownAPIKey) {
		t.Fatalf("NewResponse unknown error = %v", err)
	}
	if _, err := NewResponse(APIKeyLookup, 2); !errors.Is(err, ErrUnsupportedVersion) {
		t.Fatalf("NewResponse unsupported error = %v", err)
	}
}

func TestRequestSetVersion(t *testing.T) {
	request, err := NewRequest(APIKeyLookup, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := request.SetVersion(1); err != nil {
		t.Fatal(err)
	}
	if got, want := request.Version(), int16(1); got != want {
		t.Fatalf("Version() = %d, want %d", got, want)
	}
	if err := request.SetVersion(2); !errors.Is(err, ErrUnsupportedVersion) {
		t.Fatalf("SetVersion(2) error = %v", err)
	}
}

func TestMessageAccessors(t *testing.T) {
	request, err := NewRequest(APIKeyLookup, 0)
	if err != nil {
		t.Fatal(err)
	}
	if request.APIKey() != APIKeyLookup || request.Version() != 0 || request.Message() == nil {
		t.Fatal("request accessors returned unexpected values")
	}
	response := request.NewResponse()
	if response.APIKey() != APIKeyLookup || response.Version() != 0 || response.Message() == nil {
		t.Fatal("response accessors returned unexpected values")
	}
	var nilRequest *MessageRequest
	if nilRequest.APIKey() != 0 || nilRequest.Version() != 0 || nilRequest.Message() != nil || nilRequest.NewResponse() != nil {
		t.Fatal("nil request accessors returned unexpected values")
	}
	var nilResponse *MessageResponse
	if nilResponse.APIKey() != 0 || nilResponse.Version() != 0 || nilResponse.Message() != nil {
		t.Fatal("nil response accessors returned unexpected values")
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
