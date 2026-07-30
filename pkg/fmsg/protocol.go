// Package fmsg contains generated Apache Fluss wire messages and protocol metadata.
package fmsg

import (
	"context"
	"errors"
	"fmt"

	"google.golang.org/protobuf/proto"
)

var (
	ErrInvalidArgument    = errors.New("fmsg: invalid argument")
	ErrUnknownAPIKey      = errors.New("fmsg: unknown API key")
	ErrUnsupportedVersion = errors.New("fmsg: unsupported API version")
	ErrPrivateAPI         = errors.New("fmsg: private API")
	ErrMalformedMessage   = errors.New("fmsg: malformed message")
)

// Request is a transport-independent Fluss request.
type Request interface {
	APIKey() APIKey
	Version() int16
	SetVersion(int16) error
	Marshal() ([]byte, error)
	NewResponse() Response
}

// Response is a transport-independent Fluss response.
type Response interface {
	APIKey() APIKey
	Version() int16
	Unmarshal([]byte) error
	Message() proto.Message
}

// Requester is the minimal contract used by typed protocol helpers and clients.
type Requester interface {
	Request(context.Context, Request) (Response, error)
}

// MessageRequest holds a generated protobuf request message and protocol metadata.
// Encoding rejects unset proto2 required fields. Decoding retains unknown fields so that a
// newer server response can round-trip through the pinned client without data loss.
type MessageRequest struct {
	api     APIMetadata
	version int16
	message proto.Message
}

// NewRequest constructs an empty generated request for a public API key.
func NewRequest(key APIKey, version int16) (*MessageRequest, error) {
	api, err := validateAPI(key, version, false)
	if err != nil {
		return nil, err
	}
	return &MessageRequest{api: api, version: version, message: newRequestProto(key)}, nil
}

// Message returns the generated protobuf message owned by the request.
func (r *MessageRequest) Message() proto.Message {
	if r == nil {
		return nil
	}
	return r.message
}

func (r *MessageRequest) APIKey() APIKey {
	if r == nil {
		return 0
	}
	return r.api.Key
}

func (r *MessageRequest) Version() int16 {
	if r == nil {
		return 0
	}
	return r.version
}

// SetVersion validates and applies a supported version for this request.
func (r *MessageRequest) SetVersion(version int16) error {
	if r == nil {
		return fmt.Errorf("%w: nil request", ErrInvalidArgument)
	}
	if _, err := validateAPI(r.api.Key, version, false); err != nil {
		return err
	}
	r.version = version
	return nil
}

func (r *MessageRequest) Marshal() ([]byte, error) {
	if r == nil || r.message == nil {
		return nil, fmt.Errorf("%w: nil request message", ErrInvalidArgument)
	}
	body, err := proto.Marshal(r.message)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrMalformedMessage, err)
	}
	return body, nil
}

func (r *MessageRequest) NewResponse() Response {
	if r == nil {
		return nil
	}
	return &MessageResponse{api: r.api, version: r.version, message: newResponseProto(r.api.Key)}
}

// MessageResponse holds a generated protobuf response message and protocol metadata.
type MessageResponse struct {
	api     APIMetadata
	version int16
	message proto.Message
}

// NewResponse constructs an empty generated response for a known API key.
func NewResponse(key APIKey, version int16) (*MessageResponse, error) {
	api, err := validateAPI(key, version, true)
	if err != nil {
		return nil, err
	}
	return &MessageResponse{api: api, version: version, message: newResponseProto(key)}, nil
}

func (r *MessageResponse) APIKey() APIKey {
	if r == nil {
		return 0
	}
	return r.api.Key
}

func (r *MessageResponse) Version() int16 {
	if r == nil {
		return 0
	}
	return r.version
}

func (r *MessageResponse) Message() proto.Message {
	if r == nil {
		return nil
	}
	return r.message
}

func (r *MessageResponse) Unmarshal(body []byte) error {
	if r == nil || r.message == nil {
		return fmt.Errorf("%w: nil response message", ErrInvalidArgument)
	}
	if err := proto.Unmarshal(body, r.message); err != nil {
		return fmt.Errorf("%w: %v", ErrMalformedMessage, err)
	}
	return nil
}

func validateAPI(key APIKey, version int16, allowPrivate bool) (APIMetadata, error) {
	api, ok := LookupAPIKey(key)
	if !ok {
		return APIMetadata{}, fmt.Errorf("%w: %d", ErrUnknownAPIKey, key)
	}
	if !allowPrivate && !api.Public {
		return APIMetadata{}, fmt.Errorf("%w: %s", ErrPrivateAPI, api.Name)
	}
	if version < api.MinVersion || version > api.MaxVersion {
		return APIMetadata{}, fmt.Errorf("%w: %s supports %d through %d, got %d", ErrUnsupportedVersion, api.Name, api.MinVersion, api.MaxVersion, version)
	}
	return api, nil
}
