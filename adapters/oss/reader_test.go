package oss

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	aliyunoss "github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss"
	"github.com/pletorco/fluss-go/pkg/fgo"
)

type fakeGetObjectClient struct {
	input  *aliyunoss.GetObjectRequest
	output *aliyunoss.GetObjectResult
	err    error
	get    func(context.Context, *aliyunoss.GetObjectRequest) (*aliyunoss.GetObjectResult, error)
}

func (c *fakeGetObjectClient) GetObject(
	ctx context.Context,
	input *aliyunoss.GetObjectRequest,
	_ ...func(*aliyunoss.Options),
) (*aliyunoss.GetObjectResult, error) {
	c.input = input
	if c.get != nil {
		return c.get(ctx, input)
	}
	return c.output, c.err
}

type trackedBody struct {
	*bytes.Reader
	closed bool
	err    error
}

func (b *trackedBody) Close() error {
	b.closed = true
	return b.err
}

func validOutput(body io.ReadCloser) *aliyunoss.GetObjectResult {
	return &aliyunoss.GetObjectResult{
		Body: body, ContentLength: 4, ContentRange: aliyunoss.Ptr("bytes 2-5/8"),
	}
}

func TestReaderOpensValidatedRange(t *testing.T) {
	body := &trackedBody{Reader: bytes.NewReader([]byte("data"))}
	client := &fakeGetObjectClient{output: validOutput(body)}
	reader, err := New(client)
	if err != nil {
		t.Fatal(err)
	}
	stream, err := reader.OpenRemoteFile(context.Background(), fgo.RemoteFileRequest{
		Path: "oss://bucket/a%20folder/object", ExpectedSize: 8,
		MaxBytes: 4, Offset: 2, Length: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	data, readErr := io.ReadAll(stream)
	closeErr := stream.Close()
	if readErr != nil || closeErr != nil || string(data) != "data" {
		t.Fatalf("range body = %q, %v, close=%v", data, readErr, closeErr)
	}
	if aliyunoss.ToString(client.input.Bucket) != "bucket" ||
		aliyunoss.ToString(client.input.Key) != "a folder/object" ||
		aliyunoss.ToString(client.input.Range) != "bytes=2-5" ||
		aliyunoss.ToString(client.input.RangeBehavior) != "standard" {
		t.Fatalf("GetObject input = %#v", client.input)
	}
}

func TestReaderReadsCompleteObject(t *testing.T) {
	body := &trackedBody{Reader: bytes.NewReader([]byte("data"))}
	client := &fakeGetObjectClient{output: &aliyunoss.GetObjectResult{
		Body: body, ContentLength: 4, ContentRange: aliyunoss.Ptr("bytes 0-3/4"),
	}}
	reader, _ := New(client)
	data, err := reader.ReadRemoteFile(context.Background(), fgo.RemoteFileRequest{
		Path: "oss://bucket/object", ExpectedSize: 4, MaxBytes: 4,
	})
	if err != nil || string(data) != "data" || !body.closed {
		t.Fatalf("complete object = %q, %v, closed=%t", data, err, body.closed)
	}
}

func TestReaderRejectsInvalidRequestsBeforeSDKCall(t *testing.T) {
	tests := []fgo.RemoteFileRequest{
		{Path: "https://bucket/object", ExpectedSize: 1, MaxBytes: 1, Length: 1},
		{Path: "oss://user@bucket/object", ExpectedSize: 1, MaxBytes: 1, Length: 1},
		{Path: "oss://bucket", ExpectedSize: 1, MaxBytes: 1, Length: 1},
		{Path: "oss://bucket/object?version=1", ExpectedSize: 1, MaxBytes: 1, Length: 1},
		{Path: "oss://bucket/object", ExpectedSize: 0, MaxBytes: 1, Length: 1},
		{Path: "oss://bucket/object", ExpectedSize: 1, MaxBytes: 1, Offset: 1, Length: 1},
		{Path: "oss://bucket/object", ExpectedSize: 2, MaxBytes: 1, Length: 2},
	}
	for _, request := range tests {
		client := &fakeGetObjectClient{}
		reader, _ := New(client)
		if _, err := reader.OpenRemoteFile(context.Background(), request); !errors.Is(err, fgo.ErrInvalidConfig) {
			t.Errorf("OpenRemoteFile(%#v) error = %v", request, err)
		}
		if client.input != nil {
			t.Errorf("invalid request reached SDK: %#v", client.input)
		}
	}
	reader, _ := New(&fakeGetObjectClient{})
	if _, err := reader.OpenRemoteFile(nil, fgo.RemoteFileRequest{}); !errors.Is(err, fgo.ErrInvalidConfig) {
		t.Fatalf("nil context error = %v", err)
	}
	if _, err := (*Reader)(nil).OpenRemoteFile(context.Background(), fgo.RemoteFileRequest{}); !errors.Is(err, fgo.ErrInvalidConfig) {
		t.Fatalf("nil reader error = %v", err)
	}
	if _, err := New(nil); !errors.Is(err, fgo.ErrInvalidConfig) {
		t.Fatalf("nil client error = %v", err)
	}
}

func TestReaderClosesInvalidResponses(t *testing.T) {
	tests := []struct {
		name   string
		length int64
		range_ *string
	}{
		{"wrong length", 3, aliyunoss.Ptr("bytes 0-3/4")},
		{"missing range", 4, nil},
		{"malformed range", 4, aliyunoss.Ptr("items 0-3/4")},
		{"wrong total", 4, aliyunoss.Ptr("bytes 0-3/5")},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			body := &trackedBody{Reader: bytes.NewReader([]byte("data"))}
			reader, _ := New(&fakeGetObjectClient{output: &aliyunoss.GetObjectResult{
				Body: body, ContentLength: test.length, ContentRange: test.range_,
			}})
			_, err := reader.OpenRemoteFile(context.Background(), fgo.RemoteFileRequest{
				Path: "oss://bucket/object", ExpectedSize: 4, MaxBytes: 4, Length: 4,
			})
			if !errors.Is(err, fgo.ErrValidation) || !body.closed {
				t.Fatalf("invalid response error = %v, closed=%t", err, body.closed)
			}
		})
	}
}

func TestReaderPreservesSDKBodyAndCancellationErrors(t *testing.T) {
	sdkErr := errors.New("SDK request failed")
	reader, _ := New(&fakeGetObjectClient{err: sdkErr})
	if _, err := reader.OpenRemoteFile(context.Background(), fgo.RemoteFileRequest{
		Path: "oss://bucket/object", ExpectedSize: 1, MaxBytes: 1, Length: 1,
	}); !errors.Is(err, sdkErr) {
		t.Fatalf("SDK error = %v", err)
	}

	bodyErr := errors.New("body close failed")
	body := &trackedBody{Reader: bytes.NewReader([]byte("x")), err: bodyErr}
	reader, _ = New(&fakeGetObjectClient{output: &aliyunoss.GetObjectResult{
		Body: body, ContentLength: 1, ContentRange: aliyunoss.Ptr("bytes 0-0/1"),
	}})
	if _, err := reader.ReadRemoteFile(context.Background(), fgo.RemoteFileRequest{
		Path: "oss://bucket/object", ExpectedSize: 1, MaxBytes: 1,
	}); !errors.Is(err, bodyErr) {
		t.Fatalf("body close error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	reader, _ = New(&fakeGetObjectClient{get: func(ctx context.Context, _ *aliyunoss.GetObjectRequest) (*aliyunoss.GetObjectResult, error) {
		return nil, ctx.Err()
	}})
	if _, err := reader.OpenRemoteFile(ctx, fgo.RemoteFileRequest{
		Path: "oss://bucket/object", ExpectedSize: 1, MaxBytes: 1, Length: 1,
	}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation error = %v", err)
	}
}

func TestReaderRejectsMissingAndMismatchedCompleteBodies(t *testing.T) {
	reader, _ := New(&fakeGetObjectClient{output: nil})
	if _, err := reader.OpenRemoteFile(context.Background(), fgo.RemoteFileRequest{
		Path: "oss://bucket/object", ExpectedSize: 1, MaxBytes: 1, Length: 1,
	}); !errors.Is(err, fgo.ErrValidation) {
		t.Fatalf("missing output error = %v", err)
	}

	body := &trackedBody{Reader: bytes.NewReader([]byte("x"))}
	reader, _ = New(&fakeGetObjectClient{output: &aliyunoss.GetObjectResult{
		Body: body, ContentLength: 2, ContentRange: aliyunoss.Ptr("bytes 0-1/2"),
	}})
	if _, err := reader.ReadRemoteFile(context.Background(), fgo.RemoteFileRequest{
		Path: "oss://bucket/object", ExpectedSize: 2, MaxBytes: 2,
	}); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("short body error = %v", err)
	}
	if _, err := reader.ReadRemoteFile(context.Background(), fgo.RemoteFileRequest{
		Path: "oss://bucket/object", ExpectedSize: 0,
	}); !errors.Is(err, fgo.ErrInvalidConfig) {
		t.Fatalf("invalid complete read error = %v", err)
	}
}

func TestNewFromConfig(t *testing.T) {
	reader := NewFromConfig(aliyunoss.LoadDefaultConfig().WithRegion("cn-hangzhou"))
	if reader == nil || reader.client == nil {
		t.Fatal("NewFromConfig returned nil reader")
	}
}
