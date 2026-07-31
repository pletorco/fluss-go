package s3

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/pletorco/fluss-go/pkg/fgo"
)

type fakeGetObjectClient struct {
	input  *awss3.GetObjectInput
	output *awss3.GetObjectOutput
	err    error
}

func (c *fakeGetObjectClient) GetObject(
	_ context.Context,
	input *awss3.GetObjectInput,
	_ ...func(*awss3.Options),
) (*awss3.GetObjectOutput, error) {
	c.input = input
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

func TestReaderOpensValidatedRange(t *testing.T) {
	body := &trackedBody{Reader: bytes.NewReader([]byte("data"))}
	client := &fakeGetObjectClient{output: &awss3.GetObjectOutput{
		Body: body, ContentLength: aws.Int64(4), ContentRange: aws.String("bytes 2-5/8"),
	}}
	reader, err := New(client)
	if err != nil {
		t.Fatal(err)
	}
	stream, err := reader.OpenRemoteFile(context.Background(), fgo.RemoteFileRequest{
		Path: "s3://bucket/a%20folder/object", ExpectedSize: 8,
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
	if aws.ToString(client.input.Bucket) != "bucket" ||
		aws.ToString(client.input.Key) != "a folder/object" ||
		aws.ToString(client.input.Range) != "bytes=2-5" {
		t.Fatalf("GetObject input = %#v", client.input)
	}
}

func TestReaderReadsCompleteObject(t *testing.T) {
	body := &trackedBody{Reader: bytes.NewReader([]byte("data"))}
	client := &fakeGetObjectClient{output: &awss3.GetObjectOutput{
		Body: body, ContentLength: aws.Int64(4), ContentRange: aws.String("bytes 0-3/4"),
	}}
	reader, _ := New(client)
	data, err := reader.ReadRemoteFile(context.Background(), fgo.RemoteFileRequest{
		Path: "s3://bucket/object", ExpectedSize: 4, MaxBytes: 4,
	})
	if err != nil || string(data) != "data" || !body.closed {
		t.Fatalf("complete object = %q, %v, closed=%t", data, err, body.closed)
	}
}

func TestReaderRejectsInvalidRequestsBeforeSDKCall(t *testing.T) {
	tests := []fgo.RemoteFileRequest{
		{Path: "https://bucket/object", ExpectedSize: 1, MaxBytes: 1, Length: 1},
		{Path: "s3://user@bucket/object", ExpectedSize: 1, MaxBytes: 1, Length: 1},
		{Path: "s3://bucket", ExpectedSize: 1, MaxBytes: 1, Length: 1},
		{Path: "s3://bucket/object?version=1", ExpectedSize: 1, MaxBytes: 1, Length: 1},
		{Path: "s3://bucket/object", ExpectedSize: 0, MaxBytes: 1, Length: 1},
		{Path: "s3://bucket/object", ExpectedSize: 1, MaxBytes: 1, Offset: 1, Length: 1},
		{Path: "s3://bucket/object", ExpectedSize: 2, MaxBytes: 1, Length: 2},
	}
	for _, request := range tests {
		client := &fakeGetObjectClient{}
		reader, _ := New(client)
		if _, err := reader.OpenRemoteFile(
			context.Background(), request,
		); !errors.Is(err, fgo.ErrInvalidConfig) {
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
	if _, err := (*Reader)(nil).OpenRemoteFile(
		context.Background(), fgo.RemoteFileRequest{},
	); !errors.Is(err, fgo.ErrInvalidConfig) {
		t.Fatalf("nil reader error = %v", err)
	}
	if _, err := New(nil); !errors.Is(err, fgo.ErrInvalidConfig) {
		t.Fatalf("nil client error = %v", err)
	}
}

func TestReaderClosesInvalidResponses(t *testing.T) {
	tests := []struct {
		name   string
		length *int64
		range_ *string
	}{
		{"missing length", nil, aws.String("bytes 0-3/4")},
		{"wrong length", aws.Int64(3), aws.String("bytes 0-3/4")},
		{"missing range", aws.Int64(4), nil},
		{"malformed range", aws.Int64(4), aws.String("items 0-3/4")},
		{"wrong total", aws.Int64(4), aws.String("bytes 0-3/5")},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			body := &trackedBody{Reader: bytes.NewReader([]byte("data"))}
			reader, _ := New(&fakeGetObjectClient{output: &awss3.GetObjectOutput{
				Body: body, ContentLength: test.length, ContentRange: test.range_,
			}})
			_, err := reader.OpenRemoteFile(context.Background(), fgo.RemoteFileRequest{
				Path: "s3://bucket/object", ExpectedSize: 4, MaxBytes: 4, Length: 4,
			})
			if !errors.Is(err, fgo.ErrValidation) || !body.closed {
				t.Fatalf("invalid response error = %v, closed=%t", err, body.closed)
			}
		})
	}
}

func TestReaderPreservesSDKAndBodyErrors(t *testing.T) {
	sdkErr := errors.New("SDK request failed")
	reader, _ := New(&fakeGetObjectClient{err: sdkErr})
	if _, err := reader.OpenRemoteFile(context.Background(), fgo.RemoteFileRequest{
		Path: "s3://bucket/object", ExpectedSize: 1, MaxBytes: 1, Length: 1,
	}); !errors.Is(err, sdkErr) {
		t.Fatalf("SDK error = %v", err)
	}

	bodyErr := errors.New("body read failed")
	body := &trackedBody{Reader: bytes.NewReader([]byte("x")), err: bodyErr}
	reader, _ = New(&fakeGetObjectClient{output: &awss3.GetObjectOutput{
		Body: body, ContentLength: aws.Int64(1), ContentRange: aws.String("bytes 0-0/1"),
	}})
	if _, err := reader.ReadRemoteFile(context.Background(), fgo.RemoteFileRequest{
		Path: "s3://bucket/object", ExpectedSize: 1, MaxBytes: 1,
	}); !errors.Is(err, bodyErr) {
		t.Fatalf("body close error = %v", err)
	}
}

func TestNewFromConfig(t *testing.T) {
	reader := NewFromConfig(aws.Config{
		Region: "us-east-1", Credentials: aws.AnonymousCredentials{},
	})
	if reader == nil || reader.client == nil {
		t.Fatal("NewFromConfig returned nil reader")
	}
}

func TestParseContentRange(t *testing.T) {
	for _, value := range []string{
		"", "items 0-1/2", "bytes 0/2", "bytes a-1/2",
		"bytes 1-0/2", "bytes 0-2/2",
	} {
		if _, _, _, err := parseContentRange(value); err == nil {
			t.Errorf("parseContentRange(%q) succeeded", value)
		}
	}
	start, end, total, err := parseContentRange("bytes 10-19/20")
	if err != nil || start != 10 || end != 19 || total != 20 {
		t.Fatalf("valid content range = %d, %d, %d, %v", start, end, total, err)
	}
}
