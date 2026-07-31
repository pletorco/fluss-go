//go:build integration

package s3

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/pletorco/fluss-go/pkg/fgo"
)

const (
	serviceBucket = "fluss-go-service-test"
	serviceKey    = "fixtures/range.bin"
)

func TestS3AdapterService(t *testing.T) {
	if os.Getenv("FLUSS_GO_S3_SERVICE") != "1" {
		t.Skip("run task test:s3 to start the reproducible MinIO service")
	}
	fixture := []byte("0123456789-service-fixture")
	client := newServiceClient(t)
	seedServiceObject(t, client, fixture)
	reader, err := New(client)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("range and close", func(t *testing.T) {
		testServiceRangeAndClose(t, reader, fixture)
	})

	t.Run("metadata mismatch", func(t *testing.T) {
		testServiceMetadataMismatch(t, reader, fixture)
	})

	t.Run("cancellation", func(t *testing.T) {
		testServiceCancellation(t, reader, fixture)
	})

	t.Run("service error identity and redaction", func(t *testing.T) {
		testServiceErrorIdentityAndRedaction(t, reader)
	})
}

func testServiceRangeAndClose(t *testing.T, reader *Reader, fixture []byte) {
	t.Helper()
	stream, err := reader.OpenRemoteFile(context.Background(), fgo.RemoteFileRequest{
		Path:         "s3://" + serviceBucket + "/" + serviceKey,
		ExpectedSize: int64(len(fixture)), MaxBytes: 7, Offset: 4, Length: 7,
	})
	if err != nil {
		t.Fatal(err)
	}
	data, readErr := io.ReadAll(stream)
	closeErr := stream.Close()
	if readErr != nil || closeErr != nil || !bytes.Equal(data, fixture[4:11]) {
		t.Fatalf("range=%q read=%v close=%v", data, readErr, closeErr)
	}
}

func testServiceMetadataMismatch(t *testing.T, reader *Reader, fixture []byte) {
	t.Helper()
	_, err := reader.OpenRemoteFile(context.Background(), fgo.RemoteFileRequest{
		Path:         "s3://" + serviceBucket + "/" + serviceKey,
		ExpectedSize: int64(len(fixture) + 1), MaxBytes: 4, Length: 4,
	})
	if !errors.Is(err, fgo.ErrValidation) {
		t.Fatalf("metadata mismatch error = %v", err)
	}
}

func testServiceCancellation(t *testing.T, reader *Reader, fixture []byte) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := reader.OpenRemoteFile(ctx, fgo.RemoteFileRequest{
		Path:         "s3://" + serviceBucket + "/" + serviceKey,
		ExpectedSize: int64(len(fixture)), MaxBytes: 1, Length: 1,
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled request error = %v", err)
	}
}

func testServiceErrorIdentityAndRedaction(t *testing.T, reader *Reader) {
	t.Helper()
	_, err := reader.OpenRemoteFile(context.Background(), fgo.RemoteFileRequest{
		Path:         "s3://" + serviceBucket + "/missing-object",
		ExpectedSize: 1, MaxBytes: 1, Length: 1,
	})
	if err == nil {
		t.Fatal("missing object unexpectedly opened")
	}
	var apiError interface{ ErrorCode() string }
	if !errors.As(err, &apiError) || apiError.ErrorCode() == "" {
		t.Fatalf("service error identity was not preserved: %T %v", err, err)
	}
	assertServiceErrorRedactsCredentials(t, err)
	var temporary interface{ Temporary() bool }
	if errors.As(err, &temporary) && temporary.Temporary() {
		t.Fatal("missing object was classified as a temporary adapter error")
	}
}

func assertServiceErrorRedactsCredentials(t *testing.T, err error) {
	t.Helper()
	for _, secret := range []string{os.Getenv("AWS_ACCESS_KEY_ID"), os.Getenv("AWS_SECRET_ACCESS_KEY")} {
		if secret != "" && strings.Contains(err.Error(), secret) {
			t.Fatal("service error exposed credentials")
		}
	}
}

func newServiceClient(t *testing.T) *awss3.Client {
	t.Helper()
	endpoint := os.Getenv("FLUSS_GO_S3_ENDPOINT")
	if endpoint == "" {
		t.Fatal("FLUSS_GO_S3_ENDPOINT is required")
	}
	region := os.Getenv("AWS_REGION")
	if region == "" {
		region = "us-east-1"
	}
	config := aws.Config{
		Region: region,
		Credentials: aws.NewCredentialsCache(staticCredentials{
			accessKey: os.Getenv("AWS_ACCESS_KEY_ID"),
			secretKey: os.Getenv("AWS_SECRET_ACCESS_KEY"),
		}),
	}
	return awss3.NewFromConfig(config, func(options *awss3.Options) {
		options.BaseEndpoint = aws.String(endpoint)
		options.UsePathStyle = true
	})
}

func seedServiceObject(t *testing.T, client *awss3.Client, fixture []byte) {
	t.Helper()
	ctx := context.Background()
	if _, err := client.CreateBucket(ctx, &awss3.CreateBucketInput{
		Bucket: aws.String(serviceBucket),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: aws.String(serviceBucket), Key: aws.String(serviceKey),
		Body: bytes.NewReader(fixture),
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = client.DeleteObject(ctx, &awss3.DeleteObjectInput{
			Bucket: aws.String(serviceBucket), Key: aws.String(serviceKey),
		})
		_, _ = client.DeleteBucket(ctx, &awss3.DeleteBucketInput{
			Bucket: aws.String(serviceBucket),
		})
	})
}
