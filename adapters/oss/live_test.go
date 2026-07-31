package oss

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"
	"testing"

	aliyunoss "github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss"
	"github.com/pletorco/fluss-go/pkg/fgo"
)

func TestOSSAdapterLive(t *testing.T) {
	if os.Getenv("FLUSS_OSS_LIVE") != "1" {
		t.Skip("set FLUSS_OSS_LIVE=1 to run the OSS service test")
	}
	region := requiredEnv(t, "FLUSS_OSS_REGION")
	bucket := requiredEnv(t, "FLUSS_OSS_BUCKET")
	object := requiredEnv(t, "FLUSS_OSS_OBJECT")
	expected, err := base64.StdEncoding.DecodeString(requiredEnv(t, "FLUSS_OSS_EXPECTED_BASE64"))
	if err != nil || len(expected) == 0 {
		t.Fatalf("FLUSS_OSS_EXPECTED_BASE64 must contain non-empty base64: %v", err)
	}
	config := aliyunoss.LoadDefaultConfig().WithRegion(region)
	if endpoint := os.Getenv("FLUSS_OSS_ENDPOINT"); endpoint != "" {
		config.WithEndpoint(endpoint)
	}
	reader := NewFromConfig(config)
	path := (&url.URL{Scheme: "oss", Host: bucket, Path: "/" + object}).String()
	assertLiveObject(t, reader, path, expected)

	t.Run("metadata mismatch", func(t *testing.T) {
		testLiveMetadataMismatch(t, reader, path, expected)
	})

	t.Run("cancellation", func(t *testing.T) {
		testLiveCancellation(t, reader, path, expected)
	})

	t.Run("service error redaction", func(t *testing.T) {
		testLiveServiceErrorRedaction(t, reader, bucket, object)
	})
}

func assertLiveObject(t *testing.T, reader *Reader, path string, expected []byte) {
	t.Helper()
	body, err := reader.OpenRemoteFile(context.Background(), fgo.RemoteFileRequest{
		Path: path, ExpectedSize: int64(len(expected)), MaxBytes: int64(len(expected)),
		Length: int64(len(expected)),
	})
	if err != nil {
		t.Fatal(err)
	}
	actual, readErr := io.ReadAll(body)
	closeErr := body.Close()
	if readErr != nil || closeErr != nil {
		t.Fatalf("read=%v, close=%v", readErr, closeErr)
	}
	if !bytes.Equal(actual, expected) {
		t.Fatalf("OSS object content mismatch: got %d bytes", len(actual))
	}
}

func testLiveMetadataMismatch(t *testing.T, reader *Reader, path string, expected []byte) {
	t.Helper()
	_, err := reader.OpenRemoteFile(context.Background(), fgo.RemoteFileRequest{
		Path: path, ExpectedSize: int64(len(expected) + 1),
		MaxBytes: int64(len(expected)), Length: int64(len(expected)),
	})
	if !errors.Is(err, fgo.ErrValidation) {
		t.Fatalf("metadata mismatch error = %v", err)
	}
}

func testLiveCancellation(t *testing.T, reader *Reader, path string, expected []byte) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := reader.OpenRemoteFile(ctx, fgo.RemoteFileRequest{
		Path: path, ExpectedSize: int64(len(expected)), MaxBytes: 1, Length: 1,
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled request error = %v", err)
	}
}

func testLiveServiceErrorRedaction(t *testing.T, reader *Reader, bucket, object string) {
	t.Helper()
	missingPath := (&url.URL{
		Scheme: "oss", Host: bucket, Path: "/" + object + ".fluss-go-missing",
	}).String()
	_, err := reader.OpenRemoteFile(context.Background(), fgo.RemoteFileRequest{
		Path: missingPath, ExpectedSize: 1, MaxBytes: 1, Length: 1,
	})
	if err == nil {
		t.Fatal("missing OSS object unexpectedly opened")
	}
	for _, secret := range []string{
		os.Getenv("OSS_ACCESS_KEY_ID"), os.Getenv("OSS_ACCESS_KEY_SECRET"),
	} {
		if secret != "" && strings.Contains(err.Error(), secret) {
			t.Fatal("OSS service error exposed credentials")
		}
	}
}

func requiredEnv(t *testing.T, name string) string {
	t.Helper()
	value := os.Getenv(name)
	if value == "" {
		t.Fatal(fmt.Sprintf("%s is required", name))
	}
	return value
}
