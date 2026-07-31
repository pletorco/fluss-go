package oss

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"net/url"
	"os"
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

func requiredEnv(t *testing.T, name string) string {
	t.Helper()
	value := os.Getenv(name)
	if value == "" {
		t.Fatal(fmt.Sprintf("%s is required", name))
	}
	return value
}
