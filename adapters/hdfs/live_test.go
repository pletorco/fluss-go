package hdfs

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/pletorco/fluss-go/pkg/fgo"
)

type mountedFile struct {
	*os.File
	info os.FileInfo
}

func (f *mountedFile) Stat() os.FileInfo { return f.info }

func TestHDFSAdapterLive(t *testing.T) {
	if os.Getenv("FLUSS_HDFS_LIVE") != "1" {
		t.Skip("set FLUSS_HDFS_LIVE=1 to run the HDFS service test")
	}
	uri := requiredEnv(t, "FLUSS_HDFS_URI")
	mountPath := requiredEnv(t, "FLUSS_HDFS_MOUNT_FILE")
	expectedHash := requiredEnv(t, "FLUSS_HDFS_EXPECTED_SHA256")
	if decoded, err := hex.DecodeString(expectedHash); err != nil || len(decoded) != sha256.Size {
		t.Fatalf("FLUSS_HDFS_EXPECTED_SHA256 must be a SHA-256 hex digest: %v", err)
	}
	info, err := os.Stat(mountPath)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := New(func(context.Context, OpenRequest) (File, error) {
		file, openErr := os.Open(mountPath)
		if openErr != nil {
			return nil, openErr
		}
		return &mountedFile{File: file, info: info}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	body, err := reader.OpenRemoteFile(context.Background(), fgo.RemoteFileRequest{
		Path: uri, ExpectedSize: info.Size(), MaxBytes: info.Size(), Length: info.Size(),
	})
	if err != nil {
		t.Fatal(err)
	}
	hash := sha256.New()
	_, readErr := io.Copy(hash, body)
	closeErr := body.Close()
	if readErr != nil || closeErr != nil {
		t.Fatalf("read=%v, close=%v", readErr, closeErr)
	}
	if actual := hex.EncodeToString(hash.Sum(nil)); actual != expectedHash {
		t.Fatalf("HDFS file SHA-256 = %s, want %s", actual, expectedHash)
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
