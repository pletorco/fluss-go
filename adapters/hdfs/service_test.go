//go:build integration

package hdfs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/pletorco/fluss-go/pkg/fgo"
)

const servicePath = "/fluss-go/fixtures/range.bin"

type serviceOpenState struct {
	file  *cliFile
	token *fgo.FileSystemSecurityToken
}

func TestHDFSAdapterService(t *testing.T) {
	if os.Getenv("FLUSS_HDFS_SERVICE") != "1" {
		t.Skip("run task test:hdfs to start the reproducible Apache Hadoop service")
	}
	fixture := []byte("0123456789-service-fixture")
	seedHDFSService(t, fixture)

	state := &serviceOpenState{}
	reader, err := New(func(ctx context.Context, request OpenRequest) (File, error) {
		state.token = request.Token
		file, openErr := openHDFSCLI(ctx, request)
		state.file = file
		return file, openErr
	})
	if err != nil {
		t.Fatal(err)
	}

	t.Run("range close and token lifetime", func(t *testing.T) {
		testServiceRangeCloseAndToken(t, reader, state, fixture)
	})

	t.Run("metadata mismatch", func(t *testing.T) {
		testServiceMetadataMismatch(t, reader, fixture)
	})

	t.Run("cancellation", func(t *testing.T) {
		testServiceCancellation(t, reader, fixture)
	})

	t.Run("service error classification and redaction", func(t *testing.T) {
		testServiceErrorClassificationAndRedaction(t, reader)
	})
}

func testServiceRangeCloseAndToken(
	t *testing.T,
	reader *Reader,
	state *serviceOpenState,
	fixture []byte,
) {
	t.Helper()
	token := &fgo.FileSystemSecurityToken{Schema: "hadoop", Token: []byte("service-secret")}
	stream, err := reader.OpenRemoteFile(context.Background(), fgo.RemoteFileRequest{
		Path:         "hdfs://hdfs-namenode:8020" + servicePath,
		ExpectedSize: int64(len(fixture)), MaxBytes: 7, Offset: 4, Length: 7,
		Token: token,
	})
	if err != nil {
		t.Fatal(err)
	}
	data, readErr := io.ReadAll(stream)
	closeErr := stream.Close()
	if readErr != nil || closeErr != nil || !bytes.Equal(data, fixture[4:11]) {
		t.Fatalf("range=%q read=%v close=%v", data, readErr, closeErr)
	}
	if state.file == nil || !state.file.closed {
		t.Fatal("application-owned HDFS file was not closed")
	}
	if state.token == nil || state.token.Token != nil || string(token.Token) != "service-secret" {
		t.Fatalf("token lifetime clone=%v original=%q", state.token, token.Token)
	}
}

func testServiceMetadataMismatch(t *testing.T, reader *Reader, fixture []byte) {
	t.Helper()
	_, err := reader.OpenRemoteFile(context.Background(), fgo.RemoteFileRequest{
		Path:         "hdfs://hdfs-namenode:8020" + servicePath,
		ExpectedSize: int64(len(fixture) + 1), MaxBytes: 1, Length: 1,
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
		Path:         "hdfs://hdfs-namenode:8020" + servicePath,
		ExpectedSize: int64(len(fixture)), MaxBytes: 1, Length: 1,
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled request error = %v", err)
	}
}

func testServiceErrorClassificationAndRedaction(t *testing.T, reader *Reader) {
	t.Helper()
	secret := "hdfs-token-must-not-appear"
	_, err := reader.OpenRemoteFile(context.Background(), fgo.RemoteFileRequest{
		Path:         "hdfs://hdfs-namenode:8020/fluss-go/missing",
		ExpectedSize: 1, MaxBytes: 1, Length: 1,
		Token: &fgo.FileSystemSecurityToken{Schema: "hadoop", Token: []byte(secret)},
	})
	if err == nil {
		t.Fatal("missing HDFS file unexpectedly opened")
	}
	if strings.Contains(err.Error(), secret) {
		t.Fatal("HDFS error exposed token bytes")
	}
	var temporary interface{ Temporary() bool }
	if errors.As(err, &temporary) {
		t.Fatal("test opener unexpectedly classified a CLI error as temporary")
	}
}

type cliFile struct {
	*bytes.Reader
	info   cliFileInfo
	closed bool
}

func (f *cliFile) Close() error      { f.closed = true; return nil }
func (f *cliFile) Stat() os.FileInfo { return f.info }

type cliFileInfo struct{ size int64 }

func (f cliFileInfo) Name() string     { return "range.bin" }
func (f cliFileInfo) Size() int64      { return f.size }
func (cliFileInfo) Mode() os.FileMode  { return 0o400 }
func (cliFileInfo) ModTime() time.Time { return time.Time{} }
func (cliFileInfo) IsDir() bool        { return false }
func (cliFileInfo) Sys() any           { return nil }

func openHDFSCLI(ctx context.Context, request OpenRequest) (*cliFile, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if request.Authority != "hdfs-namenode:8020" {
		return nil, fmt.Errorf("unexpected HDFS authority %q", request.Authority)
	}
	sizeOutput, err := hdfsServiceCommand(ctx, nil, "hdfs", "dfs", "-stat", "%b", request.Path)
	if err != nil {
		return nil, err
	}
	size, err := strconv.ParseInt(strings.TrimSpace(string(sizeOutput)), 10, 64)
	if err != nil || size < 0 {
		return nil, fmt.Errorf("invalid HDFS size: %w", err)
	}
	data, err := hdfsServiceCommand(ctx, nil, "hdfs", "dfs", "-cat", request.Path)
	if err != nil {
		return nil, err
	}
	return &cliFile{Reader: bytes.NewReader(data), info: cliFileInfo{size: size}}, nil
}

func seedHDFSService(t *testing.T, fixture []byte) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := hdfsServiceCommand(ctx, nil, "hdfs", "dfs", "-mkdir", "-p", "/fluss-go/fixtures"); err != nil {
		t.Fatal(err)
	}
	if _, err := hdfsServiceCommand(ctx, fixture, "hdfs", "dfs", "-put", "-f", "-", servicePath); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = hdfsServiceCommand(context.Background(), nil, "hdfs", "dfs", "-rm", "-r", "-f", "/fluss-go")
	})
}

func hdfsServiceCommand(ctx context.Context, stdin []byte, arguments ...string) ([]byte, error) {
	composeFile := os.Getenv("FLUSS_HDFS_COMPOSE_FILE")
	project := os.Getenv("FLUSS_HDFS_COMPOSE_PROJECT")
	if composeFile == "" || project == "" {
		return nil, errors.New("HDFS compose environment is incomplete")
	}
	args := []string{"compose", "--project-name", project, "--file", composeFile, "exec", "-T", "hdfs-namenode"}
	args = append(args, arguments...)
	command := exec.CommandContext(ctx, "docker", args...)
	command.Stdin = bytes.NewReader(stdin)
	var stderr bytes.Buffer
	command.Stderr = &stderr
	output, err := command.Output()
	if err != nil {
		if contextErr := ctx.Err(); contextErr != nil {
			return nil, contextErr
		}
		message := strings.TrimSpace(stderr.String())
		if message == "" {
			message = "command failed"
		}
		return nil, fmt.Errorf("HDFS CLI: %s: %w", message, err)
	}
	return output, nil
}
