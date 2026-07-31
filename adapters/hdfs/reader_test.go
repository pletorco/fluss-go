package hdfs

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/pletorco/fluss-go/pkg/fgo"
)

type fakeFile struct {
	reader   *bytes.Reader
	info     os.FileInfo
	closeErr error
	closed   chan struct{}
	once     sync.Once
	readAt   func([]byte, int64) (int, error)
}

func (f *fakeFile) ReadAt(buffer []byte, offset int64) (int, error) {
	if f.readAt != nil {
		return f.readAt(buffer, offset)
	}
	return f.reader.ReadAt(buffer, offset)
}

func (f *fakeFile) Close() error {
	f.once.Do(func() {
		if f.closed != nil {
			close(f.closed)
		}
	})
	return f.closeErr
}

func (f *fakeFile) Stat() os.FileInfo { return f.info }

type fakeInfo struct {
	size int64
	dir  bool
}

func (i fakeInfo) Name() string       { return "file" }
func (i fakeInfo) Size() int64        { return i.size }
func (i fakeInfo) Mode() os.FileMode  { return 0 }
func (i fakeInfo) ModTime() time.Time { return time.Time{} }
func (i fakeInfo) IsDir() bool        { return i.dir }
func (i fakeInfo) Sys() any           { return nil }

func newFakeFile(data string) *fakeFile {
	return &fakeFile{reader: bytes.NewReader([]byte(data)), info: fakeInfo{size: int64(len(data))}}
}

func TestReaderOpensValidatedRangeAndClonesToken(t *testing.T) {
	file := newFakeFile("abcdefgh")
	original := &fgo.FileSystemSecurityToken{
		Schema: "hadoop", Token: []byte("secret"), AdditionalInfo: map[string]string{"service": "hdfs"},
	}
	var opened OpenRequest
	reader, err := New(func(_ context.Context, request OpenRequest) (File, error) {
		opened = request
		return file, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	stream, err := reader.OpenRemoteFile(context.Background(), fgo.RemoteFileRequest{
		Path: "hdfs://nameservice/a%20folder/file", ExpectedSize: 8,
		MaxBytes: 4, Offset: 2, Length: 4, Token: original,
	})
	if err != nil {
		t.Fatal(err)
	}
	data, readErr := io.ReadAll(stream)
	if readErr != nil || string(data) != "cdef" {
		t.Fatalf("range body = %q, %v", data, readErr)
	}
	if opened.Authority != "nameservice" || opened.Path != "/a folder/file" ||
		opened.Token == original || string(opened.Token.Token) != "secret" {
		t.Fatalf("open request = %#v", opened)
	}
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	if string(original.Token) != "secret" || opened.Token.Token != nil {
		t.Fatalf("token ownership original=%q opened=%v", original.Token, opened.Token.Token)
	}
}

func TestReaderReadsCompleteFile(t *testing.T) {
	file := newFakeFile("data")
	reader, _ := New(func(context.Context, OpenRequest) (File, error) { return file, nil })
	data, err := reader.ReadRemoteFile(context.Background(), fgo.RemoteFileRequest{
		Path: "hdfs://namenode:8020/object", ExpectedSize: 4, MaxBytes: 4,
	})
	if err != nil || string(data) != "data" {
		t.Fatalf("complete file = %q, %v", data, err)
	}
}

func TestReaderRejectsInvalidRequestsBeforeOpen(t *testing.T) {
	tests := []fgo.RemoteFileRequest{
		{Path: "file:///object", ExpectedSize: 1, MaxBytes: 1, Length: 1},
		{Path: "hdfs:///object", ExpectedSize: 1, MaxBytes: 1, Length: 1},
		{Path: "hdfs://user@host/object", ExpectedSize: 1, MaxBytes: 1, Length: 1},
		{Path: "hdfs://host/", ExpectedSize: 1, MaxBytes: 1, Length: 1},
		{Path: "hdfs://host/object?x=1", ExpectedSize: 1, MaxBytes: 1, Length: 1},
		{Path: "hdfs://host/object", ExpectedSize: 0, MaxBytes: 1, Length: 1},
		{Path: "hdfs://host/object", ExpectedSize: 1, MaxBytes: 1, Offset: 1, Length: 1},
	}
	for _, request := range tests {
		called := false
		reader, _ := New(func(context.Context, OpenRequest) (File, error) {
			called = true
			return nil, nil
		})
		if _, err := reader.OpenRemoteFile(context.Background(), request); !errors.Is(err, fgo.ErrInvalidConfig) {
			t.Errorf("OpenRemoteFile(%#v) error = %v", request, err)
		}
		if called {
			t.Errorf("invalid request reached opener: %#v", request)
		}
	}
	if _, err := New(nil); !errors.Is(err, fgo.ErrInvalidConfig) {
		t.Fatalf("nil opener error = %v", err)
	}
	reader, _ := New(func(context.Context, OpenRequest) (File, error) { return nil, nil })
	if _, err := reader.OpenRemoteFile(nil, fgo.RemoteFileRequest{}); !errors.Is(err, fgo.ErrInvalidConfig) {
		t.Fatalf("nil context error = %v", err)
	}
	if _, err := (*Reader)(nil).OpenRemoteFile(context.Background(), fgo.RemoteFileRequest{}); !errors.Is(err, fgo.ErrInvalidConfig) {
		t.Fatalf("nil reader error = %v", err)
	}
}

func TestReaderClosesInvalidFilesAndPreservesErrors(t *testing.T) {
	openErr := errors.New("HDFS open failed")
	reader, _ := New(func(context.Context, OpenRequest) (File, error) { return nil, openErr })
	request := fgo.RemoteFileRequest{
		Path: "hdfs://host/object", ExpectedSize: 4, MaxBytes: 4, Length: 4,
	}
	if _, err := reader.OpenRemoteFile(context.Background(), request); !errors.Is(err, openErr) {
		t.Fatalf("open error = %v", err)
	}
	for _, file := range []*fakeFile{
		{info: nil}, newFakeFile("bad"), {info: fakeInfo{size: 4, dir: true}},
	} {
		file.closed = make(chan struct{})
		reader, _ = New(func(context.Context, OpenRequest) (File, error) { return file, nil })
		if _, err := reader.OpenRemoteFile(context.Background(), request); !errors.Is(err, fgo.ErrValidation) {
			t.Fatalf("metadata error = %v", err)
		}
		select {
		case <-file.closed:
		default:
			t.Fatal("invalid file was not closed")
		}
	}
}

func TestCancellationClosesBlockedRead(t *testing.T) {
	closed := make(chan struct{})
	entered := make(chan struct{})
	file := &fakeFile{info: fakeInfo{size: 1}, closed: closed}
	file.readAt = func([]byte, int64) (int, error) {
		close(entered)
		<-closed
		return 0, io.ErrClosedPipe
	}
	reader, _ := New(func(context.Context, OpenRequest) (File, error) { return file, nil })
	ctx, cancel := context.WithCancel(context.Background())
	stream, err := reader.OpenRemoteFile(ctx, fgo.RemoteFileRequest{
		Path: "hdfs://host/object", ExpectedSize: 1, MaxBytes: 1, Length: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() {
		_, readErr := stream.Read(make([]byte, 1))
		result <- readErr
	}()
	<-entered
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) || !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("canceled read error = %v", err)
	}
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestReaderHandlesCanceledOpenShortReadAndCloseError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	file := newFakeFile("x")
	reader, _ := New(func(context.Context, OpenRequest) (File, error) {
		cancel()
		return file, nil
	})
	if _, err := reader.OpenRemoteFile(ctx, fgo.RemoteFileRequest{
		Path: "hdfs://host/object", ExpectedSize: 1, MaxBytes: 1, Length: 1,
	}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled open error = %v", err)
	}

	short := newFakeFile("x")
	short.info = fakeInfo{size: 2}
	reader, _ = New(func(context.Context, OpenRequest) (File, error) { return short, nil })
	if _, err := reader.ReadRemoteFile(context.Background(), fgo.RemoteFileRequest{
		Path: "hdfs://host/object", ExpectedSize: 2, MaxBytes: 2,
	}); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("short read error = %v", err)
	}

	closeErr := errors.New("HDFS close failed")
	closing := newFakeFile("x")
	closing.closeErr = closeErr
	reader, _ = New(func(context.Context, OpenRequest) (File, error) { return closing, nil })
	if _, err := reader.ReadRemoteFile(context.Background(), fgo.RemoteFileRequest{
		Path: "hdfs://host/object", ExpectedSize: 1, MaxBytes: 1,
	}); !errors.Is(err, closeErr) {
		t.Fatalf("close error = %v", err)
	}
	if _, err := reader.ReadRemoteFile(context.Background(), fgo.RemoteFileRequest{
		Path: "hdfs://host/object", ExpectedSize: 0,
	}); !errors.Is(err, fgo.ErrInvalidConfig) {
		t.Fatalf("invalid complete read error = %v", err)
	}
}
