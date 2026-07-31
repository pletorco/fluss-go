package fgo

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pletorco/fluss-go/pkg/fmsg"
	"google.golang.org/protobuf/proto"
)

type controlledRemoteStreamReader struct {
	mu        sync.Mutex
	objects   map[string][]byte
	releases  map[string]<-chan struct{}
	entered   map[string]chan struct{}
	completed map[string]chan struct{}
	requests  []RemoteFileRequest
	active    int
	maxActive int
	closed    int
}

func (r *controlledRemoteStreamReader) ReadRemoteFile(
	context.Context,
	RemoteFileRequest,
) ([]byte, error) {
	return nil, errors.New("complete-object path used")
}

func (r *controlledRemoteStreamReader) OpenRemoteFile(
	ctx context.Context,
	request RemoteFileRequest,
) (io.ReadCloser, error) {
	r.mu.Lock()
	object, ok := r.objects[request.Path]
	if !ok {
		r.mu.Unlock()
		return nil, os.ErrNotExist
	}
	end := request.Offset + request.Length
	if request.Offset < 0 || request.Length < 0 || end > int64(len(object)) {
		r.mu.Unlock()
		return nil, fmt.Errorf("%w: test range", ErrValidation)
	}
	data := append([]byte(nil), object[request.Offset:end]...)
	r.requests = append(r.requests, request)
	r.active++
	if r.active > r.maxActive {
		r.maxActive = r.active
	}
	release := r.releases[request.Path]
	entered := r.entered[request.Path]
	completed := r.completed[request.Path]
	r.mu.Unlock()
	if entered != nil {
		close(entered)
	}
	return &controlledReadCloser{
		ctx: ctx, reader: bytes.NewReader(data), release: release,
		close: func() {
			r.mu.Lock()
			r.active--
			r.closed++
			r.mu.Unlock()
			if completed != nil {
				close(completed)
			}
		},
	}, nil
}

type controlledReadCloser struct {
	ctx     context.Context
	reader  *bytes.Reader
	release <-chan struct{}
	close   func()
	once    sync.Once
	waited  bool
}

type benchmarkRemoteStreamReader struct {
	objects map[string][]byte
	delay   time.Duration
}

func (benchmarkRemoteStreamReader) ReadRemoteFile(
	context.Context,
	RemoteFileRequest,
) ([]byte, error) {
	return nil, errors.New("complete-object path used")
}

func (r benchmarkRemoteStreamReader) OpenRemoteFile(
	ctx context.Context,
	request RemoteFileRequest,
) (io.ReadCloser, error) {
	object := r.objects[request.Path]
	end := request.Offset + request.Length
	return &delayedReadCloser{
		ctx:  ctx,
		data: bytes.NewReader(object[request.Offset:end]), delay: r.delay,
	}, nil
}

type delayedReadCloser struct {
	ctx     context.Context
	data    *bytes.Reader
	delay   time.Duration
	delayed bool
}

func (r *delayedReadCloser) Read(destination []byte) (int, error) {
	if !r.delayed && r.delay > 0 {
		r.delayed = true
		timer := time.NewTimer(r.delay)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-r.ctx.Done():
			return 0, r.ctx.Err()
		}
	}
	return r.data.Read(destination)
}

func (*delayedReadCloser) Close() error { return nil }

func (r *controlledReadCloser) Read(destination []byte) (int, error) {
	if !r.waited && r.release != nil {
		r.waited = true
		select {
		case <-r.release:
		case <-r.ctx.Done():
			return 0, r.ctx.Err()
		}
	}
	return r.reader.Read(destination)
}

func (r *controlledReadCloser) Close() error {
	r.once.Do(r.close)
	return nil
}

func TestLocalRemoteFileReader(t *testing.T) {
	path := filepath.Join(t.TempDir(), "segment.log")
	if err := os.WriteFile(path, []byte("segment"), 0o600); err != nil {
		t.Fatal(err)
	}
	reader := LocalRemoteFileReader{}
	for _, remotePath := range []string{path, "file://" + path} {
		data, err := reader.ReadRemoteFile(context.Background(), RemoteFileRequest{
			Path: remotePath, ExpectedSize: 7, MaxBytes: 7,
		})
		if err != nil || string(data) != "segment" {
			t.Fatalf("ReadRemoteFile(%q) = %q, %v", remotePath, data, err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := reader.ReadRemoteFile(ctx, RemoteFileRequest{Path: path}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled read error = %v", err)
	}
	if _, err := reader.ReadRemoteFile(
		context.Background(), RemoteFileRequest{Path: "s3://bucket/key"},
	); !errors.Is(err, ErrUnsupportedAPI) {
		t.Fatalf("unsupported scheme error = %v", err)
	}
	if _, err := reader.ReadRemoteFile(
		context.Background(), RemoteFileRequest{Path: "file://remotehost/path"},
	); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("remote file host error = %v", err)
	}
	for _, request := range []RemoteFileRequest{
		{Path: "relative.log"},
		{Path: "file:relative.log"},
		{Path: "file://" + path + "?query=1"},
		{Path: path, ExpectedSize: 8, MaxBytes: 8},
		{Path: path, ExpectedSize: 7, MaxBytes: 6},
		{Path: path, ExpectedSize: -1},
	} {
		if _, err := reader.ReadRemoteFile(
			context.Background(), request,
		); !errors.Is(err, ErrInvalidConfig) && !errors.Is(err, ErrValidation) {
			t.Fatalf("ReadRemoteFile(%#v) error = %v", request, err)
		}
	}
	if _, err := reader.ReadRemoteFile(
		context.Background(), RemoteFileRequest{Path: path, MaxBytes: 6},
	); !errors.Is(err, ErrValidation) {
		t.Fatalf("oversized read error = %v", err)
	}
}

func TestLocalRemoteFileReaderStreamsExactRange(t *testing.T) {
	path := filepath.Join(t.TempDir(), "range.bin")
	if err := os.WriteFile(path, []byte("0123456789"), 0o600); err != nil {
		t.Fatal(err)
	}
	stream, err := (LocalRemoteFileReader{}).OpenRemoteFile(
		context.Background(),
		RemoteFileRequest{
			Path: path, ExpectedSize: 10, MaxBytes: 4, Offset: 3, Length: 4,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(stream)
	closeErr := stream.Close()
	if err != nil || closeErr != nil || string(data) != "3456" {
		t.Fatalf("streamed range = %q, %v, close=%v", data, err, closeErr)
	}
}

func TestRemoteFileSettingsValidation(t *testing.T) {
	var config config
	reader := RemoteFileReaderFunc(func(context.Context, RemoteFileRequest) ([]byte, error) {
		return nil, nil
	})
	if err := WithRemoteFileReader(reader, RemoteFileReadConfig{})(&config); err != nil {
		t.Fatal(err)
	}
	if config.remoteFiles.config.MaxAttempts != 3 ||
		config.remoteFiles.config.RetryBackoff != 50*time.Millisecond ||
		config.remoteFiles.config.MaxFileBytes != 256<<20 ||
		config.remoteFiles.config.MaxTotalBytes != 512<<20 ||
		config.remoteFiles.config.MaxFiles != 4096 ||
		config.remoteFiles.config.MaxConcurrentReads != 4 ||
		config.remoteFiles.config.MaxConcurrentBytes != 256<<20 {
		t.Fatalf("defaults = %#v", config.remoteFiles.config)
	}
	for _, settings := range []RemoteFileReadConfig{
		{MaxAttempts: -1},
		{MaxAttempts: 11},
		{MaxAttempts: 1, RetryBackoff: -1},
		{MaxAttempts: 1, MaxFileBytes: -1},
		{MaxAttempts: 1, MaxTotalBytes: -1},
		{MaxAttempts: 1, MaxFiles: -1},
		{MaxAttempts: 1, MaxFiles: 1_000_001},
		{MaxAttempts: 1, MaxConcurrentReads: -1},
		{MaxAttempts: 1, MaxConcurrentReads: 1025},
		{MaxAttempts: 1, MaxConcurrentBytes: -1},
	} {
		if err := WithRemoteFileReader(reader, settings)(&config); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("settings %#v error = %v", settings, err)
		}
	}
	if err := WithRemoteFileReader(nil, RemoteFileReadConfig{})(&config); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("nil reader error = %v", err)
	}
}

func TestRemoteLogRejectsAggregateLimitsBeforeReading(t *testing.T) {
	var calls atomic.Int32
	reader := RemoteFileReaderFunc(func(context.Context, RemoteFileRequest) ([]byte, error) {
		calls.Add(1)
		return nil, errors.New("must not read")
	})
	base := RemoteLogFetchInfo{
		TabletDirectory: "/remote",
		Segments: []RemoteLogSegment{
			{ID: "one", StartOffset: 0, EndOffset: 1, SizeBytes: 4},
			{ID: "two", StartOffset: 1, EndOffset: 2, SizeBytes: 4},
		},
	}
	for _, test := range []struct {
		name   string
		config RemoteFileReadConfig
		info   RemoteLogFetchInfo
	}{
		{
			name: "file count",
			config: RemoteFileReadConfig{
				MaxAttempts: 1, MaxFileBytes: 10, MaxTotalBytes: 10, MaxFiles: 1,
			},
			info: base,
		},
		{
			name: "total bytes",
			config: RemoteFileReadConfig{
				MaxAttempts: 1, MaxFileBytes: 10, MaxTotalBytes: 7, MaxFiles: 2,
			},
			info: base,
		},
		{
			name: "overflow",
			config: RemoteFileReadConfig{
				MaxAttempts: 1, MaxFileBytes: math.MaxInt64,
				MaxTotalBytes: math.MaxInt64, MaxFiles: 2,
			},
			info: RemoteLogFetchInfo{
				TabletDirectory: "/remote",
				Segments: []RemoteLogSegment{
					{ID: "one", StartOffset: 0, EndOffset: 1, SizeBytes: math.MaxInt64},
					{ID: "two", StartOffset: 1, EndOffset: 2, SizeBytes: 1},
				},
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := readRemoteLogSegments(
				context.Background(),
				remoteFileSettings{reader: reader, config: test.config},
				test.info, nil, nil,
			); !errors.Is(err, ErrValidation) {
				t.Fatalf("readRemoteLogSegments() error = %v", err)
			}
		})
	}
	if calls.Load() != 0 {
		t.Fatalf("reader calls = %d, want 0", calls.Load())
	}
}

func TestReadRemoteLogSegmentsOrdersSlicesAndRetries(t *testing.T) {
	var calls atomic.Int32
	var mu sync.Mutex
	var paths []string
	reader := RemoteFileReaderFunc(func(_ context.Context, request RemoteFileRequest) ([]byte, error) {
		mu.Lock()
		paths = append(paths, request.Path)
		mu.Unlock()
		if calls.Add(1) == 1 {
			return nil, os.ErrDeadlineExceeded
		}
		if strings.Contains(request.Path, "first") {
			return []byte("xxFIRST"), nil
		}
		return []byte("SECOND"), nil
	})
	observer := &metricRecorder{}
	info := RemoteLogFetchInfo{
		TabletDirectory: "s3://bucket/tablet", FirstStartPosition: 2,
		Segments: []RemoteLogSegment{
			{ID: "second", StartOffset: 10, EndOffset: 20, SizeBytes: 6},
			{ID: "first", StartOffset: 0, EndOffset: 10, SizeBytes: 7},
		},
	}
	data, err := readRemoteLogSegments(
		context.Background(),
		remoteFileSettings{reader: reader, config: RemoteFileReadConfig{
			MaxAttempts: 2, MaxFileBytes: 100,
		}},
		info,
		&FileSystemSecurityToken{Schema: "hadoop", Token: []byte("secret")},
		observer,
	)
	if err != nil || string(data) != "FIRSTSECOND" {
		t.Fatalf("readRemoteLogSegments() = %q, %v", data, err)
	}
	mu.Lock()
	defer mu.Unlock()
	firstCalls, secondCalls := countRemoteLogPaths(paths)
	if len(paths) != 3 || firstCalls == 0 || secondCalls == 0 {
		t.Fatalf("paths = %#v", paths)
	}
	events := observer.snapshot()
	failures, bytesRead := summarizeRemoteMetrics(events)
	if len(events) != 3 || failures != 1 || bytesRead != 11 {
		t.Fatalf("remote metrics = %#v", events)
	}
}

func countRemoteLogPaths(paths []string) (int, int) {
	first, second := 0, 0
	for _, path := range paths {
		if strings.Contains(path, "first/00000000000000000000.log") {
			first++
		}
		if strings.Contains(path, "second/00000000000000000010.log") {
			second++
		}
	}
	return first, second
}

func summarizeRemoteMetrics(events []MetricEvent) (int, int64) {
	failures, bytesRead := 0, int64(0)
	for _, event := range events {
		if event.Failed {
			failures++
			continue
		}
		bytesRead += event.Bytes
	}
	return failures, bytesRead
}

func TestReadRemoteLogSegmentsPrefetchesRangesAndPreservesOrder(t *testing.T) {
	firstPath := "/remote/first/00000000000000000000.log"
	secondPath := "/remote/second/00000000000000000001.log"
	firstRelease, secondRelease := make(chan struct{}), make(chan struct{})
	reader := &controlledRemoteStreamReader{
		objects: map[string][]byte{
			firstPath:  []byte("xxAAAA"),
			secondPath: []byte("BBBB"),
		},
		releases: map[string]<-chan struct{}{
			firstPath: firstRelease, secondPath: secondRelease,
		},
		entered: map[string]chan struct{}{
			firstPath: make(chan struct{}), secondPath: make(chan struct{}),
		},
		completed: map[string]chan struct{}{
			firstPath: make(chan struct{}), secondPath: make(chan struct{}),
		},
	}
	info := RemoteLogFetchInfo{
		TabletDirectory: "/remote", FirstStartPosition: 2,
		Segments: []RemoteLogSegment{
			{ID: "first", StartOffset: 0, EndOffset: 1, SizeBytes: 6},
			{ID: "second", StartOffset: 1, EndOffset: 2, SizeBytes: 4},
		},
	}
	result := make(chan struct {
		data []byte
		err  error
	}, 1)
	go func() {
		data, err := readRemoteLogSegments(
			context.Background(),
			remoteFileSettings{
				reader: reader,
				config: RemoteFileReadConfig{
					MaxAttempts: 1, MaxFileBytes: 6, MaxTotalBytes: 8,
					MaxFiles: 2, MaxConcurrentReads: 2, MaxConcurrentBytes: 8,
				},
			},
			info, nil, nil,
		)
		result <- struct {
			data []byte
			err  error
		}{data, err}
	}()
	<-reader.entered[firstPath]
	<-reader.entered[secondPath]
	close(secondRelease)
	<-reader.completed[secondPath]
	select {
	case <-result:
		t.Fatal("ordered result completed before first range")
	default:
	}
	close(firstRelease)
	got := <-result
	if got.err != nil || string(got.data) != "AAAABBBB" {
		t.Fatalf("prefetched log = %q, %v", got.data, got.err)
	}
	reader.mu.Lock()
	defer reader.mu.Unlock()
	requests := make(map[string]RemoteFileRequest, len(reader.requests))
	for _, request := range reader.requests {
		requests[request.Path] = request
	}
	if reader.maxActive != 2 || reader.closed != 2 ||
		len(reader.requests) != 2 ||
		requests[firstPath].Offset != 2 || requests[firstPath].Length != 4 ||
		requests[secondPath].Offset != 0 || requests[secondPath].Length != 4 {
		t.Fatalf(
			"stream state: active=%d closed=%d requests=%#v",
			reader.maxActive, reader.closed, reader.requests,
		)
	}
}

func TestRemotePrefetchCancellationClosesStreams(t *testing.T) {
	paths := []string{
		"/remote/first/00000000000000000000.log",
		"/remote/second/00000000000000000001.log",
	}
	reader := &controlledRemoteStreamReader{
		objects: map[string][]byte{paths[0]: []byte("AAAA"), paths[1]: []byte("BBBB")},
		releases: map[string]<-chan struct{}{
			paths[0]: make(chan struct{}), paths[1]: make(chan struct{}),
		},
		entered: map[string]chan struct{}{
			paths[0]: make(chan struct{}), paths[1]: make(chan struct{}),
		},
		completed: map[string]chan struct{}{
			paths[0]: make(chan struct{}), paths[1]: make(chan struct{}),
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := readRemoteLogSegments(
			ctx,
			remoteFileSettings{
				reader: reader,
				config: RemoteFileReadConfig{
					MaxAttempts: 1, MaxFileBytes: 4, MaxTotalBytes: 8,
					MaxFiles: 2, MaxConcurrentReads: 2, MaxConcurrentBytes: 8,
				},
			},
			RemoteLogFetchInfo{
				TabletDirectory: "/remote",
				Segments: []RemoteLogSegment{
					{ID: "first", StartOffset: 0, EndOffset: 1, SizeBytes: 4},
					{ID: "second", StartOffset: 1, EndOffset: 2, SizeBytes: 4},
				},
			},
			nil, nil,
		)
		done <- err
	}()
	<-reader.entered[paths[0]]
	<-reader.entered[paths[1]]
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled prefetch error = %v", err)
	}
	reader.mu.Lock()
	defer reader.mu.Unlock()
	if reader.active != 0 || reader.closed != 2 {
		t.Fatalf("stream cleanup: active=%d closed=%d", reader.active, reader.closed)
	}
}

func TestRemotePrefetchRejectsConcurrentByteLimitBeforeReading(t *testing.T) {
	var calls atomic.Int32
	reader := RemoteFileReaderFunc(func(context.Context, RemoteFileRequest) ([]byte, error) {
		calls.Add(1)
		return []byte("data"), nil
	})
	_, err := readRemoteLogSegments(
		context.Background(),
		remoteFileSettings{
			reader: reader,
			config: RemoteFileReadConfig{
				MaxAttempts: 1, MaxFileBytes: 4, MaxTotalBytes: 4,
				MaxFiles: 1, MaxConcurrentReads: 1, MaxConcurrentBytes: 3,
			},
		},
		RemoteLogFetchInfo{
			TabletDirectory: "/remote",
			Segments: []RemoteLogSegment{{
				ID: "one", StartOffset: 0, EndOffset: 1, SizeBytes: 4,
			}},
		},
		nil, nil,
	)
	if !errors.Is(err, ErrValidation) || calls.Load() != 0 {
		t.Fatalf("concurrent byte limit = %v, calls=%d", err, calls.Load())
	}
}

func TestReadRemoteLogSegmentsRejectsInvalidAndPartialData(t *testing.T) {
	reader := RemoteFileReaderFunc(func(context.Context, RemoteFileRequest) ([]byte, error) {
		return []byte("short"), nil
	})
	settings := remoteFileSettings{reader: reader, config: RemoteFileReadConfig{
		MaxAttempts: 1, MaxFileBytes: 10,
	}}
	base := RemoteLogFetchInfo{
		TabletDirectory: "/remote", Segments: []RemoteLogSegment{{
			ID: "id", StartOffset: 0, EndOffset: 1, SizeBytes: 6,
		}},
	}
	if _, err := readRemoteLogSegments(
		context.Background(), settings, base, nil, nil,
	); !errors.Is(err, ErrValidation) {
		t.Fatalf("size error = %v", err)
	}
	invalid := base
	invalid.FirstStartPosition = 10
	invalid.Segments[0].SizeBytes = 5
	if _, err := readRemoteLogSegments(
		context.Background(), settings, invalid, nil, nil,
	); !errors.Is(err, ErrMalformedRecordBatch) {
		t.Fatalf("start position error = %v", err)
	}
	overlap := base
	overlap.Segments = append(overlap.Segments, RemoteLogSegment{
		ID: "two", StartOffset: 0, EndOffset: 2, SizeBytes: 5,
	})
	if _, err := readRemoteLogSegments(
		context.Background(), settings, overlap, nil, nil,
	); !errors.Is(err, ErrValidation) {
		t.Fatalf("overlap error = %v", err)
	}
	gap := base
	gap.Segments = append(gap.Segments, RemoteLogSegment{
		ID: "two", StartOffset: 2, EndOffset: 3, SizeBytes: 5,
	})
	if _, err := readRemoteLogSegments(
		context.Background(), settings, gap, nil, nil,
	); !errors.Is(err, ErrValidation) {
		t.Fatalf("gap error = %v", err)
	}
}

func TestRemoteAndLocalLogPayloadsMergeWithoutGaps(t *testing.T) {
	table := logWriterTable()
	remote, err := (LogBatch{
		Magic: 1, BaseOffset: 0, SchemaID: int16(table.SchemaID), AppendOnly: true,
		Records: []Record{{Change: Append, Value: Row{int32(1), "remote"}}},
	}).EncodeRows(table.Schema, true)
	if err != nil {
		t.Fatal(err)
	}
	local, err := (LogBatch{
		Magic: 1, BaseOffset: 1, SchemaID: int16(table.SchemaID), AppendOnly: true,
		Records: []Record{{Change: Append, Value: Row{int32(2), "local"}}},
	}).EncodeRows(table.Schema, true)
	if err != nil {
		t.Fatal(err)
	}
	reader := RemoteFileReaderFunc(func(context.Context, RemoteFileRequest) ([]byte, error) {
		return remote, nil
	})
	info := RemoteLogFetchInfo{
		TabletDirectory: "/remote",
		Segments: []RemoteLogSegment{{
			ID: "segment", StartOffset: 0, EndOffset: 1, SizeBytes: int64(len(remote)),
		}},
	}
	downloaded, err := readRemoteLogSegments(
		context.Background(),
		remoteFileSettings{reader: reader, config: RemoteFileReadConfig{
			MaxAttempts: 1, MaxFileBytes: int64(len(remote)),
		}},
		info, nil, nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	_, rows, arrows, err := decodeFetchedLog(
		table.Schema, 0, 0, append(downloaded, local...),
	)
	releaseScanArrows(arrows)
	if err != nil || len(rows) != 2 ||
		rows[0].Record.Offset != 0 || rows[1].Record.Offset != 1 ||
		rows[0].Record.Value[1] != "remote" || rows[1].Record.Value[1] != "local" {
		t.Fatalf("merged rows = %#v, %v", rows, err)
	}
}

func TestClientRemoteLogRequiresReaderAndCopiesToken(t *testing.T) {
	info := &RemoteLogFetchInfo{
		TabletDirectory: "/remote",
		Segments:        []RemoteLogSegment{{ID: "id", StartOffset: 0, EndOffset: 1, SizeBytes: 1}},
	}
	if _, err := (&Client{}).readRemoteLog(
		context.Background(), info,
	); !errors.Is(err, ErrUnsupportedAPI) {
		t.Fatalf("missing reader error = %v", err)
	}
	if data, err := (&Client{}).readRemoteLog(context.Background(), nil); err != nil || data != nil {
		t.Fatalf("empty remote log = %v, %v", data, err)
	}

	client := &Client{remoteFiles: remoteFileSettings{
		reader: RemoteFileReaderFunc(func(_ context.Context, request RemoteFileRequest) ([]byte, error) {
			if request.Token != nil {
				t.Fatal("unexpected token")
			}
			return []byte("x"), nil
		}),
		config: RemoteFileReadConfig{MaxAttempts: 1, MaxFileBytes: 10},
	}}
	data, err := client.readRemoteLog(context.Background(), info)
	if err != nil || string(data) != "x" {
		t.Fatalf("readRemoteLog() = %q, %v", data, err)
	}
}

func TestRemoteLogFetchInfoCopiesProtocolMetadata(t *testing.T) {
	pb := &fmsg.PbRemoteLogFetchInfo{
		RemoteLogTabletDir: proto.String("s3://bucket/tablet"),
		PartitionName:      proto.String("kr"),
		FirstStartPos:      proto.Int32(12),
		RemoteLogSegments: []*fmsg.PbRemoteLogSegment{{
			RemoteLogSegmentId:   proto.String("id"),
			RemoteLogStartOffset: proto.Int64(10),
			RemoteLogEndOffset:   proto.Int64(20),
			SegmentSizeInBytes:   proto.Int32(30),
			MaxTimestamp:         proto.Int64(40),
		}},
	}
	info := remoteLogFetchInfo(pb)
	if info.TabletDirectory != "s3://bucket/tablet" || info.PartitionName != "kr" ||
		info.FirstStartPosition != 12 || info.Segments[0].StartOffset != 10 ||
		info.Segments[0].MaxTime.UnixMilli() != 40 {
		t.Fatalf("remoteLogFetchInfo() = %#v", info)
	}
	if remoteLogFetchInfo(nil) != nil {
		t.Fatal("nil protocol metadata must stay nil")
	}
}

func TestRemoteSnapshotBatchProviderDownloadsAndDecodes(t *testing.T) {
	token := FileSystemSecurityToken{Schema: "hadoop", Token: []byte("secret")}
	reader := RemoteFileReaderFunc(func(_ context.Context, request RemoteFileRequest) ([]byte, error) {
		if request.Token == nil || string(request.Token.Token) != "secret" {
			t.Fatalf("snapshot token = %#v", request.Token)
		}
		return []byte(request.Path), nil
	})
	resolver := RemoteSnapshotResolverFunc(func(
		_ context.Context,
		request SnapshotBatchRequest,
	) ([]RemoteSnapshotFile, error) {
		if request.SnapshotID != 9 {
			t.Fatalf("snapshot request = %#v", request)
		}
		return []RemoteSnapshotFile{{Path: "one", Size: 3}, {Path: "two", Size: 3}}, nil
	})
	expectedReader := &fakeSnapshotBatchReader{}
	decoder := RemoteSnapshotDecoderFunc(func(
		_ context.Context,
		_ SnapshotBatchRequest,
		files []RemoteSnapshotFile,
	) (SnapshotBatchReader, error) {
		if string(files[0].Data) != "one" || string(files[1].Data) != "two" {
			t.Fatalf("snapshot files = %#v", files)
		}
		files[0].Data[0] = 'X'
		return expectedReader, nil
	})
	provider, err := NewRemoteSnapshotBatchProvider(
		reader, RemoteFileReadConfig{MaxAttempts: 1, MaxFileBytes: 10},
		resolver, decoder, func() (FileSystemSecurityToken, bool) { return token, true }, nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	got, err := provider.OpenSnapshot(context.Background(), SnapshotBatchRequest{SnapshotID: 9})
	if err != nil || got != expectedReader || string(token.Token) != "secret" {
		t.Fatalf("OpenSnapshot() = %#v, %v token=%q", got, err, token.Token)
	}
}

func TestRemoteSnapshotProviderPrefetchesAndPreservesFileOrder(t *testing.T) {
	oneRelease, twoRelease := make(chan struct{}), make(chan struct{})
	reader := &controlledRemoteStreamReader{
		objects: map[string][]byte{"one": []byte("111"), "two": []byte("222")},
		releases: map[string]<-chan struct{}{
			"one": oneRelease, "two": twoRelease,
		},
		entered: map[string]chan struct{}{
			"one": make(chan struct{}), "two": make(chan struct{}),
		},
		completed: map[string]chan struct{}{
			"one": make(chan struct{}), "two": make(chan struct{}),
		},
	}
	resolver := RemoteSnapshotResolverFunc(func(
		context.Context,
		SnapshotBatchRequest,
	) ([]RemoteSnapshotFile, error) {
		return []RemoteSnapshotFile{{Path: "one", Size: 3}, {Path: "two", Size: 3}}, nil
	})
	decoded := make(chan []string, 1)
	decoder := RemoteSnapshotDecoderFunc(func(
		_ context.Context,
		_ SnapshotBatchRequest,
		files []RemoteSnapshotFile,
	) (SnapshotBatchReader, error) {
		decoded <- []string{string(files[0].Data), string(files[1].Data)}
		return &fakeSnapshotBatchReader{}, nil
	})
	provider, err := NewRemoteSnapshotBatchProvider(
		reader,
		RemoteFileReadConfig{
			MaxAttempts: 1, MaxFileBytes: 3, MaxTotalBytes: 6,
			MaxFiles: 2, MaxConcurrentReads: 2, MaxConcurrentBytes: 6,
		},
		resolver, decoder, nil, nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := provider.OpenSnapshot(
			context.Background(), SnapshotBatchRequest{SnapshotID: 1},
		)
		done <- err
	}()
	<-reader.entered["one"]
	<-reader.entered["two"]
	close(twoRelease)
	<-reader.completed["two"]
	close(oneRelease)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if files := <-decoded; files[0] != "111" || files[1] != "222" {
		t.Fatalf("decoded file order = %#v", files)
	}
	reader.mu.Lock()
	defer reader.mu.Unlock()
	if reader.maxActive != 2 || reader.closed != 2 {
		t.Fatalf("snapshot streams: max=%d closed=%d", reader.maxActive, reader.closed)
	}
}

func TestRemoteSnapshotBatchProviderValidation(t *testing.T) {
	reader := RemoteFileReaderFunc(func(context.Context, RemoteFileRequest) ([]byte, error) {
		return []byte{1}, nil
	})
	resolver := RemoteSnapshotResolverFunc(func(
		context.Context,
		SnapshotBatchRequest,
	) ([]RemoteSnapshotFile, error) {
		return nil, nil
	})
	decoder := RemoteSnapshotDecoderFunc(func(
		context.Context,
		SnapshotBatchRequest,
		[]RemoteSnapshotFile,
	) (SnapshotBatchReader, error) {
		return &fakeSnapshotBatchReader{}, nil
	})
	if _, err := NewRemoteSnapshotBatchProvider(
		nil, RemoteFileReadConfig{}, resolver, decoder, nil, nil,
	); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("constructor error = %v", err)
	}
	provider, err := NewRemoteSnapshotBatchProvider(
		reader, RemoteFileReadConfig{}, resolver, decoder, nil, nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := provider.OpenSnapshot(
		context.Background(), SnapshotBatchRequest{},
	); !errors.Is(err, ErrValidation) {
		t.Fatalf("empty snapshot error = %v", err)
	}
	if _, err := (*RemoteSnapshotBatchProvider)(nil).OpenSnapshot(
		context.Background(), SnapshotBatchRequest{},
	); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("nil provider error = %v", err)
	}
}

func TestRemoteSnapshotRejectsAggregateLimitsBeforeReading(t *testing.T) {
	var calls atomic.Int32
	reader := RemoteFileReaderFunc(func(context.Context, RemoteFileRequest) ([]byte, error) {
		calls.Add(1)
		return []byte("data"), nil
	})
	resolver := RemoteSnapshotResolverFunc(func(
		context.Context,
		SnapshotBatchRequest,
	) ([]RemoteSnapshotFile, error) {
		return []RemoteSnapshotFile{{Path: "one", Size: 4}, {Path: "two", Size: 4}}, nil
	})
	decoder := RemoteSnapshotDecoderFunc(func(
		context.Context,
		SnapshotBatchRequest,
		[]RemoteSnapshotFile,
	) (SnapshotBatchReader, error) {
		return nil, errors.New("must not decode")
	})
	for _, settings := range []RemoteFileReadConfig{
		{MaxAttempts: 1, MaxFileBytes: 10, MaxTotalBytes: 7, MaxFiles: 2},
		{MaxAttempts: 1, MaxFileBytes: 10, MaxTotalBytes: 10, MaxFiles: 1},
	} {
		provider, err := NewRemoteSnapshotBatchProvider(
			reader, settings, resolver, decoder, nil, nil,
		)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := provider.OpenSnapshot(
			context.Background(), SnapshotBatchRequest{},
		); !errors.Is(err, ErrValidation) {
			t.Fatalf("OpenSnapshot() error = %v", err)
		}
	}
	if calls.Load() != 0 {
		t.Fatalf("reader calls = %d, want 0", calls.Load())
	}
}

type temporaryRemoteError struct {
	temporary bool
}

func (e temporaryRemoteError) Error() string   { return "remote failure" }
func (e temporaryRemoteError) Temporary() bool { return e.temporary }

func TestRemoteReadRetriesOnlyTemporaryFailures(t *testing.T) {
	settings := RemoteFileReadConfig{
		MaxAttempts: 3, RetryBackoff: time.Nanosecond,
		MaxFileBytes: 10, MaxTotalBytes: 10, MaxFiles: 1,
	}
	t.Run("permanent", func(t *testing.T) {
		var calls atomic.Int32
		reader := RemoteFileReaderFunc(func(context.Context, RemoteFileRequest) ([]byte, error) {
			calls.Add(1)
			return nil, os.ErrPermission
		})
		_, err := readRemoteFileWithRetry(
			context.Background(), remoteFileSettings{reader: reader, config: settings},
			RemoteFileRequest{Path: "object", ExpectedSize: 1}, nil,
		)
		if !errors.Is(err, os.ErrPermission) || calls.Load() != 1 {
			t.Fatalf("read = %v calls=%d", err, calls.Load())
		}
	})
	t.Run("temporary", func(t *testing.T) {
		var calls atomic.Int32
		reader := RemoteFileReaderFunc(func(_ context.Context, request RemoteFileRequest) ([]byte, error) {
			if request.MaxBytes != settings.MaxFileBytes {
				t.Fatalf("request MaxBytes = %d", request.MaxBytes)
			}
			if calls.Add(1) < 3 {
				return nil, temporaryRemoteError{temporary: true}
			}
			return []byte("x"), nil
		})
		data, err := readRemoteFileWithRetry(
			context.Background(), remoteFileSettings{reader: reader, config: settings},
			RemoteFileRequest{Path: "object", ExpectedSize: 1}, nil,
		)
		if err != nil || string(data) != "x" || calls.Load() != 3 {
			t.Fatalf("read = %q, %v calls=%d", data, err, calls.Load())
		}
	})
}

func BenchmarkReadRemoteLogSegments(b *testing.B) {
	const segmentSize = 1024
	data := make([]byte, segmentSize)
	segments := make([]RemoteLogSegment, 8)
	for index := range segments {
		segments[index] = RemoteLogSegment{
			ID: string(rune('a' + index)), StartOffset: int64(index),
			EndOffset: int64(index + 1), SizeBytes: segmentSize,
		}
	}
	settings := remoteFileSettings{
		reader: RemoteFileReaderFunc(func(context.Context, RemoteFileRequest) ([]byte, error) {
			return data, nil
		}),
		config: RemoteFileReadConfig{
			MaxAttempts: 1, MaxFileBytes: segmentSize,
			MaxTotalBytes: segmentSize * int64(len(segments)), MaxFiles: len(segments),
		},
	}
	info := RemoteLogFetchInfo{TabletDirectory: "/remote", Segments: segments}
	b.ReportAllocs()
	for range b.N {
		if _, err := readRemoteLogSegments(
			context.Background(), settings, info, nil, nil,
		); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkReadRemoteLogStreaming(b *testing.B) {
	const segmentSize = 1024
	segments := make([]RemoteLogSegment, 8)
	objects := make(map[string][]byte, len(segments))
	for index := range segments {
		segments[index] = RemoteLogSegment{
			ID: string(rune('a' + index)), StartOffset: int64(index),
			EndOffset: int64(index + 1), SizeBytes: segmentSize,
		}
		path := remoteLogSegmentPath("/remote", segments[index])
		objects[path] = make([]byte, segmentSize)
	}
	info := RemoteLogFetchInfo{TabletDirectory: "/remote", Segments: segments}
	for _, concurrency := range []int{1, 4} {
		b.Run(fmt.Sprintf("prefetch_%d", concurrency), func(b *testing.B) {
			reader := benchmarkRemoteStreamReader{
				objects: objects, delay: 100 * time.Microsecond,
			}
			settings := remoteFileSettings{
				reader: reader,
				config: RemoteFileReadConfig{
					MaxAttempts: 1, MaxFileBytes: segmentSize,
					MaxTotalBytes: segmentSize * int64(len(segments)),
					MaxFiles:      len(segments), MaxConcurrentReads: concurrency,
					MaxConcurrentBytes: segmentSize * int64(concurrency),
				},
			}
			b.ReportAllocs()
			for range b.N {
				if _, err := readRemoteLogSegments(
					context.Background(), settings, info, nil, nil,
				); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
