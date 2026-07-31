package fgo

import (
	"context"
	"errors"
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
		config.remoteFiles.config.MaxFiles != 4096 {
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
	if len(paths) != 3 || !strings.Contains(paths[0], "first/00000000000000000000.log") ||
		!strings.Contains(paths[2], "second/00000000000000000010.log") {
		t.Fatalf("paths = %#v", paths)
	}
	if events := observer.snapshot(); len(events) != 3 ||
		events[0].Kind != MetricRemoteIO || !events[0].Failed || events[2].Bytes != 6 {
		t.Fatalf("remote metrics = %#v", events)
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
