package fgo

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

type RemoteFileRequest struct {
	Path         string
	ExpectedSize int64
	Token        *FileSystemSecurityToken
}

// RemoteFileReader reads one complete Fluss-managed remote object. Filesystem-specific adapters
// can use the cloned security token without exposing it through errors or formatting.
type RemoteFileReader interface {
	ReadRemoteFile(context.Context, RemoteFileRequest) ([]byte, error)
}

type RemoteFileReaderFunc func(context.Context, RemoteFileRequest) ([]byte, error)

func (f RemoteFileReaderFunc) ReadRemoteFile(
	ctx context.Context,
	request RemoteFileRequest,
) ([]byte, error) {
	return f(ctx, request)
}

// LocalRemoteFileReader supports absolute paths and file:// URIs without an external dependency.
type LocalRemoteFileReader struct{}

func (LocalRemoteFileReader) ReadRemoteFile(
	ctx context.Context,
	request RemoteFileRequest,
) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	parsed, err := url.Parse(request.Path)
	if err != nil {
		return nil, fmt.Errorf("%w: invalid remote file path", ErrInvalidConfig)
	}
	path := request.Path
	switch parsed.Scheme {
	case "":
	case "file":
		if parsed.Host != "" && parsed.Host != "localhost" {
			return nil, fmt.Errorf("%w: file URI host is not local", ErrInvalidConfig)
		}
		path = filepath.FromSlash(parsed.Path)
	default:
		return nil, fmt.Errorf("%w: unsupported remote file scheme %q", ErrUnsupportedAPI, parsed.Scheme)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("fgo: read remote file: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return data, nil
}

type RemoteFileReadConfig struct {
	MaxAttempts  int
	RetryBackoff time.Duration
	MaxFileBytes int64
}

func (c RemoteFileReadConfig) normalized() (RemoteFileReadConfig, error) {
	if c.MaxAttempts == 0 {
		c.MaxAttempts = 3
	}
	if c.RetryBackoff == 0 {
		c.RetryBackoff = 50 * time.Millisecond
	}
	if c.MaxFileBytes == 0 {
		c.MaxFileBytes = 256 << 20
	}
	if c.MaxAttempts < 1 || c.MaxAttempts > 10 || c.RetryBackoff < 0 ||
		c.RetryBackoff > time.Minute || c.MaxFileBytes < 1 {
		return RemoteFileReadConfig{}, fmt.Errorf("%w: invalid remote file settings", ErrInvalidConfig)
	}
	return c, nil
}

// WithRemoteFileReader enables remote log reads and supplies the shared filesystem adapter.
func WithRemoteFileReader(reader RemoteFileReader, settings RemoteFileReadConfig) Option {
	return func(config *config) error {
		if reader == nil {
			return fmt.Errorf("%w: nil remote file reader", ErrInvalidConfig)
		}
		normalized, err := settings.normalized()
		if err != nil {
			return err
		}
		config.remoteFiles = remoteFileSettings{reader: reader, config: normalized}
		return nil
	}
}

type remoteFileSettings struct {
	reader RemoteFileReader
	config RemoteFileReadConfig
}

type RemoteSnapshotFile struct {
	Path string
	Size int64
	Data []byte
}

type RemoteSnapshotResolver interface {
	ResolveSnapshotFiles(context.Context, SnapshotBatchRequest) ([]RemoteSnapshotFile, error)
}

type RemoteSnapshotResolverFunc func(
	context.Context,
	SnapshotBatchRequest,
) ([]RemoteSnapshotFile, error)

func (f RemoteSnapshotResolverFunc) ResolveSnapshotFiles(
	ctx context.Context,
	request SnapshotBatchRequest,
) ([]RemoteSnapshotFile, error) {
	return f(ctx, request)
}

type RemoteSnapshotDecoder interface {
	OpenSnapshotFiles(context.Context, SnapshotBatchRequest, []RemoteSnapshotFile) (SnapshotBatchReader, error)
}

type RemoteSnapshotDecoderFunc func(
	context.Context,
	SnapshotBatchRequest,
	[]RemoteSnapshotFile,
) (SnapshotBatchReader, error)

func (f RemoteSnapshotDecoderFunc) OpenSnapshotFiles(
	ctx context.Context,
	request SnapshotBatchRequest,
	files []RemoteSnapshotFile,
) (SnapshotBatchReader, error) {
	return f(ctx, request, files)
}

type FileSystemSecurityTokenSource func() (FileSystemSecurityToken, bool)

// RemoteSnapshotBatchProvider composes snapshot metadata and format adapters with the shared
// remote-file transport. Decoder-specific dependencies remain optional.
type RemoteSnapshotBatchProvider struct {
	files       remoteFileSettings
	resolver    RemoteSnapshotResolver
	decoder     RemoteSnapshotDecoder
	tokenSource FileSystemSecurityTokenSource
	observer    MetricsObserver
}

func NewRemoteSnapshotBatchProvider(
	reader RemoteFileReader,
	settings RemoteFileReadConfig,
	resolver RemoteSnapshotResolver,
	decoder RemoteSnapshotDecoder,
	tokenSource FileSystemSecurityTokenSource,
	observer MetricsObserver,
) (*RemoteSnapshotBatchProvider, error) {
	if reader == nil || resolver == nil || decoder == nil {
		return nil, fmt.Errorf("%w: remote snapshot reader, resolver, and decoder are required", ErrInvalidConfig)
	}
	normalized, err := settings.normalized()
	if err != nil {
		return nil, err
	}
	return &RemoteSnapshotBatchProvider{
		files:    remoteFileSettings{reader: reader, config: normalized},
		resolver: resolver, decoder: decoder, tokenSource: tokenSource, observer: observer,
	}, nil
}

func (p *RemoteSnapshotBatchProvider) OpenSnapshot(
	ctx context.Context,
	request SnapshotBatchRequest,
) (SnapshotBatchReader, error) {
	if p == nil {
		return nil, fmt.Errorf("%w: nil remote snapshot provider", ErrInvalidConfig)
	}
	files, err := p.resolver.ResolveSnapshotFiles(ctx, request)
	if err != nil {
		return nil, err
	}
	if len(files) == 0 {
		return nil, fmt.Errorf("%w: snapshot has no files", ErrValidation)
	}
	var token *FileSystemSecurityToken
	if p.tokenSource != nil {
		if current, ok := p.tokenSource(); ok {
			cloned := current.Clone()
			token = &cloned
		}
	}
	downloaded := make([]RemoteSnapshotFile, len(files))
	for index, file := range files {
		if file.Path == "" || file.Size <= 0 || file.Size > p.files.config.MaxFileBytes {
			return nil, fmt.Errorf("%w: invalid remote snapshot file", ErrValidation)
		}
		data, err := readRemoteFileWithRetry(ctx, p.files, RemoteFileRequest{
			Path: file.Path, ExpectedSize: file.Size, Token: token,
		}, p.observer)
		if err != nil {
			return nil, err
		}
		downloaded[index] = RemoteSnapshotFile{Path: file.Path, Size: file.Size, Data: data}
	}
	return p.decoder.OpenSnapshotFiles(ctx, request, downloaded)
}

type RemoteLogSegment struct {
	ID          string
	StartOffset int64
	EndOffset   int64
	SizeBytes   int64
	MaxTime     time.Time
}

type RemoteLogFetchInfo struct {
	TabletDirectory    string
	PartitionName      string
	FirstStartPosition int
	Segments           []RemoteLogSegment
}

func (c *Client) readRemoteLog(
	ctx context.Context,
	info *RemoteLogFetchInfo,
) ([]byte, error) {
	if info == nil || len(info.Segments) == 0 {
		return nil, nil
	}
	if c.remoteFiles.reader == nil {
		return nil, fmt.Errorf("%w: remote file reader is not configured", ErrUnsupportedAPI)
	}
	var token *FileSystemSecurityToken
	if current, ok := c.CurrentFileSystemSecurityToken(); ok {
		cloned := current.Clone()
		token = &cloned
	}
	return readRemoteLogSegments(ctx, c.remoteFiles, *info, token, c.observer)
}

func readRemoteLogSegments(
	ctx context.Context,
	settings remoteFileSettings,
	info RemoteLogFetchInfo,
	token *FileSystemSecurityToken,
	observer MetricsObserver,
) ([]byte, error) {
	if strings.TrimSpace(info.TabletDirectory) == "" || info.FirstStartPosition < 0 {
		return nil, fmt.Errorf("%w: invalid remote log fetch info", ErrValidation)
	}
	segments := append([]RemoteLogSegment(nil), info.Segments...)
	sort.Slice(segments, func(i, j int) bool {
		return segments[i].StartOffset < segments[j].StartOffset
	})
	var result []byte
	var previousEnd int64 = -1
	for index, segment := range segments {
		if segment.ID == "" || segment.StartOffset < 0 || segment.EndOffset <= segment.StartOffset ||
			segment.SizeBytes <= 0 || segment.SizeBytes > settings.config.MaxFileBytes {
			return nil, fmt.Errorf("%w: invalid remote log segment", ErrValidation)
		}
		if previousEnd >= 0 && previousEnd != segment.StartOffset {
			return nil, fmt.Errorf("%w: remote log segments have a gap or overlap", ErrValidation)
		}
		previousEnd = segment.EndOffset
		path := remoteLogSegmentPath(info.TabletDirectory, segment)
		data, err := readRemoteFileWithRetry(ctx, settings, RemoteFileRequest{
			Path: path, ExpectedSize: segment.SizeBytes, Token: token,
		}, observer)
		if err != nil {
			return nil, err
		}
		start := 0
		if index == 0 {
			start = info.FirstStartPosition
		}
		if start > len(data) {
			return nil, fmt.Errorf("%w: remote log start position exceeds segment", ErrMalformedRecordBatch)
		}
		result = append(result, data[start:]...)
	}
	return result, nil
}

func readRemoteFileWithRetry(
	ctx context.Context,
	settings remoteFileSettings,
	request RemoteFileRequest,
	observer MetricsObserver,
) ([]byte, error) {
	var lastErr error
	for attempt := 1; attempt <= settings.config.MaxAttempts; attempt++ {
		started := time.Now()
		data, err := settings.reader.ReadRemoteFile(ctx, request)
		if err == nil && int64(len(data)) != request.ExpectedSize {
			err = fmt.Errorf("%w: remote file size mismatch: %w", ErrValidation, io.ErrUnexpectedEOF)
		}
		observeMetric(observer, MetricEvent{
			Kind: MetricRemoteIO, Operation: MetricOperationRemoteRead,
			Duration: time.Since(started), Attempt: attempt, Bytes: int64(len(data)),
			Failed: err != nil, ErrorClass: metricErrorClass(err),
		})
		if err == nil {
			return append([]byte(nil), data...), nil
		}
		lastErr = err
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if !remoteReadTemporary(err) {
			return nil, err
		}
		if attempt != settings.config.MaxAttempts {
			if err := waitContext(ctx, settings.config.RetryBackoff); err != nil {
				return nil, err
			}
		}
	}
	return nil, fmt.Errorf("fgo: remote file read failed: %w", lastErr)
}

func remoteLogSegmentPath(directory string, segment RemoteLogSegment) string {
	return strings.TrimRight(directory, "/") + "/" + segment.ID + "/" +
		fmt.Sprintf("%020d.log", segment.StartOffset)
}

func remoteReadTemporary(err error) bool {
	return errors.Is(err, io.ErrUnexpectedEOF) ||
		(err != nil && !errors.Is(err, ErrInvalidConfig) && !errors.Is(err, ErrValidation))
}
