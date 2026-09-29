package fgo

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"slices"
	"strings"
	"time"
)

const defaultRemoteMaxFileBytes int64 = 256 << 20

// RemoteFileReadConfig bounds retries, backoff, object size, aggregate bytes,
// and file count.
// Zero fields use documented defaults.
type RemoteFileReadConfig struct {
	// MaxAttempts includes the initial read; zero defaults to 3.
	MaxAttempts int
	// RetryBackoff is the delay between attempts; zero defaults to 50ms.
	RetryBackoff time.Duration
	// MaxFileBytes bounds allocation per object; zero defaults to 256 MiB.
	MaxFileBytes int64
	// MaxTotalBytes bounds bytes retained for one snapshot or remote-log read;
	// zero defaults to 512 MiB.
	MaxTotalBytes int64
	// MaxFiles bounds objects referenced by one operation; zero defaults to 4096.
	MaxFiles int
	// MaxConcurrentReads bounds active object streams; zero defaults to 4.
	MaxConcurrentReads int
	// MaxConcurrentBytes bounds advertised bytes across active streams; zero
	// defaults to 256 MiB.
	MaxConcurrentBytes int64
}

func (c RemoteFileReadConfig) normalized() (RemoteFileReadConfig, error) {
	if c.MaxAttempts == 0 {
		c.MaxAttempts = 3
	}
	if c.RetryBackoff == 0 {
		c.RetryBackoff = 50 * time.Millisecond
	}
	if c.MaxFileBytes == 0 {
		c.MaxFileBytes = defaultRemoteMaxFileBytes
	}
	if c.MaxTotalBytes == 0 {
		c.MaxTotalBytes = 512 << 20
	}
	if c.MaxFiles == 0 {
		c.MaxFiles = 4096
	}
	if c.MaxConcurrentReads == 0 {
		c.MaxConcurrentReads = 4
	}
	if c.MaxConcurrentBytes == 0 {
		c.MaxConcurrentBytes = defaultRemoteMaxFileBytes
	}
	if c.MaxAttempts < 1 || c.MaxAttempts > 10 || c.RetryBackoff < 0 ||
		c.RetryBackoff > time.Minute || c.MaxFileBytes < 1 ||
		c.MaxTotalBytes < 1 || c.MaxFiles < 1 || c.MaxFiles > 1_000_000 ||
		c.MaxConcurrentReads < 1 || c.MaxConcurrentReads > 1024 ||
		c.MaxConcurrentBytes < 1 {
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

// RemoteLogSegment describes one immutable remote log object and offset range.
type RemoteLogSegment struct {
	// ID identifies the segment in remote storage metadata.
	ID string
	// StartOffset is the first log offset in the segment.
	StartOffset int64
	// EndOffset is the exclusive segment end offset.
	EndOffset int64
	// SizeBytes is the expected encoded object size.
	SizeBytes int64
	// MaxTime is the greatest record timestamp in the segment.
	MaxTime time.Time
}

// RemoteLogFetchInfo describes remote segments referenced by a fetch response.
type RemoteLogFetchInfo struct {
	// TabletDirectory is the server-advertised remote tablet path.
	TabletDirectory string
	// PartitionName is the optional physical partition name.
	PartitionName string
	// FirstStartPosition is the byte position in the first segment.
	FirstStartPosition int
	// Segments are ordered by StartOffset.
	Segments []RemoteLogSegment
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
	normalized, err := settings.config.normalized()
	if err != nil {
		return nil, err
	}
	settings.config = normalized
	if strings.TrimSpace(info.TabletDirectory) == "" || info.FirstStartPosition < 0 {
		return nil, fmt.Errorf("%w: invalid remote log fetch info", ErrValidation)
	}
	segments, outputBytes, err := planRemoteLogSegments(settings.config, info)
	if err != nil {
		return nil, err
	}
	maxInt := int64(^uint(0) >> 1)
	if outputBytes > maxInt {
		return nil, fmt.Errorf("%w: remote log exceeds platform allocation limit", ErrValidation)
	}
	return downloadRemoteLogSegments(ctx, settings, info, segments, outputBytes, token, observer)
}

func planRemoteLogSegments(
	config RemoteFileReadConfig,
	info RemoteLogFetchInfo,
) ([]RemoteLogSegment, int64, error) {
	segments := append([]RemoteLogSegment(nil), info.Segments...)
	if len(segments) > config.MaxFiles {
		return nil, 0, fmt.Errorf("%w: remote log exceeds file-count limit", ErrValidation)
	}
	slices.SortFunc(segments, func(a, b RemoteLogSegment) int {
		return cmp.Compare(a.StartOffset, b.StartOffset)
	})
	var previousEnd int64 = -1
	var outputBytes int64
	for index, segment := range segments {
		if segment.ID == "" || segment.StartOffset < 0 || segment.EndOffset <= segment.StartOffset ||
			segment.SizeBytes <= 0 || segment.SizeBytes > config.MaxFileBytes {
			return nil, 0, fmt.Errorf("%w: invalid remote log segment", ErrValidation)
		}
		if previousEnd >= 0 && previousEnd != segment.StartOffset {
			return nil, 0, fmt.Errorf("%w: remote log segments have a gap or overlap", ErrValidation)
		}
		previousEnd = segment.EndOffset
		start := int64(0)
		if index == 0 {
			start = int64(info.FirstStartPosition)
		}
		if start > segment.SizeBytes {
			return nil, 0, fmt.Errorf("%w: remote log start position exceeds segment", ErrMalformedRecordBatch)
		}
		retained := segment.SizeBytes - start
		if retained > config.MaxTotalBytes-outputBytes {
			return nil, 0, fmt.Errorf("%w: remote log exceeds aggregate byte limit", ErrValidation)
		}
		outputBytes += retained
	}
	return segments, outputBytes, nil
}

func downloadRemoteLogSegments(
	ctx context.Context,
	settings remoteFileSettings,
	info RemoteLogFetchInfo,
	segments []RemoteLogSegment,
	outputBytes int64,
	token *FileSystemSecurityToken,
	observer MetricsObserver,
) ([]byte, error) {
	result := make([]byte, int(outputBytes))
	jobs := make([]remoteDownloadJob, len(segments))
	position := 0
	for index, segment := range segments {
		index, segment := index, segment
		path := remoteLogSegmentPath(info.TabletDirectory, segment)
		start := int64(0)
		if index == 0 {
			start = int64(info.FirstStartPosition)
		}
		length := segment.SizeBytes - start
		destination := result[position : position+int(length)]
		position += int(length)
		jobs[index] = remoteDownloadJob{
			size: length,
			run: func(ctx context.Context) error {
				return readRemoteFileIntoWithRetry(ctx, settings, RemoteFileRequest{
					Path: path, ExpectedSize: segment.SizeBytes,
					Offset: start, Length: length, Token: cloneRemoteToken(token),
				}, destination, observer)
			},
		}
	}
	if err := runRemoteDownloads(ctx, settings.config, jobs); err != nil {
		clear(result)
		return nil, err
	}
	return result, nil
}

func readRemoteFileWithRetry(
	ctx context.Context,
	settings remoteFileSettings,
	request RemoteFileRequest,
	observer MetricsObserver,
) ([]byte, error) {
	if request.ExpectedSize <= 0 || request.ExpectedSize > int64(^uint(0)>>1) {
		return nil, fmt.Errorf("%w: remote file requires a bounded expected size", ErrInvalidConfig)
	}
	length := request.Length
	if length == 0 {
		length = request.ExpectedSize - request.Offset
	}
	if length < 0 || length > int64(^uint(0)>>1) {
		return nil, fmt.Errorf("%w: invalid remote file range", ErrInvalidConfig)
	}
	data := make([]byte, int(length))
	if err := readRemoteFileIntoWithRetry(ctx, settings, request, data, observer); err != nil {
		return nil, err
	}
	return data, nil
}

func readRemoteFileIntoWithRetry(
	ctx context.Context,
	settings remoteFileSettings,
	request RemoteFileRequest,
	destination []byte,
	observer MetricsObserver,
) error {
	var lastErr error
	for attempt := 1; attempt <= settings.config.MaxAttempts; attempt++ {
		request.MaxBytes = settings.config.MaxFileBytes
		started := time.Now()
		read, err := readRemoteFileAttempt(ctx, settings.reader, request, destination)
		observeMetric(observer, MetricEvent{
			Kind: MetricRemoteIO, Operation: MetricOperationRemoteRead,
			Duration: time.Since(started), Attempt: attempt, Bytes: int64(read),
			Failed: err != nil, ErrorClass: metricErrorClass(err),
		})
		if err == nil {
			return nil
		}
		clear(destination)
		lastErr = err
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if !remoteReadTemporary(err) {
			return err
		}
		if attempt != settings.config.MaxAttempts {
			if err := waitContext(ctx, settings.config.RetryBackoff); err != nil {
				return err
			}
		}
	}
	return fmt.Errorf("fgo: remote file read failed: %w", lastErr)
}

func readRemoteFileAttempt(
	ctx context.Context,
	reader RemoteFileReader,
	request RemoteFileRequest,
	destination []byte,
) (int, error) {
	if request.Offset < 0 || request.ExpectedSize < 0 ||
		request.Offset > request.ExpectedSize ||
		int64(len(destination)) > request.ExpectedSize-request.Offset {
		return 0, fmt.Errorf("%w: invalid remote file range", ErrValidation)
	}
	if streamReader, ok := reader.(RemoteFileStreamReader); ok {
		return readRemoteFileStreamAttempt(ctx, streamReader, request, destination)
	}
	return readRemoteFileLegacyAttempt(ctx, reader, request, destination)
}

func readRemoteFileStreamAttempt(
	ctx context.Context,
	reader RemoteFileStreamReader,
	request RemoteFileRequest,
	destination []byte,
) (int, error) {
	request.Length = int64(len(destination))
	stream, err := reader.OpenRemoteFile(ctx, request)
	if err != nil {
		return 0, err
	}
	read, readErr := io.ReadFull(stream, destination)
	if readErr == nil {
		readErr = validateRemoteStreamEnd(stream)
	}
	closeErr := stream.Close()
	if readErr != nil {
		return read, readErr
	}
	if closeErr != nil {
		return read, closeErr
	}
	return read, nil
}

func validateRemoteStreamEnd(stream io.Reader) error {
	var extra [1]byte
	extraRead, err := stream.Read(extra[:])
	if extraRead != 0 {
		return fmt.Errorf("%w: remote range exceeds expected size", ErrValidation)
	}
	if err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	return nil
}

func readRemoteFileLegacyAttempt(
	ctx context.Context,
	reader RemoteFileReader,
	request RemoteFileRequest,
	destination []byte,
) (int, error) {
	completeRequest := request
	completeRequest.Offset, completeRequest.Length = 0, 0
	data, err := reader.ReadRemoteFile(ctx, completeRequest)
	if err != nil {
		return 0, err
	}
	if int64(len(data)) != request.ExpectedSize {
		return 0, fmt.Errorf("%w: remote file size mismatch: %w", ErrValidation, io.ErrUnexpectedEOF)
	}
	end := request.Offset + int64(len(destination))
	if end > int64(len(data)) {
		return 0, fmt.Errorf("%w: remote file range exceeds object", ErrValidation)
	}
	return copy(destination, data[request.Offset:end]), nil
}

func cloneRemoteToken(token *FileSystemSecurityToken) *FileSystemSecurityToken {
	if token == nil {
		return nil
	}
	cloned := token.Clone()
	return &cloned
}

func remoteLogSegmentPath(directory string, segment RemoteLogSegment) string {
	return strings.TrimRight(directory, "/") + "/" + segment.ID + "/" +
		fmt.Sprintf("%020d.log", segment.StartOffset)
}

func remoteReadTemporary(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, ErrInvalidConfig) || errors.Is(err, ErrValidation) ||
		errors.Is(err, ErrUnsupportedAPI) || errors.Is(err, os.ErrNotExist) ||
		errors.Is(err, os.ErrPermission) {
		return errors.Is(err, io.ErrUnexpectedEOF)
	}
	if errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	var temporary interface{ Temporary() bool }
	if errors.As(err, &temporary) {
		return temporary.Temporary()
	}
	var network net.Error
	return errors.As(err, &network) && network.Timeout()
}
