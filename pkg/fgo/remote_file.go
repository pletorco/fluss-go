package fgo

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// RemoteFileRequest describes one complete remote object read.
// Token is a caller-owned clone and may be nil.
type RemoteFileRequest struct {
	// Path is the storage-specific URI or absolute local path.
	Path string
	// ExpectedSize is the server-advertised size, or zero when unavailable.
	ExpectedSize int64
	// MaxBytes is the largest complete object the caller accepts. Zero defaults
	// to 256 MiB. Implementations must enforce this limit while reading, before
	// allocating or returning the complete object.
	MaxBytes int64
	// Offset is the first requested object byte for streaming readers.
	Offset int64
	// Length is the exact requested byte count for streaming readers. Zero
	// requests from Offset through the end of the object.
	Length int64
	// Token is an optional caller-owned credential clone.
	Token *FileSystemSecurityToken
}

// RemoteFileReader reads one complete Fluss-managed remote object. Filesystem-specific adapters
// can use the cloned security token without exposing it through errors or formatting.
type RemoteFileReader interface {
	// ReadRemoteFile returns the complete object and honors ctx cancellation.
	// The returned buffer becomes caller-owned.
	ReadRemoteFile(context.Context, RemoteFileRequest) ([]byte, error)
}

// RemoteFileStreamReader opens a bounded object range. Implementations must
// honor Offset, Length, ExpectedSize, MaxBytes, and context cancellation.
// Close must release all SDK response bodies and transport resources.
type RemoteFileStreamReader interface {
	// OpenRemoteFile opens the exact requested range and transfers response
	// body ownership to the caller.
	OpenRemoteFile(context.Context, RemoteFileRequest) (io.ReadCloser, error)
}

// RemoteFileReaderFunc adapts a function to [RemoteFileReader].
type RemoteFileReaderFunc func(context.Context, RemoteFileRequest) ([]byte, error)

// ReadRemoteFile calls f with request.
func (f RemoteFileReaderFunc) ReadRemoteFile(
	ctx context.Context,
	request RemoteFileRequest,
) ([]byte, error) {
	return f(ctx, request)
}

// LocalRemoteFileReader supports absolute paths and file:// URIs without an external dependency.
type LocalRemoteFileReader struct{}

// ReadRemoteFile reads an absolute local path or file URI.
func (LocalRemoteFileReader) ReadRemoteFile(
	ctx context.Context,
	request RemoteFileRequest,
) ([]byte, error) {
	stream, err := (LocalRemoteFileReader{}).OpenRemoteFile(ctx, request)
	if err != nil {
		return nil, err
	}
	defer stream.Close()
	maxBytes := request.MaxBytes
	if maxBytes == 0 {
		maxBytes = defaultRemoteMaxFileBytes
	}
	if request.Length > 0 {
		maxBytes = request.Length
	}
	readLimit := maxBytes
	if readLimit < math.MaxInt64 {
		readLimit++
	}
	data, err := io.ReadAll(io.LimitReader(stream, readLimit))
	if err != nil {
		return nil, fmt.Errorf("fgo: read remote file: %w", err)
	}
	if int64(len(data)) > maxBytes {
		return nil, fmt.Errorf("%w: remote file exceeds byte limit", ErrValidation)
	}
	return data, nil
}

// OpenRemoteFile opens an exact local-file range without buffering the object.
func (LocalRemoteFileReader) OpenRemoteFile(
	ctx context.Context,
	request RemoteFileRequest,
) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	path, err := localRemotePath(request.Path)
	if err != nil {
		return nil, err
	}
	maxBytes := request.MaxBytes
	if maxBytes == 0 {
		maxBytes = defaultRemoteMaxFileBytes
	}
	if maxBytes < 1 || request.ExpectedSize < 0 || request.Offset < 0 ||
		request.Length < 0 {
		return nil, fmt.Errorf("%w: invalid remote file size limit", ErrInvalidConfig)
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("fgo: open remote file: %w", err)
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("fgo: stat remote file: %w", err)
	}
	size := info.Size()
	if request.ExpectedSize > 0 && size != request.ExpectedSize {
		_ = file.Close()
		return nil, fmt.Errorf("%w: remote file size mismatch: %w", ErrValidation, io.ErrUnexpectedEOF)
	}
	length := request.Length
	if length == 0 {
		length = size - request.Offset
	}
	if request.Offset > size || length < 0 || length > size-request.Offset ||
		length > maxBytes {
		_ = file.Close()
		return nil, fmt.Errorf("%w: invalid remote file range", ErrValidation)
	}
	reader := io.NewSectionReader(file, request.Offset, length)
	return &sectionReadCloser{
		Reader: &contextReader{ctx: ctx, reader: reader},
		close:  file.Close,
	}, nil
}

func localRemotePath(rawPath string) (string, error) {
	parsed, err := url.Parse(rawPath)
	if err != nil {
		return "", fmt.Errorf("%w: invalid remote file path", ErrInvalidConfig)
	}
	path := rawPath
	switch parsed.Scheme {
	case "":
		if !filepath.IsAbs(path) {
			return "", fmt.Errorf("%w: remote file path must be absolute", ErrInvalidConfig)
		}
	case "file":
		if parsed.Host != "" && parsed.Host != "localhost" {
			return "", fmt.Errorf("%w: file URI host is not local", ErrInvalidConfig)
		}
		if parsed.RawQuery != "" || parsed.Fragment != "" {
			return "", fmt.Errorf("%w: file URI query and fragment are unsupported", ErrInvalidConfig)
		}
		path = filepath.FromSlash(parsed.Path)
		if !filepath.IsAbs(path) {
			return "", fmt.Errorf("%w: file URI path must be absolute", ErrInvalidConfig)
		}
	default:
		return "", fmt.Errorf("%w: unsupported remote file scheme %q", ErrUnsupportedAPI, parsed.Scheme)
	}
	return path, nil
}

type sectionReadCloser struct {
	io.Reader
	close func() error
}

func (r *sectionReadCloser) Close() error {
	return r.close()
}

type contextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r *contextReader) Read(buffer []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.reader.Read(buffer)
}

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

// RemoteSnapshotFile describes a snapshot object before and after download.
type RemoteSnapshotFile struct {
	// Path is the storage-specific object location.
	Path string
	// Size is the expected object size in bytes.
	Size int64
	// Data is populated with caller-owned downloaded bytes before decoding.
	Data []byte
}

// RemoteSnapshotResolver discovers immutable objects for one snapshot request.
type RemoteSnapshotResolver interface {
	// ResolveSnapshotFiles returns immutable files in decoder input order.
	ResolveSnapshotFiles(context.Context, SnapshotBatchRequest) ([]RemoteSnapshotFile, error)
}

// RemoteSnapshotResolverFunc adapts a function to [RemoteSnapshotResolver].
type RemoteSnapshotResolverFunc func(
	context.Context,
	SnapshotBatchRequest,
) ([]RemoteSnapshotFile, error)

// ResolveSnapshotFiles calls f with request.
func (f RemoteSnapshotResolverFunc) ResolveSnapshotFiles(
	ctx context.Context,
	request SnapshotBatchRequest,
) ([]RemoteSnapshotFile, error) {
	return f(ctx, request)
}

// RemoteSnapshotDecoder opens downloaded snapshot objects in a storage-specific
// format.
type RemoteSnapshotDecoder interface {
	// OpenSnapshotFiles consumes downloaded file descriptors and returns a
	// reader owned by the caller.
	OpenSnapshotFiles(context.Context, SnapshotBatchRequest, []RemoteSnapshotFile) (SnapshotBatchReader, error)
}

// RemoteSnapshotDecoderFunc adapts a function to [RemoteSnapshotDecoder].
type RemoteSnapshotDecoderFunc func(
	context.Context,
	SnapshotBatchRequest,
	[]RemoteSnapshotFile,
) (SnapshotBatchReader, error)

// OpenSnapshotFiles calls f with the downloaded files.
func (f RemoteSnapshotDecoderFunc) OpenSnapshotFiles(
	ctx context.Context,
	request SnapshotBatchRequest,
	files []RemoteSnapshotFile,
) (SnapshotBatchReader, error) {
	return f(ctx, request, files)
}

// FileSystemSecurityTokenSource returns a cloned current token when available.
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

// NewRemoteSnapshotBatchProvider composes object discovery, bounded downloads,
// and format-specific decoding.
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

// OpenSnapshot downloads and opens the requested immutable snapshot.
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
	if err := validateRemoteSnapshotFiles(files, p.files.config); err != nil {
		return nil, err
	}
	token := currentRemoteToken(p.tokenSource)
	downloaded := make([]RemoteSnapshotFile, len(files))
	jobs := p.snapshotDownloadJobs(files, downloaded, token)
	if err := runRemoteDownloads(ctx, p.files.config, jobs); err != nil {
		clearRemoteSnapshotData(downloaded)
		return nil, err
	}
	return p.decoder.OpenSnapshotFiles(ctx, request, downloaded)
}

func currentRemoteToken(source FileSystemSecurityTokenSource) *FileSystemSecurityToken {
	if source == nil {
		return nil
	}
	current, ok := source()
	if !ok {
		return nil
	}
	cloned := current.Clone()
	return &cloned
}

func (p *RemoteSnapshotBatchProvider) snapshotDownloadJobs(
	files []RemoteSnapshotFile,
	downloaded []RemoteSnapshotFile,
	token *FileSystemSecurityToken,
) []remoteDownloadJob {
	jobs := make([]remoteDownloadJob, len(files))
	for index, file := range files {
		index, file := index, file
		jobs[index] = remoteDownloadJob{
			size: file.Size,
			run: func(ctx context.Context) error {
				if file.Size > int64(^uint(0)>>1) {
					return fmt.Errorf(
						"%w: snapshot file exceeds platform allocation limit",
						ErrValidation,
					)
				}
				data := make([]byte, int(file.Size))
				request := RemoteFileRequest{
					Path: file.Path, ExpectedSize: file.Size, Length: file.Size,
					Token: cloneRemoteToken(token),
				}
				if err := readRemoteFileIntoWithRetry(
					ctx, p.files, request, data, p.observer,
				); err != nil {
					return err
				}
				downloaded[index] = RemoteSnapshotFile{
					Path: file.Path, Size: file.Size, Data: data,
				}
				return nil
			},
		}
	}
	return jobs
}

func clearRemoteSnapshotData(files []RemoteSnapshotFile) {
	for index := range files {
		files[index].Data = nil
	}
}

func validateRemoteSnapshotFiles(
	files []RemoteSnapshotFile,
	config RemoteFileReadConfig,
) error {
	if len(files) > config.MaxFiles {
		return fmt.Errorf("%w: snapshot exceeds remote file-count limit", ErrValidation)
	}
	var total int64
	for _, file := range files {
		if file.Path == "" || file.Size <= 0 || file.Size > config.MaxFileBytes {
			return fmt.Errorf("%w: invalid remote snapshot file", ErrValidation)
		}
		if file.Size > config.MaxTotalBytes-total {
			return fmt.Errorf("%w: snapshot exceeds aggregate remote byte limit", ErrValidation)
		}
		total += file.Size
	}
	return nil
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
	sort.Slice(segments, func(i, j int) bool {
		return segments[i].StartOffset < segments[j].StartOffset
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

type remoteDownloadJob struct {
	size int64
	run  func(context.Context) error
}

type remoteDownloadResult struct {
	size int64
	err  error
}

type remoteDownloadScheduler struct {
	ctx         context.Context
	runCtx      context.Context
	cancel      context.CancelFunc
	config      RemoteFileReadConfig
	jobs        []remoteDownloadJob
	completed   chan remoteDownloadResult
	contextDone <-chan struct{}
	next        int
	active      int
	activeBytes int64
	firstErr    error
}

func runRemoteDownloads(
	ctx context.Context,
	config RemoteFileReadConfig,
	jobs []remoteDownloadJob,
) error {
	if len(jobs) == 0 {
		return nil
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	scheduler := remoteDownloadScheduler{
		ctx: ctx, runCtx: runCtx, cancel: cancel, config: config, jobs: jobs,
		completed:   make(chan remoteDownloadResult, config.MaxConcurrentReads),
		contextDone: ctx.Done(),
	}
	for scheduler.hasWork() {
		scheduler.startReady()
		if scheduler.active == 0 {
			return scheduler.idleError()
		}
		scheduler.awaitCompletion()
	}
	return scheduler.firstErr
}

func (s *remoteDownloadScheduler) hasWork() bool {
	return s.next < len(s.jobs) || s.active != 0
}

func (s *remoteDownloadScheduler) startReady() {
	for s.firstErr == nil && s.next < len(s.jobs) &&
		s.active < s.config.MaxConcurrentReads &&
		s.jobs[s.next].size <= s.config.MaxConcurrentBytes-s.activeBytes {
		job := s.jobs[s.next]
		s.next++
		s.active++
		s.activeBytes += job.size
		go func() {
			s.completed <- remoteDownloadResult{size: job.size, err: job.run(s.runCtx)}
		}()
	}
}

func (s *remoteDownloadScheduler) idleError() error {
	if s.firstErr != nil {
		return s.firstErr
	}
	return fmt.Errorf(
		"%w: remote object exceeds concurrent byte budget",
		ErrValidation,
	)
}

func (s *remoteDownloadScheduler) awaitCompletion() {
	select {
	case result := <-s.completed:
		s.active--
		s.activeBytes -= result.size
		if result.err != nil && s.firstErr == nil {
			s.firstErr = result.err
			s.cancel()
		}
	case <-s.contextDone:
		if s.firstErr == nil {
			s.firstErr = s.ctx.Err()
			s.cancel()
		}
		s.contextDone = nil
	}
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
