package fgo

import (
	"context"
	"fmt"
)

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
