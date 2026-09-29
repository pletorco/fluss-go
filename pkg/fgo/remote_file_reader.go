package fgo

import (
	"context"
	"fmt"
	"io"
	"math"
	"net/url"
	"os"
	"path/filepath"
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
