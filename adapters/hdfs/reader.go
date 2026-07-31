// Package hdfs provides a remote-file adapter boundary for HDFS clients.
package hdfs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net/url"
	"os"
	"strings"
	"sync"

	"github.com/pletorco/fluss-go/internal/storageadapter"
	"github.com/pletorco/fluss-go/pkg/fgo"
)

// File is the read-only HDFS file surface required by Reader. It matches the
// established HDFS Go client's FileReader methods without importing that
// unmaintained module.
type File interface {
	io.ReaderAt
	io.Closer
	// Stat returns metadata captured for the opened HDFS file.
	Stat() os.FileInfo
}

// OpenRequest describes one validated HDFS file open. Token is a private clone
// owned by Reader and remains valid until the returned stream is closed.
type OpenRequest struct {
	Authority string
	Path      string
	Token     *fgo.FileSystemSecurityToken
}

// OpenFunc opens an HDFS file with an application-owned client. Implementations
// should honor ctx, configure Kerberos or delegation-token authentication with
// maintained vendor code, and must not include token bytes in errors.
type OpenFunc func(context.Context, OpenRequest) (File, error)

// Reader validates HDFS paths and ranges around an application-owned HDFS
// client.
type Reader struct {
	open OpenFunc
}

// New creates an HDFS reader from an application-owned file opener.
func New(open OpenFunc) (*Reader, error) {
	if open == nil {
		return nil, fmt.Errorf("%w: nil HDFS opener", fgo.ErrInvalidConfig)
	}
	return &Reader{open: open}, nil
}

// ReadRemoteFile reads one bounded complete file for compatibility with
// fgo.RemoteFileReader. Prefetch uses OpenRemoteFile directly.
func (r *Reader) ReadRemoteFile(
	ctx context.Context,
	request fgo.RemoteFileRequest,
) ([]byte, error) {
	if request.ExpectedSize <= 0 || request.ExpectedSize > int64(^uint(0)>>1) {
		return nil, fmt.Errorf("%w: HDFS complete reads require expected size", fgo.ErrInvalidConfig)
	}
	request.Offset, request.Length = 0, request.ExpectedSize
	body, err := r.OpenRemoteFile(ctx, request)
	if err != nil {
		return nil, err
	}
	limit := request.ExpectedSize
	if limit < math.MaxInt64 {
		limit++
	}
	data, readErr := io.ReadAll(io.LimitReader(body, limit))
	closeErr := body.Close()
	if readErr != nil {
		return nil, readErr
	}
	if closeErr != nil {
		return nil, closeErr
	}
	if int64(len(data)) != request.ExpectedSize {
		return nil, fmt.Errorf(
			"%w: HDFS file size mismatch: %w",
			fgo.ErrValidation, io.ErrUnexpectedEOF,
		)
	}
	return data, nil
}

// OpenRemoteFile opens one exact HDFS file range. The caller must close the
// returned stream. Cancellation closes the underlying file to interrupt active
// reads.
func (r *Reader) OpenRemoteFile(
	ctx context.Context,
	request fgo.RemoteFileRequest,
) (io.ReadCloser, error) {
	if r == nil || r.open == nil {
		return nil, fmt.Errorf("%w: nil HDFS reader", fgo.ErrInvalidConfig)
	}
	if ctx == nil {
		return nil, fmt.Errorf("%w: nil context", fgo.ErrInvalidConfig)
	}
	authority, path, err := parseURI(request.Path)
	if err != nil {
		return nil, err
	}
	offset, length, err := storageadapter.ValidateRange(
		request.Offset, request.Length, request.ExpectedSize, request.MaxBytes,
	)
	if err != nil {
		return nil, fmt.Errorf("%w: invalid HDFS file range: %v", fgo.ErrInvalidConfig, err)
	}
	token := cloneToken(request.Token)
	file, err := r.open(ctx, OpenRequest{Authority: authority, Path: path, Token: token})
	if err != nil {
		clearToken(token)
		return nil, err
	}
	if file == nil {
		clearToken(token)
		return nil, fmt.Errorf("%w: HDFS opener returned nil file", fgo.ErrValidation)
	}
	if err := ctx.Err(); err != nil {
		clearToken(token)
		return nil, errors.Join(err, file.Close())
	}
	info := file.Stat()
	if info == nil || info.IsDir() || info.Size() != request.ExpectedSize {
		clearToken(token)
		return nil, errors.Join(
			fmt.Errorf("%w: HDFS file metadata mismatch", fgo.ErrValidation),
			file.Close(),
		)
	}
	return newRangeReadCloser(ctx, file, offset, length, token), nil
}

func parseURI(raw string) (string, string, error) {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme != "hdfs" || parsed.Host == "" ||
		parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", "", fmt.Errorf("%w: invalid HDFS URI", fgo.ErrInvalidConfig)
	}
	path, err := url.PathUnescape(parsed.EscapedPath())
	if err != nil || !strings.HasPrefix(path, "/") || path == "/" {
		return "", "", fmt.Errorf("%w: invalid HDFS path", fgo.ErrInvalidConfig)
	}
	return parsed.Host, path, nil
}

func cloneToken(token *fgo.FileSystemSecurityToken) *fgo.FileSystemSecurityToken {
	if token == nil {
		return nil
	}
	cloned := token.Clone()
	return &cloned
}

func clearToken(token *fgo.FileSystemSecurityToken) {
	if token == nil {
		return
	}
	clear(token.Token)
	token.Token = nil
	clear(token.AdditionalInfo)
	token.AdditionalInfo = nil
}

type rangeReadCloser struct {
	ctx      context.Context
	reader   *io.SectionReader
	file     File
	token    *fgo.FileSystemSecurityToken
	stop     func() bool
	done     chan struct{}
	once     sync.Once
	closeErr error
}

func newRangeReadCloser(
	ctx context.Context,
	file File,
	offset int64,
	length int64,
	token *fgo.FileSystemSecurityToken,
) *rangeReadCloser {
	stream := &rangeReadCloser{
		ctx: ctx, reader: io.NewSectionReader(file, offset, length),
		file: file, token: token, done: make(chan struct{}),
	}
	stream.stop = context.AfterFunc(ctx, stream.close)
	return stream
}

func (r *rangeReadCloser) Read(buffer []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	count, err := r.reader.Read(buffer)
	if contextErr := r.ctx.Err(); contextErr != nil && count == 0 {
		return 0, errors.Join(contextErr, err)
	}
	return count, err
}

func (r *rangeReadCloser) Close() error {
	if r.stop() {
		r.close()
	}
	<-r.done
	return r.closeErr
}

func (r *rangeReadCloser) close() {
	r.once.Do(func() {
		r.closeErr = r.file.Close()
		clearToken(r.token)
		close(r.done)
	})
}

var (
	_ fgo.RemoteFileReader       = (*Reader)(nil)
	_ fgo.RemoteFileStreamReader = (*Reader)(nil)
)
