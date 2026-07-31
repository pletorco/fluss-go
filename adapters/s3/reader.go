// Package s3 provides an optional AWS SDK v2 remote-file adapter.
package s3

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net/url"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/pletorco/fluss-go/pkg/fgo"
)

// GetObjectAPI is the subset of the official S3 client used by Reader.
type GetObjectAPI interface {
	// GetObject retrieves one complete object or byte range.
	GetObject(
		context.Context,
		*awss3.GetObjectInput,
		...func(*awss3.Options),
	) (*awss3.GetObjectOutput, error)
}

// Reader adapts the official AWS SDK v2 S3 client to fgo remote-file reads.
type Reader struct {
	client GetObjectAPI
}

// New creates an S3 reader from an official SDK client.
func New(client GetObjectAPI) (*Reader, error) {
	if client == nil {
		return nil, fmt.Errorf("%w: nil S3 client", fgo.ErrInvalidConfig)
	}
	return &Reader{client: client}, nil
}

// NewFromConfig creates a reader with the official S3 client and optional
// client configuration functions.
func NewFromConfig(config aws.Config, options ...func(*awss3.Options)) *Reader {
	return &Reader{client: awss3.NewFromConfig(config, options...)}
}

// ReadRemoteFile reads one bounded complete object for compatibility with
// fgo.RemoteFileReader. Prefetch uses OpenRemoteFile directly.
func (r *Reader) ReadRemoteFile(
	ctx context.Context,
	request fgo.RemoteFileRequest,
) ([]byte, error) {
	if request.ExpectedSize <= 0 || request.ExpectedSize > int64(^uint(0)>>1) {
		return nil, fmt.Errorf("%w: S3 complete reads require expected size", fgo.ErrInvalidConfig)
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
			"%w: S3 object size mismatch: %w",
			fgo.ErrValidation, io.ErrUnexpectedEOF,
		)
	}
	return data, nil
}

// OpenRemoteFile opens one exact S3 object range. The caller owns and must
// close the returned SDK response body.
func (r *Reader) OpenRemoteFile(
	ctx context.Context,
	request fgo.RemoteFileRequest,
) (io.ReadCloser, error) {
	if r == nil || r.client == nil {
		return nil, fmt.Errorf("%w: nil S3 reader", fgo.ErrInvalidConfig)
	}
	if ctx == nil {
		return nil, fmt.Errorf("%w: nil context", fgo.ErrInvalidConfig)
	}
	bucket, key, err := parseURI(request.Path)
	if err != nil {
		return nil, err
	}
	offset, length, err := validateRange(request)
	if err != nil {
		return nil, err
	}
	end := offset + length - 1
	rangeHeader := fmt.Sprintf("bytes=%d-%d", offset, end)
	output, err := r.client.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Range:  aws.String(rangeHeader),
	})
	if err != nil {
		return nil, err
	}
	if output == nil || output.Body == nil {
		return nil, fmt.Errorf("%w: S3 response omitted body", fgo.ErrValidation)
	}
	if err := validateResponse(output, offset, length, request.ExpectedSize); err != nil {
		return nil, errors.Join(err, output.Body.Close())
	}
	return output.Body, nil
}

func parseURI(raw string) (string, string, error) {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme != "s3" || parsed.Host == "" ||
		parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", "", fmt.Errorf("%w: invalid S3 URI", fgo.ErrInvalidConfig)
	}
	escapedKey := strings.TrimPrefix(parsed.EscapedPath(), "/")
	key, err := url.PathUnescape(escapedKey)
	if err != nil || key == "" {
		return "", "", fmt.Errorf("%w: invalid S3 object key", fgo.ErrInvalidConfig)
	}
	return parsed.Host, key, nil
}

func validateRange(request fgo.RemoteFileRequest) (int64, int64, error) {
	if request.Offset < 0 || request.ExpectedSize <= 0 ||
		request.Offset >= request.ExpectedSize {
		return 0, 0, fmt.Errorf("%w: invalid S3 object range", fgo.ErrInvalidConfig)
	}
	length := request.Length
	if length == 0 {
		length = request.ExpectedSize - request.Offset
	}
	maxBytes := request.MaxBytes
	if maxBytes == 0 {
		maxBytes = request.ExpectedSize
	}
	if length <= 0 || maxBytes <= 0 || length > maxBytes ||
		length > request.ExpectedSize-request.Offset {
		return 0, 0, fmt.Errorf("%w: invalid S3 object range", fgo.ErrInvalidConfig)
	}
	return request.Offset, length, nil
}

func validateResponse(
	output *awss3.GetObjectOutput,
	offset int64,
	length int64,
	expectedSize int64,
) error {
	if output.ContentLength == nil || *output.ContentLength != length {
		return fmt.Errorf("%w: S3 content length mismatch", fgo.ErrValidation)
	}
	if output.ContentRange == nil {
		return fmt.Errorf("%w: S3 response omitted content range", fgo.ErrValidation)
	}
	start, end, total, err := parseContentRange(*output.ContentRange)
	if err != nil || start != offset || end != offset+length-1 ||
		total != expectedSize {
		return fmt.Errorf("%w: S3 content range mismatch", fgo.ErrValidation)
	}
	return nil
}

func parseContentRange(value string) (int64, int64, int64, error) {
	unit, rangeAndSize, ok := strings.Cut(value, " ")
	if !ok || unit != "bytes" {
		return 0, 0, 0, fmt.Errorf("invalid content range")
	}
	byteRange, size, ok := strings.Cut(rangeAndSize, "/")
	if !ok {
		return 0, 0, 0, fmt.Errorf("invalid content range")
	}
	start, end, ok := strings.Cut(byteRange, "-")
	if !ok {
		return 0, 0, 0, fmt.Errorf("invalid content range")
	}
	startValue, startErr := strconv.ParseInt(start, 10, 64)
	endValue, endErr := strconv.ParseInt(end, 10, 64)
	sizeValue, sizeErr := strconv.ParseInt(size, 10, 64)
	if startErr != nil || endErr != nil || sizeErr != nil ||
		startValue < 0 || endValue < startValue || sizeValue <= endValue {
		return 0, 0, 0, fmt.Errorf("invalid content range")
	}
	return startValue, endValue, sizeValue, nil
}

var (
	_ fgo.RemoteFileReader       = (*Reader)(nil)
	_ fgo.RemoteFileStreamReader = (*Reader)(nil)
)
