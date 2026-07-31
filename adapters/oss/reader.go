// Package oss provides an optional Alibaba Cloud OSS SDK v2 remote-file
// adapter.
package oss

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"

	aliyunoss "github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss"
	"github.com/pletorco/fluss-go/internal/storageadapter"
	"github.com/pletorco/fluss-go/pkg/fgo"
)

// GetObjectAPI is the subset of the official OSS client used by Reader.
type GetObjectAPI interface {
	// GetObject retrieves one complete object or byte range.
	GetObject(
		context.Context,
		*aliyunoss.GetObjectRequest,
		...func(*aliyunoss.Options),
	) (*aliyunoss.GetObjectResult, error)
}

// Reader adapts the official Alibaba Cloud OSS SDK v2 client to fgo
// remote-file reads.
type Reader struct {
	client GetObjectAPI
}

// New creates an OSS reader from an official SDK client.
func New(client GetObjectAPI) (*Reader, error) {
	if client == nil {
		return nil, fmt.Errorf("%w: nil OSS client", fgo.ErrInvalidConfig)
	}
	return &Reader{client: client}, nil
}

// NewFromConfig creates a reader with the official OSS client and optional
// client configuration functions.
func NewFromConfig(config *aliyunoss.Config, options ...func(*aliyunoss.Options)) *Reader {
	return &Reader{client: aliyunoss.NewClient(config, options...)}
}

// ReadRemoteFile reads one bounded complete object for compatibility with
// fgo.RemoteFileReader. Prefetch uses OpenRemoteFile directly.
func (r *Reader) ReadRemoteFile(
	ctx context.Context,
	request fgo.RemoteFileRequest,
) ([]byte, error) {
	if request.ExpectedSize <= 0 || request.ExpectedSize > int64(^uint(0)>>1) {
		return nil, fmt.Errorf("%w: OSS complete reads require expected size", fgo.ErrInvalidConfig)
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
			"%w: OSS object size mismatch: %w",
			fgo.ErrValidation, io.ErrUnexpectedEOF,
		)
	}
	return data, nil
}

// OpenRemoteFile opens one exact OSS object range. The caller owns and must
// close the returned SDK response body.
func (r *Reader) OpenRemoteFile(
	ctx context.Context,
	request fgo.RemoteFileRequest,
) (io.ReadCloser, error) {
	if r == nil || r.client == nil {
		return nil, fmt.Errorf("%w: nil OSS reader", fgo.ErrInvalidConfig)
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
	output, err := r.client.GetObject(ctx, &aliyunoss.GetObjectRequest{
		Bucket:        aliyunoss.Ptr(bucket),
		Key:           aliyunoss.Ptr(key),
		Range:         aliyunoss.Ptr(fmt.Sprintf("bytes=%d-%d", offset, end)),
		RangeBehavior: aliyunoss.Ptr("standard"),
	})
	if err != nil {
		return nil, err
	}
	if output == nil || output.Body == nil {
		return nil, fmt.Errorf("%w: OSS response omitted body", fgo.ErrValidation)
	}
	if err := validateResponse(output, offset, length, request.ExpectedSize); err != nil {
		return nil, errors.Join(err, output.Body.Close())
	}
	return output.Body, nil
}

func parseURI(raw string) (string, string, error) {
	bucket, key, err := storageadapter.ParseObjectURI(raw, "oss")
	if err != nil {
		return "", "", fmt.Errorf("%w: %v", fgo.ErrInvalidConfig, err)
	}
	return bucket, key, nil
}

func validateRange(request fgo.RemoteFileRequest) (int64, int64, error) {
	offset, length, err := storageadapter.ValidateRange(
		request.Offset, request.Length, request.ExpectedSize, request.MaxBytes,
	)
	if err != nil {
		return 0, 0, fmt.Errorf("%w: invalid OSS object range: %v", fgo.ErrInvalidConfig, err)
	}
	return offset, length, nil
}

func validateResponse(
	output *aliyunoss.GetObjectResult,
	offset int64,
	length int64,
	expectedSize int64,
) error {
	if output.ContentLength != length {
		return fmt.Errorf("%w: OSS content length mismatch", fgo.ErrValidation)
	}
	if output.ContentRange == nil {
		return fmt.Errorf("%w: OSS response omitted content range", fgo.ErrValidation)
	}
	start, end, total, err := storageadapter.ParseContentRange(*output.ContentRange)
	if err != nil || start != offset || end != offset+length-1 || total != expectedSize {
		return fmt.Errorf("%w: OSS content range mismatch", fgo.ErrValidation)
	}
	return nil
}

var (
	_ fgo.RemoteFileReader       = (*Reader)(nil)
	_ fgo.RemoteFileStreamReader = (*Reader)(nil)
)
