package fgo

import (
	"context"
	"errors"
	"io"
	"testing"
)

type emptySnapshotReader struct{}

func (emptySnapshotReader) ReadBatch(context.Context, int) ([]Row, error) { return nil, io.EOF }
func (emptySnapshotReader) Close() error                                  { return nil }

type snapshotReaderFailure struct {
	name   string
	reader SnapshotBatchReader
	err    error
	want   error
}

func snapshotReaderFailures() []snapshotReaderFailure {
	callbackErr := errors.New("test snapshot open failure")
	return []snapshotReaderFailure{
		{name: "nil", want: ErrValidation},
		{name: "typed nil", reader: (*fakeSnapshotBatchReader)(nil), want: ErrValidation},
		{name: "open error", err: callbackErr, want: callbackErr},
	}
}

func TestSnapshotScannerRejectsInvalidProviderResultAtOpen(t *testing.T) {
	table := upsertWriterTable()
	for _, tc := range snapshotReaderFailures() {
		t.Run(tc.name, func(t *testing.T) {
			client := &Client{snapshotProvider: SnapshotBatchProviderFunc(func(context.Context, SnapshotBatchRequest) (SnapshotBatchReader, error) {
				return tc.reader, tc.err
			})}
			scanner, err := client.NewSnapshotBatchScanner(t.Context(), table, testTableBucket(table), 1)
			if scanner != nil || !errors.Is(err, tc.want) || tc.err != nil && err != tc.err {
				t.Fatalf("NewSnapshotBatchScanner() = (scanner present: %t, %v), want (nil, %v)", scanner != nil, err, tc.want)
			}
		})
	}
}

func TestSnapshotScannerAcceptsAnEmptyReader(t *testing.T) {
	table := upsertWriterTable()
	client := &Client{snapshotProvider: SnapshotBatchProviderFunc(func(context.Context, SnapshotBatchRequest) (SnapshotBatchReader, error) {
		return emptySnapshotReader{}, nil
	})}
	scanner, err := client.NewSnapshotBatchScanner(t.Context(), table, testTableBucket(table), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer scanner.Close()
	result, err := scanner.Poll(t.Context())
	if err != nil || !result.Done || !scanner.Done() || len(result.Rows) != 0 {
		t.Fatalf("empty snapshot Poll() = (%+v, %v); want no rows and Done", result, err)
	}
}

func TestRemoteSnapshotProviderRejectsInvalidDecoderResultAtOpen(t *testing.T) {
	for _, tc := range snapshotReaderFailures() {
		t.Run(tc.name, func(t *testing.T) {
			reader := RemoteFileReaderFunc(func(context.Context, RemoteFileRequest) ([]byte, error) {
				return []byte("snapshot"), nil
			})
			resolver := RemoteSnapshotResolverFunc(func(context.Context, SnapshotBatchRequest) ([]RemoteSnapshotFile, error) {
				return []RemoteSnapshotFile{{Path: "object", Size: 8}}, nil
			})
			decoder := RemoteSnapshotDecoderFunc(func(context.Context, SnapshotBatchRequest, []RemoteSnapshotFile) (SnapshotBatchReader, error) {
				return tc.reader, tc.err
			})
			provider, err := NewRemoteSnapshotBatchProvider(reader, RemoteFileReadConfig{}, resolver, decoder, nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			result, err := provider.OpenSnapshot(t.Context(), SnapshotBatchRequest{})
			if result != nil || !errors.Is(err, tc.want) || tc.err != nil && err != tc.err {
				t.Fatalf("OpenSnapshot() = (%T, %v), want (nil, %v)", result, err, tc.want)
			}
		})
	}
}
