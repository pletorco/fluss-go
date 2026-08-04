// Package docexamples_test contains compile-only sources for Markdown snippets.
package docexamples_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"log"
	"os"
	"time"

	"github.com/pletorco/fluss-go/pkg/fadm"
	"github.com/pletorco/fluss-go/pkg/fgo"
)

// doc:snippet typedCodec
type Codec[T any] interface {
	Encode(T) (fgo.Row, error)
	Decode(fgo.Row) (T, error)
}

// doc:snippet-end typedCodec

func quickStart() {
	// doc:snippet quickStart
	ctx := context.Background()
	client, err := fgo.Open(
		ctx,
		fgo.WithBootstrapServers("localhost:9123"),
		fgo.WithClientSoftware("example", "1.0.0"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := client.Close(); err != nil {
			log.Printf("close Fluss client: %v", err)
		}
	}()

	table, err := client.GetTable(ctx, fgo.TablePath{
		Database: "fluss",
		Table:    "events",
	})
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("opened table %s with %d buckets", table.Path, table.BucketCount)
	// doc:snippet-end quickStart
}

func tlsAndSASL(ctx context.Context) error {
	// doc:snippet tlsAndSASL
	caPEM, err := os.ReadFile(os.Getenv("FLUSS_CA_FILE"))
	if err != nil {
		return err
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		return errors.New("Fluss CA file contains no certificates")
	}

	tlsConfig := &tls.Config{
		MinVersion: tls.VersionTLS12,
		RootCAs:    roots,
		ServerName: os.Getenv("FLUSS_TLS_SERVER_NAME"),
	}
	client, err := fgo.Open(
		ctx,
		fgo.WithBootstrapServers("coordinator.example:9123"),
		fgo.WithTLSConfig(tlsConfig),
		fgo.WithAuthenticator(fgo.SASLPlainAuthenticator(
			os.Getenv("FLUSS_SASL_USERNAME"),
			os.Getenv("FLUSS_SASL_PASSWORD"),
		)),
	)
	// doc:snippet-end tlsAndSASL
	if err == nil {
		err = client.Close()
	}
	return err
}

func classifyAuthentication(err error) {
	// doc:snippet classifyAuthentication
	if err != nil {
		var authentication *fgo.AuthenticationError
		switch {
		case errors.As(err, &authentication) && authentication.Retriable:
			log.Printf("retry authentication through the bounded connection policy: %v", authentication)
		case errors.Is(err, fgo.ErrAuthentication):
			log.Printf("reject authentication configuration: %v", err)
		default:
			log.Printf("non-authentication connection failure: %v", err)
		}
	}
	// doc:snippet-end classifyAuthentication
}

func classifyServerError(err error) {
	// doc:snippet classifyServerError
	var server *fgo.ServerError
	if errors.As(err, &server) {
		log.Printf(
			"api=%d code=%d category-timeout=%t retriable=%t",
			server.API,
			server.Code,
			errors.Is(err, fgo.ErrTimeout),
			server.Retriable,
		)
	}
	// doc:snippet-end classifyServerError
}

func tokenRefresh() {
	// doc:snippet tokenRefresh
	receiver := fgo.FileSystemSecurityTokenReceiverFunc(
		func(token fgo.FileSystemSecurityToken) error {
			// Update an external filesystem client with the cloned token.
			return nil
		},
	)

	client, err := fgo.Open(
		context.Background(),
		fgo.WithBootstrapServers("coordinator:9123"),
		fgo.WithFileSystemSecurityTokenRefresh(
			fgo.FileSystemSecurityTokenRefreshConfig{},
			receiver,
		),
	)
	// doc:snippet-end tokenRefresh
	if err == nil {
		_ = client.Close()
	}
}

func snapshotComposition(
	ctx context.Context,
	reader fgo.RemoteFileReader,
	resolver fgo.RemoteSnapshotResolver,
	decoder fgo.RemoteSnapshotDecoder,
	currentToken fgo.FileSystemSecurityTokenSource,
	metrics fgo.MetricsObserver,
) error {
	// doc:snippet snapshotComposition
	provider, err := fgo.NewRemoteSnapshotBatchProvider(
		reader,
		fgo.RemoteFileReadConfig{},
		resolver,
		decoder,
		currentToken,
		metrics,
	)
	if err != nil {
		return err
	}

	client, err := fgo.Open(
		ctx,
		fgo.WithBootstrapServers("coordinator:9123"),
		fgo.WithRemoteFileReader(reader, fgo.RemoteFileReadConfig{}),
		fgo.WithSnapshotBatchProvider(provider),
	)
	// doc:snippet-end snapshotComposition
	if err == nil {
		err = client.Close()
	}
	return err
}

func dynamicPartitions(ctx context.Context, table fgo.Table) error {
	// doc:snippet dynamicPartitions
	client, err := fgo.Open(
		ctx,
		fgo.WithBootstrapServers("coordinator:9123"),
		fgo.WithDynamicPartitionCreation(fgo.DynamicPartitionCreationConfig{
			MetadataAttempts: 3,
			RetryBackoff:     25 * time.Millisecond,
		}),
	)
	if err != nil {
		return err
	}

	partition := fgo.PartitionSpec{"day": "2026-07-30", "region": "kr"}
	writer, err := client.NewUpsertWriter(
		ctx,
		table,
		fgo.WithUpsertPartitionSpec(table.Schema, partition),
	)
	// doc:snippet-end dynamicPartitions
	if err == nil {
		err = writer.Close(ctx)
	}
	return errors.Join(err, client.Close())
}

func logFormat(ctx context.Context, client *fgo.Client, table fgo.Table) error {
	// doc:snippet logFormat
	writer, err := client.NewAppendWriter(
		ctx,
		table,
		fgo.WithAppendLogFormat(fgo.LogFormatCompacted),
	)
	// doc:snippet-end logFormat
	if err == nil {
		err = writer.Close(ctx)
	}
	return err
}

func boundedScan(ctx context.Context, client *fgo.Client, table fgo.Table) error {
	// doc:snippet boundedScan
	scanner, err := client.NewLogScanner(
		ctx,
		table,
		fgo.Earliest(),
		fgo.WithScanRowLimit(1_000),
		fgo.WithScanStoppingOffsets(map[int32]int64{
			0: 10_000,
			1: 12_000,
		}),
	)
	if err != nil {
		return err
	}
	defer scanner.Close()
	// doc:snippet-end boundedScan
	return nil
}

func kvMerge(ctx context.Context, client *fgo.Client, table fgo.Table) error {
	// doc:snippet kvMerge
	writer, err := client.NewUpsertWriter(
		ctx,
		table,
		fgo.WithUpsertMergeMode(fgo.MergeModeOverwrite),
	)
	// doc:snippet-end kvMerge
	if err == nil {
		err = writer.Close(ctx)
	}
	return err
}

func lookupInsert(ctx context.Context, client *fgo.Client, table fgo.Table) error {
	// doc:snippet lookupInsert
	lookup, err := client.NewLookuper(
		ctx,
		table,
		fgo.WithLookupInsertIfNotExists(5*time.Second, -1),
	)
	// doc:snippet-end lookupInsert
	if err == nil {
		err = lookup.Close()
	}
	return err
}

func lookupScheduling(ctx context.Context, client *fgo.Client, table fgo.Table) error {
	// doc:snippet lookupScheduling
	lookup, err := client.NewLookuper(
		ctx,
		table,
		fgo.WithLookupBatchLimits(256, 8),
		fgo.WithLookupQueue(4_096, time.Millisecond),
		fgo.WithLookupRequestTimeout(5*time.Second),
		fgo.WithLookupRetryPolicy(fgo.RetryPolicy{MaxAttempts: 3}),
	)
	// doc:snippet-end lookupScheduling
	if err == nil {
		err = lookup.Close()
	}
	return err
}

func batchScan(ctx context.Context, client *fgo.Client, table fgo.Table) error {
	// doc:snippet batchScan
	buckets, err := client.ResolveTableBuckets(ctx, fgo.PhysicalTablePath{
		TablePath: table.Path,
	})
	if err != nil {
		return err
	}

	scanner, err := client.NewBatchScanner(
		ctx,
		table,
		buckets[0],
		fgo.WithBatchLimit(1_000),
		fgo.WithBatchProjection("id", "name"),
	)
	if err != nil {
		return err
	}
	defer scanner.Close()

	result, err := scanner.Poll(ctx)
	if err != nil {
		return err
	}
	defer result.Release()
	// doc:snippet-end batchScan
	return nil
}

func metricsObserver(ctx context.Context) {
	// doc:snippet metricsObserver
	observer := fgo.MetricsObserverFunc(func(event fgo.MetricEvent) {
		log.Printf("kind=%d operation=%d duration=%s", event.Kind, event.Operation, event.Duration)
	})

	client, err := fgo.Open(
		ctx,
		fgo.WithBootstrapServers("coordinator:9123"),
		fgo.WithMetricsObserver(observer),
	)
	// doc:snippet-end metricsObserver
	if err == nil {
		_ = client.Close()
	}
}

func serverDiscovery(ctx context.Context, client *fgo.Client) error {
	// doc:snippet serverDiscovery
	admin, err := fadm.New(client)
	if err != nil {
		return err
	}
	nodes, err := admin.GetServerNodes(ctx)
	// doc:snippet-end serverDiscovery
	_ = nodes
	return err
}
