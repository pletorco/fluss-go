package fgo_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/pletorco/fluss-go/pkg/fgo"
	"github.com/pletorco/fluss-go/pkg/fmsg"
)

func ExampleOpen_tlsAndSASL() {
	caPEM, err := os.ReadFile(os.Getenv("FLUSS_CA_FILE"))
	if err != nil {
		log.Fatal(err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		log.Fatal("Fluss CA file contains no certificates")
	}
	serverName := os.Getenv("FLUSS_TLS_SERVER_NAME")
	username := os.Getenv("FLUSS_SASL_USERNAME")
	password := os.Getenv("FLUSS_SASL_PASSWORD")
	if serverName == "" || username == "" || password == "" {
		log.Fatal("Fluss TLS and SASL configuration is incomplete")
	}

	client, err := fgo.Open(
		context.Background(),
		fgo.WithBootstrapServers("coordinator.example:9123"),
		fgo.WithTLSConfig(&tls.Config{
			MinVersion: tls.VersionTLS12,
			RootCAs:    roots,
			ServerName: serverName,
		}),
		fgo.WithAuthenticator(fgo.SASLPlainAuthenticator(username, password)),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := client.Close(); err != nil {
			log.Printf("close Fluss client: %v", err)
		}
	}()
}

func ExampleAuthenticationError() {
	err := &fgo.AuthenticationError{
		Err:       errors.New("server rejected authentication"),
		Retriable: true,
	}
	var authentication *fgo.AuthenticationError
	fmt.Println(errors.Is(err, fgo.ErrAuthentication))
	fmt.Println(errors.As(err, &authentication), authentication.Retriable)
	fmt.Println(err)
	// Output:
	// true
	// true true
	// fgo: authentication failed (retriable)
}

func ExampleServerError() {
	err := fgo.ResponseError(
		int32(fmsg.ErrorCodeRequestTimeOut),
		"tablet request timed out",
		fmsg.APIKeyListOffsets,
	)
	var server *fgo.ServerError
	fmt.Println(errors.Is(err, fgo.ErrServerFailure))
	fmt.Println(errors.Is(err, fgo.ErrTimeout))
	fmt.Println(errors.As(err, &server), server.Retriable)
	// Output:
	// true
	// true
	// true true
}

func ExampleWriteResult() {
	result := fgo.WriteResult{
		Bucket: 2,
		Err:    fmt.Errorf("%w: reconcile the previous batch", fgo.ErrWriterState),
	}
	if errors.Is(result.Err, fgo.ErrWriterState) {
		fmt.Printf("bucket %d requires reconciliation\n", result.Bucket)
	}
	// Output:
	// bucket 2 requires reconciliation
}

func ExampleLookupResult() {
	results := []fgo.LookupResult{
		{Key: fgo.PrimaryKey{int64(41)}, Row: fgo.Row{int64(41), "Ada"}, Found: true},
		{Key: fgo.PrimaryKey{int64(42)}, Err: fgo.ErrNotFound},
		{Key: fgo.PrimaryKey{int64(43)}, Err: fgo.ErrTimeout},
	}
	for _, result := range results {
		switch {
		case errors.Is(result.Err, fgo.ErrNotFound):
			fmt.Printf("%d missing\n", result.Key[0])
		case result.Err != nil:
			fmt.Printf("%d failed\n", result.Key[0])
		default:
			fmt.Printf("%d found\n", result.Key[0])
		}
	}
	// Output:
	// 41 found
	// 42 missing
	// 43 failed
}

func ExampleOpen() {
	ctx := context.Background()
	client, err := fgo.Open(
		ctx,
		fgo.WithBootstrapServers("coordinator.example:9123"),
		fgo.WithClientIdentity("orders-service", "1.0.0"),
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
		Database: "production",
		Table:    "orders",
	})
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("opened %s with %d buckets", table.Path, table.BucketCount)
}

func ExampleClient_NewAppendWriter() {
	ctx := context.Background()
	client, err := fgo.Open(ctx, fgo.WithBootstrapServers("coordinator.example:9123"))
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := client.Close(); err != nil {
			log.Printf("close Fluss client: %v", err)
		}
	}()

	table, err := client.GetTable(ctx, fgo.TablePath{
		Database: "production",
		Table:    "events",
	})
	if err != nil {
		log.Fatal(err)
	}
	writer, err := client.NewAppendWriter(
		ctx,
		table,
		fgo.WithAppendBatchLimits(1<<20, 500),
		fgo.WithAppendLinger(5*time.Millisecond),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := writer.Close(ctx); err != nil {
			log.Printf("close upsert writer: %v", err)
		}
	}()

	result := writer.Append(ctx, fgo.Row{int64(42), "created"}).Await(ctx)
	if result.Err != nil {
		log.Fatal(result.Err)
	}
	log.Printf("appended at offset %d", result.BaseOffset)
}

func ExampleClient_NewLogScanner() {
	ctx := context.Background()
	client, err := fgo.Open(ctx, fgo.WithBootstrapServers("coordinator.example:9123"))
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := client.Close(); err != nil {
			log.Printf("close Fluss client: %v", err)
		}
	}()

	table, err := client.GetTable(ctx, fgo.TablePath{
		Database: "production",
		Table:    "events",
	})
	if err != nil {
		log.Fatal(err)
	}
	scanner, err := client.NewLogScanner(
		ctx,
		table,
		fgo.Earliest(),
		fgo.WithScanProjection("event_id", "event_type"),
		fgo.WithScanRowLimit(100),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer scanner.Close()

	result, err := scanner.Poll(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer result.Release()
	for _, record := range result.Records {
		log.Printf(
			"bucket=%d offset=%d row=%v",
			record.Bucket,
			record.Record.Offset,
			record.Record.Value,
		)
	}
}

func ExampleNewTypedAppendWriter() {
	type event struct {
		ID   int64
		Kind string
	}

	codec := fgo.CodecFuncs[event]{
		EncodeFunc: func(value event) (fgo.Row, error) {
			return fgo.Row{value.ID, value.Kind}, nil
		},
		DecodeFunc: func(row fgo.Row) (event, error) {
			return event{ID: row[0].(int64), Kind: row[1].(string)}, nil
		},
	}

	ctx := context.Background()
	client, err := fgo.Open(ctx, fgo.WithBootstrapServers("coordinator.example:9123"))
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	table, err := client.GetTable(ctx, fgo.TablePath{
		Database: "production",
		Table:    "events",
	})
	if err != nil {
		log.Fatal(err)
	}
	writer, err := fgo.NewTypedAppendWriter(ctx, client, table, codec)
	if err != nil {
		log.Fatal(err)
	}
	defer writer.Close(ctx)

	result := writer.Append(ctx, event{ID: 42, Kind: "created"}).Await(ctx)
	if result.Err != nil {
		log.Fatal(result.Err)
	}
}

func ExampleWithFileSystemSecurityTokenRefresh() {
	receiver := fgo.FileSystemSecurityTokenReceiverFunc(
		func(token fgo.FileSystemSecurityToken) error {
			// Pass the cloned token to an external filesystem client.
			return nil
		},
	)
	reader := fgo.RemoteFileReaderFunc(
		func(ctx context.Context, request fgo.RemoteFileRequest) ([]byte, error) {
			// Read request.Path with request.Token using an application SDK.
			return nil, nil
		},
	)

	client, err := fgo.Open(
		context.Background(),
		fgo.WithBootstrapServers("coordinator.example:9123"),
		fgo.WithRemoteFileReader(reader, fgo.RemoteFileReadConfig{}),
		fgo.WithFileSystemSecurityTokenRefresh(
			fgo.FileSystemSecurityTokenRefreshConfig{},
			receiver,
		),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()
}

func ExampleClient_NewUpsertWriter() {
	ctx := context.Background()
	client, err := fgo.Open(ctx, fgo.WithBootstrapServers("coordinator.example:9123"))
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := client.Close(); err != nil {
			log.Printf("close Fluss client: %v", err)
		}
	}()

	table, err := client.GetTable(ctx, fgo.TablePath{
		Database: "production",
		Table:    "customers",
	})
	if err != nil {
		log.Fatal(err)
	}
	writer, err := client.NewUpsertWriter(
		ctx,
		table,
		fgo.WithUpsertBatchLimits(1<<20, 500),
		fgo.WithUpsertMergeMode(fgo.MergeModeOverwrite),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := writer.Close(ctx); err != nil {
			log.Printf("close upsert writer: %v", err)
		}
	}()

	upsert := writer.Upsert(ctx, fgo.Row{int64(42), "Ada"}).Await(ctx)
	if upsert.Err != nil {
		log.Fatal(upsert.Err)
	}
	deleted := writer.Delete(ctx, fgo.PrimaryKey{int64(42)}).Await(ctx)
	if deleted.Err != nil {
		log.Fatal(deleted.Err)
	}
	if err := writer.Flush(ctx); err != nil {
		log.Fatal(err)
	}
}

func ExampleClient_NewLookuper() {
	ctx := context.Background()
	client, err := fgo.Open(ctx, fgo.WithBootstrapServers("coordinator.example:9123"))
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := client.Close(); err != nil {
			log.Printf("close Fluss client: %v", err)
		}
	}()

	table, err := client.GetTable(ctx, fgo.TablePath{
		Database: "production",
		Table:    "customers",
	})
	if err != nil {
		log.Fatal(err)
	}
	lookup, err := client.NewLookuper(ctx, table, fgo.WithLookupBatch(100, 4))
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := lookup.Close(); err != nil {
			log.Printf("close lookuper: %v", err)
		}
	}()

	results := lookup.Lookup(
		ctx,
		fgo.PrimaryKey{int64(42)},
		fgo.PrimaryKey{int64(43)},
	)
	for _, result := range results {
		switch {
		case errors.Is(result.Err, fgo.ErrNotFound):
			log.Printf("lookup %v: not found", result.Key)
		case result.Err != nil:
			log.Printf("lookup %v: %v", result.Key, result.Err)
		default:
			log.Printf("lookup %v: %v", result.Key, result.Row)
		}
	}
}

func ExampleClient_NewBatchScanner() {
	ctx := context.Background()
	client, err := fgo.Open(ctx, fgo.WithBootstrapServers("coordinator.example:9123"))
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := client.Close(); err != nil {
			log.Printf("close Fluss client: %v", err)
		}
	}()

	table, err := client.GetTable(ctx, fgo.TablePath{
		Database: "production",
		Table:    "customers",
	})
	if err != nil {
		log.Fatal(err)
	}
	buckets, err := client.ResolveTableBuckets(ctx, fgo.PhysicalTablePath{
		TablePath: table.Path,
	})
	if err != nil {
		log.Fatal(err)
	}
	if len(buckets) == 0 {
		log.Fatal("table has no buckets")
	}
	scanner, err := client.NewBatchScanner(
		ctx,
		table,
		buckets[0],
		fgo.WithBatchLimit(1_000),
		fgo.WithBatchProjection("customer_id", "name"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := scanner.Close(); err != nil {
			log.Printf("close batch scanner: %v", err)
		}
	}()

	for !scanner.Done() {
		result, err := scanner.Poll(ctx)
		if err != nil {
			log.Fatal(err)
		}
		for _, row := range result.Rows {
			log.Printf("row=%v", row)
		}
		result.Release()
	}
}

type exampleSnapshotReader struct{}

func (exampleSnapshotReader) ReadBatch(context.Context, int) ([]fgo.Row, error) {
	return nil, io.EOF
}

func (exampleSnapshotReader) Close() error { return nil }

func ExampleClient_NewSnapshotBatchScanner() {
	ctx := context.Background()
	provider := fgo.SnapshotBatchProviderFunc(
		func(context.Context, fgo.SnapshotBatchRequest) (fgo.SnapshotBatchReader, error) {
			return exampleSnapshotReader{}, nil
		},
	)
	client, err := fgo.Open(
		ctx,
		fgo.WithBootstrapServers("coordinator.example:9123"),
		fgo.WithSnapshotBatchProvider(provider),
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
		Database: "production",
		Table:    "customers",
	})
	if err != nil {
		log.Fatal(err)
	}
	buckets, err := client.ResolveTableBuckets(ctx, fgo.PhysicalTablePath{
		TablePath: table.Path,
	})
	if err != nil {
		log.Fatal(err)
	}
	if len(buckets) == 0 {
		log.Fatal("table has no buckets")
	}
	scanner, err := client.NewSnapshotBatchScanner(
		ctx,
		table,
		buckets[0],
		101,
		fgo.WithBatchLimit(1_000),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := scanner.Close(); err != nil {
			log.Printf("close snapshot scanner: %v", err)
		}
	}()

	result, err := scanner.Poll(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer result.Release()
}

func ExampleCodecFuncs() {
	type customer struct {
		ID   int64
		Name string
	}
	codec := fgo.CodecFuncs[customer]{
		EncodeFunc: func(value customer) (fgo.Row, error) {
			return fgo.Row{value.ID, value.Name}, nil
		},
		DecodeFunc: func(row fgo.Row) (customer, error) {
			return customer{ID: row[0].(int64), Name: row[1].(string)}, nil
		},
	}

	row, err := codec.Encode(customer{ID: 42, Name: "Ada"})
	if err != nil {
		log.Fatal(err)
	}
	decoded, err := codec.Decode(row)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%d %s\n", decoded.ID, decoded.Name)
	// Output:
	// 42 Ada
}
