package fgo_test

import (
	"context"
	"log"
	"time"

	"github.com/pletorco/fluss-go/pkg/fgo"
)

func ExampleOpen() {
	ctx := context.Background()
	client, err := fgo.Open(
		ctx,
		fgo.WithSeedBrokers("coordinator.example:9123"),
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

	table, err := client.OpenTable(ctx, fgo.TablePath{
		Database: "production",
		Table:    "orders",
	})
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("opened %s with %d buckets", table.Path, table.BucketCount)
}

func ExampleClient_NewLogWriter() {
	ctx := context.Background()
	client, err := fgo.Open(ctx, fgo.WithSeedBrokers("coordinator.example:9123"))
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	table, err := client.OpenTable(ctx, fgo.TablePath{
		Database: "production",
		Table:    "events",
	})
	if err != nil {
		log.Fatal(err)
	}
	writer, err := client.NewLogWriter(
		ctx,
		table,
		fgo.WithLogBatchLimits(500, 1<<20),
		fgo.WithLogLinger(5*time.Millisecond),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer writer.Close(ctx)

	result := writer.Append(ctx, fgo.Row{int64(42), "created"}).Await(ctx)
	if result.Err != nil {
		log.Fatal(result.Err)
	}
	log.Printf("appended at offset %d", result.BaseOffset)
}

func ExampleClient_NewLogScanner() {
	ctx := context.Background()
	client, err := fgo.Open(ctx, fgo.WithSeedBrokers("coordinator.example:9123"))
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	table, err := client.OpenTable(ctx, fgo.TablePath{
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

func ExampleNewTypedLogWriter() {
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
	client, err := fgo.Open(ctx, fgo.WithSeedBrokers("coordinator.example:9123"))
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	table, err := client.OpenTable(ctx, fgo.TablePath{
		Database: "production",
		Table:    "events",
	})
	if err != nil {
		log.Fatal(err)
	}
	writer, err := fgo.NewTypedLogWriter(ctx, client, table, codec)
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
		fgo.WithSeedBrokers("coordinator.example:9123"),
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
