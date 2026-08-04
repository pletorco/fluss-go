package fgo

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pletorco/fluss-go/pkg/fmsg"
)

func TestPartitionSpecNamesAreSchemaOrderedAndDeterministic(t *testing.T) {
	schema := Schema{PartitionKey: []string{"region", "day"}}
	spec := PartitionSpec{"day": "30", "region": "kr"}
	name, err := schema.PartitionName(spec)
	if err != nil || name != "kr$30" {
		t.Fatalf("PartitionName() = %q, %v", name, err)
	}
	decoded, err := PartitionSpecFromName(schema.PartitionKey, name)
	if err != nil || decoded["region"] != "kr" || decoded["day"] != "30" {
		t.Fatalf("PartitionSpecFromName() = %#v, %v", decoded, err)
	}
	names, err := schema.PartitionNames(
		PartitionSpec{"region": "us", "day": "31"},
		spec,
		PartitionSpec{"day": "30", "region": "kr"},
	)
	if err != nil || len(names) != 2 || names[0] != "kr$30" || names[1] != "us$31" {
		t.Fatalf("PartitionNames() = %#v, %v", names, err)
	}
}

func TestPartitionSpecValidation(t *testing.T) {
	tests := []struct {
		name string
		run  func() error
	}{
		{"unpartitioned", func() error {
			_, err := (Schema{}).PartitionName(PartitionSpec{"a": "1"})
			return err
		}},
		{"missing", func() error {
			_, err := (Schema{PartitionKey: []string{"a", "b"}}).PartitionName(PartitionSpec{"a": "1"})
			return err
		}},
		{"separator", func() error {
			_, err := (Schema{PartitionKey: []string{"a"}}).PartitionName(PartitionSpec{"a": "1$2"})
			return err
		}},
		{"name count", func() error {
			_, err := PartitionSpecFromName([]string{"a", "b"}, "1")
			return err
		}},
		{"duplicate key", func() error {
			_, err := PartitionSpecFromName([]string{"a", "a"}, "1$2")
			return err
		}},
		{"empty list", func() error {
			_, err := (Schema{PartitionKey: []string{"a"}}).PartitionNames()
			return err
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := test.run(); !errors.Is(err, ErrInvalidConfig) && !errors.Is(err, ErrInvalidSchema) {
				t.Fatalf("error = %v", err)
			}
		})
	}
}

func TestPartitionSpecWriterAndScannerOptions(t *testing.T) {
	schema := Schema{PartitionKey: []string{"region", "day"}}
	spec := PartitionSpec{"day": "30", "region": "kr"}
	var logConfig AppendWriterConfig
	if err := WithAppendPartitionSpec(schema, spec)(&logConfig); err != nil || logConfig.Partition != "kr$30" {
		t.Fatalf("WithAppendPartitionSpec() = %#v, %v", logConfig, err)
	}
	var kvConfig UpsertWriterConfig
	if err := WithUpsertPartitionSpec(schema, spec)(&kvConfig); err != nil || kvConfig.Partition != "kr$30" {
		t.Fatalf("WithUpsertPartitionSpec() = %#v, %v", kvConfig, err)
	}
	var scanConfig LogScannerConfig
	if err := WithScanPartitionSpec(schema, spec)(&scanConfig); err != nil || scanConfig.Partition != "kr$30" {
		t.Fatalf("WithScanPartitionSpec() = %#v, %v", scanConfig, err)
	}
}

type partitionCreationBackendFunc struct {
	check  func(context.Context, PhysicalTablePath) error
	create func(context.Context, TablePath, PartitionSpec) error
}

func (b partitionCreationBackendFunc) checkPartition(
	ctx context.Context,
	path PhysicalTablePath,
) error {
	return b.check(ctx, path)
}

func (b partitionCreationBackendFunc) createPartition(
	ctx context.Context,
	path TablePath,
	spec PartitionSpec,
) error {
	return b.create(ctx, path, spec)
}

func TestDynamicPartitionCreatorCreatesAndRefreshes(t *testing.T) {
	var checks, creates atomic.Int32
	backend := partitionCreationBackendFunc{
		check: func(context.Context, PhysicalTablePath) error {
			if checks.Add(1) < 3 {
				return ErrUnknownPartition
			}
			return nil
		},
		create: func(_ context.Context, path TablePath, spec PartitionSpec) error {
			creates.Add(1)
			if path.String() != "db.events" || spec["region"] != "kr" || spec["day"] != "30" {
				t.Fatalf("create = %s, %#v", path, spec)
			}
			return nil
		},
	}
	creator := newDynamicPartitionCreator(backend, DynamicPartitionCreationConfig{
		MetadataAttempts: 3,
	})
	path := PhysicalTablePath{TablePath: TablePath{Database: "db", Table: "events"}, Partition: "kr$30"}
	if err := creator.Ensure(context.Background(), path, []string{"region", "day"}); err != nil {
		t.Fatalf("Ensure() error = %v", err)
	}
	if checks.Load() != 3 || creates.Load() != 1 {
		t.Fatalf("checks=%d creates=%d", checks.Load(), creates.Load())
	}
}

func TestDynamicPartitionCreatorDeduplicatesConcurrentCreation(t *testing.T) {
	var checks, creates atomic.Int32
	created := make(chan struct{})
	started := make(chan struct{})
	var startOnce sync.Once
	backend := partitionCreationBackendFunc{
		check: func(context.Context, PhysicalTablePath) error {
			if checks.Add(1) == 1 {
				return ErrUnknownPartition
			}
			return nil
		},
		create: func(context.Context, TablePath, PartitionSpec) error {
			creates.Add(1)
			startOnce.Do(func() { close(started) })
			<-created
			return nil
		},
	}
	creator := newDynamicPartitionCreator(backend, DynamicPartitionCreationConfig{MetadataAttempts: 1})
	path := PhysicalTablePath{TablePath: TablePath{Database: "db", Table: "events"}, Partition: "kr"}
	var group sync.WaitGroup
	errs := make(chan error, 8)
	for range 8 {
		group.Add(1)
		go func() {
			defer group.Done()
			errs <- creator.Ensure(context.Background(), path, []string{"region"})
		}()
	}
	<-started
	close(created)
	group.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("Ensure() error = %v", err)
		}
	}
	if creates.Load() != 1 || checks.Load() < 2 {
		t.Fatalf("creates=%d checks=%d", creates.Load(), checks.Load())
	}
}

func TestDynamicPartitionCreatorFailureAndCancellation(t *testing.T) {
	authorization := partitionCreationBackendFunc{
		check:  func(context.Context, PhysicalTablePath) error { return ErrUnknownPartition },
		create: func(context.Context, TablePath, PartitionSpec) error { return ErrAuthorization },
	}
	creator := newDynamicPartitionCreator(authorization, DynamicPartitionCreationConfig{MetadataAttempts: 1})
	path := PhysicalTablePath{TablePath: TablePath{Database: "db", Table: "events"}, Partition: "kr"}
	if err := creator.Ensure(context.Background(), path, []string{"region"}); !errors.Is(err, ErrAuthorization) {
		t.Fatalf("authorization error = %v", err)
	}

	waiting := partitionCreationBackendFunc{
		check:  func(context.Context, PhysicalTablePath) error { return ErrUnknownPartition },
		create: func(context.Context, TablePath, PartitionSpec) error { return nil },
	}
	creator = newDynamicPartitionCreator(waiting, DynamicPartitionCreationConfig{
		MetadataAttempts: 3, RetryBackoff: time.Hour,
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := creator.Ensure(ctx, path, []string{"region"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation error = %v", err)
	}
	if err := (*dynamicPartitionCreator)(nil).Ensure(ctx, path, []string{"region"}); err != nil {
		t.Fatalf("disabled Ensure() error = %v", err)
	}
	if err := creator.Ensure(context.Background(), PhysicalTablePath{
		TablePath: path.TablePath,
	}, []string{"region"}); err != nil {
		t.Fatalf("unpartitioned Ensure() error = %v", err)
	}
}

func TestDynamicPartitionCreatorWaitsForLeaderWithoutCreating(t *testing.T) {
	var checks, creates atomic.Int32
	backend := partitionCreationBackendFunc{
		check: func(context.Context, PhysicalTablePath) error {
			if checks.Add(1) < 3 {
				return ErrNoBucketLeader
			}
			return nil
		},
		create: func(context.Context, TablePath, PartitionSpec) error {
			creates.Add(1)
			return nil
		},
	}
	creator := newDynamicPartitionCreator(backend, DynamicPartitionCreationConfig{MetadataAttempts: 2})
	path := PhysicalTablePath{TablePath: TablePath{Database: "db", Table: "events"}, Partition: "kr"}
	if err := creator.Ensure(context.Background(), path, []string{"region"}); err != nil {
		t.Fatal(err)
	}
	if checks.Load() != 3 || creates.Load() != 0 {
		t.Fatalf("checks=%d creates=%d", checks.Load(), creates.Load())
	}
}

func TestDynamicPartitionCreationOption(t *testing.T) {
	var config config
	if err := WithDynamicPartitionCreation(DynamicPartitionCreationConfig{})(&config); err != nil {
		t.Fatal(err)
	}
	if config.dynamicPartitions.MetadataAttempts != 3 ||
		config.dynamicPartitions.RetryBackoff != 25*time.Millisecond {
		t.Fatalf("defaults = %#v", config.dynamicPartitions)
	}
	for _, settings := range []DynamicPartitionCreationConfig{
		{MetadataAttempts: -1},
		{MetadataAttempts: 11},
		{MetadataAttempts: 1, RetryBackoff: -1},
		{MetadataAttempts: 1, RetryBackoff: time.Minute + 1},
	} {
		if err := WithDynamicPartitionCreation(settings)(&config); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("settings %#v error = %v", settings, err)
		}
	}
}

func TestDynamicPartitionClientBackend(t *testing.T) {
	var created PartitionSpec
	client := newClient(requesterFunc(func(_ context.Context, request fmsg.Request) (fmsg.Response, error) {
		switch request.APIKey() {
		case fmsg.APIKeyCreatePartition:
			message := request.(*fmsg.MessageRequest).Message().(*fmsg.CreatePartitionRequest)
			if !message.GetIgnoreIfNotExists() || message.GetTablePath().GetTableName() != "events" {
				t.Fatalf("create request = %#v", message)
			}
			created = make(PartitionSpec)
			for _, item := range message.GetPartitionSpec().GetPartitionKeyValues() {
				created[item.GetKey()] = item.GetValue()
			}
			return fmsg.NewResponse(fmsg.APIKeyCreatePartition, 0)
		case fmsg.APIKeyGetMetadata:
			return nil, ErrUnknownPartition
		default:
			t.Fatalf("unexpected API %d", request.APIKey())
			return nil, nil
		}
	}), nil)
	client.versions[fmsg.APIKeyCreatePartition] = 0
	client.versions[fmsg.APIKeyGetMetadata] = 0
	path := TablePath{Database: "db", Table: "events"}
	if err := client.createPartition(
		context.Background(), path, PartitionSpec{"region": "kr", "day": "30"},
	); err != nil {
		t.Fatal(err)
	}
	if created["region"] != "kr" || created["day"] != "30" {
		t.Fatalf("created spec = %#v", created)
	}
	physical := PhysicalTablePath{TablePath: path, Partition: "kr$30"}
	if err := client.checkPartition(context.Background(), physical); !errors.Is(err, ErrUnknownPartition) {
		t.Fatalf("checkPartition() error = %v", err)
	}
}

func TestDynamicPartitionClientRefreshesRouter(t *testing.T) {
	path := TablePath{Database: "db", Table: "events"}
	physical := PhysicalTablePath{TablePath: path, Partition: "kr"}
	client := &Client{}
	client.router = NewRouter(ServerNode{}, func(context.Context, TablePath) (TableMetadata, error) {
		return TableMetadata{Path: path, ID: 1, Partitions: make(map[string]PartitionMetadata)}, nil
	}).WithPhysicalMetadataFetcher(func(context.Context, PhysicalTablePath) (PartitionMetadata, error) {
		return PartitionMetadata{Path: physical, ID: 2, Buckets: map[int32]ServerNode{}}, nil
	})
	if err := client.checkPartition(context.Background(), physical); err != nil {
		t.Fatal(err)
	}
	if delay := (&dynamicPartitionCreator{settings: DynamicPartitionCreationConfig{
		RetryBackoff: 700 * time.Millisecond,
	}}).retryDelay(2); delay != time.Second {
		t.Fatalf("capped retry delay = %v", delay)
	}
}
