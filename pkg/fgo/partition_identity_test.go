package fgo

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/pletorco/fluss-go/pkg/fmsg"
	"google.golang.org/protobuf/proto"
)

func TestResolveTableBucketsUsesPartitionParentIdentity(t *testing.T) {
	path := PhysicalTablePath{TablePath: appendWriterTable().Path, Partition: "day=2026-07-30"}
	metadata := metadataResponse(path.TablePath)
	metadata.PartitionMetadata[0].TableId = proto.Int64(0) // Zero is a valid Fluss table ID.
	metadata.PartitionMetadata[0].BucketMetadata = metadata.TableMetadata[0].BucketMetadata
	metadata.PartitionMetadata[0].BucketCount = proto.Int32(1)
	metadata.TableMetadata[0].TableId = proto.Int64(13)
	requests := 0
	client := newClient(requesterFunc(func(_ context.Context, request fmsg.Request) (fmsg.Response, error) {
		requests++
		response, err := fmsg.NewResponse(request.APIKey(), request.Version())
		if err != nil {
			return nil, err
		}
		message := response.Message().(*fmsg.MetadataResponse)
		input := request.(*fmsg.MessageRequest).Message().(*fmsg.MetadataRequest)
		message.TabletServers = metadata.TabletServers
		if len(input.GetPartitionsPath()) == 1 {
			message.PartitionMetadata = metadata.PartitionMetadata
		} else {
			// A second logical lookup would return a replacement table.
			message.TableMetadata = metadata.TableMetadata
		}
		return response, nil
	}), nil)
	defer client.Close()
	client.versions[fmsg.APIKeyGetMetadata] = 0
	buckets, err := client.ResolveTableBuckets(t.Context(), path)
	if err != nil || len(buckets) != 1 || buckets[0].TableID != 0 || buckets[0].PartitionID != 10 || requests != 1 {
		t.Fatalf("resolve physical buckets = %#v, %v, metadata RPCs = %d", buckets, err, requests)
	}
}

func TestPartitionConsumersRejectStaleTableHandle(t *testing.T) {
	logTable := appendWriterTable()
	kvTable := lookupTable()
	kvTable.Path, kvTable.ID = logTable.Path, logTable.ID
	metadata := metadataResponse(logTable.Path)
	metadata.PartitionMetadata[0].TableId = proto.Int64(13)
	metadata.PartitionMetadata[0].BucketCount = proto.Int32(1)
	metadata.PartitionMetadata[0].BucketMetadata = metadata.TableMetadata[0].BucketMetadata
	partition := metadata.PartitionMetadata[0].GetPartitionName()
	client := newBucketRoutingTestClient(t, metadata, func(_ context.Context, req fmsg.Request) (fmsg.Response, error) {
		return nil, fmt.Errorf("unexpected tablet RPC %v for a stale table", req.APIKey())
	})
	ctx := t.Context()
	if writer, err := client.NewAppendWriter(ctx, logTable, WithAppendPartition(partition)); !errors.Is(err, ErrMetadata) || writer != nil {
		if writer != nil {
			_ = writer.Close(ctx)
		}
		t.Errorf("AppendWriter = %v, %v; want ErrMetadata", writer, err)
	}
	if scanner, err := client.NewLogScanner(ctx, logTable, Earliest(), WithScanPartition(partition)); !errors.Is(err, ErrMetadata) || scanner != nil {
		if scanner != nil {
			_ = scanner.Close()
		}
		t.Errorf("LogScanner = %v, %v; want ErrMetadata", scanner, err)
	}
	if writer, err := client.NewUpsertWriter(ctx, kvTable, WithUpsertPartition(partition)); !errors.Is(err, ErrMetadata) || writer != nil {
		if writer != nil {
			_ = writer.Close(ctx)
		}
		t.Errorf("UpsertWriter = %v, %v; want ErrMetadata", writer, err)
	}
	if lookup, err := client.NewLookuper(ctx, kvTable, WithLookupPartition(partition)); !errors.Is(err, ErrMetadata) || lookup != nil {
		if lookup != nil {
			_ = lookup.Close()
		}
		t.Errorf("Lookuper = %v, %v; want ErrMetadata", lookup, err)
	}
}

func TestRouterRefreshesStaleParentForCurrentPartition(t *testing.T) {
	path := PhysicalTablePath{TablePath: appendWriterTable().Path, Partition: "partition"}
	var current atomic.Int64
	var parentFetches atomic.Int32
	leader := ServerNode{ID: 2, Address: "new:9123"}
	router := NewRouter(ServerNode{}, func(context.Context, TablePath) (TableMetadata, error) {
		parentFetches.Add(1)
		return TableMetadata{Path: path.TablePath, ID: current.Load()}, nil
	}).WithPhysicalMetadataFetcher(func(context.Context, PhysicalTablePath) (PartitionMetadata, error) {
		return PartitionMetadata{Path: path, ID: 42, parentTableID: current.Load(), parentTableIDKnown: true,
			BucketCount: 1, Buckets: map[int32]ServerNode{0: leader}}, nil
	})
	if err := router.Refresh(t.Context(), path.TablePath); err != nil {
		t.Fatal(err)
	}
	current.Store(13)
	if got, err := router.RoutePhysical(t.Context(), path, 0); err != nil || got != leader || parentFetches.Load() != 2 {
		t.Fatalf("route after recreation = %v, %v; parent fetches=%d", got, err, parentFetches.Load())
	}
	router.mu.RLock()
	table := router.metadata.Tables[path.TablePath]
	partition := table.Partitions[physicalTableKey(path)]
	router.mu.RUnlock()
	if table.ID != 13 || !partition.parentTableIDKnown || partition.parentTableID != table.ID || partition.ID != 42 {
		t.Fatalf("cached table/partition identities = %d, %#v", table.ID, partition)
	}
}

func TestRouterRejectsPartitionResponseFromReplacedTable(t *testing.T) {
	path := PhysicalTablePath{TablePath: appendWriterTable().Path, Partition: "partition"}
	var current atomic.Int64
	started, release := make(chan struct{}), make(chan struct{})
	oldLeader := ServerNode{ID: 1, Address: "old:9123"}
	newLeader := ServerNode{ID: 2, Address: "new:9123"}
	router := NewRouter(ServerNode{}, func(context.Context, TablePath) (TableMetadata, error) {
		return TableMetadata{Path: path.TablePath, ID: current.Load()}, nil
	}).WithPhysicalMetadataFetcher(func(ctx context.Context, _ PhysicalTablePath) (PartitionMetadata, error) {
		id := current.Load()
		leader := newLeader
		if id == 0 {
			leader = oldLeader
			close(started)
			select {
			case <-release:
			case <-ctx.Done():
				return PartitionMetadata{}, ctx.Err()
			}
		}
		return PartitionMetadata{Path: path, ID: id + 10, parentTableID: id, parentTableIDKnown: true,
			BucketCount: 1, Buckets: map[int32]ServerNode{0: leader}}, nil
	})
	result := make(chan error, 1)
	go func() { result <- router.RefreshPhysical(t.Context(), path) }()
	<-started
	current.Store(13)
	err := router.Refresh(t.Context(), path.TablePath)
	close(release)
	if err != nil {
		t.Fatal(err)
	}
	if err := <-result; !errors.Is(err, ErrMetadata) {
		t.Fatalf("old partition response returned %v, want ErrMetadata", err)
	}
	if got, _, err := router.lookupPhysical(path, 0); got != (ServerNode{}) || err != nil {
		t.Fatalf("stale partition was published: %v, %v", got, err)
	}
	if got, err := router.RoutePhysical(t.Context(), path, 0); err != nil || got != newLeader {
		t.Fatalf("route after rejecting stale partition = %v, %v", got, err)
	}
}
