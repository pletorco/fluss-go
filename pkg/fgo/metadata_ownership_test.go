package fgo

import (
	"context"
	"sync"
	"testing"
)

func TestRouterOwnsFetchedTableMetadata(t *testing.T) {
	path := TablePath{Database: "db", Table: "events"}
	physical := PhysicalTablePath{TablePath: path, Partition: "day=1"}
	key := physicalTableKey(physical)
	node := ServerNode{ID: 2, Address: "tablet:9123", ServerType: TabletServer}
	partitionLeader, tableLeader := node, node
	partition := PartitionMetadata{
		Path: physical, BucketCount: 1, Buckets: map[int32]ServerNode{0: node},
		BucketDetails: map[int32]BucketMetadata{0: {Leader: &partitionLeader, Replicas: []int32{2, 3}, ISR: []int32{2}}},
	}
	table := TableMetadata{
		Path: path, BucketCount: 1, Buckets: map[int32]ServerNode{0: node},
		BucketDetails: map[int32]BucketMetadata{0: {Leader: &tableLeader, Replicas: []int32{2, 3}, ISR: []int32{2}}},
		Partitions:    map[string]PartitionMetadata{key: partition}, tablets: map[int32]ServerNode{2: node},
	}
	router := NewRouter(ServerNode{}, func(context.Context, TablePath) (TableMetadata, error) {
		return table, nil
	})
	if err := router.Refresh(t.Context(), path); err != nil {
		t.Fatal(err)
	}
	delete(table.Buckets, 0)
	delete(partition.Buckets, 0)
	for _, details := range []map[int32]BucketMetadata{table.BucketDetails, partition.BucketDetails} {
		detail := details[0]
		detail.Leader.Address = "changed:9123"
		detail.Replicas[0], detail.ISR[0] = 9, 9
		delete(details, 0)
	}
	table.tablets[2] = ServerNode{}
	delete(table.Partitions, key)
	for _, route := range []PhysicalTablePath{{TablePath: path}, physical} {
		if got, count, err := router.lookupPhysical(route, 0); err != nil || got != node || count != 1 {
			t.Fatalf("cached route %s = %v, %d, %v", route, got, count, err)
		}
	}
	router.mu.RLock()
	cached := router.metadata.Tables[path]
	gotTablet, tableTablet := router.metadata.Tablets[2], cached.tablets[2]
	details := []BucketMetadata{cached.BucketDetails[0], cached.Partitions[key].BucketDetails[0]}
	router.mu.RUnlock()
	if gotTablet != node || tableTablet != node {
		t.Fatalf("cached tablet state = %v, %v", gotTablet, tableTablet)
	}
	for _, detail := range details {
		if detail.Leader == nil || *detail.Leader != node || detail.Replicas[0] != 2 || detail.ISR[0] != 2 {
			t.Fatalf("cached bucket detail aliased its fetch response: %#v", detail)
		}
	}
	// In-place eviction must not delete a partition from the fetcher's map.
	table.Partitions[key] = partition
	router.InvalidatePhysical(physical)
	if _, ok := table.Partitions[key]; !ok {
		t.Fatal("partition invalidation mutated the fetcher's table")
	}
}

func TestRouterCacheUpdatesWithConcurrentReaders(t *testing.T) {
	path := TablePath{Database: "db", Table: "events"}
	unrelated := TablePath{Database: "db", Table: "other"}
	target := PhysicalTablePath{TablePath: path, Partition: "target"}
	sibling := PhysicalTablePath{TablePath: path, Partition: "sibling"}
	stable := ServerNode{ID: 1, Address: "stable:9123"}
	updated := ServerNode{ID: 2, Address: "updated:9123"}
	router := NewRouter(ServerNode{}, func(_ context.Context, requested TablePath) (TableMetadata, error) {
		table := TableMetadata{Path: requested, ID: 10, BucketCount: 1, Buckets: map[int32]ServerNode{0: stable}}
		if requested == path {
			table.Partitions = map[string]PartitionMetadata{physicalTableKey(sibling): {
				Path: sibling, BucketCount: 1, Buckets: map[int32]ServerNode{0: stable},
			}}
		}
		return table, nil
	}).WithPhysicalMetadataFetcher(func(context.Context, PhysicalTablePath) (PartitionMetadata, error) {
		return PartitionMetadata{Path: target, BucketCount: 1, Buckets: map[int32]ServerNode{0: updated},
			parentTableID: 10, parentTableIDKnown: true}, nil
	})
	ctx := t.Context()
	for _, logical := range []TablePath{path, unrelated} {
		if err := router.Refresh(ctx, logical); err != nil {
			t.Fatal(err)
		}
	}
	stop := make(chan struct{})
	var readers sync.WaitGroup
	for range 4 {
		readers.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
				for _, route := range []PhysicalTablePath{{TablePath: unrelated}, sibling} {
					if got, count, err := router.lookupPhysical(route, 0); err != nil || got != stable || count != 1 {
						t.Errorf("untouched route %s = %v, %d, %v", route, got, count, err)
						return
					}
				}
				if got, _, err := router.lookupPhysical(target, 0); err != nil || (got != updated && got != (ServerNode{})) {
					t.Errorf("updated route %s = %v, %v", target, got, err)
					return
				}
			}
		})
	}
	func() {
		defer readers.Wait()
		defer close(stop)
		for range 100 {
			if err := router.RefreshPhysical(ctx, target); err != nil {
				t.Fatal(err)
			}
			router.InvalidatePhysical(target)
			if got, _, err := router.lookupPhysical(target, 0); got != (ServerNode{}) || err != nil {
				t.Fatalf("invalidated partition remains cached: %v, %v", got, err)
			}
		}
	}()
	router.Invalidate(path)
	if got, _, err := router.lookupPhysical(sibling, 0); got != (ServerNode{}) || err != nil {
		t.Fatalf("invalidating a table retained its partition: %v, %v", got, err)
	}
	if got, _, err := router.lookupPhysical(PhysicalTablePath{TablePath: unrelated}, 0); got != stable || err != nil {
		t.Fatalf("invalidating one table changed another route: %v, %v", got, err)
	}
}
