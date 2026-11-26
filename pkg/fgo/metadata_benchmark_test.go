package fgo

import (
	"context"
	"strconv"
	"testing"
)

func BenchmarkRouterRefreshPartition(b *testing.B) {
	for _, count := range []int{1, 100, 1000} {
		b.Run(strconv.Itoa(count), func(b *testing.B) {
			path := TablePath{Database: "db", Table: "events"}
			node := ServerNode{ID: 1, Address: "tablet:9123", ServerType: TabletServer}
			table := TableMetadata{Path: path, ID: 10, Partitions: make(map[string]PartitionMetadata, count)}
			for index := range count {
				physical := PhysicalTablePath{TablePath: path, Partition: strconv.Itoa(index)}
				partition := PartitionMetadata{
					Path: physical, ID: int64(index), BucketCount: 4, parentTableID: table.ID, parentTableIDKnown: true,
					Buckets: make(map[int32]ServerNode), BucketDetails: make(map[int32]BucketMetadata),
				}
				for bucket := range int32(4) {
					partition.Buckets[bucket] = node
					partition.BucketDetails[bucket] = BucketMetadata{
						ID: bucket, Leader: &node, Replicas: []int32{1, 2, 3}, ISR: []int32{1, 2, 3},
					}
				}
				table.Partitions[physicalTableKey(physical)] = partition
			}
			physical := PhysicalTablePath{TablePath: path, Partition: "0"}
			partition := table.Partitions[physicalTableKey(physical)]
			router := NewRouter(ServerNode{}, func(context.Context, TablePath) (TableMetadata, error) {
				return table, nil
			}).WithPhysicalMetadataFetcher(func(context.Context, PhysicalTablePath) (PartitionMetadata, error) {
				return partition, nil
			})
			if err := router.Refresh(b.Context(), path); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			for b.Loop() {
				if err := router.RefreshPhysical(b.Context(), physical); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
