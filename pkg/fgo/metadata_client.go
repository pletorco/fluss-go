package fgo

import (
	"context"
	"fmt"
	"net"
	"strconv"

	"github.com/pletorco/fluss-go/pkg/fmsg"
	"google.golang.org/protobuf/proto"
)

func (c *Client) fetchTableMetadata(ctx context.Context, path TablePath) (TableMetadata, error) {
	request, err := fmsg.NewRequest(fmsg.APIKeyGetMetadata, 0)
	if err != nil {
		return TableMetadata{}, err
	}
	request.Message().(*fmsg.MetadataRequest).TablePath = []*fmsg.PbTablePath{pbTablePath(path)}
	response, err := c.RequestCoordinator(ctx, request)
	if err != nil {
		return TableMetadata{}, err
	}
	metadata, ok := response.Message().(*fmsg.MetadataResponse)
	if !ok {
		return TableMetadata{}, fmt.Errorf("fgo: metadata: unexpected response %T", response.Message())
	}
	return tableMetadataFromResponse(metadata, path)
}

func (c *Client) fetchPartitionMetadata(ctx context.Context, path PhysicalTablePath) (PartitionMetadata, error) {
	request, err := fmsg.NewRequest(fmsg.APIKeyGetMetadata, 0)
	if err != nil {
		return PartitionMetadata{}, err
	}
	request.Message().(*fmsg.MetadataRequest).PartitionsPath = []*fmsg.PbPhysicalTablePath{{
		DatabaseName: proto.String(path.Database), TableName: proto.String(path.Table), PartitionName: proto.String(path.Partition),
	}}
	response, err := c.RequestCoordinator(ctx, request)
	if err != nil {
		return PartitionMetadata{}, err
	}
	metadata, ok := response.Message().(*fmsg.MetadataResponse)
	if !ok {
		return PartitionMetadata{}, fmt.Errorf("fgo: metadata: unexpected response %T", response.Message())
	}
	return partitionMetadataFromResponse(metadata, path)
}

func pbTablePath(path TablePath) *fmsg.PbTablePath {
	return &fmsg.PbTablePath{DatabaseName: proto.String(path.Database), TableName: proto.String(path.Table)}
}

func tableMetadataFromResponse(response *fmsg.MetadataResponse, path TablePath) (TableMetadata, error) {
	coordinator, tablets, err := metadataServers(response)
	if err != nil {
		return TableMetadata{}, err
	}
	for _, item := range response.GetTableMetadata() {
		if item.GetTablePath().GetDatabaseName() != path.Database || item.GetTablePath().GetTableName() != path.Table {
			continue
		}
		buckets, details, err := bucketMetadata(item.GetBucketMetadata(), tablets)
		if err != nil {
			return TableMetadata{}, err
		}
		partitions, err := partitionMetadataForTable(response.GetPartitionMetadata(), item.GetTableId(), path, coordinator, tablets)
		if err != nil {
			return TableMetadata{}, err
		}
		return TableMetadata{Path: path, ID: item.GetTableId(), SchemaID: item.GetSchemaId(), Buckets: buckets, BucketDetails: details, BucketCount: int32(len(details)), BucketCountEpoch: item.GetBucketCountEpoch(), BucketCountEpochKnown: item.BucketCountEpoch != nil, RemoteDataDirectory: item.GetRemoteDataDir(), Partitions: partitions, coordinator: coordinator, tablets: tablets}, nil
	}
	return TableMetadata{}, fmt.Errorf("%w: %s", ErrUnknownTable, path)
}

func partitionMetadataForTable(partitions []*fmsg.PbPartitionMetadata, tableID int64, path TablePath, coordinator ServerNode, tablets map[int32]ServerNode) (map[string]PartitionMetadata, error) {
	result := make(map[string]PartitionMetadata)
	for _, partition := range partitions {
		if partition.GetTableId() != tableID {
			continue
		}
		partitionPath := PhysicalTablePath{TablePath: path, Partition: partition.GetPartitionName()}
		buckets, details, err := bucketMetadata(partition.GetBucketMetadata(), tablets)
		if err != nil {
			return nil, err
		}
		count := partition.GetBucketCount()
		if partition.BucketCount == nil {
			count = int32(len(details))
		}
		result[physicalTableKey(partitionPath)] = PartitionMetadata{Path: partitionPath, ID: partition.GetPartitionId(), Buckets: buckets, BucketDetails: details, BucketCount: count, coordinator: coordinator, tablets: tablets}
	}
	return result, nil
}

func partitionMetadataFromResponse(response *fmsg.MetadataResponse, path PhysicalTablePath) (PartitionMetadata, error) {
	coordinator, tablets, err := metadataServers(response)
	if err != nil {
		return PartitionMetadata{}, err
	}
	for _, item := range response.GetPartitionMetadata() {
		if item.GetPartitionName() != path.Partition {
			continue
		}
		buckets, details, err := bucketMetadata(item.GetBucketMetadata(), tablets)
		if err != nil {
			return PartitionMetadata{}, err
		}
		count := item.GetBucketCount()
		if item.BucketCount == nil {
			count = int32(len(details))
		}
		return PartitionMetadata{Path: path, ID: item.GetPartitionId(), Buckets: buckets, BucketDetails: details, BucketCount: count, coordinator: coordinator, tablets: tablets}, nil
	}
	return PartitionMetadata{}, fmt.Errorf("%w: %s", ErrUnknownPartition, path)
}

func metadataServers(response *fmsg.MetadataResponse) (ServerNode, map[int32]ServerNode, error) {
	tablets := make(map[int32]ServerNode, len(response.GetTabletServers()))
	for _, server := range response.GetTabletServers() {
		node, err := nodeFromProto(server, TabletServer)
		if err != nil {
			return ServerNode{}, nil, err
		}
		tablets[node.ID] = node
	}
	var coordinator ServerNode
	if response.GetCoordinatorServer() != nil {
		var err error
		coordinator, err = nodeFromProto(response.GetCoordinatorServer(), Coordinator)
		if err != nil {
			return ServerNode{}, nil, err
		}
	}
	return coordinator, tablets, nil
}

func nodeFromProto(server *fmsg.PbServerNode, serverType ServerType) (ServerNode, error) {
	if server == nil || server.GetHost() == "" || server.GetPort() <= 0 || server.GetPort() > 65535 {
		return ServerNode{}, fmt.Errorf("%w: invalid server node", ErrMetadata)
	}
	return ServerNode{ID: server.GetNodeId(), Address: net.JoinHostPort(server.GetHost(), strconv.Itoa(int(server.GetPort()))), ServerType: serverType}, nil
}

func bucketMetadata(buckets []*fmsg.PbBucketMetadata, tablets map[int32]ServerNode) (map[int32]ServerNode, map[int32]BucketMetadata, error) {
	leaders := make(map[int32]ServerNode, len(buckets))
	details := make(map[int32]BucketMetadata, len(buckets))
	for _, bucket := range buckets {
		if bucket == nil || bucket.BucketId == nil || bucket.GetBucketId() < 0 {
			return nil, nil, fmt.Errorf("%w: invalid bucket metadata", ErrMetadata)
		}
		detail := BucketMetadata{
			ID: bucket.GetBucketId(), Replicas: append([]int32(nil), bucket.GetReplicaId()...),
			ISR: append([]int32(nil), bucket.GetIsr()...), LeaderEpoch: bucket.GetLeaderEpoch(),
			LeaderEpochKnown: bucket.LeaderEpoch != nil, BucketEpoch: bucket.GetBucketEpoch(),
			BucketEpochKnown: bucket.BucketEpoch != nil,
		}
		if bucket.LeaderId != nil {
			node, ok := tablets[bucket.GetLeaderId()]
			if !ok {
				return nil, nil, fmt.Errorf("%w: bucket %d leader %d", ErrNoBucketLeader, bucket.GetBucketId(), bucket.GetLeaderId())
			}
			leader := node
			detail.Leader = &leader
			leaders[bucket.GetBucketId()] = node
		}
		details[bucket.GetBucketId()] = detail
	}
	return leaders, details, nil
}
