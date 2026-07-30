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
		buckets, err := bucketLeaders(item.GetBucketMetadata(), tablets)
		if err != nil {
			return TableMetadata{}, err
		}
		partitions := make(map[string]PartitionMetadata)
		for _, partition := range response.GetPartitionMetadata() {
			if partition.GetTableId() != item.GetTableId() {
				continue
			}
			partitionPath := PhysicalTablePath{TablePath: path, Partition: partition.GetPartitionName()}
			partitionBuckets, err := bucketLeaders(partition.GetBucketMetadata(), tablets)
			if err != nil {
				return TableMetadata{}, err
			}
			partitions[physicalTableKey(partitionPath)] = PartitionMetadata{Path: partitionPath, ID: partition.GetPartitionId(), Buckets: partitionBuckets, coordinator: coordinator, tablets: tablets}
		}
		return TableMetadata{Path: path, ID: item.GetTableId(), SchemaID: item.GetSchemaId(), Buckets: buckets, Partitions: partitions, coordinator: coordinator, tablets: tablets}, nil
	}
	return TableMetadata{}, fmt.Errorf("%w: %s", ErrUnknownTable, path)
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
		buckets, err := bucketLeaders(item.GetBucketMetadata(), tablets)
		if err != nil {
			return PartitionMetadata{}, err
		}
		return PartitionMetadata{Path: path, ID: item.GetPartitionId(), Buckets: buckets, coordinator: coordinator, tablets: tablets}, nil
	}
	return PartitionMetadata{}, fmt.Errorf("%w: %s", ErrUnknownPartition, path)
}

func metadataServers(response *fmsg.MetadataResponse) (Node, map[int32]Node, error) {
	tablets := make(map[int32]Node, len(response.GetTabletServers()))
	for _, server := range response.GetTabletServers() {
		node, err := nodeFromProto(server, TabletServer)
		if err != nil {
			return Node{}, nil, err
		}
		tablets[node.ID] = node
	}
	var coordinator Node
	if response.GetCoordinatorServer() != nil {
		var err error
		coordinator, err = nodeFromProto(response.GetCoordinatorServer(), Coordinator)
		if err != nil {
			return Node{}, nil, err
		}
	}
	return coordinator, tablets, nil
}

func nodeFromProto(server *fmsg.PbServerNode, role ServerRole) (Node, error) {
	if server == nil || server.GetHost() == "" || server.GetPort() <= 0 || server.GetPort() > 65535 {
		return Node{}, fmt.Errorf("%w: invalid server node", ErrMetadata)
	}
	return Node{ID: server.GetNodeId(), Address: net.JoinHostPort(server.GetHost(), strconv.Itoa(int(server.GetPort()))), Role: role}, nil
}

func bucketLeaders(buckets []*fmsg.PbBucketMetadata, tablets map[int32]Node) (map[int32]Node, error) {
	result := make(map[int32]Node, len(buckets))
	for _, bucket := range buckets {
		if bucket == nil || bucket.LeaderId == nil {
			return nil, fmt.Errorf("%w: bucket %d", ErrNoBucketLeader, bucket.GetBucketId())
		}
		node, ok := tablets[bucket.GetLeaderId()]
		if !ok {
			return nil, fmt.Errorf("%w: bucket %d leader %d", ErrNoBucketLeader, bucket.GetBucketId(), bucket.GetLeaderId())
		}
		result[bucket.GetBucketId()] = node
	}
	return result, nil
}
