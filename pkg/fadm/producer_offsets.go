package fadm

import (
	"context"
	"fmt"
	"time"

	"github.com/pletorco/fluss-go/pkg/fgo"
	"github.com/pletorco/fluss-go/pkg/fmsg"
	"google.golang.org/protobuf/proto"
)

// ProducerBucketOffset is one producer offset for a table bucket.
type ProducerBucketOffset struct {
	// PartitionID is -1 for an unpartitioned table.
	PartitionID int64
	// Bucket identifies the logical table bucket.
	Bucket int32
	// Offset is the registered next-readable log offset.
	Offset int64
}

// ProducerTableOffsets groups producer offsets for one physical table.
type ProducerTableOffsets struct {
	// TableID is the server-assigned table identifier.
	TableID int64
	// Offsets contains independently registered bucket offsets.
	Offsets []ProducerBucketOffset
}

// ProducerOffsets contains all registered offsets and their expiration.
type ProducerOffsets struct {
	// ProducerID is the application registration key.
	ProducerID string
	// ExpiresAt is the server expiration time.
	ExpiresAt time.Time
	// Tables contains registered table offsets.
	Tables []ProducerTableOffsets
}

// RegisterProducerOffsets creates or replaces offsets for producerID. The
// boolean result is true only when the Fluss 1.0 protocol result code is
// zero; callers must handle a false result even when err is nil.
func (c *Client) RegisterProducerOffsets(
	ctx context.Context,
	producerID string,
	tables []ProducerTableOffsets,
	ttl time.Duration,
) (bool, error) {
	if producerID == "" || len(tables) == 0 || ttl <= 0 {
		return false, fmt.Errorf("%w: producer ID, offsets, and TTL are required", fgo.ErrInvalidConfig)
	}
	request, err := fmsg.NewRequest(fmsg.APIKeyRegisterProducerOffsets, 0)
	if err != nil {
		return false, err
	}
	message := request.Message().(*fmsg.RegisterProducerOffsetsRequest)
	message.ProducerId, message.TtlMs = proto.String(producerID), proto.Int64(ttl.Milliseconds())
	for _, table := range tables {
		item, err := producerTableOffsetsMessage(table)
		if err != nil {
			return false, err
		}
		message.TableOffsets = append(message.TableOffsets, item)
	}
	response, err := c.requester.RequestCoordinator(ctx, request)
	if err != nil {
		return false, err
	}
	registered, ok := response.Message().(*fmsg.RegisterProducerOffsetsResponse)
	if !ok {
		return false, unexpected("register producer offsets", response)
	}
	return registered.GetResult() == 0, nil
}

func producerTableOffsetsMessage(table ProducerTableOffsets) (*fmsg.PbProducerTableOffsets, error) {
	if table.TableID < 0 || len(table.Offsets) == 0 {
		return nil, fmt.Errorf("%w: invalid producer table offsets", fgo.ErrInvalidConfig)
	}
	item := &fmsg.PbProducerTableOffsets{TableId: proto.Int64(table.TableID)}
	for _, offset := range table.Offsets {
		if offset.Bucket < 0 || offset.Offset < 0 || offset.PartitionID < -1 {
			return nil, fmt.Errorf("%w: invalid producer bucket offset", fgo.ErrInvalidConfig)
		}
		pbOffset := &fmsg.PbBucketOffset{
			BucketId: proto.Int32(offset.Bucket), LogEndOffset: proto.Int64(offset.Offset),
		}
		if offset.PartitionID >= 0 {
			pbOffset.PartitionId = proto.Int64(offset.PartitionID)
		}
		item.BucketOffsets = append(item.BucketOffsets, pbOffset)
	}
	return item, nil
}

// GetProducerOffsets returns registered offsets for producerID.
func (c *Client) GetProducerOffsets(ctx context.Context, producerID string) (ProducerOffsets, error) {
	if producerID == "" {
		return ProducerOffsets{}, fmt.Errorf("%w: producer ID is required", fgo.ErrInvalidConfig)
	}
	request, err := fmsg.NewRequest(fmsg.APIKeyGetProducerOffsets, 0)
	if err != nil {
		return ProducerOffsets{}, err
	}
	request.Message().(*fmsg.GetProducerOffsetsRequest).ProducerId = proto.String(producerID)
	response, err := c.requester.RequestCoordinator(ctx, request)
	if err != nil {
		return ProducerOffsets{}, err
	}
	message, ok := response.Message().(*fmsg.GetProducerOffsetsResponse)
	if !ok {
		return ProducerOffsets{}, unexpected("get producer offsets", response)
	}
	result := ProducerOffsets{ProducerID: message.GetProducerId(), ExpiresAt: millis(message.GetExpirationTime())}
	result.Tables = producerOffsetsFromMessage(message.GetTableOffsets())
	return result, nil
}

// DeleteProducerOffsets removes registered offsets for producerID.
func (c *Client) DeleteProducerOffsets(ctx context.Context, producerID string) error {
	if producerID == "" {
		return fmt.Errorf("%w: producer ID is required", fgo.ErrInvalidConfig)
	}
	request, err := fmsg.NewRequest(fmsg.APIKeyDeleteProducerOffsets, 0)
	if err != nil {
		return err
	}
	request.Message().(*fmsg.DeleteProducerOffsetsRequest).ProducerId = proto.String(producerID)
	_, err = c.requester.RequestCoordinator(ctx, request)
	return err
}

func producerOffsetsFromMessage(tables []*fmsg.PbProducerTableOffsets) []ProducerTableOffsets {
	result := make([]ProducerTableOffsets, len(tables))
	for index, table := range tables {
		result[index].TableID = table.GetTableId()
		for _, offset := range table.GetBucketOffsets() {
			partitionID := int64(-1)
			if offset.PartitionId != nil {
				partitionID = offset.GetPartitionId()
			}
			result[index].Offsets = append(result[index].Offsets, ProducerBucketOffset{
				PartitionID: partitionID, Bucket: offset.GetBucketId(), Offset: offset.GetLogEndOffset(),
			})
		}
	}
	return result
}
