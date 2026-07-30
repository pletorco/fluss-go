package fgo

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/pletorco/fluss-go/pkg/fmsg"
	"google.golang.org/protobuf/proto"
)

var ErrNotFound = fmt.Errorf("fgo: record not found")

type LookupConfig struct {
	MaxBatchKeys  int
	MaxConcurrent int
	Partition     string
}

type LookupOption func(*LookupConfig) error

func WithLookupBatch(keys, concurrent int) LookupOption {
	return func(config *LookupConfig) error {
		if keys <= 0 || concurrent <= 0 {
			return fmt.Errorf("%w: lookup limits must be positive", ErrInvalidConfig)
		}
		config.MaxBatchKeys, config.MaxConcurrent = keys, concurrent
		return nil
	}
}

func WithLookupPartition(partition string) LookupOption {
	return func(config *LookupConfig) error {
		path := PhysicalTablePath{TablePath: TablePath{Database: "d", Table: "t"}, Partition: partition}
		if err := path.Validate(); err != nil {
			return err
		}
		config.Partition = partition
		return nil
	}
}

type LookupResult struct {
	Key    PrimaryKey
	Row    Row
	Found  bool
	Bucket int32
	Err    error
}

type PrefixLookupResult struct {
	Prefix PrimaryKey
	Rows   []Row
	Bucket int32
	Err    error
}

type lookupBackend interface {
	metadata(context.Context, PhysicalTablePath) (int64, map[int32]Node, error)
	lookup(context.Context, PhysicalTablePath, int32, int64, int64, [][]byte) ([][]byte, error)
	prefixLookup(context.Context, PhysicalTablePath, int32, int64, int64, [][]byte) ([][][]byte, error)
}

type clientLookupBackend struct{ client *Client }

func (b clientLookupBackend) metadata(ctx context.Context, path PhysicalTablePath) (int64, map[int32]Node, error) {
	return (clientLogWriterBackend{client: b.client}).metadata(ctx, path)
}

func (b clientLookupBackend) lookup(
	ctx context.Context,
	path PhysicalTablePath,
	bucket int32,
	tableID int64,
	partitionID int64,
	keys [][]byte,
) ([][]byte, error) {
	request, err := fmsg.NewRequest(fmsg.APIKeyLookup, 0)
	if err != nil {
		return nil, err
	}
	message := request.Message().(*fmsg.LookupRequest)
	message.TableId = proto.Int64(tableID)
	bucketRequest := &fmsg.PbLookupReqForBucket{BucketId: proto.Int32(bucket), Keys: cloneBytesList(keys)}
	if partitionID >= 0 {
		bucketRequest.PartitionId = proto.Int64(partitionID)
	}
	message.BucketsReq = []*fmsg.PbLookupReqForBucket{bucketRequest}
	response, err := b.client.RequestBucket(ctx, path, bucket, request)
	if err != nil {
		return nil, err
	}
	lookup, ok := response.Message().(*fmsg.LookupResponse)
	if !ok {
		return nil, fmt.Errorf("fgo: lookup: unexpected response %T", response.Message())
	}
	if len(lookup.GetBucketsResp()) != 1 || lookup.GetBucketsResp()[0].GetBucketId() != bucket {
		return nil, fmt.Errorf("%w: lookup response omitted bucket %d", ErrValidation, bucket)
	}
	result := lookup.GetBucketsResp()[0]
	if err := responseServerError(result.GetErrorCode(), result.GetErrorMessage(), fmsg.APIKeyLookup); err != nil {
		return nil, err
	}
	if len(result.GetValues()) != len(keys) {
		return nil, fmt.Errorf("%w: lookup returned %d values for %d keys", ErrValidation, len(result.GetValues()), len(keys))
	}
	values := make([][]byte, len(keys))
	for index, value := range result.GetValues() {
		if value != nil && value.Values != nil {
			values[index] = append([]byte(nil), value.GetValues()...)
		}
	}
	return values, nil
}

func (b clientLookupBackend) prefixLookup(
	ctx context.Context,
	path PhysicalTablePath,
	bucket int32,
	tableID int64,
	partitionID int64,
	keys [][]byte,
) ([][][]byte, error) {
	request, err := fmsg.NewRequest(fmsg.APIKeyPrefixLookup, 0)
	if err != nil {
		return nil, err
	}
	message := request.Message().(*fmsg.PrefixLookupRequest)
	message.TableId = proto.Int64(tableID)
	bucketRequest := &fmsg.PbPrefixLookupReqForBucket{BucketId: proto.Int32(bucket), Keys: cloneBytesList(keys)}
	if partitionID >= 0 {
		bucketRequest.PartitionId = proto.Int64(partitionID)
	}
	message.BucketsReq = []*fmsg.PbPrefixLookupReqForBucket{bucketRequest}
	response, err := b.client.RequestBucket(ctx, path, bucket, request)
	if err != nil {
		return nil, err
	}
	lookup, ok := response.Message().(*fmsg.PrefixLookupResponse)
	if !ok {
		return nil, fmt.Errorf("fgo: prefix lookup: unexpected response %T", response.Message())
	}
	if len(lookup.GetBucketsResp()) != 1 || lookup.GetBucketsResp()[0].GetBucketId() != bucket {
		return nil, fmt.Errorf("%w: prefix lookup response omitted bucket %d", ErrValidation, bucket)
	}
	result := lookup.GetBucketsResp()[0]
	if err := responseServerError(result.GetErrorCode(), result.GetErrorMessage(), fmsg.APIKeyPrefixLookup); err != nil {
		return nil, err
	}
	if len(result.GetValueLists()) != len(keys) {
		return nil, fmt.Errorf("%w: prefix lookup returned %d lists for %d keys", ErrValidation, len(result.GetValueLists()), len(keys))
	}
	values := make([][][]byte, len(keys))
	for index, list := range result.GetValueLists() {
		values[index] = cloneBytesList(list.GetValues())
	}
	return values, nil
}

func cloneBytesList(values [][]byte) [][]byte {
	cloned := make([][]byte, len(values))
	for index, value := range values {
		cloned[index] = append([]byte(nil), value...)
	}
	return cloned
}

type LookupClient struct {
	table       Table
	path        PhysicalTablePath
	backend     lookupBackend
	config      LookupConfig
	tableID     int64
	partitionID int64
	buckets     []int32

	mu     sync.RWMutex
	closed bool
	life   context.Context
	cancel context.CancelFunc
}

func (c *Client) NewLookupClient(ctx context.Context, table Table, options ...LookupOption) (*LookupClient, error) {
	return newLookupClient(ctx, clientLookupBackend{client: c}, table, options...)
}

func newLookupClient(ctx context.Context, backend lookupBackend, table Table, options ...LookupOption) (*LookupClient, error) {
	if err := table.RequirePrimaryKey(); err != nil {
		return nil, err
	}
	if err := table.Schema.Validate(); err != nil {
		return nil, err
	}
	config := LookupConfig{MaxBatchKeys: 1000, MaxConcurrent: 8}
	for _, option := range options {
		if option == nil {
			return nil, fmt.Errorf("%w: nil lookup option", ErrInvalidConfig)
		}
		if err := option(&config); err != nil {
			return nil, err
		}
	}
	path := PhysicalTablePath{TablePath: table.Path, Partition: config.Partition}
	physicalID, locations, err := backend.metadata(ctx, path)
	if err != nil {
		return nil, err
	}
	if path.Partition == "" && physicalID != table.ID {
		return nil, fmt.Errorf("%w: metadata table ID %d does not match opened table %d", ErrMetadata, physicalID, table.ID)
	}
	buckets, err := sortedBuckets(locations)
	if err != nil {
		return nil, err
	}
	client := &LookupClient{
		table: table, path: path, backend: backend, config: config, tableID: table.ID,
		partitionID: -1, buckets: buckets,
	}
	if path.Partition != "" {
		client.partitionID = physicalID
	}
	client.life, client.cancel = context.WithCancel(context.Background())
	return client, nil
}

type lookupInput struct {
	index   int
	bucket  int32
	encoded []byte
}

func (c *LookupClient) Lookup(ctx context.Context, keys ...PrimaryKey) []LookupResult {
	results := make([]LookupResult, len(keys))
	if len(keys) == 0 {
		return results
	}
	requestCtx, cancel, err := c.requestContext(ctx)
	if err != nil {
		for index, key := range keys {
			results[index] = LookupResult{Key: key, Err: err}
		}
		return results
	}
	defer cancel()
	groups := make(map[int32][]lookupInput)
	for index, key := range keys {
		results[index].Key = key
		encoded, bucket, err := c.encodePoint(key)
		if err != nil {
			results[index].Err = err
			continue
		}
		results[index].Bucket = bucket
		groups[bucket] = append(groups[bucket], lookupInput{index: index, bucket: bucket, encoded: encoded})
	}
	c.runPointLookups(requestCtx, groups, results)
	return results
}

func (c *LookupClient) PrefixLookup(ctx context.Context, prefixes ...PrimaryKey) []PrefixLookupResult {
	results := make([]PrefixLookupResult, len(prefixes))
	if len(prefixes) == 0 {
		return results
	}
	requestCtx, cancel, err := c.requestContext(ctx)
	if err != nil {
		for index, prefix := range prefixes {
			results[index] = PrefixLookupResult{Prefix: prefix, Err: err}
		}
		return results
	}
	defer cancel()
	groups := make(map[int32][]lookupInput)
	for index, prefix := range prefixes {
		results[index].Prefix = prefix
		encoded, bucket, err := c.encodePrefix(prefix)
		if err != nil {
			results[index].Err = err
			continue
		}
		results[index].Bucket = bucket
		groups[bucket] = append(groups[bucket], lookupInput{index: index, bucket: bucket, encoded: encoded})
	}
	c.runPrefixLookups(requestCtx, groups, results)
	return results
}

func (c *LookupClient) requestContext(ctx context.Context) (context.Context, context.CancelFunc, error) {
	if ctx == nil {
		return nil, nil, fmt.Errorf("%w: nil context", ErrInvalidConfig)
	}
	c.mu.RLock()
	closed := c.closed
	life := c.life
	c.mu.RUnlock()
	if closed {
		return nil, nil, ErrClosed
	}
	requestCtx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(life, cancel)
	return requestCtx, func() {
		stop()
		cancel()
	}, nil
}

func (c *LookupClient) encodePoint(key PrimaryKey) ([]byte, int32, error) {
	encoded, err := EncodeLookupKey(c.table.Schema, key, 1)
	if err != nil {
		return nil, 0, err
	}
	values := make(map[string]any, len(key))
	for index, name := range c.table.Schema.PrimaryKey {
		values[name] = key[index]
	}
	bucket, err := c.bucketForValues(values)
	return encoded, bucket, err
}

func (c *LookupClient) encodePrefix(prefix PrimaryKey) ([]byte, int32, error) {
	bucketNames := c.table.Schema.BucketKey
	if len(bucketNames) == 0 {
		bucketNames = c.table.Schema.PrimaryKey
	}
	if len(prefix) < len(bucketNames) {
		return nil, 0, fmt.Errorf("%w: prefix does not contain the complete bucket key", ErrInvalidRow)
	}
	for index, name := range bucketNames {
		if index >= len(c.table.Schema.PrimaryKey) || c.table.Schema.PrimaryKey[index] != name {
			return nil, 0, fmt.Errorf("%w: bucket key is not a leading primary-key prefix", ErrInvalidSchema)
		}
	}
	encoded, err := EncodePrefixLookupKey(c.table.Schema, prefix, 1)
	if err != nil {
		return nil, 0, err
	}
	values := make(map[string]any, len(prefix))
	for index := range prefix {
		values[c.table.Schema.PrimaryKey[index]] = prefix[index]
	}
	bucket, err := c.bucketForValues(values)
	return encoded, bucket, err
}

func (c *LookupClient) bucketForValues(values map[string]any) (int32, error) {
	names := c.table.Schema.BucketKey
	if len(names) == 0 {
		names = c.table.Schema.PrimaryKey
	}
	key := make(PrimaryKey, len(names))
	for index, name := range names {
		key[index] = values[name]
	}
	encoded, err := encodeKeyColumns(c.table.Schema, names, key)
	if err != nil {
		return 0, err
	}
	bucket, err := flussBucket(encoded, len(c.buckets))
	if err != nil {
		return 0, err
	}
	return c.buckets[int(bucket)], nil
}

func (c *LookupClient) runPointLookups(ctx context.Context, groups map[int32][]lookupInput, results []LookupResult) {
	var wait sync.WaitGroup
	sem := make(chan struct{}, c.config.MaxConcurrent)
	for bucket, inputs := range groups {
		for start := 0; start < len(inputs); start += c.config.MaxBatchKeys {
			end := start + c.config.MaxBatchKeys
			if end > len(inputs) {
				end = len(inputs)
			}
			chunk := append([]lookupInput(nil), inputs[start:end]...)
			wait.Add(1)
			go func(bucket int32) {
				defer wait.Done()
				select {
				case sem <- struct{}{}:
					defer func() { <-sem }()
				case <-ctx.Done():
					setPointErrors(chunk, results, ctx.Err())
					return
				}
				encoded := make([][]byte, len(chunk))
				for index := range chunk {
					encoded[index] = chunk[index].encoded
				}
				values, err := c.backend.lookup(ctx, c.path, bucket, c.tableID, c.partitionID, encoded)
				if err != nil {
					setPointErrors(chunk, results, err)
					return
				}
				for index, value := range values {
					if value == nil {
						results[chunk[index].index].Err = ErrNotFound
						continue
					}
					row, err := decodeLookupValue(c.table, value)
					results[chunk[index].index].Row = row
					results[chunk[index].index].Found = err == nil
					results[chunk[index].index].Err = err
				}
			}(bucket)
		}
	}
	wait.Wait()
}

func (c *LookupClient) runPrefixLookups(
	ctx context.Context,
	groups map[int32][]lookupInput,
	results []PrefixLookupResult,
) {
	var wait sync.WaitGroup
	sem := make(chan struct{}, c.config.MaxConcurrent)
	for bucket, inputs := range groups {
		for start := 0; start < len(inputs); start += c.config.MaxBatchKeys {
			end := start + c.config.MaxBatchKeys
			if end > len(inputs) {
				end = len(inputs)
			}
			chunk := append([]lookupInput(nil), inputs[start:end]...)
			wait.Add(1)
			go func(bucket int32) {
				defer wait.Done()
				select {
				case sem <- struct{}{}:
					defer func() { <-sem }()
				case <-ctx.Done():
					setPrefixErrors(chunk, results, ctx.Err())
					return
				}
				encoded := make([][]byte, len(chunk))
				for index := range chunk {
					encoded[index] = chunk[index].encoded
				}
				values, err := c.backend.prefixLookup(ctx, c.path, bucket, c.tableID, c.partitionID, encoded)
				if err != nil {
					setPrefixErrors(chunk, results, err)
					return
				}
				for index, rows := range values {
					for _, value := range rows {
						row, err := decodeLookupValue(c.table, value)
						if err != nil {
							results[chunk[index].index].Err = err
							break
						}
						results[chunk[index].index].Rows = append(results[chunk[index].index].Rows, row)
					}
				}
			}(bucket)
		}
	}
	wait.Wait()
}

func decodeLookupValue(table Table, value []byte) (Row, error) {
	if len(value) < 2 {
		return nil, fmt.Errorf("%w: lookup value omits schema ID", ErrMalformedRow)
	}
	schemaID := int16(binary.LittleEndian.Uint16(value))
	if int32(schemaID) != table.SchemaID {
		return nil, fmt.Errorf(
			"%w: lookup value schema ID %d does not match table schema ID %d",
			ErrInvalidSchema, schemaID, table.SchemaID,
		)
	}
	return DecodeCompactedRow(table.Schema, value[2:])
}

func setPointErrors(inputs []lookupInput, results []LookupResult, err error) {
	for _, input := range inputs {
		results[input.index].Err = err
	}
}

func setPrefixErrors(inputs []lookupInput, results []PrefixLookupResult, err error) {
	for _, input := range inputs {
		results[input.index].Err = err
	}
}

func (c *LookupClient) Close() error {
	c.mu.Lock()
	if !c.closed {
		c.closed = true
		c.cancel()
	}
	c.mu.Unlock()
	return nil
}
