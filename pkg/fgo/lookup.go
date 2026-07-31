package fgo

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/pletorco/fluss-go/pkg/fmsg"
	"google.golang.org/protobuf/proto"
)

// ErrNotFound reports a missing primary-key row.
var ErrNotFound = fmt.Errorf("fgo: record not found")

// LookupConfig controls batching, concurrency, partition routing, and optional
// atomic insertion of missing rows.
type LookupConfig struct {
	// MaxBatchKeys bounds keys in one lookup request.
	MaxBatchKeys int
	// MaxConcurrent bounds in-flight lookup requests.
	MaxConcurrent int
	// Partition selects one named physical partition; empty selects the table.
	Partition string
	// InsertIfNotExists enables atomic insertion for missing full keys.
	InsertIfNotExists bool
	// Timeout is the server timeout used only by insertion.
	Timeout time.Duration
	// Acks is the insertion acknowledgement mode.
	Acks int32
}

// LookupOption configures a [LookupClient].
type LookupOption func(*LookupConfig) error

// WithLookupBatch sets the maximum keys per request and concurrent requests.
func WithLookupBatch(keys, concurrent int) LookupOption {
	return func(config *LookupConfig) error {
		if keys <= 0 || concurrent <= 0 {
			return fmt.Errorf("%w: lookup limits must be positive", ErrInvalidConfig)
		}
		config.MaxBatchKeys, config.MaxConcurrent = keys, concurrent
		return nil
	}
}

// WithLookupPartition routes lookups to the named physical partition.
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

// WithLookupInsertIfNotExists atomically inserts a missing primary-key row before returning it.
// Fluss fills auto-increment columns and sets nullable non-key columns to null.
func WithLookupInsertIfNotExists(timeout time.Duration, acks int32) LookupOption {
	return func(config *LookupConfig) error {
		if timeout <= 0 || timeout/time.Millisecond > math.MaxInt32 ||
			(acks != 0 && acks != 1 && acks != -1) {
			return fmt.Errorf("%w: invalid insert-if-not-exists request settings", ErrInvalidConfig)
		}
		config.InsertIfNotExists, config.Timeout, config.Acks = true, timeout, acks
		return nil
	}
}

// LookupResult is the outcome associated with one requested primary key.
type LookupResult struct {
	// Key is the requested primary key.
	Key PrimaryKey
	// Row is valid only when Found is true and Err is nil.
	Row Row
	// Found distinguishes a missing row from an empty row.
	Found bool
	// Bucket is the routed bucket when routing succeeded.
	Bucket int32
	// Err is a key-local encoding, routing, request, or decoding failure.
	Err error
}

// PrefixLookupResult contains rows matching one leading primary-key prefix.
type PrefixLookupResult struct {
	// Prefix is the requested leading primary-key prefix.
	Prefix PrimaryKey
	// Rows contains all decoded matches when Err is nil.
	Rows []Row
	// Bucket is the routed bucket when routing succeeded.
	Bucket int32
	// Err is a prefix-local encoding, routing, request, or decoding failure.
	Err error
}

type lookupBackend interface {
	metadata(context.Context, PhysicalTablePath) (int64, map[int32]Node, error)
	lookup(context.Context, lookupRequest) ([][]byte, error)
	prefixLookup(context.Context, PhysicalTablePath, int32, int64, int64, [][]byte) ([][][]byte, error)
}

type clientLookupBackend struct{ client *Client }

type lookupRequest struct {
	path             PhysicalTablePath
	bucket           int32
	tableID          int64
	partitionID      int64
	keys             [][]byte
	insertIfNotExist bool
	timeout          time.Duration
	acks             int32
}

func (b clientLookupBackend) metadata(ctx context.Context, path PhysicalTablePath) (int64, map[int32]Node, error) {
	return (clientLogWriterBackend{client: b.client}).metadata(ctx, path)
}

func (b clientLookupBackend) lookup(
	ctx context.Context,
	input lookupRequest,
) ([][]byte, error) {
	request, err := fmsg.NewRequest(fmsg.APIKeyLookup, 0)
	if err != nil {
		return nil, err
	}
	message := request.Message().(*fmsg.LookupRequest)
	message.TableId = proto.Int64(input.tableID)
	if input.insertIfNotExist {
		message.InsertIfNotExists = proto.Bool(true)
		message.TimeoutMs = proto.Int32(int32(input.timeout / time.Millisecond))
		message.Acks = proto.Int32(input.acks)
	}
	bucketRequest := &fmsg.PbLookupReqForBucket{
		BucketId: proto.Int32(input.bucket),
		Keys:     cloneBytesList(input.keys),
	}
	if input.partitionID >= 0 {
		bucketRequest.PartitionId = proto.Int64(input.partitionID)
	}
	message.BucketsReq = []*fmsg.PbLookupReqForBucket{bucketRequest}
	response, err := b.client.RequestBucket(ctx, input.path, input.bucket, request)
	if err != nil {
		return nil, err
	}
	lookup, ok := response.Message().(*fmsg.LookupResponse)
	if !ok {
		return nil, fmt.Errorf("fgo: lookup: unexpected response %T", response.Message())
	}
	if len(lookup.GetBucketsResp()) != 1 || lookup.GetBucketsResp()[0].GetBucketId() != input.bucket {
		return nil, fmt.Errorf("%w: lookup response omitted bucket %d", ErrValidation, input.bucket)
	}
	result := lookup.GetBucketsResp()[0]
	if err := responseServerError(result.GetErrorCode(), result.GetErrorMessage(), fmsg.APIKeyLookup); err != nil {
		return nil, err
	}
	if len(result.GetValues()) != len(input.keys) {
		return nil, fmt.Errorf(
			"%w: lookup returned %d values for %d keys",
			ErrValidation, len(result.GetValues()), len(input.keys),
		)
	}
	values := make([][]byte, len(input.keys))
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

// LookupClient performs batched point and prefix lookups for a primary-key
// table. A LookupClient is safe for concurrent calls and must be closed.
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

// NewLookupClient creates a point and prefix lookup client for table.
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
	config, err := lookupConfig(options)
	if err != nil {
		return nil, err
	}
	if err := validateLookupInsertSchema(table.Schema, config); err != nil {
		return nil, err
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

func lookupConfig(options []LookupOption) (LookupConfig, error) {
	config := LookupConfig{
		MaxBatchKeys: 1000, MaxConcurrent: 8,
		Timeout: 30 * time.Second, Acks: -1,
	}
	for _, option := range options {
		if option == nil {
			return LookupConfig{}, fmt.Errorf("%w: nil lookup option", ErrInvalidConfig)
		}
		if err := option(&config); err != nil {
			return LookupConfig{}, err
		}
	}
	return config, nil
}

func validateLookupInsertSchema(schema Schema, config LookupConfig) error {
	if !config.InsertIfNotExists {
		return nil
	}
	for _, column := range schema.Columns {
		if !column.Nullable && !contains(schema.PrimaryKey, column.Name) &&
			!contains(schema.AutoIncrement, column.Name) {
			return fmt.Errorf(
				"%w: insert-if-not-exists cannot fill required column %q",
				ErrInvalidSchema, column.Name,
			)
		}
	}
	return nil
}

type lookupInput struct {
	index   int
	bucket  int32
	encoded []byte
}

// Lookup returns one result for each input key in input order.
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

// PrefixLookup returns one result for each leading primary-key prefix in input
// order.
func (c *LookupClient) PrefixLookup(ctx context.Context, prefixes ...PrimaryKey) []PrefixLookupResult {
	results := make([]PrefixLookupResult, len(prefixes))
	if len(prefixes) == 0 {
		return results
	}
	if c.config.InsertIfNotExists {
		for index, prefix := range prefixes {
			results[index] = PrefixLookupResult{
				Prefix: prefix,
				Err:    fmt.Errorf("%w: insert-if-not-exists does not support prefix lookup", ErrInvalidConfig),
			}
		}
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
			chunk := lookupChunk(inputs, start, c.config.MaxBatchKeys)
			wait.Add(1)
			go func(bucket int32) {
				defer wait.Done()
				c.runPointChunk(ctx, sem, bucket, chunk, results)
			}(bucket)
		}
	}
	wait.Wait()
}

func (c *LookupClient) runPointChunk(
	ctx context.Context,
	sem chan struct{},
	bucket int32,
	chunk []lookupInput,
	results []LookupResult,
) {
	if !acquireLookupSlot(ctx, sem) {
		setPointErrors(chunk, results, ctx.Err())
		return
	}
	defer func() { <-sem }()
	values, err := c.backend.lookup(ctx, lookupRequest{
		path: c.path, bucket: bucket, tableID: c.tableID, partitionID: c.partitionID,
		keys: encodedLookupInputs(chunk), insertIfNotExist: c.config.InsertIfNotExists,
		timeout: c.config.Timeout, acks: c.config.Acks,
	})
	if err != nil {
		setPointErrors(chunk, results, err)
		return
	}
	for index, value := range values {
		result := &results[chunk[index].index]
		if value == nil {
			result.Err = ErrNotFound
			continue
		}
		result.Row, result.Err = decodeLookupValue(c.table, value)
		result.Found = result.Err == nil
	}
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
			chunk := lookupChunk(inputs, start, c.config.MaxBatchKeys)
			wait.Add(1)
			go func(bucket int32) {
				defer wait.Done()
				c.runPrefixChunk(ctx, sem, bucket, chunk, results)
			}(bucket)
		}
	}
	wait.Wait()
}

func (c *LookupClient) runPrefixChunk(
	ctx context.Context,
	sem chan struct{},
	bucket int32,
	chunk []lookupInput,
	results []PrefixLookupResult,
) {
	if !acquireLookupSlot(ctx, sem) {
		setPrefixErrors(chunk, results, ctx.Err())
		return
	}
	defer func() { <-sem }()
	values, err := c.backend.prefixLookup(ctx, c.path, bucket, c.tableID, c.partitionID, encodedLookupInputs(chunk))
	if err != nil {
		setPrefixErrors(chunk, results, err)
		return
	}
	for index, rows := range values {
		result := &results[chunk[index].index]
		for _, value := range rows {
			row, err := decodeLookupValue(c.table, value)
			if err != nil {
				result.Err = err
				break
			}
			result.Rows = append(result.Rows, row)
		}
	}
}

func lookupChunk(inputs []lookupInput, start, maximum int) []lookupInput {
	end := start + maximum
	if end > len(inputs) {
		end = len(inputs)
	}
	return append([]lookupInput(nil), inputs[start:end]...)
}

func acquireLookupSlot(ctx context.Context, sem chan struct{}) bool {
	select {
	case sem <- struct{}{}:
		return true
	case <-ctx.Done():
		return false
	}
}

func encodedLookupInputs(inputs []lookupInput) [][]byte {
	encoded := make([][]byte, len(inputs))
	for index := range inputs {
		encoded[index] = inputs[index].encoded
	}
	return encoded
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

// Close cancels active requests and rejects new lookups.
// Close is idempotent.
func (c *LookupClient) Close() error {
	c.mu.Lock()
	if !c.closed {
		c.closed = true
		c.cancel()
	}
	c.mu.Unlock()
	return nil
}
