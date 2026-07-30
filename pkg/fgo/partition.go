package fgo

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/pletorco/fluss-go/pkg/fmsg"
	"google.golang.org/protobuf/proto"
)

const partitionValueSeparator = "$"

// PartitionSpec maps every partition key to one value.
type PartitionSpec map[string]string

// PartitionName resolves a spec in schema partition-key order. Fluss 0.9.1 joins values with "$".
func (s Schema) PartitionName(spec PartitionSpec) (string, error) {
	if len(s.PartitionKey) == 0 {
		return "", fmt.Errorf("%w: table is not partitioned", ErrInvalidSchema)
	}
	if len(spec) != len(s.PartitionKey) {
		return "", fmt.Errorf(
			"%w: partition spec has %d values for %d keys",
			ErrInvalidConfig, len(spec), len(s.PartitionKey),
		)
	}
	values := make([]string, len(s.PartitionKey))
	for index, key := range s.PartitionKey {
		value, ok := spec[key]
		if !ok || value == "" || strings.Contains(value, partitionValueSeparator) {
			return "", fmt.Errorf("%w: invalid value for partition key %q", ErrInvalidConfig, key)
		}
		values[index] = value
	}
	return strings.Join(values, partitionValueSeparator), nil
}

// PartitionSpecFromName decodes a Fluss partition name using the ordered schema keys.
func PartitionSpecFromName(keys []string, name string) (PartitionSpec, error) {
	if len(keys) == 0 || name == "" {
		return nil, fmt.Errorf("%w: partition keys and name are required", ErrInvalidConfig)
	}
	values := strings.Split(name, partitionValueSeparator)
	if len(values) != len(keys) {
		return nil, fmt.Errorf(
			"%w: partition name has %d values for %d keys",
			ErrInvalidConfig, len(values), len(keys),
		)
	}
	spec := make(PartitionSpec, len(keys))
	for index, key := range keys {
		if key == "" || values[index] == "" {
			return nil, fmt.Errorf("%w: partition keys and values are required", ErrInvalidConfig)
		}
		if _, exists := spec[key]; exists {
			return nil, fmt.Errorf("%w: duplicate partition key %q", ErrInvalidConfig, key)
		}
		spec[key] = values[index]
	}
	return spec, nil
}

// PartitionNames resolves, deduplicates, and sorts specs for deterministic multi-partition use.
func (s Schema) PartitionNames(specs ...PartitionSpec) ([]string, error) {
	if len(specs) == 0 {
		return nil, fmt.Errorf("%w: partition specs are empty", ErrInvalidConfig)
	}
	unique := make(map[string]struct{}, len(specs))
	for _, spec := range specs {
		name, err := s.PartitionName(spec)
		if err != nil {
			return nil, err
		}
		unique[name] = struct{}{}
	}
	names := make([]string, 0, len(unique))
	for name := range unique {
		names = append(names, name)
	}
	sort.Strings(names)
	return names, nil
}

// DynamicPartitionCreationConfig controls opt-in creation of missing writer partitions.
type DynamicPartitionCreationConfig struct {
	MetadataAttempts int
	RetryBackoff     time.Duration
}

// WithDynamicPartitionCreation enables automatic creation for partitioned log and KV writers.
func WithDynamicPartitionCreation(settings DynamicPartitionCreationConfig) Option {
	return func(config *config) error {
		if settings.MetadataAttempts == 0 {
			settings.MetadataAttempts = 3
		}
		if settings.RetryBackoff == 0 {
			settings.RetryBackoff = 25 * time.Millisecond
		}
		if settings.MetadataAttempts < 1 || settings.MetadataAttempts > 10 ||
			settings.RetryBackoff < 0 || settings.RetryBackoff > time.Minute {
			return fmt.Errorf("%w: invalid dynamic partition creation settings", ErrInvalidConfig)
		}
		config.dynamicPartitions = &settings
		return nil
	}
}

type partitionCreationBackend interface {
	checkPartition(context.Context, PhysicalTablePath) error
	createPartition(context.Context, TablePath, PartitionSpec) error
}

type partitionCreationFlight struct {
	done chan struct{}
	err  error
}

type dynamicPartitionCreator struct {
	backend  partitionCreationBackend
	settings DynamicPartitionCreationConfig

	mu      sync.Mutex
	flights map[string]*partitionCreationFlight
}

func newDynamicPartitionCreator(
	backend partitionCreationBackend,
	settings DynamicPartitionCreationConfig,
) *dynamicPartitionCreator {
	return &dynamicPartitionCreator{
		backend: backend, settings: settings, flights: make(map[string]*partitionCreationFlight),
	}
}

func (c *dynamicPartitionCreator) Ensure(
	ctx context.Context,
	path PhysicalTablePath,
	partitionKeys []string,
) error {
	if c == nil {
		return nil
	}
	if err := path.Validate(); err != nil {
		return err
	}
	if path.Partition == "" {
		return nil
	}
	spec, err := PartitionSpecFromName(partitionKeys, path.Partition)
	if err != nil {
		return err
	}
	key := physicalTableKey(path)
	c.mu.Lock()
	if flight := c.flights[key]; flight != nil {
		c.mu.Unlock()
		select {
		case <-flight.done:
			return flight.err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	flight := &partitionCreationFlight{done: make(chan struct{})}
	c.flights[key] = flight
	c.mu.Unlock()

	err = c.ensure(ctx, path, spec)
	c.mu.Lock()
	flight.err = err
	delete(c.flights, key)
	close(flight.done)
	c.mu.Unlock()
	return err
}

func (c *dynamicPartitionCreator) ensure(
	ctx context.Context,
	path PhysicalTablePath,
	spec PartitionSpec,
) error {
	err := c.backend.checkPartition(ctx, path)
	if err == nil {
		return nil
	}
	missing := errors.Is(err, ErrUnknownPartition)
	if !missing && !pendingPartitionMetadata(err) {
		return err
	}
	if missing {
		if err := c.backend.createPartition(ctx, path.TablePath, spec); err != nil {
			return fmt.Errorf("fgo: create dynamic partition %s: %w", path, err)
		}
	}
	for attempt := 1; attempt <= c.settings.MetadataAttempts; attempt++ {
		err = c.backend.checkPartition(ctx, path)
		if err == nil {
			return nil
		}
		if !pendingPartitionMetadata(err) || attempt == c.settings.MetadataAttempts {
			return err
		}
		if err := waitContext(ctx, c.retryDelay(attempt)); err != nil {
			return err
		}
	}
	return err
}

func pendingPartitionMetadata(err error) bool {
	return errors.Is(err, ErrUnknownPartition) ||
		errors.Is(err, ErrNoBucketLeader) ||
		errors.Is(err, ErrMetadata)
}

func (c *dynamicPartitionCreator) retryDelay(attempt int) time.Duration {
	delay := c.settings.RetryBackoff
	for current := 1; current < attempt && delay < time.Second; current++ {
		delay *= 2
	}
	if delay > time.Second {
		return time.Second
	}
	return delay
}

func waitContext(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *Client) checkPartition(ctx context.Context, path PhysicalTablePath) error {
	if c.router == nil {
		_, err := c.fetchPartitionMetadata(ctx, path)
		return err
	}
	c.router.InvalidatePhysical(path)
	return c.router.RefreshPhysical(ctx, path)
}

func (c *Client) createPartition(ctx context.Context, path TablePath, spec PartitionSpec) error {
	request, err := fmsg.NewRequest(fmsg.APIKeyCreatePartition, 0)
	if err != nil {
		return err
	}
	message := request.Message().(*fmsg.CreatePartitionRequest)
	message.TablePath = pbTablePath(path)
	message.PartitionSpec = partitionSpecProto(spec)
	message.IgnoreIfNotExists = proto.Bool(true)
	_, err = c.RequestCoordinator(ctx, request)
	return err
}

func partitionSpecProto(spec PartitionSpec) *fmsg.PbPartitionSpec {
	keys := make([]string, 0, len(spec))
	for key := range spec {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	result := &fmsg.PbPartitionSpec{}
	for _, key := range keys {
		result.PartitionKeyValues = append(result.PartitionKeyValues, &fmsg.PbKeyValue{
			Key: proto.String(key), Value: proto.String(spec[key]),
		})
	}
	return result
}

func (c *Client) ensureDynamicPartition(
	ctx context.Context,
	path PhysicalTablePath,
	partitionKeys []string,
) error {
	return c.partitionCreator.Ensure(ctx, path, partitionKeys)
}
