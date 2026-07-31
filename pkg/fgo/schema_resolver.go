package fgo

import (
	"context"
	"fmt"
	"reflect"
	"sync"
)

const defaultSchemaCacheEntries = 256

type schemaResolver interface {
	resolveSchema(context.Context, TablePath, int32) (Schema, error)
}

type schemaResolverProvider interface {
	schemaResolver() schemaResolver
}

type fixedSchemaResolver struct {
	path     TablePath
	schemaID int32
	schema   Schema
}

func (r fixedSchemaResolver) resolveSchema(
	_ context.Context,
	path TablePath,
	schemaID int32,
) (Schema, error) {
	if path != r.path || schemaID != r.schemaID {
		return Schema{}, fmt.Errorf(
			"%w: schema ID %d is unavailable for %s",
			ErrInvalidSchema, schemaID, path,
		)
	}
	return r.schema, nil
}

func resolverFor(source any, table Table) schemaResolver {
	if provider, ok := source.(schemaResolverProvider); ok {
		if resolver := provider.schemaResolver(); resolver != nil {
			if client, ok := resolver.(*Client); ok && client.schemas != nil {
				client.schemas.store(table.Path, table.SchemaID, table.Schema)
			}
			return resolver
		}
	}
	return fixedSchemaResolver{path: table.Path, schemaID: table.SchemaID, schema: table.Schema}
}

type schemaCacheKey struct {
	path TablePath
	id   int32
}

type schemaCacheEntry struct {
	schema Schema
	used   uint64
}

type schemaFetch struct {
	done      chan struct{}
	cancel    context.CancelFunc
	schema    Schema
	err       error
	waiters   int
	completed bool
}

type schemaCache struct {
	fetch func(context.Context, TablePath, int32) (Schema, error)
	max   int

	mu      sync.Mutex
	entries map[schemaCacheKey]schemaCacheEntry
	fetches map[schemaCacheKey]*schemaFetch
	clock   uint64
	closed  bool
}

func newSchemaCache(
	maxEntries int,
	fetch func(context.Context, TablePath, int32) (Schema, error),
) *schemaCache {
	return &schemaCache{
		fetch: fetch, max: maxEntries,
		entries: make(map[schemaCacheKey]schemaCacheEntry),
		fetches: make(map[schemaCacheKey]*schemaFetch),
	}
}

func (c *schemaCache) resolveSchema(
	ctx context.Context,
	path TablePath,
	schemaID int32,
) (Schema, error) {
	if ctx == nil {
		return Schema{}, fmt.Errorf("%w: nil schema lookup context", ErrInvalidConfig)
	}
	if err := path.Validate(); err != nil {
		return Schema{}, err
	}
	if schemaID < 0 {
		return Schema{}, fmt.Errorf("%w: negative schema ID %d", ErrInvalidSchema, schemaID)
	}
	key := schemaCacheKey{path: path, id: schemaID}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return Schema{}, ErrClosed
	}
	if entry, ok := c.entries[key]; ok {
		c.clock++
		entry.used = c.clock
		c.entries[key] = entry
		c.mu.Unlock()
		return entry.schema, nil
	}
	fetch := c.fetches[key]
	if fetch == nil {
		if c.fetch == nil {
			c.mu.Unlock()
			return Schema{}, fmt.Errorf("%w: schema resolver is unavailable", ErrInvalidConfig)
		}
		fetchCtx, cancel := context.WithCancel(context.Background())
		fetch = &schemaFetch{done: make(chan struct{}), cancel: cancel}
		c.fetches[key] = fetch
		go c.runFetch(fetchCtx, key, fetch)
	}
	fetch.waiters++
	c.mu.Unlock()

	select {
	case <-fetch.done:
		c.releaseFetch(key, fetch)
		return fetch.schema, fetch.err
	case <-ctx.Done():
		c.releaseFetch(key, fetch)
		return Schema{}, ctx.Err()
	}
}

func (c *schemaCache) runFetch(ctx context.Context, key schemaCacheKey, fetch *schemaFetch) {
	schema, err := c.fetch(ctx, key.path, key.id)
	fetch.cancel()
	if err == nil {
		err = schema.Validate()
	}
	c.mu.Lock()
	fetch.schema, fetch.err, fetch.completed = schema, err, true
	current := c.fetches[key] == fetch
	if err == nil && !c.closed && current {
		c.clock++
		c.entries[key] = schemaCacheEntry{schema: schema, used: c.clock}
		c.evictLocked()
	}
	if current {
		delete(c.fetches, key)
	}
	close(fetch.done)
	c.mu.Unlock()
}

func (c *schemaCache) releaseFetch(key schemaCacheKey, fetch *schemaFetch) {
	c.mu.Lock()
	fetch.waiters--
	if fetch.waiters == 0 && !fetch.completed {
		if c.fetches[key] == fetch {
			delete(c.fetches, key)
		}
		fetch.cancel()
	}
	c.mu.Unlock()
}

func (c *schemaCache) store(path TablePath, schemaID int32, schema Schema) {
	if schemaID < 0 || path.Validate() != nil || schema.Validate() != nil {
		return
	}
	key := schemaCacheKey{path: path, id: schemaID}
	c.mu.Lock()
	if !c.closed {
		c.clock++
		c.entries[key] = schemaCacheEntry{schema: schema, used: c.clock}
		c.evictLocked()
	}
	c.mu.Unlock()
}

func (c *schemaCache) evictLocked() {
	for len(c.entries) > c.max {
		var oldest schemaCacheKey
		var oldestUse uint64
		first := true
		for key, entry := range c.entries {
			if first || entry.used < oldestUse {
				oldest, oldestUse, first = key, entry.used, false
			}
		}
		delete(c.entries, oldest)
	}
}

func (c *schemaCache) close() {
	c.mu.Lock()
	if !c.closed {
		c.closed = true
		for _, fetch := range c.fetches {
			fetch.cancel()
		}
	}
	c.mu.Unlock()
}

func evolveRow(source, target Schema, row Row) (Row, error) {
	if len(row) != len(source.Columns) {
		return nil, fmt.Errorf(
			"%w: source row has %d values for %d columns",
			ErrMalformedRow, len(row), len(source.Columns),
		)
	}
	mapping, err := schemaColumnMapping(source, target)
	if err != nil {
		return nil, err
	}
	result := make(Row, len(target.Columns))
	for targetIndex, targetColumn := range target.Columns {
		sourceIndex := mapping[targetIndex]
		if sourceIndex < 0 {
			continue
		}
		if row[sourceIndex] == nil && !targetColumn.Nullable {
			return nil, fmt.Errorf("%w: column %q evolved to NOT NULL", ErrInvalidSchema, targetColumn.Name)
		}
		result[targetIndex] = row[sourceIndex]
	}
	return result, nil
}

func compatibleColumnType(source, target Column) bool {
	sourceType := logicalTypeForColumn(source)
	targetType := logicalTypeForColumn(target)
	sourceType.Nullable = false
	targetType.Nullable = false
	return reflect.DeepEqual(sourceType, targetType)
}

func schemaColumnMapping(source, target Schema) ([]int, error) {
	sourceByID := make(map[int]int, len(source.Columns))
	sourceByName := make(map[string]int, len(source.Columns))
	for index, column := range source.Columns {
		sourceByID[column.ID] = index
		sourceByName[column.Name] = index
	}
	stableIDs := !source.hasUnassignedFieldIDs() && !target.hasUnassignedFieldIDs()
	mapping := make([]int, len(target.Columns))
	for targetIndex, targetColumn := range target.Columns {
		sourceIndex, found := sourceByName[targetColumn.Name]
		if stableIDs {
			sourceIndex, found = sourceByID[targetColumn.ID]
		}
		if !found {
			if !targetColumn.Nullable {
				return nil, fmt.Errorf(
					"%w: required column %q does not exist in writer schema",
					ErrInvalidSchema, targetColumn.Name,
				)
			}
			mapping[targetIndex] = -1
			continue
		}
		if !compatibleColumnType(source.Columns[sourceIndex], targetColumn) {
			return nil, fmt.Errorf(
				"%w: column %q changed from %s to %s",
				ErrInvalidSchema, targetColumn.Name,
				source.Columns[sourceIndex].Type, targetColumn.Type,
			)
		}
		mapping[targetIndex] = sourceIndex
	}
	return mapping, nil
}
