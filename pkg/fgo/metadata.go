package fgo

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

var (
	ErrUnknownTable  = errors.New("fgo: unknown table")
	ErrUnknownBucket = errors.New("fgo: unknown bucket")
)

type TablePath struct {
	Database string
	Table    string
}

func (p TablePath) String() string { return p.Database + "." + p.Table }

func (p TablePath) Validate() error {
	if p.Database == "" || p.Table == "" {
		return fmt.Errorf("%w: table path requires database and table", ErrInvalidConfig)
	}
	return nil
}

type Node struct {
	ID      int32
	Address string
}

type TableMetadata struct {
	Path    TablePath
	Buckets map[int32]Node
}

type Metadata struct {
	Coordinator Node
	Tables      map[TablePath]TableMetadata
}

type MetadataFetcher func(context.Context, TablePath) (TableMetadata, error)

type Router struct {
	mu       sync.RWMutex
	metadata Metadata
	fetch    MetadataFetcher
	flights  map[TablePath]*metadataFlight
}

type metadataFlight struct {
	done  chan struct{}
	table TableMetadata
	err   error
}

func NewRouter(coordinator Node, fetch MetadataFetcher) *Router {
	return &Router{
		metadata: Metadata{Coordinator: coordinator, Tables: make(map[TablePath]TableMetadata)},
		fetch:    fetch,
		flights:  make(map[TablePath]*metadataFlight),
	}
}

func (r *Router) Coordinator() Node {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.metadata.Coordinator
}

func (r *Router) Route(ctx context.Context, path TablePath, bucket int32) (Node, error) {
	if err := path.Validate(); err != nil {
		return Node{}, err
	}
	if node, ok := r.lookup(path, bucket); ok {
		return node, nil
	}
	if err := r.Refresh(ctx, path); err != nil {
		return Node{}, err
	}
	if node, ok := r.lookup(path, bucket); ok {
		return node, nil
	}
	return Node{}, fmt.Errorf("%w: %s bucket %d", ErrUnknownBucket, path, bucket)
}

func (r *Router) Refresh(ctx context.Context, path TablePath) error {
	if r.fetch == nil {
		return fmt.Errorf("%w: %s", ErrUnknownTable, path)
	}
	r.mu.Lock()
	if flight := r.flights[path]; flight != nil {
		r.mu.Unlock()
		select {
		case <-flight.done:
			return flight.err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	flight := &metadataFlight{done: make(chan struct{})}
	r.flights[path] = flight
	r.mu.Unlock()

	table, err := r.fetch(ctx, path)
	if err == nil && table.Path != path {
		err = fmt.Errorf("%w: refresh returned %s for %s", ErrUnknownTable, table.Path, path)
	}
	r.mu.Lock()
	if err == nil {
		next := cloneMetadata(r.metadata)
		next.Tables[path] = cloneTableMetadata(table)
		r.metadata = next
		flight.table = table
	}
	flight.err = err
	delete(r.flights, path)
	close(flight.done)
	r.mu.Unlock()
	return err
}

func (r *Router) Invalidate(path TablePath) {
	r.mu.Lock()
	next := cloneMetadata(r.metadata)
	delete(next.Tables, path)
	r.metadata = next
	r.mu.Unlock()
}

func (r *Router) lookup(path TablePath, bucket int32) (Node, bool) {
	r.mu.RLock()
	table, ok := r.metadata.Tables[path]
	if !ok {
		r.mu.RUnlock()
		return Node{}, false
	}
	node, ok := table.Buckets[bucket]
	r.mu.RUnlock()
	return node, ok
}

func cloneMetadata(metadata Metadata) Metadata {
	next := Metadata{Coordinator: metadata.Coordinator, Tables: make(map[TablePath]TableMetadata, len(metadata.Tables))}
	for path, table := range metadata.Tables {
		next.Tables[path] = cloneTableMetadata(table)
	}
	return next
}

func cloneTableMetadata(table TableMetadata) TableMetadata {
	next := TableMetadata{Path: table.Path, Buckets: make(map[int32]Node, len(table.Buckets))}
	for bucket, node := range table.Buckets {
		next.Buckets[bucket] = node
	}
	return next
}
