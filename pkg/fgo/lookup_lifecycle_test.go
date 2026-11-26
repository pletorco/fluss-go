package fgo

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

type lookupDecodeBackend struct {
	*fakeLookupBackend
	resolver schemaResolver
}

func (b lookupDecodeBackend) schemaResolver() schemaResolver { return b.resolver }

type lookupDecodeOutcome struct {
	rows []Row
	err  error
}

func runLookupDecode(ctx context.Context, lookup *Lookuper, prefix bool) lookupDecodeOutcome {
	if prefix {
		result := lookup.PrefixLookup(ctx, PrimaryKey{"tenant"})[0]
		return lookupDecodeOutcome{rows: result.Rows, err: result.Err}
	}
	var rows []Row
	for _, result := range lookup.Lookup(ctx, PrimaryKey{"tenant", int32(1)}, PrimaryKey{"tenant", int32(2)}) {
		if result.Err != nil {
			return lookupDecodeOutcome{rows: rows, err: result.Err}
		}
		rows = append(rows, result.Row)
	}
	return lookupDecodeOutcome{rows: rows}
}

func newLookupDecodeFixture(t *testing.T, resolver schemaResolver) *Lookuper {
	t.Helper()
	table := lookupTable()
	historical := table
	historical.SchemaID--
	backend := lookupBackendFor(table, 0, 1)
	first, second := Row{"tenant", int32(1), "cached"}, Row{"tenant", int32(2), "historical"}
	putLookupValue(t, backend, table, PrimaryKey{"tenant", int32(1)}, first)
	putLookupValue(t, backend, historical, PrimaryKey{"tenant", int32(2)}, second)
	encoded, err := EncodePrefixLookupKey(table.Schema, PrimaryKey{"tenant"}, 1)
	if err != nil {
		t.Fatal(err)
	}
	backend.prefixes[string(encoded)] = [][]byte{
		encodeLookupValue(t, table, first), encodeLookupValue(t, historical, second),
	}
	lookup, err := newLookuper(t.Context(), lookupDecodeBackend{backend, resolver}, table, WithLookupQueue(8, 0))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lookup.Close() })
	return lookup
}

func TestLookupCloseInterruptsSchemaWait(t *testing.T) {
	for _, prefix := range []bool{false, true} {
		for _, shared := range []bool{false, true} {
			t.Run(fmt.Sprintf("prefix=%t/shared=%t", prefix, shared), func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					testLookupCloseInterruptsSchemaWait(t, prefix, shared)
				})
			})
		}
	}
}

func testLookupCloseInterruptsSchemaWait(t *testing.T, prefix, shared bool) {
	t.Helper()
	table := lookupTable()
	started, release := make(chan struct{}), make(chan struct{})
	canceled := make(chan struct{}, 2)
	var fetches atomic.Int32
	cache := newSchemaCache(2, func(ctx context.Context, _ TablePath, _ int64, _ int32) (Schema, error) {
		if fetches.Add(1) == 1 {
			close(started)
		}
		select {
		case <-release:
			return table.Schema, nil
		case <-ctx.Done():
			canceled <- struct{}{}
			return Schema{}, ctx.Err()
		}
	})
	defer cache.close()
	cache.store(table.Path, table.ID, table.SchemaID, table.Schema)
	lookup := newLookupDecodeFixture(t, cache)
	result := make(chan lookupDecodeOutcome, 1)
	go func() { result <- runLookupDecode(t.Context(), lookup, prefix) }()
	<-started

	var peerResult chan lookupDecodeOutcome
	if shared {
		peer := newLookupDecodeFixture(t, cache)
		peerResult = make(chan lookupDecodeOutcome, 1)
		go func() { peerResult <- runLookupDecode(t.Context(), peer, !prefix) }()
		synctest.Wait()
		key := schemaCacheKey{path: table.Path, tableID: table.ID, id: table.SchemaID - 1}
		cache.flights.mu.Lock()
		call := cache.flights.calls[key]
		waiters := 0
		if call != nil {
			waiters = call.waiters
		}
		cache.flights.mu.Unlock()
		if waiters != 2 {
			t.Fatalf("shared schema lookup has %d waiters, want 2", waiters)
		}
	}
	if err := lookup.Close(); err != nil {
		t.Fatal(err)
	}
	synctest.Wait()
	select {
	case got := <-result:
		if !errors.Is(got.err, ErrClosed) || !reflect.DeepEqual(got.rows, []Row{{"tenant", int32(1), "cached"}}) {
			t.Fatalf("close result = %#v, want decoded first row and ErrClosed", got)
		}
	default:
		t.Fatal("Close left a lookup blocked during schema resolution")
	}
	if !shared {
		select {
		case <-canceled:
		default:
			t.Fatal("the schema fetch outlived its last waiter")
		}
		return
	}
	select {
	case <-canceled:
		t.Fatal("closing one lookuper canceled the other lookuper's schema fetch")
	default:
	}
	close(release)
	got := <-peerResult
	if want := []Row{{"tenant", int32(1), "cached"}, {"tenant", int32(2), "historical"}}; got.err != nil || !reflect.DeepEqual(got.rows, want) || fetches.Load() != 1 {
		t.Fatalf("other lookuper result = %#v, fetches = %d", got, fetches.Load())
	}
}

func TestLookupSchemaWaitPreservesCallerDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		table := lookupTable()
		cache := newSchemaCache(2, func(ctx context.Context, _ TablePath, _ int64, _ int32) (Schema, error) {
			<-ctx.Done()
			return Schema{}, ctx.Err()
		})
		defer cache.close()
		cache.store(table.Path, table.ID, table.SchemaID, table.Schema)
		lookup := newLookupDecodeFixture(t, cache)
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		if got := runLookupDecode(ctx, lookup, false); !errors.Is(got.err, context.DeadlineExceeded) || len(got.rows) != 1 {
			t.Fatalf("deadline result = %#v, want the decoded row and DeadlineExceeded", got)
		}
	})
}

type lookupSchemaResolverFunc func(context.Context, TablePath, int64, int32) (Schema, error)

func (f lookupSchemaResolverFunc) resolveSchema(ctx context.Context, path TablePath, tableID int64, schemaID int32) (Schema, error) {
	return f(ctx, path, tableID, schemaID)
}

func TestLookupDoesNotReturnRowDecodedAfterClose(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		table := lookupTable()
		started, release := make(chan struct{}), make(chan struct{})
		lookup := newLookupDecodeFixture(t, lookupSchemaResolverFunc(func(_ context.Context, _ TablePath, _ int64, schemaID int32) (Schema, error) {
			if schemaID != table.SchemaID {
				close(started)
				<-release
			}
			return table.Schema, nil // A synchronous decoder can succeed without observing cancellation.
		}))
		result := make(chan lookupDecodeOutcome, 1)
		go func() { result <- runLookupDecode(t.Context(), lookup, true) }()
		<-started
		err := lookup.Close()
		close(release)
		if err != nil {
			t.Fatal(err)
		}
		if got := <-result; !errors.Is(got.err, ErrClosed) || !reflect.DeepEqual(got.rows, []Row{{"tenant", int32(1), "cached"}}) {
			t.Fatalf("decode completed during close = %#v", got)
		}
	})
}
