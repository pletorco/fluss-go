package fgo

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestRouterCoalescesConcurrentRefresh(t *testing.T) {
	path := TablePath{Database: "db", Table: "events"}
	var calls atomic.Int32
	release := make(chan struct{})
	router := NewRouter(Node{ID: 1}, func(context.Context, TablePath) (TableMetadata, error) {
		calls.Add(1)
		<-release
		return TableMetadata{Path: path, Buckets: map[int32]Node{3: {ID: 8, Address: "tablet:9123"}}}, nil
	})
	var wait sync.WaitGroup
	ready := make(chan struct{}, 8)
	start := make(chan struct{})
	for range 8 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			ready <- struct{}{}
			<-start
			node, err := router.Route(context.Background(), path, 3)
			if err != nil || node.ID != 8 {
				t.Errorf("Route() = %#v, %v", node, err)
			}
		}()
	}
	for range 8 {
		<-ready
	}
	close(start)
	for calls.Load() == 0 {
	}
	time.Sleep(10 * time.Millisecond)
	close(release)
	wait.Wait()
	if got, want := calls.Load(), int32(1); got != want {
		t.Fatalf("refresh calls = %d, want %d", got, want)
	}
}

func TestRouterInvalidationRefreshesLeader(t *testing.T) {
	path := TablePath{Database: "db", Table: "events"}
	leader := atomic.Int32{}
	router := NewRouter(Node{}, func(context.Context, TablePath) (TableMetadata, error) {
		return TableMetadata{Path: path, Buckets: map[int32]Node{0: {ID: leader.Add(1)}}}, nil
	})
	first, err := router.Route(context.Background(), path, 0)
	if err != nil {
		t.Fatal(err)
	}
	router.Invalidate(path)
	second, err := router.Route(context.Background(), path, 0)
	if err != nil {
		t.Fatal(err)
	}
	if first.ID == second.ID {
		t.Fatal("invalidation did not refresh leader")
	}
}

func TestRouterRoutesPhysicalPartitionAndCoalescesRefresh(t *testing.T) {
	table := TablePath{Database: "db", Table: "events"}
	path := PhysicalTablePath{TablePath: table, Partition: map[string]string{"day": "2026-07-30"}}
	var tableCalls, partitionCalls atomic.Int32
	release := make(chan struct{})
	router := NewRouter(Node{ID: 1, Role: Coordinator}, func(context.Context, TablePath) (TableMetadata, error) {
		tableCalls.Add(1)
		return TableMetadata{Path: table}, nil
	}).WithPhysicalMetadataFetcher(func(context.Context, PhysicalTablePath) (PartitionMetadata, error) {
		partitionCalls.Add(1)
		<-release
		return PartitionMetadata{Path: path, ID: 11, Buckets: map[int32]Node{2: {ID: 4, Address: "tablet:9123", Role: TabletServer}}}, nil
	})

	var wait sync.WaitGroup
	for range 6 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			node, err := router.RoutePhysical(context.Background(), path, 2)
			if err != nil || node.ID != 4 {
				t.Errorf("RoutePhysical() = %#v, %v", node, err)
			}
		}()
	}
	for partitionCalls.Load() == 0 {
		time.Sleep(time.Millisecond)
	}
	close(release)
	wait.Wait()
	if got := partitionCalls.Load(); got != 1 {
		t.Fatalf("partition refreshes = %d, want 1", got)
	}
	if got := tableCalls.Load(); got != 1 {
		t.Fatalf("table refreshes = %d, want 1", got)
	}
}

func TestRouterPhysicalAndTableFailuresAreTyped(t *testing.T) {
	table := TablePath{Database: "db", Table: "missing"}
	missing := NewRouter(Node{}, nil)
	if _, err := missing.Route(context.Background(), table, 0); !errors.Is(err, ErrUnknownTable) {
		t.Fatalf("Route() error = %v, want unknown table", err)
	}

	path := PhysicalTablePath{TablePath: TablePath{Database: "db", Table: "events"}, Partition: map[string]string{"day": "x"}}
	router := NewRouter(Node{}, func(context.Context, TablePath) (TableMetadata, error) {
		return TableMetadata{Path: path.TablePath}, nil
	})
	if _, err := router.RoutePhysical(context.Background(), path, 0); !errors.Is(err, ErrUnknownPartition) {
		t.Fatalf("RoutePhysical() error = %v, want unknown partition", err)
	}
	if _, err := router.Route(context.Background(), path.TablePath, 5); !errors.Is(err, ErrUnknownBucket) {
		t.Fatalf("Route() error = %v, want unknown bucket", err)
	}
}

func TestRouterMetadataErrorRefreshesOnce(t *testing.T) {
	path := TablePath{Database: "db", Table: "events"}
	var leader atomic.Int32
	var calls atomic.Int32
	router := NewRouter(Node{}, func(context.Context, TablePath) (TableMetadata, error) {
		calls.Add(1)
		return TableMetadata{Path: path, Buckets: map[int32]Node{0: {ID: leader.Add(1), Address: "tablet:9123", Role: TabletServer}}}, nil
	})
	if _, err := router.Route(context.Background(), path, 0); err != nil {
		t.Fatal(err)
	}
	node, err := router.RouteAfterMetadataError(context.Background(), path, 0, ErrMetadata)
	if err != nil || node.ID != 2 {
		t.Fatalf("RouteAfterMetadataError() = %#v, %v", node, err)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("refreshes = %d, want 2", got)
	}
	if _, err := router.RouteAfterMetadataError(context.Background(), path, 0, context.Canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("non-metadata error = %v", err)
	}
}

func TestRouterRefreshHonorsWaitingContext(t *testing.T) {
	path := TablePath{Database: "db", Table: "events"}
	release := make(chan struct{})
	router := NewRouter(Node{}, func(context.Context, TablePath) (TableMetadata, error) {
		<-release
		return TableMetadata{Path: path}, nil
	})
	go func() { _ = router.Refresh(context.Background(), path) }()
	for {
		router.mu.RLock()
		inFlight := router.flights[path] != nil
		router.mu.RUnlock()
		if inFlight {
			break
		}
		time.Sleep(time.Millisecond)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := router.Refresh(ctx, path); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Refresh() error = %v, want deadline exceeded", err)
	}
	close(release)
}
