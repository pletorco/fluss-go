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
	started := make(chan struct{})
	var startOnce sync.Once
	router := NewRouter(ServerNode{ID: 1}, func(context.Context, TablePath) (TableMetadata, error) {
		calls.Add(1)
		startOnce.Do(func() { close(started) })
		<-release
		return TableMetadata{Path: path, Buckets: map[int32]ServerNode{3: {ID: 8, Address: "tablet:9123"}}}, nil
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
	<-started
	waitForTestCondition(t, "all refresh waiters", func() bool {
		router.flights.mu.Lock()
		defer router.flights.mu.Unlock()
		call := router.flights.calls[path]
		return call != nil && call.waiters == 8
	})
	close(release)
	wait.Wait()
	if got, want := calls.Load(), int32(1); got != want {
		t.Fatalf("refresh calls = %d, want %d", got, want)
	}
}

func TestRouterInvalidationRefreshesLeader(t *testing.T) {
	path := TablePath{Database: "db", Table: "events"}
	leader := atomic.Int32{}
	router := NewRouter(ServerNode{}, func(context.Context, TablePath) (TableMetadata, error) {
		return TableMetadata{Path: path, Buckets: map[int32]ServerNode{0: {ID: leader.Add(1)}}}, nil
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
	path := PhysicalTablePath{TablePath: table, Partition: "day=2026-07-30"}
	var tableCalls, partitionCalls atomic.Int32
	release := make(chan struct{})
	started := make(chan struct{})
	var startOnce sync.Once
	router := NewRouter(ServerNode{ID: 1, ServerType: Coordinator}, func(context.Context, TablePath) (TableMetadata, error) {
		tableCalls.Add(1)
		return TableMetadata{Path: table}, nil
	}).WithPhysicalMetadataFetcher(func(context.Context, PhysicalTablePath) (PartitionMetadata, error) {
		partitionCalls.Add(1)
		startOnce.Do(func() { close(started) })
		<-release
		return PartitionMetadata{Path: path, ID: 11, Buckets: map[int32]ServerNode{2: {ID: 4, Address: "tablet:9123", ServerType: TabletServer}}}, nil
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
	<-started
	waitForTestCondition(t, "all partition refresh waiters", func() bool {
		router.partitionFlights.mu.Lock()
		defer router.partitionFlights.mu.Unlock()
		call := router.partitionFlights.calls[physicalTableKey(path)]
		return call != nil && call.waiters == 6
	})
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
	missing := NewRouter(ServerNode{}, nil)
	if _, err := missing.Route(context.Background(), table, 0); !errors.Is(err, ErrUnknownTable) {
		t.Fatalf("Route() error = %v, want unknown table", err)
	}

	path := PhysicalTablePath{TablePath: TablePath{Database: "db", Table: "events"}, Partition: "day=x"}
	router := NewRouter(ServerNode{}, func(context.Context, TablePath) (TableMetadata, error) {
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
	router := NewRouter(ServerNode{}, func(context.Context, TablePath) (TableMetadata, error) {
		calls.Add(1)
		return TableMetadata{Path: path, Buckets: map[int32]ServerNode{0: {ID: leader.Add(1), Address: "tablet:9123", ServerType: TabletServer}}}, nil
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
	started := make(chan struct{})
	router := NewRouter(ServerNode{}, func(context.Context, TablePath) (TableMetadata, error) {
		close(started)
		<-release
		return TableMetadata{Path: path}, nil
	})
	go func() { _ = router.Refresh(context.Background(), path) }()
	<-started
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := router.Refresh(ctx, path); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Refresh() error = %v, want deadline exceeded", err)
	}
	close(release)
}

func TestRouterAppliesServerSnapshot(t *testing.T) {
	path := TablePath{Database: "db", Table: "events"}
	router := NewRouter(ServerNode{}, func(context.Context, TablePath) (TableMetadata, error) {
		return TableMetadata{
			Path: path, coordinator: ServerNode{ID: 1, Address: "coordinator:9123", ServerType: Coordinator},
			tablets: map[int32]ServerNode{2: {ID: 2, Address: "tablet:9123", ServerType: TabletServer}},
		}, nil
	})
	if err := router.Refresh(context.Background(), path); err != nil {
		t.Fatal(err)
	}
	if got := router.Coordinator(); got.Address != "coordinator:9123" {
		t.Fatalf("Coordinator() = %#v", got)
	}
	router.InvalidatePhysical(PhysicalTablePath{TablePath: path})
}
