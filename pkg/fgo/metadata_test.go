package fgo

import (
	"context"
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
