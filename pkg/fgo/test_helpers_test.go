package fgo

import (
	"runtime"
	"testing"
	"time"
)

func waitForTestCondition(t *testing.T, description string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", description)
		}
		runtime.Gosched()
	}
}
