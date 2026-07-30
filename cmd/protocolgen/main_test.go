package main

import (
	"path/filepath"
	"testing"
)

func TestLoadAPIsFromPinnedInputs(t *testing.T) {
	inputs := filepath.Join("..", "..", "third_party", "apache-fluss")
	apis, err := loadAPIs(inputs)
	if err != nil {
		t.Fatalf("loadAPIs() error = %v", err)
	}
	if got, want := len(apis), 60; got != want {
		t.Fatalf("len(apis) = %d, want %d", got, want)
	}
	for index, api := range apis {
		if got, want := api.key, 1000+index; got != want {
			t.Fatalf("apis[%d].key = %d, want %d", index, got, want)
		}
	}
	if got, want := apis[16].max, 1; got != want {
		t.Fatalf("PUT_KV max version = %d, want %d", got, want)
	}
	if got, want := apis[17].max, 1; got != want {
		t.Fatalf("LOOKUP max version = %d, want %d", got, want)
	}
	if got, want := apis[34].max, 1; got != want {
		t.Fatalf("PREFIX_LOOKUP max version = %d, want %d", got, want)
	}
}

func TestGenerateIsDeterministic(t *testing.T) {
	apis, err := loadAPIs(filepath.Join("..", "..", "third_party", "apache-fluss"))
	if err != nil {
		t.Fatalf("loadAPIs() error = %v", err)
	}
	first, err := generate(apis)
	if err != nil {
		t.Fatalf("first generate() error = %v", err)
	}
	second, err := generate(apis)
	if err != nil {
		t.Fatalf("second generate() error = %v", err)
	}
	if string(first) != string(second) {
		t.Fatal("generate() was not deterministic")
	}
}
