package main

import (
	"errors"
	"os"
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

func TestRunAndMain(t *testing.T) {
	output := filepath.Join(t.TempDir(), "nested", "api_keys_gen.go")
	inputs := filepath.Join("..", "..", "third_party", "apache-fluss")
	if err := run([]string{"-inputs", inputs, "-output", output}, "", ""); err != nil {
		t.Fatalf("run() error = %v", err)
	}
	if _, err := os.Stat(output); err != nil {
		t.Fatalf("generated output: %v", err)
	}
	if err := run([]string{"-unknown"}, inputs, output); err == nil {
		t.Fatal("run() unknown flag error = nil")
	}
}

func TestFatalUsesExitHook(t *testing.T) {
	original := exit
	defer func() { exit = original }()
	code := 0
	exit = func(value int) { code = value }
	fatal(errors.New("expected"))
	if code != 1 {
		t.Fatalf("exit code = %d, want 1", code)
	}
}
