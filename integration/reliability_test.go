//go:build integration

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pletorco/fluss-go/pkg/fadm"
	"github.com/pletorco/fluss-go/pkg/fgo"
)

const (
	reliabilityDefaultSeed = int64(104091)
	maxReliabilityDuration = 30 * time.Minute
	reliabilitySeedRows    = int32(8)
)

type reliabilityConfig struct {
	Profile            string        `json:"profile"`
	Duration           time.Duration `json:"-"`
	Seed               int64         `json:"seed"`
	MaxOperations      int64         `json:"max_operations"`
	Workers            int           `json:"workers"`
	MaxGoroutineGrowth int           `json:"max_goroutine_growth"`
	MaxHeapGrowth      uint64        `json:"max_heap_growth_bytes"`
	ReportPath         string        `json:"-"`
}

type reliabilityReport struct {
	Version               int                         `json:"version"`
	Profile               string                      `json:"profile"`
	Seed                  int64                       `json:"seed"`
	ConfiguredDuration    string                      `json:"configured_duration"`
	Elapsed               string                      `json:"elapsed"`
	MaxOperations         int64                       `json:"max_operations"`
	Workers               int                         `json:"workers"`
	Operations            map[string]int64            `json:"operations"`
	ExpectedErrors        map[string]int64            `json:"expected_errors"`
	Latencies             map[string]reliabilityStats `json:"latencies"`
	ThroughputPerSecond   float64                     `json:"throughput_per_second"`
	GoroutinesBefore      int                         `json:"goroutines_before"`
	GoroutinesAfter       int                         `json:"goroutines_after"`
	GoroutineGrowth       int                         `json:"goroutine_growth"`
	HeapBeforeBytes       uint64                      `json:"heap_before_bytes"`
	HeapAfterBytes        uint64                      `json:"heap_after_bytes"`
	HeapGrowthBytes       int64                       `json:"heap_growth_bytes"`
	AcknowledgedLogRows   int                         `json:"acknowledged_log_rows"`
	VerifiedLogRows       int                         `json:"verified_log_rows"`
	ObservedLogRows       int                         `json:"observed_log_rows"`
	UnacknowledgedLogRows int                         `json:"unacknowledged_log_rows"`
	AcknowledgedKVRows    int                         `json:"acknowledged_kv_rows"`
	VerifiedKVRows        int                         `json:"verified_kv_rows"`
	Faults                []string                    `json:"faults"`
	FinalDataCorrect      bool                        `json:"final_data_correct"`
	ResourceBoundsCorrect bool                        `json:"resource_bounds_correct"`
	Failure               string                      `json:"failure,omitempty"`
}

type reliabilityStats struct {
	Count int64  `json:"count"`
	P50   string `json:"p50"`
	P95   string `json:"p95"`
	P99   string `json:"p99"`
	Max   string `json:"max"`
}

type reliabilityMetrics struct {
	mu              sync.Mutex
	allowTransient  bool
	latencies       map[string][]time.Duration
	operations      map[string]int64
	expectedErrors  map[string]int64
	logRows         map[int32]string
	kvRows          map[string]fgo.Row
	faults          []string
	unexpectedError error
}

type reliabilityEnvironment struct {
	address           string
	database          string
	logPath, kvPath   fgo.TablePath
	logTable, kvTable fgo.Table
	client            *fgo.Client
	admin             *fadm.Client
}

type reliabilityResources struct {
	appendWriter *fgo.AppendWriter
	upsertWriter *fgo.UpsertWriter
	lookup       *fgo.Lookuper
}

type reliabilityWorker struct {
	ctx            context.Context
	operation      string
	random         *rand.Rand
	sequence       *atomic.Int64
	operationLimit int64
	client         *fgo.Client
	logTable       fgo.Table
	resources      *reliabilityResources
	metrics        *reliabilityMetrics
}

func TestFluss091Reliability(t *testing.T) {
	requireEnvironment(t)
	config, err := loadReliabilityConfig()
	if err != nil {
		t.Fatal(err)
	}
	report, runErr := runReliability(t, config)
	if err := writeReliabilityReport(config.ReportPath, report); err != nil {
		t.Errorf("write reliability report: %v", err)
	}
	if runErr != nil {
		t.Fatal(runErr)
	}
}

func TestReliabilityConfigValidation(t *testing.T) {
	keys := []string{
		"FLUSS_RELIABILITY_PROFILE", "FLUSS_RELIABILITY_DURATION",
		"FLUSS_RELIABILITY_SEED", "FLUSS_RELIABILITY_MAX_OPS",
		"FLUSS_RELIABILITY_WORKERS", "FLUSS_RELIABILITY_REPORT",
	}
	for _, key := range keys {
		t.Setenv(key, "")
	}
	config, err := loadReliabilityConfig()
	if err != nil || config.Profile != "smoke" || config.Seed != reliabilityDefaultSeed {
		t.Fatalf("default reliability config = %#v, %v", config, err)
	}
	for _, test := range []struct {
		key, value string
	}{
		{"FLUSS_RELIABILITY_PROFILE", "unknown"},
		{"FLUSS_RELIABILITY_DURATION", "0s"},
		{"FLUSS_RELIABILITY_DURATION", "31m"},
		{"FLUSS_RELIABILITY_SEED", "not-an-integer"},
		{"FLUSS_RELIABILITY_MAX_OPS", "15"},
		{"FLUSS_RELIABILITY_MAX_OPS", "1000001"},
		{"FLUSS_RELIABILITY_WORKERS", "0"},
		{"FLUSS_RELIABILITY_WORKERS", "33"},
	} {
		t.Run(test.key+"="+test.value, func(t *testing.T) {
			t.Setenv(test.key, test.value)
			if _, err := loadReliabilityConfig(); err == nil {
				t.Fatal("invalid reliability config was accepted")
			}
		})
	}
}

func loadReliabilityConfig() (reliabilityConfig, error) {
	profile := env("FLUSS_RELIABILITY_PROFILE", "smoke")
	defaults := map[string]struct {
		duration time.Duration
		maxOps   int64
		workers  int
	}{
		"smoke":  {8 * time.Second, 256, 4},
		"log":    {2 * time.Minute, 10_000, 4},
		"kv":     {2 * time.Minute, 10_000, 4},
		"lookup": {2 * time.Minute, 20_000, 8},
		"scan":   {2 * time.Minute, 2_000, 2},
		"mixed":  {5 * time.Minute, 30_000, 8},
		"soak":   {15 * time.Minute, 5_000, 2},
		"fault":  {10 * time.Minute, 30_000, 8},
	}
	selected, ok := defaults[profile]
	if !ok {
		return reliabilityConfig{}, fmt.Errorf("unsupported FLUSS_RELIABILITY_PROFILE %q", profile)
	}
	duration, err := envDuration("FLUSS_RELIABILITY_DURATION", selected.duration)
	if err != nil || duration < time.Second || duration > maxReliabilityDuration {
		return reliabilityConfig{}, fmt.Errorf("FLUSS_RELIABILITY_DURATION must be between 1s and %s: %w", maxReliabilityDuration, err)
	}
	seed, err := envInt64("FLUSS_RELIABILITY_SEED", reliabilityDefaultSeed)
	if err != nil {
		return reliabilityConfig{}, err
	}
	maxOps, err := envInt64("FLUSS_RELIABILITY_MAX_OPS", selected.maxOps)
	if err != nil || maxOps < 16 || maxOps > 1_000_000 {
		return reliabilityConfig{}, fmt.Errorf("FLUSS_RELIABILITY_MAX_OPS must be between 16 and 1000000: %w", err)
	}
	workers64, err := envInt64("FLUSS_RELIABILITY_WORKERS", int64(selected.workers))
	if err != nil || workers64 < 1 || workers64 > 32 {
		return reliabilityConfig{}, fmt.Errorf("FLUSS_RELIABILITY_WORKERS must be between 1 and 32: %w", err)
	}
	reportPath := env("FLUSS_RELIABILITY_REPORT", ".task/reliability/"+profile+".json")
	return reliabilityConfig{
		Profile: profile, Duration: duration, Seed: seed, MaxOperations: maxOps,
		Workers: int(workers64), MaxGoroutineGrowth: 12, MaxHeapGrowth: 64 << 20,
		ReportPath: reportPath,
	}, nil
}

func envDuration(key string, fallback time.Duration) (time.Duration, error) {
	value := os.Getenv(key)
	if value == "" {
		return fallback, nil
	}
	return time.ParseDuration(value)
}

func envInt64(key string, fallback int64) (int64, error) {
	value := os.Getenv(key)
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%s must be an integer: %w", key, err)
	}
	return parsed, nil
}

func runReliability(t *testing.T, config reliabilityConfig) (report reliabilityReport, resultErr error) {
	t.Helper()
	report = reliabilityReport{
		Version: 1, Profile: config.Profile, Seed: config.Seed,
		ConfiguredDuration: config.Duration.String(), MaxOperations: config.MaxOperations,
		Workers: config.Workers,
	}
	started := time.Now()
	metrics := &reliabilityMetrics{
		allowTransient: config.Profile == "fault",
		latencies:      make(map[string][]time.Duration), operations: make(map[string]int64),
		expectedErrors: make(map[string]int64), logRows: make(map[int32]string),
		kvRows: make(map[string]fgo.Row),
	}
	defer func() { finishReliabilityReport(&report, metrics, started, resultErr) }()
	address := net.JoinHostPort("127.0.0.1", env("FLUSS_PLAIN_COORDINATOR_PORT", "19123"))
	if config.Profile == "smoke" || config.Profile == "fault" {
		if err := exerciseReliabilityDialFaults(address, config.Seed, metrics); err != nil {
			return report, err
		}
	}
	environment, err := openReliabilityEnvironment(t, address)
	if err != nil {
		return report, err
	}
	defer func() {
		if err := environment.close(); err != nil && resultErr == nil {
			resultErr = err
		}
	}()
	resultErr = executeReliability(config, environment, metrics, &report)
	return report, resultErr
}

func finishReliabilityReport(
	report *reliabilityReport,
	metrics *reliabilityMetrics,
	started time.Time,
	resultErr error,
) {
	report.Elapsed = time.Since(started).Round(time.Millisecond).String()
	report.Operations, report.ExpectedErrors = metrics.snapshotCounts()
	report.Latencies = metrics.summarizeLatencies()
	var total int64
	for _, count := range report.Operations {
		total += count
	}
	if elapsed := time.Since(started).Seconds(); elapsed > 0 {
		report.ThroughputPerSecond = float64(total) / elapsed
	}
	report.Faults = slices.Clone(metrics.faults)
	if resultErr != nil {
		report.Failure = "reliability checks failed; see redacted job diagnostics"
	}
}

func openReliabilityEnvironment(t *testing.T, address string) (*reliabilityEnvironment, error) {
	t.Helper()
	environment := &reliabilityEnvironment{address: address}
	environment.client = openClient(t, []string{address})
	admin, err := fadm.New(environment.client)
	if err != nil {
		environment.client.Close()
		return nil, err
	}
	environment.admin = admin
	environment.database = fmt.Sprintf("go_reliability_%d", time.Now().UnixNano())
	environment.logPath = fgo.TablePath{Database: environment.database, Table: "events"}
	environment.kvPath = fgo.TablePath{Database: environment.database, Table: "state"}
	if err := admin.CreateDatabase(
		context.Background(), environment.database, fadm.DatabaseDescriptor{}, false,
	); err != nil {
		environment.client.Close()
		return nil, err
	}
	complete := false
	defer func() {
		if !complete {
			_ = environment.close()
		}
	}()
	createTables(t, admin, environment.logPath, environment.kvPath)
	environment.logTable, err = environment.client.GetTable(context.Background(), environment.logPath)
	if err == nil {
		environment.kvTable, err = environment.client.GetTable(context.Background(), environment.kvPath)
	}
	if err != nil {
		_ = environment.close()
		return nil, err
	}
	complete = true
	return environment, nil
}

func (e *reliabilityEnvironment) close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	dropErr := e.admin.DropDatabase(ctx, e.database, true, true)
	return errors.Join(dropErr, e.client.Close())
}

func executeReliability(
	config reliabilityConfig,
	environment *reliabilityEnvironment,
	metrics *reliabilityMetrics,
	report *reliabilityReport,
) error {
	runtime.GC()
	report.GoroutinesBefore = runtime.NumGoroutine()
	report.HeapBeforeBytes = currentHeap()
	var resultErr error
	if config.Profile == "soak" {
		resultErr = runSoakProfile(
			config, environment.address, environment.logPath, environment.kvPath, metrics,
		)
	} else {
		resultErr = runLoadProfile(
			config, environment.client, environment.admin,
			environment.logTable, environment.kvTable, metrics,
		)
	}
	if resultErr == nil {
		resultErr = metrics.unexpected()
	}
	if resultErr == nil {
		resultErr = verifyReliabilityResults(environment, metrics, report)
	}
	measureReliabilityResources(config, report)
	if resultErr == nil && !report.ResourceBoundsCorrect {
		return fmt.Errorf(
			"resource growth exceeded bounds: goroutines %+d/%d, heap %+d/%d bytes",
			report.GoroutineGrowth, config.MaxGoroutineGrowth,
			report.HeapGrowthBytes, config.MaxHeapGrowth,
		)
	}
	return resultErr
}

func verifyReliabilityResults(
	environment *reliabilityEnvironment,
	metrics *reliabilityMetrics,
	report *reliabilityReport,
) error {
	acknowledged, verified, observed, err := verifyReliabilityLog(
		environment.client, environment.admin, environment.logTable, metrics,
	)
	report.AcknowledgedLogRows, report.VerifiedLogRows = acknowledged, verified
	report.ObservedLogRows = observed
	report.UnacknowledgedLogRows = max(0, observed-acknowledged)
	if err == nil {
		report.AcknowledgedKVRows, report.VerifiedKVRows, err =
			verifyReliabilityKV(environment.client, environment.kvTable, metrics)
	}
	report.FinalDataCorrect = err == nil
	return err
}

func measureReliabilityResources(config reliabilityConfig, report *reliabilityReport) {
	runtime.GC()
	time.Sleep(100 * time.Millisecond)
	runtime.GC()
	report.GoroutinesAfter = runtime.NumGoroutine()
	report.HeapAfterBytes = currentHeap()
	report.GoroutineGrowth = report.GoroutinesAfter - report.GoroutinesBefore
	report.HeapGrowthBytes = int64(report.HeapAfterBytes) - int64(report.HeapBeforeBytes)
	report.ResourceBoundsCorrect = report.GoroutineGrowth <= config.MaxGoroutineGrowth &&
		report.HeapGrowthBytes <= int64(config.MaxHeapGrowth)
}

func runLoadProfile(
	config reliabilityConfig,
	client *fgo.Client,
	admin *fadm.Client,
	logTable, kvTable fgo.Table,
	metrics *reliabilityMetrics,
) error {
	setupCtx, setupCancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer setupCancel()
	resources, err := openReliabilityResources(setupCtx, client, logTable, kvTable, metrics)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), config.Duration)
	defer cancel()
	runReliabilityWorkload(config, ctx, cancel, client, logTable, resources, metrics)
	closeCtx, closeCancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer closeCancel()
	closeReliabilityResources(closeCtx, admin, resources, metrics)
	return metrics.unexpected()
}

func openReliabilityResources(
	ctx context.Context,
	client *fgo.Client,
	logTable, kvTable fgo.Table,
	metrics *reliabilityMetrics,
) (*reliabilityResources, error) {
	resources := &reliabilityResources{}
	appendWriter, err := client.NewAppendWriter(
		ctx, logTable, fgo.WithAppendBatchTimeout(0), fgo.WithAppendConcurrency(4),
		fgo.WithAppendRetryPolicy(reliabilityWriterRetry()),
	)
	if err != nil {
		return nil, err
	}
	resources.appendWriter = appendWriter
	upsertWriter, err := client.NewUpsertWriter(
		ctx, kvTable, fgo.WithUpsertBatchTimeout(0), fgo.WithUpsertConcurrency(4),
		fgo.WithUpsertRetryPolicy(reliabilityWriterRetry()),
	)
	if err != nil {
		_ = appendWriter.Close(context.Background())
		return nil, err
	}
	resources.upsertWriter = upsertWriter
	lookup, err := client.NewLookuper(
		ctx, kvTable, fgo.WithLookupBatchLimits(64, 4),
		fgo.WithLookupRetryPolicy(fgo.RetryPolicy{MaxAttempts: 3, Backoff: reliabilityBackoff}),
	)
	if err != nil {
		_ = appendWriter.Close(context.Background())
		_ = upsertWriter.Close(context.Background())
		return nil, err
	}
	resources.lookup = lookup
	if err := seedReliabilityData(ctx, appendWriter, upsertWriter, metrics); err != nil {
		_ = lookup.Close()
		_ = appendWriter.Close(context.Background())
		_ = upsertWriter.Close(context.Background())
		return nil, err
	}
	return resources, nil
}

func runReliabilityWorkload(
	config reliabilityConfig,
	ctx context.Context,
	cancel context.CancelFunc,
	client *fgo.Client,
	logTable fgo.Table,
	resources *reliabilityResources,
	metrics *reliabilityMetrics,
) {
	var sequence atomic.Int64
	var workers sync.WaitGroup
	workerKinds := reliabilityWorkerKinds(config.Profile, config.Workers)
	for index, kind := range workerKinds {
		workerLimit := config.MaxOperations / int64(len(workerKinds))
		if int64(index) < config.MaxOperations%int64(len(workerKinds)) {
			workerLimit++
		}
		workers.Add(1)
		go func(worker int, operation string) {
			defer workers.Done()
			reliabilityWorker{
				ctx: ctx, operation: operation,
				random:   rand.New(rand.NewSource(config.Seed + int64(worker+1))),
				sequence: &sequence, operationLimit: workerLimit,
				client: client, logTable: logTable, resources: resources, metrics: metrics,
			}.run()
		}(index, kind)
	}
	faultDone := make(chan error, 1)
	if config.Profile == "fault" {
		go func() {
			faultDone <- injectReliabilityServerRestart(ctx, config.Duration, metrics)
		}()
	} else {
		close(faultDone)
	}
	injectCancellationBurst(client, logTable.Path, metrics)
	workers.Wait()
	cancel()
	if config.Profile == "fault" {
		if faultErr := <-faultDone; faultErr != nil {
			metrics.fail(faultErr)
		}
	}
	return
}

func closeReliabilityResources(
	ctx context.Context,
	admin *fadm.Client,
	resources *reliabilityResources,
	metrics *reliabilityMetrics,
) {
	if err := resources.appendWriter.Close(ctx); err != nil && !errors.Is(err, context.Canceled) {
		metrics.fail(fmt.Errorf("close append writer: %w", err))
	}
	if err := resources.upsertWriter.Close(ctx); err != nil && !errors.Is(err, context.Canceled) {
		metrics.fail(fmt.Errorf("close upsert writer: %w", err))
	}
	if err := resources.lookup.Close(); err != nil {
		metrics.fail(fmt.Errorf("close lookup: %w", err))
	}
	if _, err := admin.GetServerNodes(ctx); err != nil {
		metrics.fail(fmt.Errorf("post-load admin request: %w", err))
	}
}

func seedReliabilityData(
	ctx context.Context,
	appendWriter *fgo.AppendWriter,
	upsertWriter *fgo.UpsertWriter,
	metrics *reliabilityMetrics,
) error {
	for index := -reliabilitySeedRows; index < 0; index++ {
		value := fmt.Sprintf("seed-%d", -index)
		if result := appendWriter.Append(ctx, fgo.Row{index, value}).Await(ctx); result.Err != nil {
			return fmt.Errorf("seed log row %d: %w", index, result.Err)
		}
		metrics.recordLog(index, value)
		tenant := fmt.Sprintf("tenant-%02d", (-index)%16)
		row := fgo.Row{tenant, index, value}
		if result := upsertWriter.Upsert(ctx, row).Await(ctx); result.Err != nil {
			return fmt.Errorf("seed KV row %d: %w", index, result.Err)
		}
		metrics.recordKV(reliabilityKey(tenant, index), row)
	}
	return nil
}

func reliabilityWriterRetry() fgo.WriterRetryPolicy {
	return fgo.WriterRetryPolicy{MaxAttempts: 5, Backoff: reliabilityBackoff}
}

func reliabilityBackoff(attempt int) time.Duration {
	return time.Duration(attempt) * 25 * time.Millisecond
}

func reliabilityWorkerKinds(profile string, workers int) []string {
	if profile != "smoke" && profile != "mixed" && profile != "fault" {
		return slices.Repeat([]string{profile}, workers)
	}
	kinds := []string{"log", "kv", "lookup", "scan"}
	result := make([]string, workers)
	for index := range result {
		result[index] = kinds[index%len(kinds)]
	}
	return result
}

func (w reliabilityWorker) run() {
	for operationIndex := int64(0); operationIndex < w.operationLimit &&
		w.ctx.Err() == nil && w.metrics.unexpected() == nil; operationIndex++ {
		started := time.Now()
		var err error
		switch w.operation {
		case "log":
			id := int32(w.sequence.Add(1))
			value := fmt.Sprintf("value-%d", id)
			result := w.resources.appendWriter.Append(w.ctx, fgo.Row{id, value}).Await(w.ctx)
			err = result.Err
			if err == nil {
				w.metrics.recordLog(id, value)
			}
		case "kv":
			id := int32(w.sequence.Add(1))
			tenant := fmt.Sprintf("tenant-%02d", id%16)
			row := fgo.Row{tenant, id, fmt.Sprintf("value-%d", id)}
			result := w.resources.upsertWriter.Upsert(w.ctx, row).Await(w.ctx)
			err = result.Err
			if err == nil {
				w.metrics.recordKV(reliabilityKey(tenant, id), row)
			}
		case "lookup":
			id := -1 - w.random.Int31n(reliabilitySeedRows)
			tenant := fmt.Sprintf("tenant-%02d", id%16)
			result := w.resources.lookup.Lookup(w.ctx, fgo.PrimaryKey{tenant, id})[0]
			if result.Err != nil && !errors.Is(result.Err, fgo.ErrNotFound) {
				err = result.Err
			}
		case "scan":
			err = reliabilityScanOnce(w.ctx, w.client, w.logTable)
		}
		w.metrics.record(w.operation, time.Since(started), err, w.ctx.Err() != nil)
	}
}

func reliabilityScanOnce(ctx context.Context, client *fgo.Client, table fgo.Table) error {
	scanner, err := client.NewLogScanner(
		ctx, table, fgo.Earliest(), fgo.WithScanRowLimit(16),
		fgo.WithLogFetchLimits(1<<20, 1<<20, 1, 50*time.Millisecond),
	)
	if err != nil {
		return err
	}
	defer scanner.Close()
	for !scanner.Done() && ctx.Err() == nil {
		result, err := scanner.Poll(ctx)
		if err != nil {
			return err
		}
		result.Release()
	}
	return ctx.Err()
}

func runSoakProfile(
	config reliabilityConfig,
	address string,
	logPath, kvPath fgo.TablePath,
	metrics *reliabilityMetrics,
) error {
	ctx, cancel := context.WithTimeout(context.Background(), config.Duration)
	defer cancel()
	for operation := int64(0); operation < config.MaxOperations && ctx.Err() == nil; operation++ {
		started := time.Now()
		cycleCtx, cycleCancel := context.WithTimeout(ctx, 10*time.Second)
		err := runSoakCycle(cycleCtx, address, logPath, kvPath)
		cycleCancel()
		metrics.record("soak_cycle", time.Since(started), err, false)
		if err != nil {
			return fmt.Errorf("soak cycle %d: %w", operation, err)
		}
	}
	return nil
}

func runSoakCycle(
	ctx context.Context,
	address string,
	logPath, kvPath fgo.TablePath,
) (resultErr error) {
	client, err := fgo.Open(ctx, fgo.WithBootstrapServers(address), fgo.WithConnectTimeout(3*time.Second))
	if err != nil {
		return err
	}
	defer func() {
		if err := client.Close(); resultErr == nil {
			resultErr = err
		}
	}()
	logTable, err := client.GetTable(ctx, logPath)
	if err != nil {
		return err
	}
	if _, err := client.ResolveTableBuckets(ctx, fgo.PhysicalTablePath{TablePath: logPath}); err != nil {
		return err
	}
	kvTable, err := client.GetTable(ctx, kvPath)
	if err != nil {
		return err
	}
	lookup, err := client.NewLookuper(ctx, kvTable)
	if err != nil {
		return err
	}
	if err := lookup.Close(); err != nil {
		return err
	}
	scanner, err := client.NewLogScanner(
		ctx, logTable, fgo.Earliest(), fgo.WithScanRowLimit(1),
		fgo.WithLogFetchLimits(1<<20, 1<<20, 1, 50*time.Millisecond),
	)
	if err != nil {
		return err
	}
	if err := scanner.Close(); err != nil {
		return err
	}
	writer, err := client.NewAppendWriter(ctx, logTable, fgo.WithAppendBatchTimeout(0))
	if err != nil {
		return err
	}
	return writer.Close(ctx)
}

func exerciseReliabilityDialFaults(address string, seed int64, metrics *reliabilityMetrics) error {
	dialer := &net.Dialer{Timeout: 3 * time.Second}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	truncated, err := fgo.Open(
		ctx,
		fgo.WithBootstrapServers(address),
		fgo.WithDialContext(func(ctx context.Context, network, target string) (net.Conn, error) {
			connection, dialErr := dialer.DialContext(ctx, network, target)
			if dialErr == nil {
				return &truncatedReadConn{Conn: connection, remaining: 8}, nil
			}
			return connection, dialErr
		}),
	)
	if truncated != nil {
		_ = truncated.Close()
	}
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		return fmt.Errorf("truncated bootstrap error = %v", err)
	}
	delay := time.Duration((seed%7)+1) * time.Millisecond
	client, err := fgo.Open(
		ctx,
		fgo.WithBootstrapServers(address),
		fgo.WithDialContext(func(ctx context.Context, network, target string) (net.Conn, error) {
			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil, context.Cause(ctx)
			case <-timer.C:
			}
			return dialer.DialContext(ctx, network, target)
		}),
	)
	if err != nil {
		return fmt.Errorf("open after delayed/truncated bootstrap: %w", err)
	}
	_ = client.Close()
	metrics.addFault("delayed_connection")
	metrics.addFault("truncated_read")
	metrics.expected("truncated_read")
	return nil
}

type truncatedReadConn struct {
	net.Conn
	remaining int
}

func (c *truncatedReadConn) Read(buffer []byte) (int, error) {
	if c.remaining == 0 {
		return 0, io.ErrUnexpectedEOF
	}
	if len(buffer) > c.remaining {
		buffer = buffer[:c.remaining]
	}
	read, err := c.Conn.Read(buffer)
	c.remaining -= read
	return read, err
}

func injectCancellationBurst(client *fgo.Client, path fgo.TablePath, metrics *reliabilityMetrics) {
	const requests = 32
	var wait sync.WaitGroup
	for range requests {
		wait.Add(1)
		go func() {
			defer wait.Done()
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			_, err := client.GetTable(ctx, path)
			if errors.Is(err, context.Canceled) {
				metrics.expected("canceled_request")
				return
			}
			metrics.fail(fmt.Errorf("cancellation burst error = %v", err))
		}()
	}
	wait.Wait()
	metrics.addFault("cancellation_burst")
}

func injectReliabilityServerRestart(
	ctx context.Context,
	duration time.Duration,
	metrics *reliabilityMetrics,
) error {
	delay := min(duration/3, 20*time.Second)
	if delay < time.Second {
		delay = time.Second
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return nil
	case <-timer.C:
	}
	file, project := os.Getenv("FLUSS_COMPOSE_FILE"), os.Getenv("FLUSS_COMPOSE_PROJECT")
	commandCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	command := exec.CommandContext(
		commandCtx, "docker", "compose", "--project-name", project, "--file", file,
		"restart", "plaintext-tablet-0",
	)
	if output, err := command.CombinedOutput(); err != nil {
		return fmt.Errorf("restart plaintext-tablet-0: %w: %s", err, output)
	}
	command = exec.CommandContext(
		commandCtx, "docker", "compose", "--project-name", project, "--file", file,
		"up", "--detach", "--wait", "--wait-timeout", "120", "plaintext-tablet-0",
	)
	if output, err := command.CombinedOutput(); err != nil {
		return fmt.Errorf("wait for plaintext-tablet-0: %w: %s", err, output)
	}
	metrics.addFault("tablet_restart")
	return nil
}

func verifyReliabilityLog(
	client *fgo.Client,
	admin *fadm.Client,
	table fgo.Table,
	metrics *reliabilityMetrics,
) (int, int, int, error) {
	expected := metrics.logSnapshot()
	if len(expected) == 0 {
		return 0, 0, 0, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	ends, err := reliabilityLogEnds(ctx, admin, table)
	if err != nil {
		return len(expected), 0, 0, err
	}
	scanner, err := client.NewLogScanner(
		ctx, table, fgo.Earliest(), fgo.WithScanStoppingOffsets(ends),
		fgo.WithLogFetchLimits(1<<20, 1<<20, 1, 100*time.Millisecond),
	)
	if err != nil {
		return len(expected), 0, 0, err
	}
	defer scanner.Close()
	found, observed, err := scanReliabilityLog(ctx, scanner)
	if err != nil {
		return len(expected), len(found), observed, err
	}
	verified, err := verifyExpectedReliabilityLog(expected, found)
	return len(expected), verified, observed, err
}

func scanReliabilityLog(
	ctx context.Context,
	scanner *fgo.LogScanner,
) (map[int32]string, int, error) {
	found := make(map[int32]string)
	observed := 0
	for !scanner.Done() && ctx.Err() == nil {
		result, pollErr := scanner.Poll(ctx)
		if pollErr != nil {
			return found, observed, pollErr
		}
		for _, record := range result.Records {
			observed++
			id, ok := record.Record.Value[0].(int32)
			if !ok {
				result.Release()
				return found, observed, errors.New("reliability log returned a non-int32 ID")
			}
			if _, duplicate := found[id]; duplicate {
				result.Release()
				return found, observed, fmt.Errorf("duplicate reliability log ID %d", id)
			}
			found[id] = record.Record.Value[1].(string)
		}
		result.Release()
	}
	if ctx.Err() != nil {
		return found, observed, ctx.Err()
	}
	return found, observed, nil
}

func verifyExpectedReliabilityLog(expected, found map[int32]string) (int, error) {
	verified := 0
	for id, value := range expected {
		if found[id] != value {
			return verified, fmt.Errorf("log ID %d = %q, want %q", id, found[id], value)
		}
		verified++
	}
	return verified, nil
}

func reliabilityLogEnds(
	ctx context.Context,
	admin *fadm.Client,
	table fgo.Table,
) (map[int32]int64, error) {
	buckets := make([]int32, table.BucketCount)
	for index := range buckets {
		buckets[index] = int32(index)
	}
	var lastErr error
	for ctx.Err() == nil {
		results := admin.ListOffsets(
			ctx, table, fgo.PhysicalTablePath{TablePath: table.Path}, -1, buckets, fgo.Latest(),
		)
		ends := make(map[int32]int64, len(results))
		lastErr = nil
		for _, result := range results {
			if result.Err != nil {
				lastErr = result.Err
				break
			}
			ends[result.Bucket] = result.Offset
		}
		if lastErr == nil && len(ends) == len(buckets) {
			return ends, nil
		}
		if err := waitRetryInterval(ctx, 100*time.Millisecond); err != nil {
			break
		}
	}
	return nil, fmt.Errorf("load final log offsets: %w", errors.Join(lastErr, ctx.Err()))
}

func verifyReliabilityKV(
	client *fgo.Client,
	table fgo.Table,
	metrics *reliabilityMetrics,
) (int, int, error) {
	expected := metrics.kvSnapshot()
	if len(expected) == 0 {
		return 0, 0, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	lookup, err := client.NewLookuper(ctx, table, fgo.WithLookupBatchLimits(128, 4))
	if err != nil {
		return len(expected), 0, err
	}
	defer lookup.Close()
	keys := make([]string, 0, len(expected))
	for key := range expected {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	verified := 0
	for start := 0; start < len(keys); start += 128 {
		end := min(start+128, len(keys))
		batch := make([]fgo.PrimaryKey, 0, end-start)
		for _, key := range keys[start:end] {
			row := expected[key]
			batch = append(batch, fgo.PrimaryKey{row[0], row[1]})
		}
		results := lookup.Lookup(ctx, batch...)
		if len(results) != len(batch) {
			return len(expected), verified, fmt.Errorf("KV lookup count = %d, want %d", len(results), len(batch))
		}
		for index, result := range results {
			want := expected[keys[start+index]]
			if result.Err != nil || !result.Found || !slices.Equal(result.Row, want) {
				return len(expected), verified, fmt.Errorf("KV result %d = %#v, want %#v", start+index, result, want)
			}
			verified++
		}
	}
	return len(expected), verified, nil
}

func reliabilityKey(tenant string, id int32) string {
	return tenant + "/" + strconv.FormatInt(int64(id), 10)
}

func (m *reliabilityMetrics) record(operation string, latency time.Duration, err error, stopping bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err != nil {
		if stopping && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
			m.expectedErrors["profile_deadline"]++
			return
		}
		if m.allowTransient && isTransientConnectionFailure(err) {
			m.expectedErrors["injected_transient"]++
			return
		}
		if m.unexpectedError == nil {
			m.unexpectedError = fmt.Errorf("%s: %w", operation, err)
		}
		return
	}
	m.operations[operation]++
	m.latencies[operation] = append(m.latencies[operation], latency)
}

func (m *reliabilityMetrics) recordLog(id int32, value string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if previous, exists := m.logRows[id]; exists && previous != value && m.unexpectedError == nil {
		m.unexpectedError = fmt.Errorf("conflicting acknowledged log ID %d", id)
	}
	m.logRows[id] = value
}

func (m *reliabilityMetrics) recordKV(key string, row fgo.Row) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.kvRows[key] = slices.Clone(row)
}

func (m *reliabilityMetrics) expected(kind string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.expectedErrors[kind]++
}

func (m *reliabilityMetrics) fail(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err != nil && m.unexpectedError == nil {
		m.unexpectedError = err
	}
}

func (m *reliabilityMetrics) unexpected() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.unexpectedError
}

func (m *reliabilityMetrics) addFault(name string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.faults = append(m.faults, name)
}

func (m *reliabilityMetrics) snapshotCounts() (map[string]int64, map[string]int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return cloneCounts(m.operations), cloneCounts(m.expectedErrors)
}

func cloneCounts(source map[string]int64) map[string]int64 {
	clone := make(map[string]int64, len(source))
	for key, value := range source {
		clone[key] = value
	}
	return clone
}

func (m *reliabilityMetrics) summarizeLatencies() map[string]reliabilityStats {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make(map[string]reliabilityStats, len(m.latencies))
	for operation, values := range m.latencies {
		ordered := slices.Clone(values)
		slices.Sort(ordered)
		result[operation] = reliabilityStats{
			Count: int64(len(ordered)), P50: percentile(ordered, 0.50).String(),
			P95: percentile(ordered, 0.95).String(), P99: percentile(ordered, 0.99).String(),
			Max: ordered[len(ordered)-1].String(),
		}
	}
	return result
}

func percentile(values []time.Duration, fraction float64) time.Duration {
	if len(values) == 0 {
		return 0
	}
	index := int(float64(len(values)-1) * fraction)
	return values[index]
}

func (m *reliabilityMetrics) logSnapshot() map[int32]string {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make(map[int32]string, len(m.logRows))
	for key, value := range m.logRows {
		result[key] = value
	}
	return result
}

func (m *reliabilityMetrics) kvSnapshot() map[string]fgo.Row {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make(map[string]fgo.Row, len(m.kvRows))
	for key, value := range m.kvRows {
		result[key] = slices.Clone(value)
	}
	return result
}

func currentHeap() uint64 {
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	return memory.HeapAlloc
}

func writeReliabilityReport(path string, report reliabilityReport) error {
	if strings.TrimSpace(path) == "" {
		return errors.New("FLUSS_RELIABILITY_REPORT is empty")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	encoded, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	encoded = append(encoded, '\n')
	return os.WriteFile(path, encoded, 0o600)
}
