module github.com/pletorco/fluss-go/adapters/otel

go 1.25.0

toolchain go1.26.6

require (
	github.com/pletorco/fluss-go v0.1.0-beta.11
	go.opentelemetry.io/otel v1.46.0
	go.opentelemetry.io/otel/metric v1.46.0
)

require (
	github.com/apache/arrow-go/v18 v18.8.0 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/goccy/go-json v0.10.6 // indirect
	github.com/google/flatbuffers v25.12.19+incompatible // indirect
	github.com/klauspost/compress v1.19.2 // indirect
	github.com/klauspost/cpuid/v2 v2.4.0 // indirect
	github.com/pierrec/lz4/v4 v4.1.29 // indirect
	github.com/zeebo/xxh3 v1.1.0 // indirect
	golang.org/x/exp v0.0.0-20260112195511-716be5621a96 // indirect
	golang.org/x/sys v0.47.0 // indirect
	google.golang.org/protobuf v1.36.12 // indirect
)

replace github.com/pletorco/fluss-go => ../..
