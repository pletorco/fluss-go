package otel_test

import (
	"fmt"

	oteladapter "github.com/pletorco/fluss-go/adapters/otel"
	"github.com/pletorco/fluss-go/pkg/fgo"
	"go.opentelemetry.io/otel/metric/noop"
)

func ExampleNew() {
	// Applications normally pass their SDK-backed provider and own its
	// exporter and Shutdown lifecycle. The no-op provider keeps this example
	// deterministic.
	observer, err := oteladapter.New(noop.NewMeterProvider())
	if err != nil {
		panic(err)
	}
	option := fgo.WithMetricsObserver(observer)

	fmt.Printf("%T %T\n", observer, option)
	// Output: *otel.Observer fgo.Option
}
