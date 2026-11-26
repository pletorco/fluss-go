package fgo

import (
	"errors"
	"strings"
	"testing"
)

func TestKeyedKVConstructorsRejectLakeBoundTablesBeforeRouting(t *testing.T) {
	for _, format := range []string{"paimon", " ICEBERG ", "hudi", "lance", "future-format"} {
		t.Run(format, func(t *testing.T) {
			table := upsertWriterTable()
			table.Properties = map[string]string{
				"table.datalake.format": format, "table.datalake.enabled": "false", "table.kv.format-version": "2",
			}
			// No backend: accessing partition metadata or initializing an RPC
			// before the guard would panic.
			_, writeErr := newUpsertWriter(t.Context(), nil, table)
			_, lookupErr := newLookuper(t.Context(), nil, table)
			for _, err := range []error{writeErr, lookupErr} {
				if !errors.Is(err, ErrUnsupportedAPI) || !strings.Contains(err.Error(), strings.TrimSpace(format)) {
					t.Fatalf("lake format %q: got %v, want format-specific ErrUnsupportedAPI", format, err)
				}
			}
		})
	}
}
