package fgo

import (
	"errors"
	"strings"
	"testing"
)

func TestAppendWriterRejectsLakeBoundRowRoutingBeforeMetadata(t *testing.T) {
	for _, format := range []LogFormat{LogFormatCompacted, LogFormatIndexed} {
		t.Run(string(format), func(t *testing.T) {
			table := appendWriterTable()
			table.Schema.BucketKey = []string{"id"}
			table.Properties["table.log.format"] = string(format)
			table.Properties["table.datalake.format"] = " PAIMON "
			table.Properties["table.datalake.enabled"] = "false"
			// The guard must run before metadata, partition creation or writer
			// initialization. No backend is needed in this case.
			writer, err := newAppendWriter(t.Context(), nil, table)
			if writer != nil || !errors.Is(err, ErrUnsupportedAPI) || !strings.Contains(err.Error(), "PAIMON") {
				t.Fatalf("open writer = %v, %v; want lake-specific ErrUnsupportedAPI", writer, err)
			}
		})
	}
}
