package fgo

import (
	"errors"
	"testing"
)

func TestResolveAppendWriterFormat(t *testing.T) {
	for _, tc := range []struct {
		name, property string
		format, want   LogFormat
		compression    bool
	}{
		{name: "omitted", format: LogFormatAuto, want: LogFormatAuto},
		{name: "omitted with Arrow compression", format: LogFormatAuto, want: LogFormatAuto, compression: true},
		{name: "compacted", property: " COMPACTED ", format: LogFormatAuto, want: LogFormatCompacted},
		{name: "indexed", property: "INDEXED", format: LogFormatAuto, want: LogFormatIndexed},
		{name: "Arrow", property: "ARROW", format: LogFormatAuto, want: LogFormatArrow, compression: true},
		{name: "unknown", property: "unknown", format: LogFormatAuto},
		{name: "not concrete", property: "auto", format: LogFormatAuto},
		{name: "Arrow compression on indexed", property: "INDEXED", format: LogFormatAuto, compression: true},
		{name: "explicit mismatch", property: "ARROW", format: LogFormatCompacted},
		{name: "explicit match", property: "INDEXED", format: LogFormatIndexed, want: LogFormatIndexed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			table := appendWriterTable()
			table.Properties = map[string]string{"table.log.format": tc.property}
			config := AppendWriterConfig{Format: tc.format, arrowCompressionSet: tc.compression}
			err := resolveAppendWriterFormat(table, &config)
			if tc.want == "" {
				if !errors.Is(err, ErrInvalidConfig) {
					t.Fatalf("resolve error = %v, want ErrInvalidConfig", err)
				}
				return
			}
			if err != nil || config.Format != tc.want {
				t.Fatalf("resolve = %q, %v, want %q", config.Format, err, tc.want)
			}
		})
	}
}
