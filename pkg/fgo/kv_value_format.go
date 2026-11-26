package fgo

import (
	"fmt"
	"strings"
)

// Fluss stores the row value separately from its compacted physical key.
func kvValueEncoding(table Table) (rowEncoding, error) {
	format := strings.ToLower(strings.TrimSpace(table.Properties["table.kv.format"]))
	switch format {
	case "", "compacted":
		return compactedEncoding, nil
	case "indexed":
		return indexedEncoding, nil
	default:
		return compactedEncoding, fmt.Errorf("%w: unsupported table.kv.format %q", ErrInvalidConfig, format)
	}
}
