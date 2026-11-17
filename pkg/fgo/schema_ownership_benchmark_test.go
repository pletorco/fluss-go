package fgo

import (
	"context"
	"testing"
)

func BenchmarkSchemaOwnership(b *testing.B) {
	b.Run("cache_hit", func(b *testing.B) {
		client, path, _ := schemaOwnershipClient(b)
		if _, err := client.GetTable(context.Background(), path); err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			if _, err := client.resolveSchema(context.Background(), path, 1); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("get_table", func(b *testing.B) {
		client, path, _ := schemaOwnershipClient(b)
		if _, err := client.GetTable(context.Background(), path); err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			if _, err := client.GetTable(context.Background(), path); err != nil {
				b.Fatal(err)
			}
		}
	})
}
