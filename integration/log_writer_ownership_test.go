//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/pletorco/fluss-go/pkg/fadm"
	"github.com/pletorco/fluss-go/pkg/fgo"
)

func testAppendInputOwnership(t *testing.T, client *fgo.Client, admin *fadm.Client, database string) {
	t.Helper()
	for _, format := range []fgo.LogFormat{fgo.LogFormatCompacted, fgo.LogFormatIndexed} {
		t.Run(string(format), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			path := fgo.TablePath{Database: database, Table: "ownership_" + string(format)}
			if err := admin.CreateTable(ctx, path, fadm.TableDescriptor{
				Schema: fgo.Schema{Columns: []fgo.Column{
					{Name: "id", Type: fgo.IntType}, {Name: "payload", Type: fgo.BytesType},
				}},
				BucketCount: 1, Properties: map[string]string{"table.log.format": strings.ToUpper(string(format))},
			}, false); err != nil {
				t.Fatal(err)
			}
			waitForTableReady(t, admin, path)
			table, err := client.GetTable(ctx, path)
			if err != nil {
				t.Fatal(err)
			}
			writer, err := client.NewAppendWriter(ctx, table,
				fgo.WithAppendLogFormat(format), fgo.WithAppendBatchTimeout(time.Hour))
			if err != nil {
				t.Fatal(err)
			}
			defer writer.Close(context.Background())
			payload := []byte("old")
			future := writer.Append(ctx, fgo.Row{int32(1), payload})
			copy(payload, "new")
			if err := writer.Close(ctx); err != nil {
				t.Fatal(err)
			}
			if result := future.Await(ctx); result.Err != nil {
				t.Fatal(result.Err)
			}
			scanner, err := client.NewLogScanner(ctx, table, fgo.Earliest(), fgo.WithScanRowLimit(1))
			if err != nil {
				t.Fatal(err)
			}
			defer scanner.Close()
			result, err := scanner.Poll(ctx)
			defer result.Release()
			if err != nil || len(result.BucketErrors) != 0 || len(result.Records) != 1 {
				t.Fatalf("scan accepted payload = %#v, %v", result, err)
			}
			if got := result.Records[0].Record.Value[1].([]byte); !bytes.Equal(got, []byte("old")) {
				t.Fatalf("stored %q after caller reused the buffer, want old", got)
			}
		})
	}
}
