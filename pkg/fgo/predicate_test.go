package fgo

import (
	"context"
	"errors"
	"math"
	"math/big"
	"testing"
	"time"

	"github.com/pletorco/fluss-go/pkg/fmsg"
	"google.golang.org/protobuf/proto"
)

func predicateSchema() Schema {
	decimal := func(precision int) *LogicalType {
		return &LogicalType{Root: "DECIMAL", Precision: precision, Scale: 2}
	}
	timestamp := &LogicalType{Root: "TIMESTAMP_WITHOUT_TIME_ZONE", Precision: 6}
	return Schema{Columns: []Column{
		{Name: "flag", Type: BoolType, ID: 0},
		{Name: "tiny", Type: TinyIntType, ID: 1},
		{Name: "small", Type: SmallIntType, ID: 2},
		{Name: "id", Type: IntType, ID: 3},
		{Name: "big", Type: BigIntType, ID: 4},
		{Name: "ratio", Type: FloatType, ID: 5},
		{Name: "score", Type: DoubleType, ID: 6},
		{Name: "code", Type: CharType, ID: 7, LogicalType: &LogicalType{Root: "CHAR", Length: 3}},
		{Name: "name", Type: StringType, ID: 8, Nullable: true},
		{Name: "price", Type: DecimalType, ID: 9, LogicalType: decimal(8)},
		{Name: "wide", Type: DecimalType, ID: 10, LogicalType: decimal(20)},
		{Name: "day", Type: DateType, ID: 11},
		{Name: "at", Type: TimeType, ID: 12},
		{Name: "ts", Type: TimestampType, ID: 13, LogicalType: timestamp},
		{Name: "ltz", Type: TimestampLTZType, ID: 14, LogicalType: &LogicalType{Root: "TIMESTAMP_WITH_LOCAL_TIME_ZONE", Precision: 3}},
		{Name: "bin", Type: BinaryType, ID: 15, LogicalType: &LogicalType{Root: "BINARY", Length: 2}},
		{Name: "raw", Type: BytesType, ID: 16},
		{Name: "list", Type: ArrayType, ID: 17, LogicalType: &LogicalType{Root: "ARRAY", Element: &LogicalType{Root: "INTEGER"}}},
	}}
}

func TestCompilePredicateLiteralEncodings(t *testing.T) {
	day := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)
	stamp := time.Date(2024, 1, 2, 3, 4, 5, 6_007_000, time.UTC)
	tests := []struct {
		name      string
		predicate Predicate
		function  int32
		fieldID   int32
		literal   *fmsg.PbLiteralValue
	}{
		{"bool", Col("flag").Eq(true), leafEqual, 0, &fmsg.PbLiteralValue{LiteralType: proto.Int32(0), IsNull: proto.Bool(false), BooleanValue: proto.Bool(true)}},
		{"tinyint", Col("tiny").Ne(int8(-5)), leafNotEqual, 1, &fmsg.PbLiteralValue{LiteralType: proto.Int32(1), IsNull: proto.Bool(false), IntValue: proto.Int32(-5)}},
		{"smallint from int", Col("small").Lt(300), leafLessThan, 2, &fmsg.PbLiteralValue{LiteralType: proto.Int32(2), IsNull: proto.Bool(false), IntValue: proto.Int32(300)}},
		{"int", Col("id").Le(int32(7)), leafLessOrEqual, 3, &fmsg.PbLiteralValue{LiteralType: proto.Int32(3), IsNull: proto.Bool(false), IntValue: proto.Int32(7)}},
		{"bigint", Col("big").Gt(int64(math.MaxInt64)), leafGreaterThan, 4, &fmsg.PbLiteralValue{LiteralType: proto.Int32(4), IsNull: proto.Bool(false), BigintValue: proto.Int64(math.MaxInt64)}},
		{"float", Col("ratio").Ge(0.5), leafGreaterOrEqual, 5, &fmsg.PbLiteralValue{LiteralType: proto.Int32(5), IsNull: proto.Bool(false), FloatValue: proto.Float32(0.5)}},
		{"double from int", Col("score").Eq(3), leafEqual, 6, &fmsg.PbLiteralValue{LiteralType: proto.Int32(6), IsNull: proto.Bool(false), DoubleValue: proto.Float64(3)}},
		{"char", Col("code").Eq("abc"), leafEqual, 7, &fmsg.PbLiteralValue{LiteralType: proto.Int32(7), IsNull: proto.Bool(false), StringValue: proto.String("abc")}},
		{"string prefix", Col("name").StartsWith("ab"), leafStartsWith, 8, &fmsg.PbLiteralValue{LiteralType: proto.Int32(8), IsNull: proto.Bool(false), StringValue: proto.String("ab")}},
		{"string suffix", Col("name").EndsWith("yz"), leafEndsWith, 8, &fmsg.PbLiteralValue{LiteralType: proto.Int32(8), IsNull: proto.Bool(false), StringValue: proto.String("yz")}},
		{"string contains", Col("name").Contains("mid"), leafContains, 8, &fmsg.PbLiteralValue{LiteralType: proto.Int32(8), IsNull: proto.Bool(false), StringValue: proto.String("mid")}},
		{"compact decimal", Col("price").Eq(big.NewRat(-123, 100)), leafEqual, 9, &fmsg.PbLiteralValue{LiteralType: proto.Int32(9), IsNull: proto.Bool(false), DecimalValue: proto.Int64(-123)}},
		{"decimal from int", Col("price").Eq(2), leafEqual, 9, &fmsg.PbLiteralValue{LiteralType: proto.Int32(9), IsNull: proto.Bool(false), DecimalValue: proto.Int64(200)}},
		{"wide decimal", Col("wide").Eq(big.NewRat(-128, 100)), leafEqual, 10, &fmsg.PbLiteralValue{LiteralType: proto.Int32(9), IsNull: proto.Bool(false), DecimalBytes: []byte{0xff, 0x80}}},
		{"date", Col("day").Eq(day), leafEqual, 11, &fmsg.PbLiteralValue{LiteralType: proto.Int32(10), IsNull: proto.Bool(false), BigintValue: proto.Int64(day.Unix() / 86400)}},
		{"time", Col("at").Lt(time.Date(1970, 1, 1, 1, 2, 3, 4_000_000, time.UTC)), leafLessThan, 12, &fmsg.PbLiteralValue{LiteralType: proto.Int32(11), IsNull: proto.Bool(false), IntValue: proto.Int32(3_723_004)}},
		{"timestamp", Col("ts").Ge(stamp), leafGreaterOrEqual, 13, &fmsg.PbLiteralValue{LiteralType: proto.Int32(12), IsNull: proto.Bool(false), TimestampMillisValue: proto.Int64(stamp.UnixMilli()), TimestampNanoOfMillisValue: proto.Int32(7000)}},
		{"timestamp ltz", Col("ltz").Gt(stamp.Truncate(time.Millisecond)), leafGreaterThan, 14, &fmsg.PbLiteralValue{LiteralType: proto.Int32(13), IsNull: proto.Bool(false), TimestampMillisValue: proto.Int64(stamp.UnixMilli()), TimestampNanoOfMillisValue: proto.Int32(0)}},
		{"binary", Col("bin").Eq([]byte{1, 2}), leafEqual, 15, &fmsg.PbLiteralValue{LiteralType: proto.Int32(14), IsNull: proto.Bool(false), BinaryValue: []byte{1, 2}}},
		{"bytes", Col("raw").Ne([]byte{9}), leafNotEqual, 16, &fmsg.PbLiteralValue{LiteralType: proto.Int32(15), IsNull: proto.Bool(false), BinaryValue: []byte{9}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := compilePredicate(test.predicate, predicateSchema())
			if err != nil {
				t.Fatal(err)
			}
			leaf := got.GetLeaf()
			if got.GetType() != predicateTypeLeaf || leaf.GetFunction() != test.function ||
				leaf.GetFieldId() != test.fieldID || len(leaf.GetLiterals()) != 1 ||
				!proto.Equal(leaf.GetLiterals()[0], test.literal) {
				t.Fatalf("compilePredicate() = %v, want function %d field %d literal %v",
					got, test.function, test.fieldID, test.literal)
			}
		})
	}
}

func TestCompilePredicateFunctionsAndCompounds(t *testing.T) {
	got, err := compilePredicate(And(
		Col("id").IsNotNull(),
		Or(Col("id").In(1, int32(2)), Col("name").In("a", nil), Col("id").NotIn(9)),
		Col("name").IsNull(),
	), predicateSchema())
	if err != nil {
		t.Fatal(err)
	}
	and := got.GetCompound()
	if got.GetType() != predicateTypeCompound || and.GetFunction() != compoundAnd || len(and.GetChildren()) != 3 {
		t.Fatalf("root = %v", got)
	}
	if leaf := and.GetChildren()[0].GetLeaf(); leaf.GetFunction() != leafIsNotNull || len(leaf.GetLiterals()) != 0 {
		t.Fatalf("IsNotNull leaf = %v", leaf)
	}
	or := and.GetChildren()[1].GetCompound()
	if or.GetFunction() != compoundOr || len(or.GetChildren()) != 3 {
		t.Fatalf("or = %v", or)
	}
	in := or.GetChildren()[0].GetLeaf()
	if in.GetFunction() != leafIn || len(in.GetLiterals()) != 2 || in.GetLiterals()[1].GetIntValue() != 2 {
		t.Fatalf("IN leaf = %v", in)
	}
	withNull := or.GetChildren()[1].GetLeaf().GetLiterals()
	if len(withNull) != 2 || !withNull[1].GetIsNull() || withNull[1].StringValue != nil {
		t.Fatalf("IN null literal = %v", withNull)
	}
	if or.GetChildren()[2].GetLeaf().GetFunction() != leafNotIn {
		t.Fatalf("NOT IN leaf = %v", or.GetChildren()[2])
	}
	single, err := compilePredicate(And(Col("id").Eq(1)), predicateSchema())
	if err != nil || single.GetType() != predicateTypeLeaf {
		t.Fatalf("single And() = %v, %v", single, err)
	}
}

func TestCompilePredicateRejectsInvalid(t *testing.T) {
	duplicateIDs := Schema{Columns: []Column{{Name: "a", Type: IntType}, {Name: "b", Type: IntType}}}
	negativeID := Schema{Columns: []Column{{Name: "a", Type: IntType, ID: -1}}}
	tests := []struct {
		name      string
		schema    Schema
		predicate Predicate
	}{
		{"zero predicate", predicateSchema(), Predicate{}},
		{"unknown column", predicateSchema(), Col("missing").Eq(1)},
		{"nested type", predicateSchema(), Col("list").IsNull()},
		{"null comparison", predicateSchema(), Col("name").Eq(nil)},
		{"empty in", predicateSchema(), Col("id").In()},
		{"literal on null check", predicateSchema(), Predicate{function: leafIsNull, column: "id", literals: []any{1}}},
		{"two literals", predicateSchema(), Predicate{function: leafEqual, column: "id", literals: []any{1, 2}}},
		{"string match on int", predicateSchema(), Col("id").StartsWith("1")},
		{"empty and", predicateSchema(), And()},
		{"empty or", predicateSchema(), Or()},
		{"empty child", predicateSchema(), And(Col("id").Eq(1), Predicate{})},
		{"wrong bool", predicateSchema(), Col("flag").Eq(1)},
		{"wrong string", predicateSchema(), Col("name").Eq(1)},
		{"wrong bytes", predicateSchema(), Col("raw").Eq("x")},
		{"wrong int", predicateSchema(), Col("id").Eq("1")},
		{"tinyint overflow", predicateSchema(), Col("tiny").Eq(128)},
		{"int overflow", predicateSchema(), Col("id").Eq(int64(math.MaxInt32) + 1)},
		{"uint64 overflow", predicateSchema(), Col("big").Eq(uint64(math.MaxUint64))},
		{"inexact float", predicateSchema(), Col("ratio").Eq(0.1)},
		{"inexact double from int", predicateSchema(), Col("score").Eq(int64(1) << 60)},
		{"decimal scale", predicateSchema(), Col("price").Eq(big.NewRat(1, 1000))},
		{"decimal precision", predicateSchema(), Col("price").Eq(1_000_000)},
		{"wrong decimal", predicateSchema(), Col("price").Eq("1.00")},
		{"nil decimal", predicateSchema(), Col("price").Eq((*big.Rat)(nil))},
		{"wrong date", predicateSchema(), Col("day").Eq("2024-01-02")},
		{"date with time", predicateSchema(), Col("day").Eq(time.Date(2024, 1, 2, 1, 0, 0, 0, time.UTC))},
		{"sub-millisecond time", predicateSchema(), Col("at").Eq(time.Date(1970, 1, 1, 0, 0, 0, 1, time.UTC))},
		{"sub-millisecond ltz", predicateSchema(), Col("ltz").Eq(time.Unix(0, 1).UTC())},
		{"sub-precision timestamp", predicateSchema(), Col("ts").Eq(time.Unix(0, 1).UTC())},
		{"duplicate field ids", duplicateIDs, Col("a").Eq(1)},
		{"negative field id", negativeID, Col("a").Eq(1)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := compilePredicate(test.predicate, test.schema); !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("compilePredicate() error = %v, want ErrInvalidConfig", err)
			}
		})
	}
}

func filterScannerTable() Table {
	table := appendWriterTable()
	table.Schema.Columns[0].ID, table.Schema.Columns[1].ID = 0, 1
	return table
}

func TestLogScannerSendsFilterWithSchemaID(t *testing.T) {
	table := filterScannerTable()
	backend := scannerBackend(0)
	backend.listOffsets[0] = 5
	backend.fetches[0] = scannerFetch{records: encodedRows(t, table.Schema, 5, 1)}
	scanner, err := newLogScanner(
		context.Background(), backend, table, Earliest(),
		WithScanProjection("name"), WithScanFilter(Col("id").Gt(4)),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer scanner.Close()
	if _, err := scanner.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	calls := backend.fetchCalls()
	if len(calls) == 0 || calls[0].filter == nil || calls[0].filter.schemaID != table.SchemaID {
		t.Fatalf("fetch calls = %#v", calls)
	}
	leaf := calls[0].filter.predicate.GetLeaf()
	// The filter references the unprojected id column (field ID 0).
	if leaf.GetFunction() != leafGreaterThan || leaf.GetFieldId() != 0 {
		t.Fatalf("filter = %v", calls[0].filter.predicate)
	}
}

func TestLogScannerRejectsInvalidFilters(t *testing.T) {
	arrow := filterScannerTable()
	arrow.Properties = map[string]string{"table.log.format": "ARROW"}
	indexed := filterScannerTable()
	indexed.Properties = map[string]string{"table.log.format": "INDEXED"}
	tests := []struct {
		name  string
		table Table
		pred  Predicate
	}{
		{"indexed log format", indexed, Col("id").Eq(1)},
		{"unknown column", arrow, Col("missing").Eq(1)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := newLogScanner(
				context.Background(), scannerBackend(0), test.table, Earliest(), WithScanFilter(test.pred),
			)
			if !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("newLogScanner() error = %v, want ErrInvalidConfig", err)
			}
		})
	}
	if _, err := newLogScanner(
		context.Background(), scannerBackend(0), arrow, Earliest(), WithScanFilter(Col("id").Eq(1)),
	); err != nil {
		t.Fatalf("ARROW table filter error = %v", err)
	}
}

func TestClientLogScannerBackendEncodesFilter(t *testing.T) {
	path := PhysicalTablePath{TablePath: TablePath{Database: "db", Table: "events"}}
	var fetched *fmsg.FetchLogRequest
	client := routedWriterClient(t,
		metadataRequester(path.TablePath),
		func(_ context.Context, request fmsg.Request) (fmsg.Response, error) {
			response, _ := fmsg.NewResponse(request.APIKey(), request.Version())
			if message, ok := response.Message().(*fmsg.FetchLogResponse); ok {
				fetched = request.(*fmsg.MessageRequest).Message().(*fmsg.FetchLogRequest)
				message.TablesResp = []*fmsg.PbFetchLogRespForTable{{
					TableId: proto.Int64(9), BucketsResp: []*fmsg.PbFetchLogRespForBucket{{BucketId: proto.Int32(0)}},
				}}
			}
			return response, nil
		},
	)
	tablet := client.manager.clients[connectionKey{id: 2, address: "tablet:9123", serverType: TabletServer}]
	tablet.versions[fmsg.APIKeyFetchLog] = 0
	backend := clientLogScannerBackend{client: client}
	predicate, err := compilePredicate(Col("id").Eq(1), filterScannerTable().Schema)
	if err != nil {
		t.Fatal(err)
	}
	config := LogScannerConfig{FetchMaxBytes: 2 << 20, FetchMaxBytesForBucket: 1 << 20}
	if _, err := backend.fetch(context.Background(), logFetchRequest{
		path: path, bucket: 0, tableID: 9, partitionID: -1, config: config,
		filter: &scanFilter{predicate: predicate, schemaID: 3},
	}); err != nil {
		t.Fatal(err)
	}
	table := fetched.GetTablesReq()[0]
	if table.FilterSchemaId == nil || table.GetFilterSchemaId() != 3 || !proto.Equal(table.GetFilterPredicate(), predicate) {
		t.Fatalf("FetchLog table request = %v", table)
	}
	if _, err := backend.fetch(context.Background(), logFetchRequest{
		path: path, bucket: 0, tableID: 9, partitionID: -1, config: config,
	}); err != nil {
		t.Fatal(err)
	}
	if table := fetched.GetTablesReq()[0]; table.FilterPredicate != nil || table.FilterSchemaId != nil {
		t.Fatalf("unfiltered FetchLog table request = %v", table)
	}
}
