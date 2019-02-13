package vtable

import (
	"context"
	"encoding/json"
	"fmt"
	"io"

	"git.liveramp.net/jgraet/factable/pkg/factable"
	. "git.liveramp.net/jgraet/factable/pkg/internal"
	"git.liveramp.net/jgraet/factable/pkg/testing/quotes"
	"github.com/LiveRamp/gazette/v2/pkg/client"
	"github.com/LiveRamp/gazette/v2/pkg/consumer"
	"github.com/LiveRamp/gazette/v2/pkg/consumertest"
	pb "github.com/LiveRamp/gazette/v2/pkg/protocol"
	gc "github.com/go-check/check"
	"github.com/golang/protobuf/ptypes/empty"
	"google.golang.org/grpc"
)

type APISuite struct{}

func (s *APISuite) TestSupportsFetchingSchema(c *gc.C) {
	var tc, cleanup = quotes.NewTestCase(c, quotes.BuildSpecs(1))
	defer cleanup()

	var cmr = quotes.StartApplication(tc, new(VTable))

	var resp, err = factable.NewSchemaClient(cmr.Service.Loopback).GetSchema(tc.Ctx, &empty.Empty{})
	c.Check(err, gc.IsNil)
	c.Check(resp.Spec, gc.DeepEquals, quotes.BuildSchemaSpec())

	// Shutdown.
	cmr.RevokeLease(c)
	cmr.WaitForExit(c)
}

func (s *APISuite) TestScanShardCases(c *gc.C) {
	var tc, cleanup = quotes.NewTestCase(c, quotes.BuildSpecs(1))
	defer cleanup()

	var cmr = quotes.StartApplication(tc, new(VTable))
	consumertest.CreateShards(c, cmr, tc.VTableShards...)

	// Write fixture data into a single shard partition.
	var wc = client.NewAppender(tc.Ctx, tc.Journals, pb.AppendRequest{Journal: "deltas/part-000"})
	writeDataFixtures(c, wc, 0, 30, 2)
	c.Check(consumertest.WaitForShards(tc.Ctx, tc.Journals, cmr.Service.Loopback, pb.LabelSelector{}), gc.IsNil)

	// Verify queries run against that shard.
	runQueryCases(c, tc, cmr.Service.Loopback, "vtable-part-000")

	// Shutdown.
	cmr.RevokeLease(c)
	cmr.WaitForExit(c)
}

func (s *APISuite) TestScanTableCases(c *gc.C) {
	var tc, cleanup = quotes.NewTestCase(c, quotes.BuildSpecs(3))
	defer cleanup()

	var cmr = quotes.StartApplication(tc, new(VTable))
	consumertest.CreateShards(c, cmr, tc.VTableShards...)

	// Write fixture data across multiple shard partition.
	var wc = client.NewAppender(tc.Ctx, tc.Journals, pb.AppendRequest{Journal: "deltas/part-000"})
	writeDataFixtures(c, wc, 0, 10, 2)
	wc = client.NewAppender(tc.Ctx, tc.Journals, pb.AppendRequest{Journal: "deltas/part-001"})
	writeDataFixtures(c, wc, 10, 20, 2)
	wc = client.NewAppender(tc.Ctx, tc.Journals, pb.AppendRequest{Journal: "deltas/part-002"})
	writeDataFixtures(c, wc, 20, 30, 2)

	c.Check(consumertest.WaitForShards(tc.Ctx, tc.Journals, cmr.Service.Loopback, pb.LabelSelector{}), gc.IsNil)

	// Verify queries run against their tables.
	runQueryCases(c, tc, cmr.Service.Loopback, "")

	// Shutdown.
	cmr.RevokeLease(c)
	cmr.WaitForExit(c)
}

func runQueryCases(c *gc.C, tc quotes.TestCase, conn *grpc.ClientConn, shard consumer.ShardID) {
	// Case: Query requires aggregation but no sorting.
	verifyQuery(c, tc.Ctx, conn, factable.QueryRequest{
		View:  quotes.MVQuoteStats,
		Shard: shard,
		Query: factable.QuerySpec{
			View: factable.ViewSpec{
				RelTag:     quotes.RelQuoteWords,
				Dimensions: []factable.DimTag{quotes.DimQuoteAuthor},
				Metrics:    []factable.MetTag{quotes.MetricSumWordQuoteCount, quotes.MetricUniqueWords},
			},
		},
	}, [][]interface{}{
		// Expect we see aggregations of three rows per DimQuoteAuthor.
		{"00", boxInt(30), factable.BuildStrHLL("0000", "0010", "0020")},
		{"01", boxInt(30), factable.BuildStrHLL("0006", "0016", "0026")},
		{"02", boxInt(30), factable.BuildStrHLL("0002", "0012", "0022")},
		{"03", boxInt(30), factable.BuildStrHLL("0008", "0018", "0028")},
		{"04", boxInt(30), factable.BuildStrHLL("0004", "0014", "0024")},
	})

	// Case: Query requires filtering and sorting, but not post-sort merging.
	verifyQuery(c, tc.Ctx, conn, factable.QueryRequest{
		View:  quotes.MVQuoteStats,
		Shard: shard,
		Query: factable.QuerySpec{
			View: factable.ViewSpec{
				RelTag:     quotes.RelQuoteWords,
				Dimensions: []factable.DimTag{quotes.DimQuoteID},
				Metrics:    []factable.MetTag{quotes.MetricSumQuoteCount},
			},
			Filters: []factable.QuerySpec_Filter{
				{Dimension: quotes.DimQuoteAuthor, Ranges: []factable.Range{
					{Str: &factable.Range_String{Begin: "00", End: "01"}},
				}},
			},
		},
	}, [][]interface{}{
		// Expect we see rows re-ordered on QuoteID
		{int64(0), boxInt(1)},  // Author "00"
		{int64(6), boxInt(1)},  // "01"
		{int64(10), boxInt(1)}, // "00"
		{int64(16), boxInt(1)}, // "01"
		{int64(20), boxInt(1)}, // "00"
		{int64(26), boxInt(1)}, // "01"
	})

	// Case: Query of a different relation. Requires sorting and a post-sort merge.
	verifyQuery(c, tc.Ctx, conn, factable.QueryRequest{
		View:  quotes.MVWordStats,
		Shard: shard,
		Query: factable.QuerySpec{
			View: factable.ViewSpec{
				RelTag:     quotes.RelQuoteWords,
				Dimensions: []factable.DimTag{quotes.DimQuoteAuthor},
				Metrics:    []factable.MetTag{quotes.MetricSumWordQuoteCount},
			},
		},
	}, [][]interface{}{
		{"00", boxInt(3)}, // Word "0000", "0010", "0020".
		{"01", boxInt(3)}, // "0006", "0016", "0026".
		{"02", boxInt(3)}, // "0002", "0012", "0022".
		{"03", boxInt(3)}, // "0008", "0018", "0028".
		{"04", boxInt(3)}, // "0004", "0014", "0024".
	})
}

func (s *APISuite) TestScanErrorCases(c *gc.C) {
	var tc, cleanup = quotes.NewTestCase(c, quotes.BuildSpecs(1))
	defer cleanup()

	var cmr = quotes.StartApplication(tc, new(VTable))
	consumertest.CreateShards(c, cmr, tc.VTableShards...)

	c.Check(consumertest.WaitForShards(tc.Ctx, tc.Journals, cmr.Service.Loopback, pb.LabelSelector{}), gc.IsNil)
	var vtable = factable.NewQueryClient(cmr.Service.Loopback)

	// Case: Malformed QuerySpec.
	var stream, _ = vtable.Query(tc.Ctx, &factable.QueryRequest{
		View: quotes.MVWordStats,
		Query: factable.QuerySpec{
			View: factable.ViewSpec{
				RelTag:     quotes.RelQuoteWords,
				Dimensions: []factable.DimTag{quotes.DimQuoteAuthor, quotes.DimQuoteAuthor},
			},
		},
	})
	var _, err = stream.Recv()
	c.Check(err, gc.ErrorMatches, `rpc error: code = Unknown desc = Query.View.Dimensions\[1\]: duplicated Dimension Tag .*`)

	// Case: Query of an unknown view.
	stream, _ = vtable.Query(tc.Ctx, &factable.QueryRequest{
		View: 9999,
		Query: factable.QuerySpec{
			View: factable.ViewSpec{RelTag: quotes.RelQuoteWords},
		},
	})
	_, err = stream.Recv()
	c.Check(err, gc.ErrorMatches, `rpc error: code = Unknown desc = view not found: 9999`)

	// Case: Query of an shard which fails to resolve.
	stream, _ = vtable.Query(tc.Ctx, &factable.QueryRequest{
		View:  quotes.MVWordStats,
		Shard: "some-other-shard",
		Query: factable.QuerySpec{
			View: factable.ViewSpec{RelTag: quotes.RelQuoteWords},
		},
	})
	_, err = stream.Recv()
	c.Check(err, gc.ErrorMatches, `rpc error: code = Unknown desc = SHARD_NOT_FOUND`)

	// Case: Query with an incorrect dimension.
	for _, shard := range []consumer.ShardID{"", "vtable-part-000"} {
		stream, _ = vtable.Query(tc.Ctx, &factable.QueryRequest{
			View:  quotes.MVWordStats,
			Shard: shard,
			Query: factable.QuerySpec{
				View: factable.ViewSpec{
					RelTag:     quotes.RelQuoteWords,
					Dimensions: []factable.DimTag{quotes.DimQuoteID},
				},
			},
		})
		_, err = stream.Recv()
		c.Check(err, gc.ErrorMatches, `rpc error: code = Unknown desc = Query Dimension tag \d not in input ViewSpec .*`)
	}

	// Case: Query missing the RelTag.
	stream, _ = vtable.Query(tc.Ctx, &factable.QueryRequest{
		View: quotes.MVWordStats,
		Query: factable.QuerySpec{
			View: factable.ViewSpec{},
		},
	})
	_, err = stream.Recv()
	c.Check(err, gc.ErrorMatches, `rpc error: code = Unknown desc = Query.View: invalid RelTag \(0; expected > 0\)`)

	// Shutdown.
	cmr.RevokeLease(c)
	cmr.WaitForExit(c)
}

func writeDataFixtures(c *gc.C, wc io.WriteCloser, begin, end, stride int64) {
	var enc = json.NewEncoder(wc)

	// Dimensions: []factable.DimTag{DimQuoteAuthor, DimQuoteID},
	// Metrics:    []factable.MetTag{MetricSumQuoteCount, MetricSumWordQuoteCount, MetricSumWordTotalCount, MetricUniqueWords},
	for i := begin; i < end; i += stride {
		c.Check(enc.Encode(&DeltaEvent{
			Extractor: "quote-extractor",
			SeqNo:     i + 1,
			RowKey:    factable.PackKey(quotes.MVQuoteStats, fmt.Sprintf("%02d", i%5), i),
			RowValue:  factable.PackValue(1, 10, 100, factable.BuildStrHLL(fmt.Sprintf("%04d", i))),
		}), gc.IsNil)
	}
	c.Check(enc.Encode(&DeltaEvent{Extractor: "quote-extractor", SeqNo: end}), gc.IsNil) // Commit.

	// Dimensions: []factable.DimTag{DimQuoteWord, DimQuoteAuthor},
	// Metrics:    []factable.MetTag{MetricSumWordQuoteCount, MetricLastQuoteID, MetricSumWordTotalCount},
	for i := begin; i < end; i += stride {
		c.Check(enc.Encode(&DeltaEvent{
			Extractor: "word-extractor",
			SeqNo:     i + 1,
			RowKey:    factable.PackKey(quotes.MVWordStats, fmt.Sprintf("%04d", i), fmt.Sprintf("%02d", i%5)),
			RowValue:  factable.PackValue(1, 10, 100),
		}), gc.IsNil)
	}
	c.Check(enc.Encode(&DeltaEvent{Extractor: "word-extractor", SeqNo: end}), gc.IsNil) // Commit.

	c.Check(wc.Close(), gc.IsNil)
}

func verifyQuery(c *gc.C, ctx context.Context, conn *grpc.ClientConn, req factable.QueryRequest, expect [][]interface{}) {
	var vtable = factable.NewQueryClient(conn)
	var stream, err = vtable.Query(ctx, &req)
	c.Assert(err, gc.IsNil)

	var it = factable.NewStreamIterator(stream.RecvMsg)
	var schema, _ = factable.NewSchema(nil, quotes.BuildSchemaSpec())

	key, val, err := it.Next()
	for ; err == nil; key, val, err = it.Next() {
		var o []interface{}

		// Unpack Fields and Aggregates sent by the server.
		c.Check(schema.UnmarshalDimensions(key, req.Query.View.Dimensions, func(f factable.Field) error {
			o = append(o, f)
			return nil
		}), gc.IsNil)
		c.Check(schema.UnmarshalMetrics(val, req.Query.View.Metrics, func(a factable.Aggregate) error {
			o = append(o, a)
			return nil
		}), gc.IsNil)

		// Check they match our expectation.
		c.Check(o, gc.DeepEquals, expect[0])
		expect = expect[1:]
	}
	c.Check(err, gc.Equals, factable.KVIteratorDone)
	c.Check(expect, gc.HasLen, 0)
}

var _ = gc.Suite(&APISuite{})
