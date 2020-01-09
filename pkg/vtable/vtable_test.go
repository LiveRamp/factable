package vtable

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/LiveRamp/factable/pkg/factable"
	. "github.com/LiveRamp/factable/pkg/internal"
	"github.com/LiveRamp/factable/pkg/testing/quotes"
	"go.gazette.dev/core/mainboilerplate"
	"go.gazette.dev/core/consumer"
	"go.gazette.dev/core/consumertest"
	"go.gazette.dev/core/mainboilerplate/runconsumer"
	pb "go.gazette.dev/core/consumer/protocol"
	gc "github.com/go-check/check"
)

type VTableSuite struct{}

func (s *VTableSuite) TestAccumulateCases(c *gc.C) {
	var tc, cleanup = quotes.NewTestCase(c, quotes.BuildSpecs(1))
	defer cleanup()

	var cmr = quotes.StartApplication(tc, new(VTable))
	consumertest.CreateShards(c, cmr, tc.VTableShards...)

	var res, err = cmr.Service.Resolver.Resolve(consumer.ResolveArgs{
		Context: tc.Ctx,
		ShardID: "vtable-part-000",
	})
	c.Assert(err, gc.IsNil)

	var (
		rdb   = res.Store.(*consumer.RocksDBStore)
		state = rdb.Cache.(*shardState)
	)

	// Note that Relation RelWordStats is:
	// Dimensions: []schema.DimTag{DimQuoteWord, DimQuoteAuthor},
	// Metrics:    []schema.MetTag{MetricSumWordQuoteCount, MetricLastQuoteID, MetricSumWordTotalCount},

	// Write a row fixture to the database which accumulate() will read later.
	c.Check(rdb.DB.Put(rdb.WriteOptions,
		factable.PackKey(quotes.MVWordStatsTag, "present", "record"),
		factable.PackValue(100, 200, 300),
	), gc.IsNil)

	// Case: New record not already in DB or updates.
	c.Check(accumulate(state, rdb, DeltaEvent{
		RowKey:   factable.PackKey(quotes.MVWordStatsTag, "missing", "record"),
		RowValue: factable.PackValue(1, 2, 3),
	}), gc.IsNil)
	// Case: Record which is in DB but not updates.
	c.Check(accumulate(state, rdb, DeltaEvent{
		RowKey:   factable.PackKey(quotes.MVWordStatsTag, "present", "record"),
		RowValue: factable.PackValue(4, 5, 6),
	}), gc.IsNil)

	// Expect |state.updates| represents merged records.
	c.Check(state.updates, gc.DeepEquals, map[string][]factable.Aggregate{
		string(factable.PackKey(quotes.MVWordStatsTag, "missing", "record")): boxInts(1, 2, 3),
		string(factable.PackKey(quotes.MVWordStatsTag, "present", "record")): boxInts(104, 5, 306),
	})

	// Case: Record which is in updates but not DB.
	c.Check(accumulate(state, rdb, DeltaEvent{
		RowKey:   factable.PackKey(quotes.MVWordStatsTag, "missing", "record"),
		RowValue: factable.PackValue(7, 8, 9),
	}), gc.IsNil)
	// Case: Record which is in updates, and stale in DB.
	c.Check(accumulate(state, rdb, DeltaEvent{
		RowKey:   factable.PackKey(quotes.MVWordStatsTag, "present", "record"),
		RowValue: factable.PackValue(10, 11, 12),
	}), gc.IsNil)
	// Case: Record having a RelTag which is not in current schema. Expect it's ignored.
	c.Check(accumulate(state, rdb, DeltaEvent{
		RowKey:   factable.PackKey(9999, "foo", "bar"),
		RowValue: factable.PackValue(1),
	}), gc.IsNil)

	// Expect |state.updates| represents record updates.
	c.Check(state.updates, gc.DeepEquals, map[string][]factable.Aggregate{
		string(factable.PackKey(quotes.MVWordStatsTag, "missing", "record")): boxInts(8, 8, 12),
		string(factable.PackKey(quotes.MVWordStatsTag, "present", "record")): boxInts(114, 11, 318),
	})

	res.Done()
	cmr.Tasks.Cancel()
	c.Check(cmr.Tasks.Wait(), gc.IsNil)
}

func (s *VTableSuite) TestExtractorTransactions(c *gc.C) {
	var tc, cleanup = quotes.NewTestCase(c, quotes.BuildSpecs(1))
	defer cleanup()

	var cmr = quotes.StartApplication(tc, new(VTable))
	consumertest.CreateShards(c, cmr, tc.VTableShards...)

	// Write deltas for the same record from two extractors, only one of which commits.
	var wc = client.NewAppender(tc.Ctx, tc.Journals, pb.AppendRequest{Journal: "deltas/part-000"})
	var enc = json.NewEncoder(wc)

	c.Check(enc.Encode(&DeltaEvent{
		Extractor: "extractor-A",
		SeqNo:     1,
		RowKey:    factable.PackKey(quotes.MVWordStatsTag, "a", "record"),
		RowValue:  factable.PackValue(1, 2, 3),
	}), gc.IsNil)
	c.Check(enc.Encode(&DeltaEvent{
		Extractor: "extractor-B",
		SeqNo:     1,
		RowKey:    factable.PackKey(quotes.MVWordStatsTag, "a", "record"),
		RowValue:  factable.PackValue(4, 5, 6),
	}), gc.IsNil)
	c.Check(enc.Encode(&DeltaEvent{Extractor: "extractor-A", SeqNo: 1}), gc.IsNil) // Commit.
	c.Check(wc.Close(), gc.IsNil)

	// Expect that we can read extractor-A's delta, but not extractor-B.
	c.Check(consumertest.WaitForShards(tc.Ctx, tc.Journals, cmr.Service.Loopback, pb.LabelSelector{}), gc.IsNil)
	expectDB(c, cmr, map[string][]byte{
		string(factable.PackKey(quotes.MVWordStatsTag, "a", "record")): factable.PackValue(1, 2, 3),
	})

	// Write a new uncommitted update from extractor-A, and a commit for extractor-B.
	wc = client.NewAppender(tc.Ctx, tc.Journals, pb.AppendRequest{Journal: "deltas/part-000"})
	enc = json.NewEncoder(wc)

	c.Check(enc.Encode(&DeltaEvent{
		Extractor: "extractor-A",
		SeqNo:     2,
		RowKey:    factable.PackKey(quotes.MVWordStatsTag, "a", "record"),
		RowValue:  factable.PackValue(100, 200, 300),
	}), gc.IsNil)
	c.Check(enc.Encode(&DeltaEvent{Extractor: "extractor-B", SeqNo: 1}), gc.IsNil) // Commit.
	c.Check(wc.Close(), gc.IsNil)

	// Expect that we read both initial deltas, but not extractor-A's new one.
	c.Check(consumertest.WaitForShards(tc.Ctx, tc.Journals, cmr.Service.Loopback, pb.LabelSelector{}), gc.IsNil)
	expectDB(c, cmr, map[string][]byte{
		string(factable.PackKey(quotes.MVWordStatsTag, "a", "record")): factable.PackValue(5, 5, 9),
	})

	// Crash this consumer, and start a new one.
	cmr.Tasks.Cancel()
	c.Check(cmr.Tasks.Wait(), gc.IsNil)

	cmr = quotes.StartApplication(tc, new(VTable))
	<-cmr.AllocateIdleCh() // Shard is assigned.

	// Write a commit of extractor-A's uncommitted delta.
	wc = client.NewAppender(tc.Ctx, tc.Journals, pb.AppendRequest{Journal: "deltas/part-000"})
	c.Check(json.NewEncoder(wc).Encode(&DeltaEvent{Extractor: "extractor-A", SeqNo: 2}), gc.IsNil)
	c.Check(wc.Close(), gc.IsNil)

	// Expect that we merged all records.
	c.Check(consumertest.WaitForShards(tc.Ctx, tc.Journals, cmr.Service.Loopback, pb.LabelSelector{}), gc.IsNil)
	expectDB(c, cmr, map[string][]byte{
		string(factable.PackKey(quotes.MVWordStatsTag, "a", "record")): factable.PackValue(105, 200, 309),
	})

	cmr.Tasks.Cancel()
	c.Check(cmr.Tasks.Wait(), gc.IsNil)
}

func expectDB(c *gc.C, cmr *consumertest.Consumer, expect map[string][]byte) {
	var res, err = cmr.Service.Resolver.Resolve(consumer.ResolveArgs{
		Context: context.Background(),
		ShardID: "vtable-part-000",
	})
	c.Assert(err, gc.IsNil)
	defer res.Done()

	var (
		rdb = res.Store.(*consumer.RocksDBStore)
		it  = rdb.DB.NewIterator(rdb.ReadOptions)
	)
	for it.Seek([]byte{0x1}); it.Valid(); it.Next() {
		c.Check(it.Value().Data(), gc.DeepEquals, expect[string(it.Key().Data())])
		delete(expect, string(it.Key().Data()))
	}
	c.Check(expect, gc.HasLen, 0)
}

func boxInt(i int64) factable.Aggregate { return &i }

func boxInts(args ...int64) (out []factable.Aggregate) {
	for i := range args {
		out = append(out, boxInt(args[i]))
	}
	return
}

var (
	_ = gc.Suite(&VTableSuite{})
	// Expect VTable implements BeginFinisher
	_ consumer.BeginFinisher = new(VTable)
	// Expect VTable implements Application
	_ runconsumer.Application = new(VTable)
)

func TestT(t *testing.T) { gc.TestingT(t) }
