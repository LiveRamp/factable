package extractor

import (
	"encoding/json"
	"fmt"
	"io"
	"testing"

	"git.liveramp.net/jgraet/factable/pkg/factable"
	. "git.liveramp.net/jgraet/factable/pkg/internal"
	"git.liveramp.net/jgraet/factable/pkg/testing/quotes"
	"github.com/LiveRamp/gazette/v2/pkg/client"
	"github.com/LiveRamp/gazette/v2/pkg/consumer"
	"github.com/LiveRamp/gazette/v2/pkg/consumertest"
	"github.com/LiveRamp/gazette/v2/pkg/mainboilerplate/runconsumer"
	"github.com/LiveRamp/gazette/v2/pkg/message"
	pb "github.com/LiveRamp/gazette/v2/pkg/protocol"
	gc "github.com/go-check/check"
	"github.com/golang/protobuf/ptypes/empty"
)

type ExtractorSuite struct{}

func (s *ExtractorSuite) TestFoldsOverUniqueWords(c *gc.C) {
	var tc, cleanup = quotes.NewTestCase(c, quotes.BuildSpecs(1, quotes.MVQuoteStatsTag))
	defer cleanup()

	var app = &Extractor{Extractors: quotes.BuildExtractors()}
	var cmr = quotes.StartApplication(tc, app)
	consumertest.CreateShards(c, cmr, tc.ExtractorShards...)

	var as = client.NewAppendService(tc.Ctx, tc.Journals)
	var _, err = message.Publish(as, quotes.Mapping, quotes.Quote{
		ID: 1234, Author: "John Doe", Text: "One two, one four?"})
	c.Assert(err, gc.IsNil)

	var txn = newTxnReader(client.NewReader(tc.Ctx, tc.Journals,
		pb.ReadRequest{Journal: "deltas/part-000", Block: true}))

	// The Mapping function emits an input record for each unique word, which a Extractor for
	// "quoteStats" should have folded over, producing a total word count and unique word HLL.
	c.Check(txn.next(c), gc.DeepEquals, DeltaEvent{
		Extractor: fmt.Sprintf("extractor-%d", quotes.MVQuoteStatsTag),
		SeqNo:     1,
		RowKey:    factable.PackKey(quotes.MVQuoteStatsTag, "John Doe", 1234),
		RowValue:  factable.PackValue(1, 3, 4, factable.BuildStrHLL("one", "two", "four")),
	})

	// Shutdown.
	cmr.Tasks.Cancel()
	c.Check(cmr.Tasks.Wait(), gc.IsNil)
}

func (s *ExtractorSuite) TestDistinctRowsForUniqueWords(c *gc.C) {
	var tc, cleanup = quotes.NewTestCase(c, quotes.BuildSpecs(1, quotes.MVWordStatsTag))
	defer cleanup()

	var cmr = quotes.StartApplication(tc, &Extractor{Extractors: quotes.BuildExtractors()})
	consumertest.CreateShards(c, cmr, tc.ExtractorShards...)

	var as = client.NewAppendService(tc.Ctx, tc.Journals)
	var _, err = message.Publish(as, quotes.Mapping, quotes.Quote{
		ID: 1234, Author: "John Doe", Text: "One two, one three?"})
	c.Assert(err, gc.IsNil)

	var txn = newTxnReader(client.NewReader(tc.Ctx, tc.Journals,
		pb.ReadRequest{Journal: "deltas/part-000", Block: true}))

	// The Mapping function emits an input record for each unique word. A Extractor for
	// "wordStats" should have emitted a separate DeltaEvent for each.
	var expect = map[string][]byte{
		string(factable.PackKey(quotes.MVWordStatsTag, "one", "John Doe")):   factable.PackValue(1, 1234, 2),
		string(factable.PackKey(quotes.MVWordStatsTag, "two", "John Doe")):   factable.PackValue(1, 1234, 1),
		string(factable.PackKey(quotes.MVWordStatsTag, "three", "John Doe")): factable.PackValue(1, 1234, 1),
	}
	for len(expect) != 0 {
		var delta = txn.next(c)
		c.Check(delta.Extractor, gc.Equals, fmt.Sprintf("extractor-%d", quotes.MVWordStatsTag))

		c.Check(expect[string(delta.RowKey)], gc.DeepEquals, delta.RowValue)
		delete(expect, string(delta.RowKey))
	}

	// Shutdown.
	cmr.Tasks.Cancel()
	c.Check(cmr.Tasks.Wait(), gc.IsNil)
}

func (s *ExtractorSuite) TestMultipleTransactionsAndRecovery(c *gc.C) {
	var tc, cleanup = quotes.NewTestCase(c, quotes.BuildSpecs(1, quotes.MVQuoteStatsTag))
	defer cleanup()

	var cmr = quotes.StartApplication(tc, &Extractor{Extractors: quotes.BuildExtractors()})
	consumertest.CreateShards(c, cmr, tc.ExtractorShards...)

	var expectRows int // Number of committed rows we expect to see.

	// Publish quotes, waiting for the shard each time to force multiple consumer transactions.
	var as = client.NewAppendService(tc.Ctx, tc.Journals)
	for _, rng := range [][2]int{{0, 50}, {50, 99}, {99, 150}} {
		c.Assert(quotes.PublishQuotes(rng[0], rng[1], "..", as), gc.IsNil)
		c.Assert(consumertest.WaitForShards(tc.Ctx, tc.Journals, cmr.Service.Loopback, pb.LabelSelector{}), gc.IsNil)

		expectRows += rng[1] - rng[0] // This RelationSpec fixture results in one row per input quote.
	}

	// Read extractor output, collecting into a Transactions instance.
	var txn = newTxnReader(client.NewReader(tc.Ctx, tc.Journals,
		pb.ReadRequest{Journal: "deltas/part-000", Block: true}))

	for expectRows != 0 {
		txn.next(c)
		expectRows--
	}
	c.Assert(txn.nextDeltas, gc.HasLen, 0)

	// Crash this consumer.
	cmr.Tasks.Cancel()
	c.Check(cmr.Tasks.Wait(), gc.IsNil)

	// Start a new consumer.
	cmr = quotes.StartApplication(tc, &Extractor{Extractors: quotes.BuildExtractors()})
	<-cmr.AllocateIdleCh() // Shard is assigned.

	// Expect the new consumer writes a replay of the previous acknowledgement.
	var msg DeltaEvent
	c.Assert(txn.Decode(&msg), gc.IsNil)
	c.Check(msg, gc.DeepEquals, txn.Extractor[fmt.Sprintf("extractor-%d", quotes.MVQuoteStatsTag)].Events[0])

	// Shutdown.
	cmr.Tasks.Cancel()
	c.Check(cmr.Tasks.Wait(), gc.IsNil)
}

func (s *ExtractorSuite) TestSupportsFetchingSchema(c *gc.C) {
	var tc, cleanup = quotes.NewTestCase(c, quotes.BuildSpecs(1))
	defer cleanup()

	var cmr = quotes.StartApplication(tc, &Extractor{Extractors: quotes.BuildExtractors()})

	var resp, err = factable.NewSchemaClient(cmr.Service.Loopback).GetSchema(tc.Ctx, &empty.Empty{})
	c.Check(err, gc.IsNil)
	c.Check(resp.Spec, gc.DeepEquals, quotes.BuildSchemaSpec())

	// Shutdown.
	cmr.Tasks.Cancel()
	c.Check(cmr.Tasks.Wait(), gc.IsNil)
}

type txnReader struct {
	Transactions
	*json.Decoder
	nextDeltas []DeltaEvent
}

func newTxnReader(r io.Reader) *txnReader {
	return &txnReader{
		Transactions: Transactions{Extractor: make(map[string]Transactions_State)},
		Decoder:      json.NewDecoder(r),
	}
}

func (tr *txnReader) next(c *gc.C) DeltaEvent {
	for len(tr.nextDeltas) == 0 {
		var msg DeltaEvent
		var err error

		c.Assert(tr.Decode(&msg), gc.IsNil)

		tr.nextDeltas, err = tr.Apply(msg)
		c.Check(err, gc.IsNil)
	}

	var de = tr.nextDeltas[0]
	tr.nextDeltas = tr.nextDeltas[1:]
	return de
}

var (
	_ = gc.Suite(&ExtractorSuite{})
	// Expect Extractor implements BeginFinisher
	_ consumer.BeginFinisher = new(Extractor)
	// Expect Extractor implements Application
	_ runconsumer.Application = new(Extractor)
)

func TestT(t *testing.T) { gc.TestingT(t) }
