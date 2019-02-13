package e2e

import (
	"context"
	"encoding/csv"
	"fmt"
	"strings"
	"testing"

	"git.liveramp.net/jgraet/factable/pkg/extractor"
	"git.liveramp.net/jgraet/factable/pkg/factable"
	"git.liveramp.net/jgraet/factable/pkg/testing/quotes"
	"git.liveramp.net/jgraet/factable/pkg/vtable"
	"github.com/LiveRamp/gazette/v2/pkg/client"
	"github.com/LiveRamp/gazette/v2/pkg/consumertest"
	"github.com/LiveRamp/gazette/v2/pkg/message"
	pb "github.com/LiveRamp/gazette/v2/pkg/protocol"
	gc "github.com/go-check/check"
	"github.com/golang/protobuf/ptypes/empty"
	"google.golang.org/grpc"
)

type E2ESuite struct{}

func (s *E2ESuite) TestE2EWithFixtures(c *gc.C) {
	var tc, cleanup = quotes.NewTestCase(c, quotes.BuildSpecs(4, quotes.MVQuoteStats, quotes.MVWordStats))
	defer cleanup()

	// Start extractor and vtable consumer modules.
	var extCmr = quotes.StartApplication(tc, &extractor.Extractor{Extractors: quotes.BuildExtractors()})
	consumertest.CreateShards(c, extCmr, tc.ExtractorShards...)
	var vtCmr = quotes.StartApplication(tc, new(vtable.VTable))
	consumertest.CreateShards(c, vtCmr, tc.VTableShards...)

	// Publish Quote fixtures.
	var as = client.NewAppendService(tc.Ctx, tc.Journals)
	for _, quote := range []quotes.Quote{
		{ID: 123, Author: "John Doe", Text: "One two, one four?"},
		{ID: 456, Author: "Doe Jane", Text: "Two, two one!"},
		{ID: 789, Author: "Doe Jane", Text: "one one."},
	} {
		_, _ = message.Publish(as, quotes.Mapping, quote)
	}
	client.WaitForPendingAppends(as.PendingExcept(""))

	c.Check(consumertest.WaitForShards(tc.Ctx, tc.Journals, extCmr.Service.Loopback, pb.LabelSelector{}), gc.IsNil)
	c.Check(consumertest.WaitForShards(tc.Ctx, tc.Journals, vtCmr.Service.Loopback, pb.LabelSelector{}), gc.IsNil)

	c.Check(queryRelationCSV(c, tc.Ctx, vtCmr.Service.Loopback, quotes.MVQuoteStats), gc.Equals, ""+
		"Doe Jane,456,1,2,3,2\n"+
		"Doe Jane,789,1,1,2,1\n"+
		"John Doe,123,1,3,4,3\n",
	)
	c.Check(queryRelationCSV(c, tc.Ctx, vtCmr.Service.Loopback, quotes.MVWordStats), gc.Equals, ""+
		"four,John Doe,1,123,1\n"+
		"one,Doe Jane,2,789,3\n"+
		"one,John Doe,1,123,2\n"+
		"two,Doe Jane,1,456,2\n"+
		"two,John Doe,1,123,1\n",
	)

	// Shutdown.
	extCmr.RevokeLease(c)
	vtCmr.RevokeLease(c)
	extCmr.WaitForExit(c)
	vtCmr.WaitForExit(c)
}

func (s *E2ESuite) TestE2EWithSmallQuoteSet(c *gc.C) {
	var tc, cleanup = quotes.NewTestCase(c, quotes.BuildSpecs(4, quotes.MVQuoteStats, quotes.MVWordStats))
	defer cleanup()

	// Start extractor and vtable consumer modules.
	var extCmr = quotes.StartApplication(tc, &extractor.Extractor{Extractors: quotes.BuildExtractors()})
	consumertest.CreateShards(c, extCmr, tc.ExtractorShards...)
	var vtCmr = quotes.StartApplication(tc, new(vtable.VTable))
	consumertest.CreateShards(c, vtCmr, tc.VTableShards...)

	// Publish test quote data.
	var as = client.NewAppendService(tc.Ctx, tc.Journals)
	c.Check(quotes.PublishQuotes(0, 5, "../..", as), gc.IsNil)
	c.Check(quotes.PublishQuotes(100, 105, "../..", as), gc.IsNil)
	c.Check(quotes.PublishQuotes(200, 205, "../..", as), gc.IsNil)

	// Wait for consumers to catch up.
	c.Check(consumertest.WaitForShards(tc.Ctx, tc.Journals, extCmr.Service.Loopback, pb.LabelSelector{}), gc.IsNil)
	c.Check(consumertest.WaitForShards(tc.Ctx, tc.Journals, vtCmr.Service.Loopback, pb.LabelSelector{}), gc.IsNil)

	c.Check(queryRelationCSV(c, tc.Ctx, vtCmr.Service.Loopback, quotes.MVQuoteStats), gc.Equals, ""+
		"A. A. Milne,0,1,16,26,16\n"+
		"A. A. Milne,1,1,15,20,15\n"+
		"A. A. Milne,2,1,10,11,10\n"+
		"A. A. Milne,3,1,15,21,15\n"+
		"A. A. Milne,4,1,10,10,10\n"+
		"Aaron Eckhart,100,1,12,12,12\n"+
		"Aaron Eckhart,101,1,20,20,20\n"+
		"Aaron Eckhart,102,1,17,17,17\n"+
		"Aaron Eckhart,103,1,18,19,18\n"+
		"Aaron Eckhart,104,1,7,7,7\n"+
		"Abhishek Bachchan,200,1,44,52,44\n"+
		"Abhishek Bachchan,201,1,22,23,22\n"+
		"Abhishek Bachchan,202,1,12,12,12\n"+
		"Abhishek Bachchan,203,1,19,21,19\n"+
		"Abhishek Bachchan,204,1,19,24,19\n",
	)

	// Shutdown.
	extCmr.RevokeLease(c)
	vtCmr.RevokeLease(c)
	extCmr.WaitForExit(c)
	vtCmr.WaitForExit(c)
}

func queryRelationCSV(c *gc.C, ctx context.Context, conn *grpc.ClientConn, tag factable.MVTag) string {
	resp, err := factable.NewSchemaClient(conn).GetSchema(ctx, new(empty.Empty))
	c.Check(err, gc.IsNil)
	schema, err := factable.NewSchema(nil, resp.Spec)
	c.Check(err, gc.IsNil)

	var view = schema.Views[tag].View
	var sb strings.Builder
	var enc = csv.NewWriter(&sb)

	stream, err := factable.NewQueryClient(conn).Query(ctx, &factable.QueryRequest{
		View:  tag,
		Query: factable.QuerySpec{View: view},
	})
	c.Check(err, gc.IsNil)

	var it = factable.NewStreamIterator(stream.RecvMsg)
	key, val, err := it.Next()

	for ; err == nil; key, val, err = it.Next() {
		var rec []string

		c.Check(schema.UnmarshalDimensions(key, view.Dimensions, func(f factable.Field) error {
			rec = append(rec, fmt.Sprintf("%v", f))
			return nil
		}), gc.IsNil)
		c.Check(schema.UnmarshalMetrics(val, view.Metrics, func(a factable.Aggregate) error {
			rec = append(rec, fmt.Sprintf("%v", factable.Flatten(a)))
			return nil
		}), gc.IsNil)

		c.Check(enc.Write(rec), gc.IsNil)
	}
	c.Check(err, gc.Equals, factable.KVIteratorDone)

	enc.Flush()
	return sb.String()
}

var _ = gc.Suite(&E2ESuite{})

func TestT(t *testing.T) { gc.TestingT(t) }
