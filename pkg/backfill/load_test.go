package backfill

import (
	"context"
	"github.com/LiveRamp/gazette/v2/pkg/client"
	"github.com/LiveRamp/gazette/v2/pkg/labels"
	"io/ioutil"

	"github.com/LiveRamp/gazette/v2/pkg/brokertest"
	"github.com/LiveRamp/gazette/v2/pkg/etcdtest"
	pb "github.com/LiveRamp/gazette/v2/pkg/protocol"
	gc "github.com/go-check/check"
)

type LoadSuite struct{}

func (s *LoadSuite) TestCombineWithMultipleViews(c *gc.C) {
	var etcd = etcdtest.TestClient()
	defer etcdtest.Cleanup()

	var broker = brokertest.NewBroker(c, etcd, "local", "broker")
	brokertest.CreateJournals(c, broker, brokertest.Journal(pb.JournalSpec{
		Name:     "a/journal",
		LabelSet: pb.MustLabelSet(labels.ContentType, labels.ContentType_JSONLines),
	}))

	var ctx = pb.WithDispatchDefault(context.Background())
	var rjc = pb.NewRoutedJournalClient(broker.Client(), pb.NoopDispatchRouter{})

	var input = buildInput(c, [][2][]byte{
		{[]byte{1}, []byte{2}},
		{[]byte{3}, []byte{4}},
		{[]byte{5}, []byte{6}},
	})

	// Load |input| fixture. All message map to our test journal.
	var selector = pb.LabelSelector{Include: pb.MustLabelSet("name", "a/journal")}
	c.Check(Load(ctx, &input, rjc, selector, "an-extractor"), gc.IsNil)

	var b, err = ioutil.ReadAll(client.NewReader(ctx, rjc, pb.ReadRequest{Journal: "a/journal"}))
	c.Check(err, gc.Equals, client.ErrOffsetNotYetAvailable)

	// Expect to read DeltaEvents with increasing SeqNo, each followed by an acknowledgement.
	c.Check(string(b), gc.Equals, ""+
		"{\"extractor\":\"an-extractor\",\"seq_no\":1,\"row_key\":\"AQ==\",\"row_value\":\"Ag==\"}\n"+
		"{\"extractor\":\"an-extractor\",\"seq_no\":1}\n"+
		"{\"extractor\":\"an-extractor\",\"seq_no\":2,\"row_key\":\"Aw==\",\"row_value\":\"BA==\"}\n"+
		"{\"extractor\":\"an-extractor\",\"seq_no\":2}\n"+
		"{\"extractor\":\"an-extractor\",\"seq_no\":3,\"row_key\":\"BQ==\",\"row_value\":\"Bg==\"}\n"+
		"{\"extractor\":\"an-extractor\",\"seq_no\":3}\n")

	broker.Tasks.Cancel()
	c.Check(broker.Tasks.Wait(), gc.IsNil)
}

var _ = gc.Suite(&LoadSuite{})
