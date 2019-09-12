package internal

import (
	"context"
	"testing"

	"github.com/LiveRamp/factable/pkg/factable"
	"github.com/LiveRamp/factable/pkg/testing/quotes"
	"github.com/LiveRamp/gazette/v2/pkg/etcdtest"
	pb "github.com/LiveRamp/gazette/v2/pkg/protocol"
	gc "github.com/go-check/check"
	"github.com/golang/protobuf/ptypes/empty"
)

type SchemaSuite struct{}

func (s *SchemaSuite) TestRoundTrip(c *gc.C) {
	var etcd = etcdtest.TestClient()
	defer etcdtest.Cleanup()

	var ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	var ks = NewSchemaKeySpace("/path/to/shared/schema", nil)
	ks.WatchApplyDelay = 0

	c.Check(ks.Load(ctx, etcd, 0), gc.IsNil)
	go ks.Watch(ctx, etcd)

	var extractFns = quotes.BuildExtractors()
	var ss = SchemaService{
		Config: CommonConfig{
			Instance: "test-instance",
			Deltas:   "topic = my/topic",
		},
		KS:         ks,
		Etcd:       etcd,
		ExtractFns: &extractFns,
	}
	c.Check(ss.Schema(), gc.DeepEquals, new(factable.Schema))

	var _, err = ss.UpdateSchema(ctx, &factable.UpdateSchemaRequest{
		ExpectModRevision: 0,
		ExpectInstance:    "test-instance",
		Update:            quotes.BuildSchemaSpec(),
	})
	c.Check(err, gc.IsNil)

	resp, err := ss.GetSchema(ctx, new(empty.Empty))
	c.Check(err, gc.IsNil)
	c.Check(resp.Spec, gc.DeepEquals, quotes.BuildSchemaSpec())
	c.Check(resp.ModRevision, gc.Not(gc.Equals), int64(0))
	c.Check(resp.Instance, gc.Equals, "test-instance")
	c.Check(resp.DeltaPartitions, gc.DeepEquals, pb.LabelSelector{
		Include: pb.MustLabelSet("topic", "my/topic"),
	})

	// Expect we see our updates applied.
	c.Check(ss.Schema().Spec, gc.DeepEquals, quotes.BuildSchemaSpec())
}

var _ = gc.Suite(&SchemaSuite{})

func TestT(t *testing.T) { gc.TestingT(t) }
