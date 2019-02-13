package backfill

import (
	"bufio"
	"bytes"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"git.liveramp.net/jgraet/factable/pkg/factable"
	"git.liveramp.net/jgraet/factable/pkg/testing/quotes"
	"github.com/LiveRamp/gazette/v2/pkg/client"
	"github.com/LiveRamp/gazette/v2/pkg/labels"
	"github.com/LiveRamp/gazette/v2/pkg/message"
	pb "github.com/LiveRamp/gazette/v2/pkg/protocol"
	gc "github.com/go-check/check"
)

type MapSuite struct{}

func (s *MapSuite) TestDecode(c *gc.C) {
	var ch, _, task = goDecodeFixture(c)

	var env = <-ch
	c.Check(*env.Fragment, gc.DeepEquals, task.Fragment)
	c.Check(*env.JournalSpec, gc.DeepEquals, task.JobSpec.Inputs[quotes.InputJournal])
	c.Check(env.Message, gc.DeepEquals, &quotes.Quote{ID: 1234, Author: "John Doe", Text: "One two, one four?"})

	var _, ok = <-ch
	c.Check(ok, gc.Equals, false)
}

func (s *MapSuite) TestExtractNoAggregation(c *gc.C) {
	var ch, schema, task = goDecodeFixture(c)

	var buf bytes.Buffer
	c.Check(extract(ch, schema, task, 1, bufio.NewWriter(&buf)), gc.IsNil)

	// Expect to see one output aggregate for each processed RelationRow.
	var expect = [][2][]byte{
		{
			factable.PackKey(quotes.MVQuoteStats, "John Doe", 1234),
			factable.PackValue(1, 1, 1, factable.BuildStrHLL("four")),
		},
		{
			factable.PackKey(quotes.MVQuoteStats, "John Doe", 1234),
			factable.PackValue(0, 1, 2, factable.BuildStrHLL("one")),
		},
		{
			factable.PackKey(quotes.MVQuoteStats, "John Doe", 1234),
			factable.PackValue(0, 1, 1, factable.BuildStrHLL("two")),
		},
		{
			factable.PackKey(quotes.MVWordStats, "four", "John Doe"),
			factable.PackValue(1, 1234, 1),
		},
		{
			factable.PackKey(quotes.MVWordStats, "one", "John Doe"),
			factable.PackValue(1, 1234, 2),
		},
		{
			factable.PackKey(quotes.MVWordStats, "two", "John Doe"),
			factable.PackValue(1, 1234, 1),
		},
	}

	verifyIter(c,
		factable.NewHexIterator(bufio.NewReader(&buf)),
		factable.NewSliceIterator(expect...))
}

func (s *MapSuite) TestExtractWithAggregation(c *gc.C) {
	var ch, schema, task = goDecodeFixture(c)

	var buf bytes.Buffer
	c.Check(extract(ch, schema, task, 100, bufio.NewWriter(&buf)), gc.IsNil)

	var it factable.KVIterator = factable.NewHexIterator(bufio.NewReader(&buf))

	// Expect to see one aggregate for each rolled-up view key.
	var expect = [][2][]byte{
		{
			factable.PackKey(quotes.MVWordStats, "four", "John Doe"),
			factable.PackValue(1, 1234, 1),
		},
		{
			factable.PackKey(quotes.MVWordStats, "one", "John Doe"),
			factable.PackValue(1, 1234, 2),
		},
		{
			factable.PackKey(quotes.MVWordStats, "two", "John Doe"),
			factable.PackValue(1, 1234, 1),
		},
		{
			factable.PackKey(quotes.MVQuoteStats, "John Doe", 1234),
			factable.PackValue(1, 3, 4, factable.BuildStrHLL("one", "two", "four")),
		},
	}

	// extract's output order is random, as it walks a map to encode in-memory aggregates.
	// We must sort to stably compare against the expectation.
	it = factable.NewSortingIterator(it, 4*1024, 10)

	verifyIter(c, it, factable.NewSliceIterator(expect...))
}

func goDecodeFixture(c *gc.C) (<-chan message.Envelope, factable.Schema, MapTaskSpec) {
	var extractFns = quotes.BuildExtractors()
	var schema, _ = factable.NewSchema(&extractFns, quotes.BuildSchemaSpec())

	var jobSpec = buildJobFixture(schema)
	var task, cleanup = buildTaskFixture(c, &jobSpec)

	var ch = make(chan message.Envelope)
	go func() {
		c.Check(decode(task, schema, ch), gc.IsNil)
		cleanup()
		close(ch)
	}()

	return ch, schema, task
}

func buildJobFixture(schema factable.Schema) JobSpec {
	return JobSpec{
		SchemaSpec: schema.Spec,

		Inputs: map[pb.Journal]pb.JournalSpec{
			quotes.InputJournal: {
				Name: quotes.InputJournal,
				LabelSet: pb.MustLabelSet(
					labels.MessageType, "Quote",
					labels.ContentType, labels.ContentType_JSONLines,
				),
			},
		},
	}
}

func buildTaskFixture(c *gc.C, jobSpec *JobSpec) (task MapTaskSpec, cleanup func()) {
	const data = `` +
		`{"ID": 1234, "Author": "John Doe", "Text": "One two, one four?"}
`

	var dir, err = ioutil.TempDir("", "MapSuite")
	c.Assert(err, gc.IsNil)

	task = MapTaskSpec{
		JobSpec: jobSpec,
		Fragment: pb.Fragment{
			Journal:          quotes.InputJournal,
			Begin:            100,
			End:              100 + int64(len(data)),
			Sum:              pb.SHA1SumOf(data),
			CompressionCodec: pb.CompressionCodec_NONE,
			BackingStore:     pb.FragmentStore("file:///"),
		},
		MVTags: []factable.MVTag{quotes.MVQuoteStats, quotes.MVWordStats},
	}
	c.Check(ioutil.WriteFile(filepath.Join(dir, task.Fragment.ContentName()), []byte(data), 0600), gc.IsNil)
	task.URL = string(task.Fragment.BackingStore) + task.Fragment.ContentName()

	var removeFileTransport = client.InstallFileTransport(dir)

	return task, func() {
		removeFileTransport()
		c.Check(os.RemoveAll(dir), gc.IsNil)
	}
}

func verifyIter(c *gc.C, it, expect factable.KVIterator) {
	for {
		var key, value, err = it.Next()
		var exKey, exValue, exErr = expect.Next()

		c.Check(err, gc.Equals, exErr)
		if err != nil {
			break
		}
		c.Check(key, gc.DeepEquals, exKey)
		c.Check(value, gc.DeepEquals, exValue)
	}
}

var _ = gc.Suite(&MapSuite{})

func TestT(t *testing.T) { gc.TestingT(t) }
