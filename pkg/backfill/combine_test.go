package backfill

import (
	"bufio"
	"bytes"

	"git.liveramp.net/jgraet/factable/pkg/factable"
	"git.liveramp.net/jgraet/factable/pkg/testing/quotes"
	gc "github.com/go-check/check"
)

type CombineSuite struct{}

func (s *CombineSuite) TestCombineWithMultipleViews(c *gc.C) {
	var extractFns = quotes.BuildExtractors()
	var schema, _ = factable.NewSchema(&extractFns, quotes.BuildSchemaSpec())

	var input = buildInput(c, [][2][]byte{
		{
			factable.PackKey(quotes.MVWordStatsTag, "sweet", "Zach Zeta"),
			factable.PackValue(1, 5678, 1),
		},
		{
			factable.PackKey(quotes.MVWordStatsTag, "sweet", "Zach Zeta"),
			factable.PackValue(2, 7890, 2),
		},
		{
			factable.PackKey(quotes.MVQuoteStatsTag, "John Doe", 1234),
			factable.PackValue(1, 1, 1, factable.BuildStrHLL("four")),
		},
		{
			factable.PackKey(quotes.MVQuoteStatsTag, "John Doe", 1234),
			factable.PackValue(0, 1, 2, factable.BuildStrHLL("one")),
		},
		{
			factable.PackKey(quotes.MVQuoteStatsTag, "John Doe", 1234),
			factable.PackValue(0, 1, 1, factable.BuildStrHLL("two")),
		},
		{
			factable.PackKey(quotes.MVQuoteStatsTag, "Zach Zeta", 5678),
			factable.PackValue(1, 1, 3, factable.BuildStrHLL("sweet")),
		},
	})

	var output bytes.Buffer
	c.Check(combine(bufio.NewReader(&input), schema, bufio.NewWriter(&output)), gc.IsNil)

	var expect = [][2][]byte{
		{
			factable.PackKey(quotes.MVWordStatsTag, "sweet", "Zach Zeta"),
			factable.PackValue(3, 7890, 3),
		},
		{
			factable.PackKey(quotes.MVQuoteStatsTag, "John Doe", 1234),
			factable.PackValue(1, 3, 4, factable.BuildStrHLL("one", "two", "four")),
		},
		{
			factable.PackKey(quotes.MVQuoteStatsTag, "Zach Zeta", 5678),
			factable.PackValue(1, 1, 3, factable.BuildStrHLL("sweet")),
		},
	}
	verifyIter(c,
		factable.NewHexIterator(bufio.NewReader(&output)),
		factable.NewSliceIterator(expect...))
}

func buildInput(c *gc.C, kvs [][2][]byte) bytes.Buffer {
	var (
		input bytes.Buffer
		bw    = bufio.NewWriter(&input)
		enc   = factable.NewHexEncoder(bw)
	)
	for _, kv := range kvs {
		c.Check(enc.Encode(kv[0], kv[1]), gc.IsNil)
	}
	c.Check(bw.Flush(), gc.IsNil)
	return input
}

var _ = gc.Suite(&CombineSuite{})
