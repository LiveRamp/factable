package vtable

import (
	"time"

	"github.com/LiveRamp/factable/pkg/factable"
	"github.com/LiveRamp/factable/pkg/testing/quotes"
	gc "github.com/go-check/check"
	"github.com/tecbot/gorocksdb"
)

type CFSuite struct{}

func (s *CFSuite) TestFilterCases(c *gc.C) {
	var schema, err = factable.NewSchema(nil, quotes.BuildSchemaSpec())
	c.Assert(err, gc.IsNil)

	var schemaFn = func() *factable.Schema { return &schema }
	var cf = compactionFilter(schemaFn)

	// Temporarily fix |approxUnixTime|.
	var timeFixture int64 = 10000
	defer func(src *int64) { approxUnixTime = src }(approxUnixTime)
	approxUnixTime = &timeFixture

	var cases = []struct {
		key    []byte
		expect bool
	}{
		// MVQuoteStats row.
		{
			key:    factable.PackKey(quotes.MVQuoteStatsTag, "author", 1234),
			expect: false,
		},
		// MVWordStats row.
		{
			key:    factable.PackKey(quotes.MVWordStatsTag, "word", "author"),
			expect: false,
		},
		// MVRecentQuotes row which is within its time horizon.
		{
			key:    factable.PackKey(quotes.MVRecentQuotesTag, 1234, "author", time.Unix(timeFixture-59, 0)),
			expect: false,
		},
		// MVRecentQuotes row which is outside its time horizon.
		{
			key:    factable.PackKey(quotes.MVRecentQuotesTag, 1234, "author", time.Unix(timeFixture-61, 0)),
			expect: true,
		},
		// MVTag which is not part of the current schema.
		{
			key:    factable.PackKey(9999, 1234, 5678),
			expect: true,
		},
		// Row MVTag fails to parse.
		{
			key:    factable.PackKey("not-an-mvtag", 1234),
			expect: false,
		},
		// MVRecentQuotes leading dimensions fail to parse.
		{
			key:    factable.PackKey(quotes.MVRecentQuotesTag, "not an author", "author", time.Unix(1, 0)),
			expect: false,
		},
		{
			key:    factable.PackKey(quotes.MVRecentQuotesTag, 1234, 9999999, time.Unix(1, 0)),
			expect: false,
		},
		// Consumer metadata key prefix \x00.
		{
			key:    []byte{0x00, 0x01, 0x02},
			expect: false,
		},
	}

	for _, tc := range cases {
		var remove, newVal = cf.Filter(0, tc.key, nil)
		c.Check(newVal, gc.IsNil)
		c.Check(remove, gc.Equals, tc.expect)
	}

	// Swap in a zero-valued schema.
	cf = func() *factable.Schema { return new(factable.Schema) }

	// Run cases again. Expect that this time, no rows are dropped.
	for _, tc := range cases {
		var remove, _ = cf.Filter(0, tc.key, nil)
		c.Check(remove, gc.Equals, false)
	}
}

var (
	expectCompactionFilterIface gorocksdb.CompactionFilter = compactionFilter(nil)
	_                                                      = gc.Suite(&CFSuite{})
)
