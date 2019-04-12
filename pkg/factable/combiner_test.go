package factable

import (
	"testing"
	"time"

	"github.com/LiveRamp/gazette/v2/pkg/message"
	"github.com/LiveRamp/gazette/v2/pkg/protocol"
	"github.com/cockroachdb/cockroach/util/encoding"
	gc "github.com/go-check/check"
)

type CombinerSuite struct{}

func (s *CombinerSuite) TestNewCombinerWithFixture(c *gc.C) {
	var schema, err = NewSchema(makeExtractors(), makeTestConfig(MaterializedViewSpec{
		Name:     mvTest,
		Relation: relTest,
		View: ViewSpec{
			Dimensions: []string{dimAnInt, dimAFloat, dimATime, dimAStr, dimOtherStr},
			Metrics:    []string{metAnIntGauge, metAFloatSum, metOtherStrUniq, metAnIntSum},
		},
		Tag: 100,
	}))
	c.Assert(err, gc.IsNil)

	var input = schema.Views[100].ResolvedView

	query, err := schema.ResolveQuery(QuerySpec{
		MaterializedView: mvTest,
		View: ViewSpec{
			Dimensions: []string{dimATime, dimAFloat},
			Metrics:    []string{metOtherStrUniq, metAFloatSum},
		},
		Filters: []QuerySpec_Filter{
			{
				Dimension: dimAStr,
				Strings:   []QuerySpec_Filter_String{{Begin: "beg", End: "end"}},
			},
		}})
	c.Check(err, gc.IsNil)

	cb, err := NewCombiner(schema, input, query)
	c.Check(err, gc.IsNil)

	// Expect trailing input dimension was dropped (as it's not needed).
	c.Check(cb.input.dimension.tags, gc.DeepEquals, input.DimTags[:4])
	c.Check(cb.input.dimension.ranges, gc.DeepEquals, [][]ResolvedQuery_Filter_Range{
		{{nil, nil}},
		{{nil, nil}},
		{{nil, nil}},
		{{encoding.EncodeStringAscending(nil, "beg"), encoding.EncodeStringAscending(nil, "end")}},
	})
	c.Check(cb.output.dimOrder, gc.DeepEquals, []int{2, 1})

	// Expect trailing input metric was dropped, and first metric is not aggregated over.
	c.Check(cb.input.metric.tags, gc.DeepEquals, input.MetTags[:3])
	c.Check(cb.input.metric.reorder, gc.DeepEquals, []int{-1, 1, 0})
	c.Check(cb.output.metrics, gc.DeepEquals, query.View.MetTags)
}

func (s *CombinerSuite) TestNewCombinerErrorCases(c *gc.C) {
	var (
		input = ResolvedView{
			DimTags: []DimTag{dimAnIntTag, dimAFloatTag, 9999, dimAnIntTag},
			MetTags: []MetTag{metAnIntGaugeTag, metOtherStrUniqTag, 9999, metAnIntSumTag},
		}
		query = ResolvedQuery{
			View: ResolvedView{
				DimTags: []DimTag{dimAnIntTag, dimAFloatTag, 9999, dimAnIntTag},
				MetTags: []MetTag{metAnIntGaugeTag, metOtherStrUniqTag, 9999, metAFloatSumTag},
			},
			Filters: []ResolvedQuery_Filter{
				{
					DimTag: 9999,
					Ranges: []ResolvedQuery_Filter_Range{{Begin: []byte("begin")}},
				},
				{
					DimTag: dimAStrTag,
					Ranges: []ResolvedQuery_Filter_Range{{Begin: []byte("begin")}},
				},
			},
		}
	)

	var schema, err = NewSchema(makeExtractors(), makeTestConfig())
	c.Assert(err, gc.IsNil)

	var verifyErr = func(s string) {
		_, err = NewCombiner(schema, input, query)
		c.Check(err, gc.ErrorMatches, s)
	}

	verifyErr(`Input Dimension tag 9999 not in Schema`)
	input.DimTags[2] = dimAStrTag
	verifyErr(`Input Dimension tag [\d]+ appears more than once`)
	input.DimTags[3] = dimOtherStrTag
	verifyErr(`Query Dimension tag 9999 not in input ViewSpec .*`)
	query.View.DimTags[2] = dimAStrTag
	verifyErr(`Query Dimension tag [\d]+ appears more than once`)
	query.View.DimTags[3] = dimOtherStrTag

	verifyErr(`Filter Dimension tag 9999 not in input ViewSpec .*`)
	query.Filters[0].DimTag = dimAStrTag
	verifyErr(`Filter Dimension tag [\d]+ appears more than once`)
	query.Filters[0].DimTag = dimOtherStrTag

	verifyErr(`Query Metric tag 9999 not in Schema`)
	query.View.MetTags[2] = metAnIntGaugeTag
	verifyErr(`Query Metric tag [\d]+ appears more than once`)
	query.View.MetTags[2] = metAnIntSumTag
	verifyErr(`Input Metric tag 9999 not in Schema`)
	input.MetTags[2] = metAnIntSumTag
	verifyErr(`Query Metrics not in Input ViewSpec .*`)
	input.MetTags[3] = metAFloatSumTag

	_, err = NewCombiner(schema, input, query)
	c.Check(err, gc.IsNil)
}

func (s *CombinerSuite) TestTransitionCases(c *gc.C) {
	// Create a view fixture with 3 dimensions of mixed types, and a query filters
	// fixture which constrains the permitted ranges of each dimension.
	var schema, err = NewSchema(makeExtractors(), makeTestConfig(MaterializedViewSpec{
		Name:     mvTest,
		Relation: relTest,
		View: ViewSpec{
			Dimensions: []string{dimAStr, dimAnInt, dimOtherStr},
		},
		Tag: 100,
	}))
	c.Assert(err, gc.IsNil)

	query, err := schema.ResolveQuery(QuerySpec{
		MaterializedView: mvTest,
		Filters: []QuerySpec_Filter{
			{
				Dimension: dimAStr,
				Strings: []QuerySpec_Filter_String{
					{End: "cc"},
					{Begin: "ee", End: "gg"},
					{Begin: "jj"},
				},
			},
			{
				Dimension: dimAnInt,
				Ints: []QuerySpec_Filter_Int{
					{Begin: 100, End: 200},
					{Begin: 300, End: 400},
				},
			},
			{
				Dimension: dimOtherStr,
				Strings: []QuerySpec_Filter_String{
					{Begin: "C", End: "D"},
					{Begin: "F", End: "G"},
				},
			},
		},
	})
	c.Check(err, gc.IsNil)

	cb, err := NewCombiner(schema, schema.Views[100].ResolvedView, query)
	c.Check(err, gc.IsNil)

	var mkKey = func(d1 string, d2 int64, d3 string) (b []byte) {
		b = encoding.EncodeStringAscending(b, d1)
		b = encoding.EncodeVarintAscending(b, d2)
		b = encoding.EncodeStringAscending(b, d3)
		return b
	}
	// Walk the Combiner through a number of transition() cases,
	// verifying expected intermediate states.

	var cases = []struct {
		key      []byte
		rejected bool
		rangeInd []int
		updated  []bool
	}{
		{ // Initialization case (d2 out of range).
			key:      mkKey("aa", 12, "A"),
			rejected: true,
			rangeInd: []int{0, 0},
			updated:  []bool{true, true},
		},
		{ // Initialization case (d3 out of range).
			key:      mkKey("aa", 120, "A"),
			rejected: true,
			rangeInd: []int{0, 0, 0},
			updated:  []bool{true, true, true},
		},
		{ // Initialization case (accepted).
			key:      mkKey("aa", 120, "C"),
			rejected: false,
			rangeInd: []int{0, 0, 0},
			updated:  []bool{true, true, true},
		},
		{ // Update d2 (only).
			key:      mkKey("aa", 121, "C"),
			rejected: false,
			rangeInd: []int{0, 0, 0},
			updated:  []bool{false, true, false},
		},
		{ // Update d3 (only).
			key:      mkKey("aa", 121, "D"),
			rejected: false,
			rangeInd: []int{0, 0, 0},
			updated:  []bool{false, false, true},
		},
		{ // Update d1 (only).
			key:      mkKey("ff", 121, "D"),
			rejected: false,
			rangeInd: []int{1, 0, 0},
			updated:  []bool{true, false, false},
		},
		{ // Out of range (d3).
			key:      mkKey("ff", 121, "E"),
			rejected: true,
			rangeInd: []int{1, 0, 1},
			updated:  []bool{false, false, true},
		},
		{ // Back in the next range (d3).
			key:      mkKey("ff", 121, "F"),
			rejected: false,
			rangeInd: []int{1, 0, 1},
			updated:  []bool{false, false, true},
		},
		{ // Range exhausted (d3).
			key:      mkKey("ff", 350, "H"),
			rejected: true,
			rangeInd: []int{1, 1, 2},
			updated:  []bool{false, true, true},
		},
		{ // Update d1, d2, & d3 w.r.t. the last accepted key.
			key:      mkKey("gg", 350, "G"),
			rejected: false,
			rangeInd: []int{1, 1, 1},
			updated:  []bool{true, true, true},
		},
		{ // Update d1 & d2 (not d3).
			key:      mkKey("jj", 399, "G"),
			rejected: false,
			rangeInd: []int{2, 1, 1},
			updated:  []bool{true, true, false},
		},
		{ // Update d1 & d3 (not d2).
			key:      mkKey("kk", 399, "C"),
			rejected: false,
			rangeInd: []int{2, 1, 0},
			updated:  []bool{true, false, true},
		},
		{ // Update d2 & d3 (not d1).
			key:      mkKey("kk", 400, "D"),
			rejected: false,
			rangeInd: []int{2, 1, 0},
			updated:  []bool{false, true, true},
		},
		{ // Replay the same key.
			key:      mkKey("kk", 400, "D"),
			rejected: false,
			rangeInd: []int{2, 1, 0},
			updated:  []bool{false, false, false},
		},
	}
	for _, tc := range cases {
		var rejected, err = cb.transition(tc.key)
		c.Check(err, gc.IsNil)
		c.Check(rejected, gc.Equals, tc.rejected)

		if rejected {
			c.Check(cb.input.next.updated, gc.DeepEquals, tc.updated)
			c.Check(cb.input.next.rangeInd, gc.DeepEquals, tc.rangeInd)
		} else {
			c.Check(cb.input.cur.updated, gc.DeepEquals, tc.updated)
			c.Check(cb.input.cur.rangeInd, gc.DeepEquals, tc.rangeInd)
		}
	}
}

func (s *CombinerSuite) TestSeekKeyConstruction(c *gc.C) {
	var (
		ranges = [][]ResolvedQuery_Filter_Range{
			{ // Dimension 1:
				{[]byte("aa"), []byte("zz")},
			},
			{ // 2:
				{[]byte("1"), []byte("1")},
				{[]byte("3"), []byte("3")},
				{[]byte("4"), []byte("7")},
			},
			{ // 3:
				{[]byte("A"), []byte("A")},
			},
		}
		cases = []struct {
			key, expect string
			ind         []int
		}{
			// Initial case: seeks to first accepted key.
			{key: "", ind: []int{0}, expect: "aa1A"},
			{key: "aa", ind: []int{0, 0}, expect: "aa1A"},
			{key: "aa1", ind: []int{0, 0, 0}, expect: "aa1A"},
			// Dim 3 is exhausted, as is the current range of dim 2.
			// Expect it seeks to the next dim 2 range.
			{key: "aa1B", ind: []int{0, 0, 1}, expect: "aa3A"},
			{key: "aa3B", ind: []int{0, 1, 1}, expect: "aa4A"},
			// Continue stepping through dim 2 ranges. Successive keys within
			// a range are generated by incrementing the field by one.
			{key: "aa4B", ind: []int{0, 2, 1}, expect: "aa5A"},
			{key: "aa5B", ind: []int{0, 2, 1}, expect: "aa6A"},
			{key: "aa6B", ind: []int{0, 2, 1}, expect: "aa7A"},
			// Dim 2 is exhausted. The next key of dim 1 is stepped to.
			{key: "aa7B", ind: []int{0, 2, 1}, expect: "ab1A"},
			// We reach the end of the admissible input sequence.
			{key: "zz6B", ind: []int{0, 2, 1}, expect: "zz7A"},
			{key: "zz7B", ind: []int{0, 2, 1}, expect: ""},
		}
	)
	for _, tc := range cases {
		var b = buildSeekKey(nil,
			inputRowState{
				key:      []byte(tc.key),
				offsets:  []int{0, 2, 3, 4},
				rangeInd: tc.ind,
			},
			ranges)
		c.Check(string(b), gc.Equals, tc.expect)
	}
	// Prepare a new fixture, which now includes open begin/end ranges.
	ranges = [][]ResolvedQuery_Filter_Range{
		{ // Dimension 1:
			{[]byte("aa"), nil},
		},
		{ // 2:
			{[]byte("1"), []byte("1")},
			{[]byte("3"), nil},
		},
		{ // 3:
			{nil, []byte("C")},
			{[]byte("F"), []byte("F")},
		},
	}
	cases = []struct {
		key, expect string
		ind         []int
	}{
		// Initial case: seeks to first possible prefix (doesn't include dim 3).
		{key: "", ind: []int{0}, expect: "aa1"},
		// Dim 3 is stepped as expected.
		{key: "yy1D", ind: []int{0, 0, 1}, expect: "yy1F"},
		// Dim 2's open second range can be stepped indefinitely.
		{key: "yy1G", ind: []int{0, 0, 2}, expect: "yy3"},
		{key: "yy3G", ind: []int{0, 1, 2}, expect: "yy4"},
		{key: "yy9G", ind: []int{0, 1, 2}, expect: "yy:"}, // ASCII ':' follows '9'.
		// The increment will even roll up to dim 1 if required.
		{key: "yy\xffG", ind: []int{0, 1, 2}, expect: "yz\x00"},
		{key: "y\xff\xffG", ind: []int{0, 1, 2}, expect: "z\x00\x00"},
	}
	for _, tc := range cases {
		var b = buildSeekKey(nil,
			inputRowState{
				key:      []byte(tc.key),
				offsets:  []int{0, 2, 3, 4},
				rangeInd: tc.ind,
			},
			ranges)
		c.Check(string(b), gc.Equals, tc.expect)
	}
}

func (s *CombinerSuite) TestFilterExamples(c *gc.C) {
	var cb = buildFixtureCombiner(c, []string{})

	var cases = []struct {
		key             []byte
		expectSeek      bool
		expectSeekValue []byte
	}{
		// Walk through all ranges of the innermost dimension. Expect seeks which
		// skip to valid parts of the range.
		{buildKey(1, 1, 0), false, nil},
		{buildKey(1, 1, 1), false, nil},
		{buildKey(1, 1, 2), true, buildKey(1, 1, 4)},
		{buildKey(1, 1, 4), false, nil},
		{buildKey(1, 1, 5), false, nil},
		{buildKey(1, 1, 6), true, buildKey(1, 1, 8)},
		{buildKey(1, 1, 8), false, nil},
		{buildKey(1, 1, 9), false, nil},
		{buildKey(1, 1, 10), true, buildKey(1, 1, 12)},
		{buildKey(1, 1, 12), false, nil},
		{buildKey(1, 1, 13), false, nil},
		// Innermost range is exhausted. Seeks to next value of middle dimension.
		{buildKey(1, 1, 14), true, buildKey(1, 2)},

		// Expect a repetition of a key producing a seek, still seeks.
		{buildKey(1, 1, 14), true, buildKey(1, 2)},
		{buildKey(1, 1, 14, 1), true, buildKey(1, 2)},
		{buildKey(1, 1, 14, 2), true, buildKey(1, 2)},

		// Jump to the second range of the innermost dimension. We expect the
		// inner range filter was restarted, and that cb skips over ranges the
		// initial key is already beyond.
		{buildKey(1, 2, 5), false, nil},
		// Middle dimension pivots. Outer dimension restarts.
		{buildKey(1, 3, 2), true, buildKey(1, 3, 4)},
		// Middle dimension seeks to next range.
		{buildKey(1, 4, 2), true, buildKey(1, 8)},
		{buildKey(1, 8, 1), false, nil}, // Outer dimension restarts.
		{buildKey(1, 8, 3), true, buildKey(1, 8, 4)},
		{buildKey(1, 8, 5), false, nil},
		// Middle dimension pivots.
		{buildKey(1, 11, 2), true, buildKey(1, 11, 4)},
		// Middle dimension exhausts its range.
		{buildKey(1, 12, 3), true, buildKey(2)},
		// Outer dimension pivots, and exhausts its range. No further input is
		// possible; expect a nil Seek is returned.
		{buildKey(7, 11, 13), false, nil},
		{buildKey(7, 11, 14), true, nil},
	}

	// Aggregate over 1 for each input row, and expect to see the correct total count.
	var valueOne = encoding.EncodeVarintAscending(nil, 1)
	var expectValue int64

	for _, tc := range cases {
		var flushed, seek, err = cb.Combine(tc.key, valueOne)
		c.Assert(err, gc.IsNil)
		c.Check(flushed, gc.Equals, false)
		c.Check(seek, gc.Equals, tc.expectSeek)

		if seek {
			c.Check(cb.Seek(nil), gc.DeepEquals, tc.expectSeekValue)
		}
		if !tc.expectSeek {
			expectValue += 1
		}
	}

	c.Check(cb.Flush(), gc.Equals, true)
	c.Check(cb.Aggregates(), gc.DeepEquals, []Aggregate{&expectValue})
}

func (s *CombinerSuite) TestGroupingExamples(c *gc.C) {
	var value = encoding.EncodeVarintAscending(nil, 1)

	// Cases iterating over all dimensions.
	var cb = buildFixtureCombiner(c, []string{dimAnIntTwo, dimAnIntThree, dimAnInt})

	var cases = []struct {
		key         []byte
		expectSkip  bool
		expectEmit  []byte
		expectValue int64
	}{
		{buildKey(0, 0, 0), false, nil, 0},
		{buildKey(0, 0, 0), false, nil, 0},
		{buildKey(0, 0, 1), false, buildKey(0, 0, 0), 2},
		{buildKey(0, 0, 2), true, nil, 0}, // Filtered.
		{buildKey(0, 0, 2), true, nil, 0}, // Filtered (repeat).
		{buildKey(0, 0, 3), true, nil, 0}, // Filtered.
		{buildKey(0, 1, 4), false, buildKey(0, 1, 0), 1},
		{buildKey(0, 1, 4), false, nil, 0},
		{buildKey(0, 1, 4), false, nil, 0},
		{buildKey(1, 1, 4), false, buildKey(1, 4, 0), 3},
	}

	var runCases = func() {
		for _, tc := range cases {
			var emit, skip, err = cb.Combine(tc.key, value)
			c.Check(err, gc.IsNil)
			c.Check(skip, gc.Equals, tc.expectSkip)

			c.Check(emit, gc.Equals, len(tc.expectEmit) != 0)
			if emit {
				c.Check(cb.Key(), gc.DeepEquals, tc.expectEmit)
				c.Check(cb.Value(nil), gc.DeepEquals,
					encoding.EncodeVarintAscending(nil, tc.expectValue))
			}
		}
		c.Check(cb.Flush(), gc.Equals, true)
	}

	runCases()
	c.Check(cb.Key(), gc.DeepEquals, buildKey(1, 4, 1))
	c.Check(cb.Value(nil), gc.DeepEquals, encoding.EncodeVarintAscending(nil, 1))

	// Cases using a subset of dimensions.
	cb = buildFixtureCombiner(c, []string{dimAnInt, dimAnIntTwo})
	cases = []struct {
		key         []byte
		expectSkip  bool
		expectEmit  []byte
		expectValue int64
	}{
		{buildKey(0, 0, 0), false, nil, 0},
		{buildKey(0, 0, 0), false, nil, 0},
		{buildKey(0, 0, 1), false, nil, 0},
		{buildKey(0, 0, 2), true, nil, 0}, // Filtered.
		{buildKey(0, 0, 3), true, nil, 0}, // Filtered.
		{buildKey(0, 1, 4), false, buildKey(0, 0), 3},
		{buildKey(0, 1, 5), false, nil, 0},
		{buildKey(1, 1, 4), false, buildKey(0, 1), 2},
	}

	runCases()
	c.Check(cb.Key(), gc.DeepEquals, buildKey(1, 1))
	c.Check(cb.Value(nil), gc.DeepEquals, encoding.EncodeVarintAscending(nil, 1))

	// Cases using an iterator having no grouped dimensions.
	cb = buildFixtureCombiner(c, []string{})
	cases = []struct {
		key         []byte
		expectSkip  bool
		expectEmit  []byte
		expectValue int64
	}{
		{buildKey(0, 0, 1), false, nil, 0},
		{buildKey(0, 0, 2), true, nil, 0},    // Filtered.
		{buildKey(0, 4, 0), true, nil, 0},    // Filtered.
		{buildKey(0, 4, 0), true, nil, 0},    // Filtered (repeat).
		{buildKey(0, 4, 0, 1), true, nil, 0}, // Filtered (repeat).
		{buildKey(7, 1, 4), false, nil, 0},
		{buildKey(7, 1, 6), true, nil, 0}, // Filtered.
		{buildKey(7, 2, 1), false, nil, 0},
		{buildKey(8, 0, 1), true, nil, 0}, // Filtered.
	}

	runCases()
	c.Check(cb.Key(), gc.IsNil)
	c.Check(cb.Value(nil), gc.DeepEquals, encoding.EncodeVarintAscending(nil, 3))
}

func (s *CombinerSuite) TestCarryTheOne(c *gc.C) {
	var table = []struct {
		in, expect []byte
	}{
		{[]byte{0x00}, []byte{0x01}},
		{[]byte{0x01}, []byte{0x02}},
		{[]byte{0xfe}, []byte{0xff}},
		{[]byte{0x00, 0x00}, []byte{0x00, 0x01}},
		{[]byte{0x00, 0x01}, []byte{0x00, 0x02}},
		{[]byte{0x01, 0x02}, []byte{0x01, 0x03}},
		{[]byte{0xff, 0x01, 0x03}, []byte{0xff, 0x01, 0x04}},
		{[]byte{0xff, 0xff, 0x04}, []byte{0xff, 0xff, 0x05}},
		{[]byte{0xff, 0xfe, 0xff}, []byte{0xff, 0xff, 0x00}},
		{[]byte{0xfe, 0xff, 0xff}, []byte{0xff, 0x00, 0x00}},
	}

	for _, tc := range table {
		incrementByOne(tc.in)
		c.Check(tc.in, gc.DeepEquals, tc.expect)
	}

	c.Check(func() { incrementByOne([]byte{0xff}) },
		gc.Panics, "b is already max-value")
	c.Check(func() { incrementByOne([]byte{0xff, 0xff, 0xff}) },
		gc.Panics, "b is already max-value")
}

const (
	// Under varint encoding, 109 uses one byte, and 110+ use more than one.
	// Starting from this point ensures encodings are a mix of byte lengths.
	startAt = 109

	// Define mapping, dimension, metric, relation, & view name fixtures for test use.
	mapIdent        = "mapIdent"
	mapFixed        = "mapFixed"
	dimAStr         = "dimAStr"
	dimAnInt        = "dimAnInt"
	dimAnIntTwo     = "dimAnIntTwo"
	dimAnIntThree   = "dimAnIntThree"
	dimOtherStr     = "dimOtherStr"
	dimAFloat       = "dimAFloat"
	dimATime        = "dimATime"
	metAnIntSum     = "metAnIntSum"
	metAnIntGauge   = "metAnIntGauge"
	metAFloatSum    = "metAFloatSum"
	metAStrUniq     = "metAStrUniq"
	metOtherStrUniq = "metOtherStrUniq"
	relTest         = "relTest"
	mvTest          = "mvTest"

	mapIdentTag        = 1
	mapFixedTag        = iota
	dimAStrTag         = iota
	dimAnIntTag        = iota
	dimAnIntTwoTag     = iota
	dimAnIntThreeTag   = iota
	dimOtherStrTag     = iota
	dimAFloatTag       = iota
	dimATimeTag        = iota
	metAnIntSumTag     = iota
	metAnIntGaugeTag   = iota
	metAFloatSumTag    = iota
	metAStrUniqTag     = iota
	metOtherStrUniqTag = iota
	relTestTag         = iota
	mvTestTag          = iota
	resMVTag           = iota
	resOtherMVTag      = iota
)

func makeTestConfig(views ...MaterializedViewSpec) SchemaSpec {
	return SchemaSpec{
		Mappings: []MappingSpec{
			{Name: mapIdent, Tag: mapIdentTag},
			{Name: mapFixed, Tag: mapFixedTag},
		},
		Dimensions: []DimensionSpec{
			{Name: dimAFloat, Type: DimensionType_FLOAT, Tag: dimAFloatTag},
			{Name: dimAStr, Type: DimensionType_STRING, Tag: dimAStrTag},
			{Name: dimATime, Type: DimensionType_TIMESTAMP, Tag: dimATimeTag},
			{Name: dimAnInt, Type: DimensionType_VARINT, Tag: dimAnIntTag},
			{Name: dimAnIntTwo, Type: DimensionType_VARINT, Tag: dimAnIntTwoTag},
			{Name: dimAnIntThree, Type: DimensionType_VARINT, Tag: dimAnIntThreeTag},
			{Name: dimOtherStr, Type: DimensionType_STRING, Tag: dimOtherStrTag},
		},
		Metrics: []MetricSpec{
			{Name: metAFloatSum, Type: MetricType_FLOAT_SUM, Dimension: dimAFloat, Tag: metAFloatSumTag},
			{Name: metAnIntGauge, Type: MetricType_VARINT_GAUGE, Dimension: dimAnInt, Tag: metAnIntGaugeTag},
			{Name: metAnIntSum, Type: MetricType_VARINT_SUM, Dimension: dimAnInt, Tag: metAnIntSumTag},
			{Name: metAStrUniq, Type: MetricType_STRING_HLL, Dimension: dimAStr, Tag: metAStrUniqTag},
			{Name: metOtherStrUniq, Type: MetricType_STRING_HLL, Dimension: dimOtherStr, Tag: metOtherStrUniqTag},
		},
		Relations: []RelationSpec{
			{
				Name:       relTest,
				Mapping:    mapIdent,
				Dimensions: []string{dimAStr, dimAnInt, dimAnIntTwo, dimAnIntThree, dimOtherStr, dimAFloat, dimATime},
				Tag:        relTestTag,
			},
		},
		Views: views,
	}
}

type testRecord struct {
	anInt          int64
	aStr, otherStr string
	aFloat         float64
	aTime          time.Time
}

func makeExtractors() *ExtractFns {
	return &ExtractFns{
		NewMessage: func(spec *protocol.JournalSpec) (message.Message, error) { return &testRecord{}, nil },
		Mapping: map[MapTag]func(env message.Envelope) []RelationRow{
			mapIdentTag: func(env message.Envelope) []RelationRow { return []RelationRow{{"extra", "stuff", env.Message}} },
			mapFixedTag: func(env message.Envelope) []RelationRow { return []RelationRow{{"more", "stuff", env.Message}} },
		},
		Int: map[DimTag]func(r RelationRow) int64{
			dimAnIntTag:      func(r RelationRow) int64 { return r[2].(testRecord).anInt },
			dimAnIntTwoTag:   func(r RelationRow) int64 { return r[2].(testRecord).anInt },
			dimAnIntThreeTag: func(r RelationRow) int64 { return r[2].(testRecord).anInt },
		},
		String: map[DimTag]func(r RelationRow) string{
			dimAStrTag:     func(r RelationRow) string { return r[2].(testRecord).aStr },
			dimOtherStrTag: func(r RelationRow) string { return r[2].(testRecord).otherStr },
		},
		Float: map[DimTag]func(r RelationRow) float64{
			dimAFloatTag: func(r RelationRow) float64 { return r[2].(testRecord).aFloat },
		},
		Time: map[DimTag]func(r RelationRow) time.Time{
			dimATimeTag: func(r RelationRow) time.Time { return r[2].(testRecord).aTime },
		},
	}
}

func buildFixtureCombiner(c *gc.C, dimensions []string) *Combiner {
	var schema, err = NewSchema(makeExtractors(), makeTestConfig(MaterializedViewSpec{
		Name:     mvTest,
		Relation: relTest,
		View: ViewSpec{
			Dimensions: []string{dimAnInt, dimAnIntTwo, dimAnIntThree},
			Metrics:    []string{metAnIntSum},
		},
		Tag: 100,
	}))
	c.Assert(err, gc.IsNil)

	var input = schema.Views[100].ResolvedView

	var box = func(a int64) int64 {
		return a + startAt
	}

	query, err := schema.ResolveQuery(QuerySpec{
		MaterializedView: "mvTest",
		View: ViewSpec{
			Dimensions: dimensions,
			Metrics:    []string{metAnIntSum},
		},
		Filters: []QuerySpec_Filter{
			// Filter stride of 8.
			{Dimension: dimAnInt, Ints: []QuerySpec_Filter_Int{
				{End: box(7)},
			}},
			// Filter stride of 4.
			{Dimension: dimAnIntTwo, Ints: []QuerySpec_Filter_Int{
				{End: box(3)},
				{Begin: box(8), End: box(11)},
			}},
			// Filter stride of 2.
			{Dimension: dimAnIntThree, Ints: []QuerySpec_Filter_Int{
				{End: box(1)},
				{Begin: box(4), End: box(5)},
				{Begin: box(8), End: box(9)},
				{Begin: box(12), End: box(13)},
			}},
		},
	})
	c.Assert(err, gc.IsNil)

	cb, err := NewCombiner(schema, input, query)
	c.Assert(err, gc.IsNil)

	return cb
}

func buildKey(fields ...int) []byte {
	var key []byte
	for _, f := range fields {
		key = encoding.EncodeVarintAscending(key, int64(startAt+f))
	}
	return key
}

var _ = gc.Suite(&CombinerSuite{})

func TestT(t *testing.T) { gc.TestingT(t) }
