package schema

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
	var (
		input = Shape{
			Dimensions: []DimTag{dimAnInt, dimAFloat, dimATimestamp, dimAStr, dimOtherStr},
			Metrics:    []MetTag{metAnIntGauge, metAFloatSum, metOtherStrUniq, metAnIntSum},
		}
		query = QuerySpec{
			Shape: Shape{
				Dimensions: []DimTag{dimATimestamp, dimAFloat},
				Metrics:    []MetTag{metOtherStrUniq, metAFloatSum},
			},
			Filters: []QuerySpec_Filter{
				{
					Dimension: dimAStr,
					Ranges:    []Range{{String_: &Range_String{Begin: "beg", End: "end"}}},
				},
			},
		}
	)

	var schema, err = NewSchema(makeExtractors(), makeTestConfig())
	c.Assert(err, gc.IsNil)

	cb, err := NewCombiner(schema, input, query)
	c.Check(err, gc.IsNil)

	// Expect trailing input dimension was dropped (as it's not needed).
	c.Check(cb.input.dimension.tags, gc.DeepEquals, input.Dimensions[:4])
	c.Check(cb.input.dimension.ranges, gc.DeepEquals, [][][2][]byte{
		{{nil, nil}},
		{{nil, nil}},
		{{nil, nil}},
		{{encoding.EncodeStringAscending(nil, "beg"), encoding.EncodeStringAscending(nil, "end")}},
	})
	c.Check(cb.output.dimOrder, gc.DeepEquals, []int{2, 1})

	// Expect trailing input metric was dropped, and first metric is not aggregated over.
	c.Check(cb.input.metric.tags, gc.DeepEquals, input.Metrics[:3])
	c.Check(cb.input.metric.reorder, gc.DeepEquals, []int{-1, 1, 0})
	c.Check(cb.output.metrics, gc.DeepEquals, query.Shape.Metrics)
}

func (s *CombinerSuite) TestNewCombinerErrorCases(c *gc.C) {
	var (
		input = Shape{
			Dimensions: []DimTag{dimAnInt, dimAFloat, 9999, dimAnInt},
			Metrics:    []MetTag{metAnIntGauge, metOtherStrUniq, 9999, metAnIntSum},
		}
		query = QuerySpec{
			Shape: Shape{
				Dimensions: []DimTag{dimAnInt, dimAFloat, 9999, dimAnInt},
				Metrics:    []MetTag{metAnIntGauge, metOtherStrUniq, 9999, metAFloatSum},
			},
			Filters: []QuerySpec_Filter{
				{
					Dimension: 9999,
					Ranges:    []Range{{String_: &Range_String{Begin: "beg", End: "end"}}},
				},
				{
					Dimension: dimAStr,
					Ranges:    []Range{{String_: &Range_String{Begin: "beg", End: "end"}}},
				},
			},
		}
	)

	var schema, err = NewSchema(makeExtractors(), makeTestConfig())
	c.Assert(err, gc.IsNil)

	var verifyErr = func(s string) {
		var _, err = NewCombiner(schema, input, query)
		c.Check(err, gc.ErrorMatches, s)
	}

	verifyErr(`Input Dimension tag 9999 not in Schema`)
	input.Dimensions[2] = dimAStr
	verifyErr(`Input Dimension tag [\d]+ appears more than once`)
	input.Dimensions[3] = dimOtherStr
	verifyErr(`Query Dimension tag 9999 not in input Shape .*`)
	query.Shape.Dimensions[2] = dimAStr
	verifyErr(`Query Dimension tag [\d]+ appears more than once`)
	query.Shape.Dimensions[3] = dimOtherStr

	verifyErr(`Filter Dimension tag 9999 not in input Shape .*`)
	query.Filters[0].Dimension = dimAnInt
	verifyErr(`Dimension tag [\d]+ expected Range_Int`)
	query.Filters[0].Dimension = dimAStr
	verifyErr(`Filter Dimension tag [\d]+ appears more than once`)
	query.Filters[0].Dimension = dimOtherStr

	verifyErr(`Query Metric tag 9999 not in Schema`)
	query.Metrics[2] = metAnIntGauge
	verifyErr(`Query Metric tag [\d]+ appears more than once`)
	query.Metrics[2] = metAnIntSum
	verifyErr(`Input Metric tag 9999 not in Schema`)
	input.Metrics[2] = metAnIntSum
	verifyErr(`Query Metrics not in Input Shape .*`)
	input.Metrics[3] = metAFloatSum

	_, err = NewCombiner(schema, input, query)
	c.Check(err, gc.IsNil)
}

func (s *CombinerSuite) TestTransitionCases(c *gc.C) {
	// Use a RelationSpec fixture with a fixed dimension ordering for this test.
	var relSpec = RelationSpec{
		Name:    "relTest",
		Tag:     relOne,
		Mapping: mapIdent,
		Shape:   Shape{Dimensions: []DimTag{dimAStr, dimAnInt, dimOtherStr}},
	}
	var schema, err = NewSchema(makeExtractors(), makeTestConfig(relSpec))
	c.Assert(err, gc.IsNil)

	// Create a Query fixture with 3 dimensions of mixed types, and a filters
	// fixture which constrains the permitted ranges of each dimension.
	var (
		filters = []QuerySpec_Filter{
			{
				Dimension: dimAStr,
				Ranges: []Range{
					{String_: &Range_String{End: "cc"}},
					{String_: &Range_String{Begin: "ee", End: "gg"}},
					{String_: &Range_String{Begin: "jj"}},
				},
			},
			{
				Dimension: dimAnInt,
				Ranges: []Range{
					{Int: &Range_Int{Begin: 100, End: 200}},
					{Int: &Range_Int{Begin: 300, End: 400}},
				},
			},
			{
				Dimension: dimOtherStr,
				Ranges: []Range{
					{String_: &Range_String{Begin: "C", End: "D"}},
					{String_: &Range_String{Begin: "F", End: "G"}},
				},
			},
		}
	)
	var mkKey = func(d1 string, d2 int64, d3 string) (b []byte) {
		b = encoding.EncodeStringAscending(b, d1)
		b = encoding.EncodeVarintAscending(b, d2)
		b = encoding.EncodeStringAscending(b, d3)
		return b
	}
	// Walk a combinerFSM through a number of transition() cases,
	// verifying expected intermediate states.
	cb, err := NewCombiner(schema, relSpec.Shape, QuerySpec{Filters: filters})
	c.Assert(err, gc.IsNil)

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
			c.Check(cb.input.prev.updated, gc.DeepEquals, tc.updated)
			c.Check(cb.input.prev.rangeInd, gc.DeepEquals, tc.rangeInd)
		}
	}
}

func (s *CombinerSuite) TestSeekKeyConstruction(c *gc.C) {
	var (
		ranges = [][][2][]byte{
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
	ranges = [][][2][]byte{
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
	var schema, err = NewSchema(makeExtractors(), makeTestConfig())
	c.Assert(err, gc.IsNil)

	var cb = buildCombiner(c, schema, []DimTag{})

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

	cb.Flush()
	c.Check(cb.Aggregates(), gc.DeepEquals, []Aggregate{&expectValue})
}

func (s *CombinerSuite) TestGroupingExamples(c *gc.C) {
	var schema, err = NewSchema(makeExtractors(), makeTestConfig())
	c.Assert(err, gc.IsNil)

	var value = encoding.EncodeVarintAscending(nil, 1)

	// Cases iterating over all dimensions.
	var cb = buildCombiner(c, schema, []DimTag{dimAnIntTwo, dimAnIntThree, dimAnInt})

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
		cb.Flush()
	}

	runCases()
	c.Check(cb.Key(), gc.DeepEquals, buildKey(1, 4, 1))
	c.Check(cb.Value(nil), gc.DeepEquals, encoding.EncodeVarintAscending(nil, 1))

	// Cases using a subset of dimensions.
	cb = buildCombiner(c, schema, []DimTag{dimAnInt, dimAnIntTwo})
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
	cb = buildCombiner(c, schema, []DimTag{})
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

	// Define mapping, dimension, and metric tag fixtures for test use.
	mapIdent MapTag = 1
	mapFixed MapTag = 2

	dimAStr       DimTag = 3
	dimAnInt      DimTag = 4
	dimAnIntTwo   DimTag = 13
	dimAnIntThree DimTag = 14
	dimOtherStr   DimTag = 5
	dimAFloat     DimTag = 6
	dimATimestamp DimTag = 7

	metAnIntSum     MetTag = 8
	metAnIntGauge   MetTag = 9
	metAFloatSum    MetTag = 10
	metOtherStrUniq MetTag = 11

	relOne = 12
)

func makeTestConfig(relations ...RelationSpec) Config {
	return Config{
		Mappings: []MappingSpec{
			{Tag: mapFixed, Name: "fixed-mapping"},
			{Tag: mapIdent, Name: "ident-mapping"},
		},
		Dimensions: []DimensionSpec{
			{Name: "a-float", Tag: dimAFloat, Type: DimensionType_FLOAT},
			{Name: "a-str", Tag: dimAStr, Type: DimensionType_STRING},
			{Name: "a-timestamp", Tag: dimATimestamp, Type: DimensionType_TIMESTAMP},
			{Name: "an-int", Tag: dimAnInt, Type: DimensionType_VARINT},
			{Name: "an-int-two", Tag: dimAnIntTwo, Type: DimensionType_VARINT},
			{Name: "an-int-three", Tag: dimAnIntThree, Type: DimensionType_VARINT},
			{Name: "other-str", Tag: dimOtherStr, Type: DimensionType_STRING},
		},
		Metrics: []MetricSpec{
			{Name: "a-float-sum", Tag: metAFloatSum, DimTag: dimAFloat, Type: MetricType_FLOAT_SUM},
			{Name: "an-int-gauge", Tag: metAnIntGauge, DimTag: dimAnInt, Type: MetricType_VARINT_GUAGE},
			{Name: "an-int-sum", Tag: metAnIntSum, DimTag: dimAnInt, Type: MetricType_VARINT_SUM},
			{Name: "other-str-uniq", Tag: metOtherStrUniq, DimTag: dimOtherStr, Type: MetricType_STRING_HLL},
		},
		Relations: relations,
	}
}

type testRecord struct {
	anInt          int64
	aStr, otherStr string
	aFloat         float64
	aTime          time.Time
}

func makeExtractors() *Extractors {
	return &Extractors{
		NewMessage: func(spec *protocol.JournalSpec) (message.Message, error) { return &testRecord{}, nil },
		Mapping: map[MapTag]func(env message.Envelope) []InputRecord{
			mapIdent: func(env message.Envelope) []InputRecord { return []InputRecord{{"extra", "stuff", env.Message}} },
			mapFixed: func(env message.Envelope) []InputRecord { return []InputRecord{{"more", "stuff", env.Message}} },
		},
		Int: map[DimTag]func(r InputRecord) int64{
			dimAnInt:      func(r InputRecord) int64 { return r[2].(testRecord).anInt },
			dimAnIntTwo:   func(r InputRecord) int64 { return r[2].(testRecord).anInt },
			dimAnIntThree: func(r InputRecord) int64 { return r[2].(testRecord).anInt },
		},
		String: map[DimTag]func(r InputRecord) string{
			dimAStr:     func(r InputRecord) string { return r[2].(testRecord).aStr },
			dimOtherStr: func(r InputRecord) string { return r[2].(testRecord).otherStr },
		},
		Float: map[DimTag]func(r InputRecord) float64{
			dimAFloat: func(r InputRecord) float64 { return r[2].(testRecord).aFloat },
		},
		Time: map[DimTag]func(r InputRecord) time.Time{
			dimATimestamp: func(r InputRecord) time.Time { return r[2].(testRecord).aTime },
		},
	}
}

func buildCombiner(c *gc.C, schema Schema, dimensions []DimTag) *Combiner {
	var box = func(a int64) int64 {
		return a + startAt
	}

	var input = Shape{
		Dimensions: []DimTag{dimAnInt, dimAnIntTwo, dimAnIntThree},
		Metrics:    []MetTag{metAnIntSum},
	}
	var query = QuerySpec{
		Shape: Shape{
			Dimensions: dimensions,
			Metrics:    []MetTag{metAnIntSum},
		},
		Filters: []QuerySpec_Filter{
			// Filter stride of 8.
			{Dimension: dimAnInt, Ranges: []Range{
				{Int: &Range_Int{End: box(7)}},
			}},
			// Filter stride of 4.
			{Dimension: dimAnIntTwo, Ranges: []Range{
				{Int: &Range_Int{End: box(3)}},
				{Int: &Range_Int{Begin: box(8), End: box(11)}},
			}},
			// Filter stride of 2.
			{Dimension: dimAnIntThree, Ranges: []Range{
				{Int: &Range_Int{End: box(1)}},
				{Int: &Range_Int{Begin: box(4), End: box(5)}},
				{Int: &Range_Int{Begin: box(8), End: box(9)}},
				{Int: &Range_Int{Begin: box(12), End: box(13)}},
			}},
		},
	}
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
