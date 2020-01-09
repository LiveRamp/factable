package factable

import (
	"bytes"
	"errors"
	"time"

	"go.gazette.dev/core/message"
	"github.com/cockroachdb/cockroach/util/encoding"
	gc "github.com/go-check/check"
)

func (s *SchemaSuite) TestDimensionRoundTripRegressionCases(c *gc.C) {
	var schema, err = NewSchema(makeExtractors(), makeTestConfig())
	c.Assert(err, gc.IsNil)

	var input = schema.Extract.Mapping[mapIdentTag](message.Envelope{
		Message: testRecord{
			anInt:  3,
			aTime:  time.Unix(12345, 0),
			aStr:   "hello",
			aFloat: 12345.0,
		}})[0]

	var cases = []struct {
		dim            DimTag
		expect         Field
		expectEncoding []byte
	}{
		{DimMVTag, int64(9999), []byte{0xf7, 0x27, 0xf}},                                  // MVTag prefix.
		{dimAnIntTag, int64(3), []byte{0x8b}},                                             // DimensionType_VARINT.
		{dimAFloatTag, 12345.0, []byte{0x05, 0x40, 0xc8, 0x1c, 0x80, 0x0, 0x0, 0x0, 0x0}}, // DimensionType_FLOAT.
		{dimAStrTag, "hello", []byte{0x12, 0x68, 0x65, 0x6c, 0x6c, 0x6f, 0x0, 0x1}},       // DimensionType_STRING.
		{dimATimeTag, time.Unix(12345, 0), []byte{0x14, 0xf7, 0x30, 0x39, 0x88}},          // DimensionType_TIMESTAMP.
	}

	var tags []DimTag
	for _, tc := range cases {
		tags = append(tags, tc.dim)
	}

	var b = []byte{0xff}                                       // Arbitrary prefix, which is passed through.
	b = encoding.EncodeVarintAscending(b, 9999)                // MVTag prefix.
	b = schema.ExtractAndMarshalDimensions(b, tags[1:], input) // Extract fields, excepting DimMVTag.

	var bb = b[1:]
	for _, tc := range cases {
		// Use DequeDimension to pop individual dimensions and confirm their expected encoding.
		var next []byte
		next, err = schema.DequeDimension(bb, tc.dim)
		c.Check(err, gc.IsNil)
		c.Check(bb[:len(bb)-len(next)], gc.DeepEquals, tc.expectEncoding)
		bb = next
	}
	c.Check(bb, gc.HasLen, 0) // Input fully consumed.

	// Expect UnmarshalDimensions recovers expected input fields, including MVTag.
	c.Check(schema.UnmarshalDimensions(b[1:], tags, func(field Field) error {
		c.Check(field, gc.Equals, cases[0].expect)
		cases = cases[1:]
		return nil
	}), gc.IsNil)

	// Expect UnmarshalDimensions passes through an error.
	c.Check(schema.UnmarshalDimensions(b[1:], tags, func(Field) error {
		return errors.New("foobar")
	}), gc.ErrorMatches, "foobar")
}

func (s *SchemaSuite) TestMetricRoundTripRegressionCases(c *gc.C) {
	var schema, err = NewSchema(makeExtractors(), makeTestConfig())
	c.Assert(err, gc.IsNil)

	var input = schema.Extract.Mapping[mapIdentTag](message.Envelope{
		Message: testRecord{
			anInt:    1,
			otherStr: "hello",
			aFloat:   12345.0,
		}})[0]

	var otherInput = schema.Extract.Mapping[mapIdentTag](message.Envelope{
		Message: testRecord{
			anInt:    3,
			otherStr: "world",
			aFloat:   678910.0,
		}})[0]

	var cases = []struct {
		met          MetTag
		expect       []byte
		expectReduce []byte
	}{
		{
			met:          metAnIntSumTag, // MetricType_VARINT_SUM.
			expect:       []byte{0x89},   // 1.
			expectReduce: []byte{0x8c},   // 4.
		},
		{
			met:          metAnIntGaugeTag, // MetricType_VARINT_GAUGE.
			expect:       []byte{0x89},     // 1.
			expectReduce: []byte{0x89},     // 1 (|input| is reduced into |otherInput|).
		},
		{
			met:          metAFloatSumTag, // MetricType_FLOAT_SUM.
			expect:       []byte{0x05, 0x40, 0xc8, 0x1c, 0x80, 0x0, 0x0, 0x0, 0x0},
			expectReduce: []byte{0x05, 0x41, 0x25, 0x18, 0x6e, 0x0, 0x0, 0x0, 0x0},
		},
		{
			met: metOtherStrUniqTag, // MetricType_STRING_HLL.
			expect: []byte{
				0x9d,                   // Varint length prefix.
				0x48, 0x59, 0x4c, 0x4c, // "HYLL"
				0x1, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x72, 0xf5, 0x88, 0x4d, 0x8},
			expectReduce: []byte{
				0xa0,
				0x48, 0x59, 0x4c, 0x4c,
				0x1, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x5c, 0x70, 0x84, 0x56, 0x83, 0x88, 0x4d, 0x8},
		},
	}
	for _, tc := range cases {
		var tags = []MetTag{tc.met}
		var aggs = make([]Aggregate, 1)

		schema.InitAggregates(tags, aggs)
		schema.FoldMetrics(tags, aggs, input)

		// Expect MarshalMetric passes through the prefix, appending the encoding.
		var b = schema.MarshalMetrics([]byte{0xff}, tags, aggs)
		c.Check(b[1:], gc.DeepEquals, tc.expect)

		var bb = append(b[:1:1], bytes.Repeat(b[1:], 2)...)

		// Expect UnmarshalMetrics recovers the input Aggregate, and consumes the precise encoding.
		c.Check(schema.UnmarshalMetrics(bb[1:], []MetTag{tc.met, tc.met}, func(agg2 Aggregate) error {
			c.Check(agg2, gc.DeepEquals, aggs[0])
			return nil
		}), gc.IsNil)

		// Expect dequeMetric pops the precise metric encoding.
		rem, err := schema.dequeMetric(bb[1:], tc.met)
		c.Check(err, gc.IsNil)
		c.Check(rem, gc.DeepEquals, b[1:])

		// Reset Aggregate & fold second RelationRow.
		schema.InitAggregates([]MetTag{tc.met}, aggs)
		schema.FoldMetrics([]MetTag{tc.met}, aggs, otherInput)

		// Expect ReduceMetrics over |b| pops the precise metric encoding.
		rem, err = schema.ReduceMetrics(bb[1:], []MetTag{tc.met}, aggs)
		c.Check(err, gc.IsNil)
		c.Check(rem, gc.DeepEquals, b[1:])
		// And that it reduces the expected aggregate.
		c.Check(schema.MarshalMetrics(nil, []MetTag{tc.met}, aggs), gc.DeepEquals, tc.expectReduce)
	}
}

func (s *SchemaSuite) TestFlatten(c *gc.C) {
	var (
		anInt   int64   = 12345
		aFloat  float64 = 5678
		aStrHLL         = BuildStrHLL("foo", "bar", "baz")
	)
	c.Check(Flatten(&anInt), gc.Equals, anInt)
	c.Check(Flatten(&aFloat), gc.Equals, aFloat)
	c.Check(Flatten(aStrHLL), gc.Equals, int64(3))
}
