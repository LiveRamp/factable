package schema

import (
	"bytes"
	"time"

	"github.com/LiveRamp/gazette/v2/pkg/message"
	gc "github.com/go-check/check"
)

type SchemaSuite struct{}

func (s *SchemaSuite) TestSchemaValidationCases(c *gc.C) {
	var ext = *makeExtractors()

	// Begin with a stripped down, but valid Config which we'll extend.
	var cfg = Config{
		Mappings: []MappingSpec{
			{Tag: mapIdent, Name: "ident"},
		},
		Dimensions: []DimensionSpec{
			{Tag: dimOtherStr, Type: DimensionType_STRING, Name: "otherStr"},
		},
		Metrics: []MetricSpec{
			{Tag: metOtherStrUniq, DimTag: dimOtherStr, Type: MetricType_STRING_HLL, Name: "otherStrUniq"},
		},
		Relations: []RelationSpec{
			{
				Tag:     relOne,
				Name:    "relOne",
				Mapping: mapIdent,
				Shape:   Shape{Dimensions: []DimTag{dimOtherStr}, Metrics: []MetTag{metOtherStrUniq}},
			},
		},
	}

	var verify = func(str string) {
		var _, err = NewSchema(&ext, cfg)
		if str == "" {
			c.Check(err, gc.IsNil)
		} else {
			c.Check(err, gc.ErrorMatches, str)
		}
	}
	verify("")

	// Mapping verification cases.
	cfg.Mappings = append(cfg.Mappings, MappingSpec{
		Tag: 9999, Name: "in valid",
	})
	verify(`Mappings\[1\].Name: not a valid token \(in valid\)`)
	cfg.Mappings[1].Name = "ident"
	verify(`Mappings\[1\]: Mapping extractor not registered \(9999\)`)
	cfg.Mappings[1].Tag = mapIdent
	verify(`Mappings\[1\]: Mapping already specified \([\d]+\)`)
	cfg.Mappings[1].Tag = mapFixed
	verify(`Mappings\[1\]: duplicated Name \(ident\)`)
	cfg.Mappings[1].Name = "fixed"
	verify("")

	// Dimension verification cases.
	cfg.Dimensions = append(cfg.Dimensions, DimensionSpec{
		Tag: 9999, Type: DimensionType_STRING, Name: "in valid",
	})
	verify(`Dimensions\[1\].Name: not a valid token \(in valid\)`)
	cfg.Dimensions[1].Name = "otherStr"
	verify(`Dimensions\[1\]: Dimension extractor not registered \(9999; type STRING\)`)
	cfg.Dimensions[1].Tag = dimOtherStr
	verify(`Dimensions\[1\]: Dimension already specified \([\d]+\)`)
	cfg.Dimensions[1].Tag = dimAStr
	verify(`Dimensions\[1\]: duplicated Name \(otherStr\)`)
	cfg.Dimensions[1].Name = "aStr"
	verify("")

	// Metrics verification cases.
	cfg.Metrics = append(cfg.Metrics, MetricSpec{
		Tag: metOtherStrUniq, DimTag: 9999, Type: MetricType_VARINT_SUM, Name: "in valid",
	})

	verify(`Metrics\[1\].Name: not a valid token \(in valid\)`)
	cfg.Metrics[1].Name = "otherStrUniq"
	verify(`Metrics\[1\]: Metric DimTag is not specified \(9999\)`)
	cfg.Metrics[1].DimTag = dimAStr
	verify(`Metrics\[1\]: Metric Type mismatch \(DimTag [\d]+ has type STRING; expected VARINT\)`)
	cfg.Metrics[1].Type = MetricType_STRING_HLL
	verify(`Metrics\[1\]: Metric already specified \([\d]+\)`)
	cfg.Metrics[1].Tag = 1234 // Invent new tag.
	verify(`Metrics\[1\]: duplicated Name \(otherStrUniq\)`)
	cfg.Metrics[1].Name = "aStrUniq"
	verify("")

	// Interlude: add a valid time dimension.
	cfg.Dimensions = append(cfg.Dimensions, DimensionSpec{
		Tag: dimATimestamp, Type: DimensionType_TIMESTAMP, Name: "aTimestamp",
	})
	verify("")

	// Relations verification cases.
	cfg.Relations = append(cfg.Relations, RelationSpec{
		Tag:       relOne,
		Name:      "in valid",
		Mapping:   9999,
		Shape:     Shape{Dimensions: []DimTag{9999, dimATimestamp}, Metrics: []MetTag{9999}},
		Retention: &RelationSpec_Retention{RemoveAfter: time.Hour, RelativeTo: 9999},
	})

	verify(`Relations\[1\].Name: not a valid token \(in valid\)`)
	cfg.Relations[1].Name = "relOne"
	verify(`Relations\[1\]: Mapping is not specified \(9999\)`)
	cfg.Relations[1].Mapping = mapIdent
	verify(`Relations\[1\].Shape.Dimensions\[0\]: Dimension not specified \(9999\)`)
	cfg.Relations[1].Dimensions[0] = dimAStr
	cfg.Relations[1].Retention.RelativeTo = dimAStr
	verify(`Relations\[1\].Shape.Metrics\[0\]: Metric not specified \(9999\)`)
	cfg.Relations[1].Metrics[0] = metOtherStrUniq
	verify(`Relations\[1\].Retention.RelativeTo: Dimension Type mismatch \(STRING; expected TIMESTAMP\)`)
	cfg.Relations[1].Retention.RelativeTo = dimATimestamp
	verify(`Relations\[1\]: Relation already specified \([\d]+\)`)
	cfg.Relations[1].Tag = 1234 // Invent.
	verify(`Relations\[1\]: duplicated Name \(relOne\)`)
	cfg.Relations[1].Name = "relTwo"
	verify("")

	// Expect it errors if NewMessage is not defined.
	ext.NewMessage = nil
	verify("Extractors: NewMessage not defined")

	// However, Extractors may be omitted altogether.
	var _, err = NewSchema(nil, cfg)
	c.Check(err, gc.IsNil)
}

func (s *SchemaSuite) TestDimensionRoundTripRegressionCases(c *gc.C) {
	var schema, err = NewSchema(makeExtractors(), makeTestConfig())
	c.Assert(err, gc.IsNil)

	var input = schema.Mapping[mapIdent](message.Envelope{
		Message: testRecord{
			anInt:  3,
			aTime:  time.Unix(12345, 0),
			aStr:   "hello",
			aFloat: 12345.0,
		}})[0]

	var cases = []struct {
		dim    DimTag
		expect []byte
	}{
		{dimAnInt, []byte{0x8b}}, // DimensionType_VARINT.
		{dimAFloat, []byte{0x05, 0x40, 0xc8, 0x1c, 0x80, 0x0, 0x0, 0x0, 0x0}}, // DimensionType_FLOAT.
		{dimAStr, []byte{0x12, 0x68, 0x65, 0x6c, 0x6c, 0x6f, 0x0, 0x1}},       // DimensionType_STRING.
		{dimATimestamp, []byte{0x14, 0xf7, 0x30, 0x39, 0x88}},                 // DimensionType_TIMESTAMP.
	}
	for _, tc := range cases {
		// Expect ExtractAndEncode passes through the prefix, appending the encoding.
		var b = schema.ExtractAndEncodeDimension([]byte{0xff}, tc.dim, input)
		c.Check(b[1:], gc.DeepEquals, tc.expect)

		// Expect DequeEncodedDimension pops the precise dimension encoding.
		var rem, err = schema.DequeEncodedDimension(bytes.Repeat(b, 2)[1:], tc.dim)
		c.Check(err, gc.IsNil)
		c.Check(rem, gc.DeepEquals, b)
	}
}

func (s *SchemaSuite) TestMetricRoundTripRegressionCases(c *gc.C) {
	var schema, err = NewSchema(makeExtractors(), makeTestConfig())
	c.Assert(err, gc.IsNil)

	var input = schema.Mapping[mapIdent](message.Envelope{
		Message: testRecord{
			anInt:    1,
			otherStr: "hello",
			aFloat:   12345.0,
		}})[0]

	var otherInput = schema.Mapping[mapIdent](message.Envelope{
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
			met:          metAnIntSum,  // MetricType_VARINT_SUM.
			expect:       []byte{0x89}, // 1.
			expectReduce: []byte{0x8c}, // 4.
		},
		{
			met:          metAnIntGauge, // MetricType_VARINT_GAUGE.
			expect:       []byte{0x89},  // 1.
			expectReduce: []byte{0x89},  // 1 (|input| is reduced into |otherInput|).
		},
		{
			met:          metAFloatSum, // MetricType_FLOAT_SUM.
			expect:       []byte{0x05, 0x40, 0xc8, 0x1c, 0x80, 0x0, 0x0, 0x0, 0x0},
			expectReduce: []byte{0x05, 0x41, 0x25, 0x18, 0x6e, 0x0, 0x0, 0x0, 0x0},
		},
		{
			met: metOtherStrUniq, // MetricType_STRING_HLL.
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
		var agg = schema.InitMetric(tc.met, nil)
		schema.FoldMetric(tc.met, agg, input)

		// Expect EncodeMetric passes through the prefix, appending the encoding.
		var b = schema.EncodeMetric([]byte{0xff}, tc.met, agg)
		c.Check(b[1:], gc.DeepEquals, tc.expect)

		// Expect DequeEncodedMetric pops the precise metric encoding.
		var rem, err = schema.DequeEncodedMetric(bytes.Repeat(b, 2)[1:], tc.met)
		c.Check(err, gc.IsNil)
		c.Check(rem, gc.DeepEquals, b)

		// Reset Aggregate & fold second InputRecord.
		schema.InitMetric(tc.met, agg)
		schema.FoldMetric(tc.met, agg, otherInput)

		// Expect ReduceMetric over |b| pops the precise metric encoding.
		rem, err = schema.ReduceMetric(bytes.Repeat(b, 2)[1:], tc.met, agg)
		c.Check(err, gc.IsNil)
		c.Check(rem, gc.DeepEquals, b)

		c.Check(schema.EncodeMetric(nil, tc.met, agg), gc.DeepEquals, tc.expectReduce)
	}
}

var _ = gc.Suite(&SchemaSuite{})
