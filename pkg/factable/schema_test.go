package factable

import (
	"bytes"
	"time"

	"github.com/LiveRamp/gazette/v2/pkg/message"
	gc "github.com/go-check/check"
)

type SchemaSuite struct{}

func (s *SchemaSuite) TestSchemaValidationCases(c *gc.C) {
	var ext = *makeExtractors()

	// Begin with a stripped down, but valid Spec which we'll extend.
	var cfg = SchemaSpec{
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
				Tag:        relTest,
				Name:       "relTest",
				Mapping:    mapIdent,
				Dimensions: []DimTag{dimOtherStr},
			},
		},
		Views: []MaterializedViewSpec{
			{
				Tag:  mvOne,
				Name: "mvOne",
				View: ViewSpec{
					RelTag:     relTest,
					Dimensions: []DimTag{dimOtherStr},
					Metrics:    []MetTag{metOtherStrUniq},
				},
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
		Tag:        relTest,
		Name:       "in valid",
		Mapping:    9999,
		Dimensions: []DimTag{9999, dimATimestamp},
	})

	verify(`Relations\[1\].Name: not a valid token \(in valid\)`)
	cfg.Relations[1].Name = "relTest"
	verify(`Relations\[1\]: Mapping is not specified \(9999\)`)
	cfg.Relations[1].Mapping = mapIdent
	verify(`Relations\[1\].Dimensions\[0\]: Dimension not specified \(9999\)`)
	cfg.Relations[1].Dimensions[0] = dimAStr
	verify(`Relations\[1\]: Relation already specified \([\d]+\)`)
	cfg.Relations[1].Tag = 1234 // Invent.
	verify(`Relations\[1\]: duplicated Name \(relTest\)`)
	cfg.Relations[1].Name = "relOther"
	verify("")

	// MaterializedView verification cases.
	cfg.Views = append(cfg.Views, MaterializedViewSpec{
		Tag:  mvOne,
		Name: "in valid",
		View: ViewSpec{
			RelTag:     9999,
			Dimensions: []DimTag{9999, dimATimestamp},
			Metrics:    []MetTag{9999},
		},
		Retention: &MaterializedViewSpec_Retention{RemoveAfter: time.Hour, RelativeTo: 9999},
	})

	verify(`Views\[1\].Name: not a valid token \(in valid\)`)
	cfg.Views[1].Name = "mvOne"
	verify(`Views\[1\]: View.RelTag is not specified \(9999\)`)
	cfg.Views[1].View.RelTag = 1234
	verify(`Views\[1\].View.Dimensions\[0\]: Dimension not part of Relation \(9999; RelTag 1234\)`)
	cfg.Views[1].View.Dimensions[0] = dimAStr
	cfg.Views[1].Retention.RelativeTo = dimAStr
	verify(`Views\[1\].View.Metrics\[0\]: Metric not specified \(9999\)`)
	cfg.Views[1].View.Metrics[0] = metOtherStrUniq
	verify(`Views\[1\].View.Metrics\[0\]: Metric Dimension not part of Relation \([\d]+; DimTag [\d]+; RelTag 1234\)`)
	cfg.Relations[1].Dimensions = append(cfg.Relations[1].Dimensions, dimOtherStr)
	verify(`Views\[1\].Retention.RelativeTo: Dimension Type mismatch \(STRING; expected TIMESTAMP\)`)
	cfg.Views[1].Retention.RelativeTo = dimATimestamp
	verify(`Views\[1\]: MaterializedView already specified \([\d]+\)`)
	cfg.Views[1].Tag = 1234 // Invent.
	verify(`Views\[1\]: duplicated Name \(mvOne\)`)
	cfg.Views[1].Name = "mvTwo"
	verify("")

	// Expect it errors if NewMessage is not defined.
	ext.NewMessage = nil
	verify("ExtractFns: NewMessage not defined")

	// However, ExtractFns may be omitted altogether.
	var schema, err = NewSchema(nil, cfg)
	c.Check(err, gc.IsNil)

	c.Check(schema.Spec, gc.DeepEquals, cfg)
	c.Check(schema.Mappings, gc.HasLen, len(cfg.Mappings))
	c.Check(schema.Dimensions, gc.HasLen, len(cfg.Dimensions)+1) // +1 for DimMVTag.
	c.Check(schema.Metrics, gc.HasLen, len(cfg.Metrics))
	c.Check(schema.Relations, gc.HasLen, len(cfg.Relations))
	c.Check(schema.Views, gc.HasLen, len(cfg.Views))
}

func (s *SchemaSuite) TestTransitionErrorCases(c *gc.C) {
	var ext = makeExtractors()

	var buildCfg = func() SchemaSpec {
		return SchemaSpec{
			Mappings: []MappingSpec{
				{Tag: mapIdent, Name: "ident"},
				{Tag: mapFixed, Name: "fixed"},
			},
			Dimensions: []DimensionSpec{
				{Tag: dimAStr, Type: DimensionType_STRING, Name: "aStr"},
				{Tag: dimOtherStr, Type: DimensionType_STRING, Name: "otherStr"},
				{Tag: dimAnInt, Type: DimensionType_VARINT, Name: "anInt"},
				{Tag: dimAnIntTwo, Type: DimensionType_VARINT, Name: "anIntTwo"},
			},
			Metrics: []MetricSpec{
				{Tag: metAnIntSum, DimTag: dimAnInt, Type: MetricType_VARINT_SUM, Name: "anIntSum"},
				{Tag: metAStrUniq, DimTag: dimAStr, Type: MetricType_STRING_HLL, Name: "aStrUniq"},
			},
			Relations: []RelationSpec{
				{
					Tag:        relTest,
					Name:       "relTest",
					Mapping:    mapIdent,
					Dimensions: []DimTag{dimAStr, dimOtherStr, dimAnInt, dimAnIntTwo},
				},
			},
			Views: []MaterializedViewSpec{
				{
					Tag:  mvOne,
					Name: "mvOne",
					View: ViewSpec{
						RelTag:     relTest,
						Dimensions: []DimTag{dimAnInt, dimAStr},
						Metrics:    []MetTag{metAStrUniq},
					},
				},
			},
		}
	}

	var from, err = NewSchema(ext, buildCfg())
	c.Assert(err, gc.IsNil)

	var cases = []struct {
		expect string
		fn     func(SchemaSpec) SchemaSpec
	}{
		// Case: Cannot alter Dimension.Type.
		{`Dimensions\[1]: cannot alter immutable Type \(STRING != VARINT\)`, func(s SchemaSpec) SchemaSpec {
			s.Dimensions[1].Type = DimensionType_VARINT
			return s
		}},
		// Case: Cannot alter Dimension.Type.
		{`Metrics\[0\]: cannot alter immutable Type \(VARINT_SUM != VARINT_GUAGE\)`, func(s SchemaSpec) SchemaSpec {
			s.Metrics[0].Type = MetricType_VARINT_GUAGE
			return s
		}},
		{`Metrics\[1\]: cannot alter immutable DimTag \([\d]+ != [\d]+\)`, func(s SchemaSpec) SchemaSpec {
			s.Metrics[1].DimTag = dimOtherStr
			return s
		}},
		{`Relations\[0\]: cannot alter immutable Mapping \([\d]+ != [\d]+\)`, func(s SchemaSpec) SchemaSpec {
			s.Relations[0].Mapping = mapFixed
			return s
		}},
		{`Views\[0\]: cannot alter immutable View: Dimensions this\[1\]\([\d]+\) Not Equal that\[1\]\([\d]+\)`, func(s SchemaSpec) SchemaSpec {
			s.Views[0].View.Dimensions[1] = dimAnIntTwo
			return s
		}},
		// Case: transition to oneself.
		{``, func(s SchemaSpec) SchemaSpec { return s }},
		// Case: transition to an empty SchemaSpec (eg, remove all models).
		{``, func(s SchemaSpec) SchemaSpec { return SchemaSpec{} }},
		// Case: other allowed mutations.
		{``, func(s SchemaSpec) SchemaSpec {
			s.Relations[0].Name = "other"
			s.Dimensions[2].Name = "assorted"
			s.Metrics[1].Desc = "mutations"
			s.Views[0].Name = "mutations"
			return s
		}},
	}
	for _, tc := range cases {
		var to, err = NewSchema(nil, tc.fn(buildCfg()))
		c.Assert(err, gc.IsNil)

		if tc.expect == "" {
			c.Check(ValidateSchemaTransition(from, to), gc.IsNil)
		} else {
			c.Check(ValidateSchemaTransition(from, to), gc.ErrorMatches, tc.expect)
		}
	}
}

func (s *SchemaSuite) TestDimensionRoundTripRegressionCases(c *gc.C) {
	var schema, err = NewSchema(makeExtractors(), makeTestConfig())
	c.Assert(err, gc.IsNil)

	var input = schema.Extract.Mapping[mapIdent](message.Envelope{
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
		{dimAnInt, int64(3), []byte{0x8b}},                                             // DimensionType_VARINT.
		{dimAFloat, 12345.0, []byte{0x05, 0x40, 0xc8, 0x1c, 0x80, 0x0, 0x0, 0x0, 0x0}}, // DimensionType_FLOAT.
		{dimAStr, "hello", []byte{0x12, 0x68, 0x65, 0x6c, 0x6c, 0x6f, 0x0, 0x1}},       // DimensionType_STRING.
		{dimATimestamp, time.Unix(12345, 0), []byte{0x14, 0xf7, 0x30, 0x39, 0x88}},     // DimensionType_TIMESTAMP.
	}
	for _, tc := range cases {
		// Expect ExtractAndMarshalDimension passes through the prefix, appending the encoding.
		var b = schema.ExtractAndMarshalDimension([]byte{0xff}, tc.dim, input)
		c.Check(b[1:], gc.DeepEquals, tc.expectEncoding)

		var bb = append(b[:1:1], bytes.Repeat(b[1:], 2)...)

		// Expect UnmarshalDimensions recovers the input field, and consumes the precise dimension encoding.
		c.Check(schema.UnmarshalDimensions(bb[1:], []DimTag{tc.dim, tc.dim}, func(field Field) error {
			c.Check(field, gc.Equals, tc.expect)
			return nil
		}), gc.IsNil)

		// Expect dequeDimension also consumes the precise dimension encoding.
		rem, err := schema.dequeDimension(bb[1:], tc.dim)
		c.Check(err, gc.IsNil)
		c.Check(rem, gc.DeepEquals, b[1:])
	}
}

func (s *SchemaSuite) TestMetricRoundTripRegressionCases(c *gc.C) {
	var schema, err = NewSchema(makeExtractors(), makeTestConfig())
	c.Assert(err, gc.IsNil)

	var input = schema.Extract.Mapping[mapIdent](message.Envelope{
		Message: testRecord{
			anInt:    1,
			otherStr: "hello",
			aFloat:   12345.0,
		}})[0]

	var otherInput = schema.Extract.Mapping[mapIdent](message.Envelope{
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

		// Expect MarshalMetric passes through the prefix, appending the encoding.
		var b = schema.MarshalMetric([]byte{0xff}, tc.met, agg)
		c.Check(b[1:], gc.DeepEquals, tc.expect)

		var bb = append(b[:1:1], bytes.Repeat(b[1:], 2)...)

		// Expect UnmarshalMetrics recovers the input Aggregate, and consumes the precise encoding.
		c.Check(schema.UnmarshalMetrics(bb[1:], []MetTag{tc.met, tc.met}, func(agg2 Aggregate) error {
			c.Check(agg2, gc.DeepEquals, agg)
			return nil
		}), gc.IsNil)

		// Expect dequeMetric pops the precise metric encoding.
		rem, err := schema.dequeMetric(bb[1:], tc.met)
		c.Check(err, gc.IsNil)
		c.Check(rem, gc.DeepEquals, b[1:])

		// Reset Aggregate & fold second RelationRow.
		schema.InitMetric(tc.met, agg)
		schema.FoldMetric(tc.met, agg, otherInput)

		// Expect ReduceMetric over |b| pops the precise metric encoding.
		rem, err = schema.ReduceMetric(bb[1:], tc.met, agg)
		c.Check(err, gc.IsNil)
		c.Check(rem, gc.DeepEquals, b[1:])
		// And that it reduces the expected aggregate.
		c.Check(schema.MarshalMetric(nil, tc.met, agg), gc.DeepEquals, tc.expectReduce)
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

var _ = gc.Suite(&SchemaSuite{})
