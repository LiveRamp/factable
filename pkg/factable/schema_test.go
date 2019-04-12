package factable

import (
	"time"

	pb "github.com/LiveRamp/gazette/v2/pkg/protocol"
	gc "github.com/go-check/check"
)

type SchemaSuite struct{}

func (s *SchemaSuite) TestSchemaValidationCases(c *gc.C) {
	var ext = *makeExtractors()

	// Begin with a stripped down, but valid Spec which we'll extend.
	var spec = SchemaSpec{
		Mappings: []MappingSpec{
			{Name: mapIdent, Tag: mapIdentTag},
		},
		Dimensions: []DimensionSpec{
			{Name: dimOtherStr, Type: DimensionType_STRING, Tag: dimOtherStrTag},
		},
		Metrics: []MetricSpec{
			{Name: metOtherStrUniq, Type: MetricType_STRING_HLL, Dimension: dimOtherStr, Tag: metOtherStrUniqTag},
		},
		Relations: []RelationSpec{
			{
				Name:       relTest,
				Mapping:    mapIdent,
				Dimensions: []string{dimOtherStr},
				Tag:        relTestTag,
			},
		},
		Views: []MaterializedViewSpec{
			{
				Name:     mvTest,
				Relation: relTest,
				View: ViewSpec{
					Dimensions: []string{dimOtherStr},
					Metrics:    []string{metOtherStrUniq},
				},
				Tag: mvTestTag,
			},
		},
		ReservedViewTags: []ReservedMVTagSpec{
			{Tag: resMVTag},
		},
	}

	var verify = func(str string) {
		var _, err = NewSchema(&ext, spec)
		if str == "" {
			c.Assert(err, gc.IsNil)
		} else {
			c.Assert(err, gc.ErrorMatches, str)
		}
	}
	verify("") // Initial fixture builds without error.

	// Mapping verification cases.
	spec.Mappings = append(spec.Mappings, MappingSpec{
		Tag: 9999, Name: "in valid",
	})
	verify(`Mappings\[1\].Name: not a valid token \(in valid\)`)
	spec.Mappings[1].Name = mapIdent
	verify(`Mappings\[1\]: duplicated Name \(mapIdent\)`)
	spec.Mappings[1].Name = mapFixed
	verify(`Mappings\[1\]: extractor not registered \(9999\)`)
	spec.Mappings[1].Tag = mapIdentTag
	verify(`Mappings\[1\]: MapTag already specified \(\d+\)`)
	spec.Mappings[1].Tag = mapFixedTag
	verify("")

	// Dimension verification cases.
	spec.Dimensions = append(spec.Dimensions, DimensionSpec{
		Tag: 9999, Type: DimensionType_INVALID_DIM_TYPE, Name: "in valid",
	})
	verify(`Dimensions\[1\].Name: not a valid token \(in valid\)`)
	spec.Dimensions[1].Name = dimOtherStr
	verify(`Dimensions\[1\]: duplicated Name \(dimOtherStr\)`)
	spec.Dimensions[1].Name = dimAStr
	verify(`Dimensions\[1\]: invalid DimensionType \(INVALID_DIM_TYPE\)`)
	spec.Dimensions[1].Type = DimensionType_STRING
	verify(`Dimensions\[1\]: extractor not registered \(9999; type STRING\)`)
	spec.Dimensions[1].Tag = dimOtherStrTag
	verify(`Dimensions\[1\]: DimTag already specified \(\d+\)`)
	spec.Dimensions[1].Tag = dimAStrTag
	verify("")

	// Metrics verification cases.
	spec.Metrics = append(spec.Metrics, MetricSpec{
		Name: "in valid", Tag: metOtherStrUniqTag, Dimension: "unknown", Type: MetricType_INVALID_METRIC_TYPE,
	})

	verify(`Metrics\[1\].Name: not a valid token \(in valid\)`)
	spec.Metrics[1].Name = metOtherStrUniq
	verify(`Metrics\[1\]: duplicated Name \(metOtherStrUniq\)`)
	spec.Metrics[1].Name = metAStrUniq
	verify(`Metrics\[1\]: MetTag already specified \(\d+\)`)
	spec.Metrics[1].Tag = 1234 // Invent new tag.
	verify(`Metrics\[1\]: no such Dimension \(unknown\)`)
	spec.Metrics[1].Dimension = dimAStr
	verify(`Metrics\[1\]: invalid MetricType \(INVALID_METRIC_TYPE\)`)
	spec.Metrics[1].Type = MetricType_VARINT_GAUGE
	verify(`Metrics\[1\]: Metric Type mismatch \(dimAStr has type STRING; expected VARINT\)`)
	spec.Metrics[1].Type = MetricType_STRING_HLL
	verify("")

	// Interlude: add a valid time dimension.
	spec.Dimensions = append(spec.Dimensions, DimensionSpec{
		Name: dimATime, Type: DimensionType_TIMESTAMP, Tag: dimATimeTag,
	})
	verify("")

	// Relations verification cases.
	spec.Relations = append(spec.Relations, RelationSpec{
		Name: "in valid",
		Selector: pb.LabelSelector{
			Include: pb.LabelSet{Labels: []pb.Label{{Name: "in valid"}}}},
		Tag:        relTestTag,
		Mapping:    "unknown-mapping",
		Dimensions: []string{"unknown-dim", dimATime},
	})

	verify(`Relations\[1\].Name: not a valid token \(in valid\)`)
	spec.Relations[1].Name = relTest
	verify(`Relations\[1\]: duplicated Name \(relTest\)`)
	spec.Relations[1].Name = "relOther"
	verify(`Relations\[1\]: RelTag already specified \(\d+\)`)
	spec.Relations[1].Tag = 1234 // Invent.
	verify(`Relations\[1\].Selector.Include.Labels\[0\].Name: not a valid token \(in valid\)`)
	spec.Relations[1].Selector.Include.Labels[0].Name = "valid"
	verify(`Relations\[1\]: no such Mapping \(unknown-mapping\)`)
	spec.Relations[1].Mapping = mapIdent
	verify(`Relations\[1\].Dimensions\[0\]: no such Dimension \(unknown-dim\)`)
	spec.Relations[1].Dimensions[0] = dimATime
	verify(`Relations\[1\].Dimensions\[1\]: duplicated Dimension \(dimATime\)`)
	spec.Relations[1].Dimensions[0] = dimAStr
	verify("")

	// ReservedMVTag verification cases.
	spec.ReservedViewTags = append(spec.ReservedViewTags, ReservedMVTagSpec{
		Tag: resMVTag,
	})
	verify(`ReservedViewTags\[1\]: MVTag already reserved \(\d+\)`)
	spec.ReservedViewTags[1].Tag = resOtherMVTag

	// MaterializedView verification cases.
	spec.Views = append(spec.Views, MaterializedViewSpec{
		Name:     "in valid",
		Relation: "unknown-relation",
		View: ViewSpec{
			Dimensions: []string{"unknown-dim", dimOtherStr},
			Metrics:    []string{"unknown-metric", metAStrUniq},
		},
		Retention: &MaterializedViewSpec_Retention{RemoveAfter: time.Second, RelativeTo: "unknown-relative-to"},
		Tag:       mvTestTag,
	})

	verify(`Views\[1\].Name: not a valid token \(in valid\)`)
	spec.Views[1].Name = mvTest
	verify(`Views\[1\]: duplicated Name \(mvTest\)`)
	spec.Views[1].Name = "mvOther"
	verify(`Views\[1\]: MVTag already specified \(\d+\)`)
	spec.Views[1].Tag = resMVTag
	verify(`Views\[1\]: MVTag reserved \(\d+\)`)
	spec.Views[1].Tag = 1234 // Invent new tag.
	verify(`Views\[1\]: no such Relation \(unknown-relation\)`)
	spec.Views[1].Relation = relTest
	verify(`Views\[1\].Dimensions\[0\]: no such Dimension \(unknown-dim\)`)
	spec.Views[1].View.Dimensions[0] = dimOtherStr
	verify(`Views\[1\].Metrics\[0\]: no such Metric \(unknown-metric\)`)
	spec.Views[1].View.Metrics[0] = metOtherStrUniq
	verify(`Views\[1\].Dimensions\[1\]: duplicated Dimension \(dimOtherStr\)`)
	spec.Views[1].View.Dimensions[0] = dimATime
	verify(`Views\[1\].Dimensions\[0\]: not part of Relation \(dimATime\)`)
	spec.Views[1].Relation = "relOther"
	spec.Views[1].View.Dimensions[1] = dimAStr

	verify(`Views\[1\].Retention: invalid RemoveAfter \(1s; expected >= 1m\)`)
	spec.Views[1].Retention.RemoveAfter = time.Minute
	verify(`Views\[1\].Retention.RelativeTo: no such Dimension \(unknown-relative-to\)`)
	spec.Views[1].Retention.RelativeTo = dimOtherStr
	verify(`Views\[1\].Retention.RelativeTo: not a View Dimension \(dimOtherStr\)`)
	spec.Views[1].Retention.RelativeTo = dimAStr
	verify(`Views\[1\].Retention.RelativeTo: mismatched Type \(STRING; expected TIMESTAMP\)`)
	spec.Views[1].Retention.RelativeTo = dimATime

	verify(`Views\[1\].Metrics\[0\]: Dimension not part of Relation \(dimOtherStr; metric metOtherStrUniq\)`)
	spec.Views[1].View.Metrics[0] = metAStrUniq
	verify(`Views\[1\].Metrics\[1\]: duplicated Metric \(metAStrUniq\)`)
	spec.Views[1].View.Metrics = spec.Views[1].View.Metrics[:1]

	verify(``)

	// Expect it errors if NewMessage is not defined.
	ext.NewMessage = nil
	verify("ExtractFns: NewMessage not defined")

	// However, ExtractFns may be omitted altogether.
	var schema, err = NewSchema(nil, spec)
	c.Check(err, gc.IsNil)

	c.Check(schema.Spec, gc.DeepEquals, spec)
	c.Check(schema.Mappings, gc.HasLen, len(spec.Mappings))
	c.Check(schema.Dimensions, gc.HasLen, len(spec.Dimensions)+1) // +1 for DimMVTag.
	c.Check(schema.Metrics, gc.HasLen, len(spec.Metrics))
	c.Check(schema.Relations, gc.HasLen, len(spec.Relations))
	c.Check(schema.Views, gc.HasLen, len(spec.Views))
}

func (s *SchemaSuite) TestTransitionCases(c *gc.C) {
	var ext = makeExtractors()

	var buildCfg = func() SchemaSpec {
		return makeTestConfig(MaterializedViewSpec{
			Name:     mvTest,
			Relation: relTest,
			View: ViewSpec{
				Dimensions: []string{dimAnInt, dimAStr},
				Metrics:    []string{metAStrUniq},
			},
			Tag: mvTestTag,
		})
	}

	var from, err = NewSchema(ext, buildCfg())
	c.Assert(err, gc.IsNil)

	var cases = []struct {
		expect string
		fn     func() SchemaSpec
	}{
		// Case: Cannot alter Dimension.Type.
		{`Dimensions\[0]: cannot alter immutable Type \(TIMESTAMP != STRING\)`, func() SchemaSpec {
			return SchemaSpec{
				Dimensions: []DimensionSpec{
					{Name: "test", Type: DimensionType_STRING, Tag: dimATimeTag},
				},
			}
		}},
		// Case: Cannot alter Metric.Type.
		{`Metrics\[0\]: cannot alter immutable Type \(VARINT_SUM != FLOAT_SUM\)`, func() SchemaSpec {
			return SchemaSpec{
				Dimensions: []DimensionSpec{
					{Name: "testDim", Type: DimensionType_FLOAT, Tag: dimAFloatTag},
				},
				Metrics: []MetricSpec{
					{Name: "testMet", Type: MetricType_FLOAT_SUM, Dimension: "testDim", Tag: metAnIntSumTag},
				},
			}
		}},
		// Case: Cannot alter Metric.DimTag.
		{`Metrics\[0\]: cannot alter immutable DimTag \(\d+ != \d+\)`, func() SchemaSpec {
			return SchemaSpec{
				Dimensions: []DimensionSpec{
					{Name: "testDim", Type: DimensionType_VARINT, Tag: dimAnIntTwoTag},
				},
				Metrics: []MetricSpec{
					{Name: "testMet", Type: MetricType_VARINT_SUM, Dimension: "testDim", Tag: metAnIntSumTag},
				},
			}
		}},
		// Case: Cannot alter Relation.MapTag.
		{`Relations\[0\]: cannot alter immutable MapTag \(\d+ != \d+\)`, func() SchemaSpec {
			return SchemaSpec{
				Mappings: []MappingSpec{
					{Name: "testMapping", Tag: mapFixedTag},
				},
				Relations: []RelationSpec{{Name: "testRel", Mapping: "testMapping", Tag: relTestTag}},
			}
		}},
		// Case: Cannot alter MaterializedView.ResolvedView.
		{`Views\[0\]: cannot alter immutable View: DimTags this\[1\]\(\d+\) Not Equal that\[1\]\(\d+\)`, func() SchemaSpec {
			var s = buildCfg()
			s.Views[0].View.Dimensions[1] = dimAnIntTwo
			return s
		}},
		// Case: Identity transition to oneself.
		{``, func() SchemaSpec { return buildCfg() }},
		// Case: transition to an empty SchemaSpec (eg, remove all models).
		{``, func() SchemaSpec { return SchemaSpec{} }},
		// Case: Update all Names of the SchemaSpec, but preserve referential integrity.
		{``, func() SchemaSpec {
			var s = buildCfg()

			const suffix = "-suffix"

			for i := range s.Mappings {
				s.Mappings[i].Name += suffix
			}
			for i := range s.Dimensions {
				s.Dimensions[i].Name += suffix
			}
			for i := range s.Metrics {
				s.Metrics[i].Name += suffix
				s.Metrics[i].Dimension += suffix
			}
			for i := range s.Relations {
				s.Relations[i].Name += suffix
				s.Relations[i].Mapping += suffix

				for j := range s.Relations[i].Dimensions {
					s.Relations[i].Dimensions[j] += suffix
				}
			}
			for i := range s.Views {
				s.Views[i].Name += suffix
				s.Views[i].Relation += suffix

				for j := range s.Views[i].View.Dimensions {
					s.Views[i].View.Dimensions[j] += suffix
				}
				for j := range s.Views[i].View.Metrics {
					s.Views[i].View.Metrics[j] += suffix
				}
			}
			return s
		}},
	}
	for _, tc := range cases {
		var to, err = NewSchema(nil, tc.fn())
		c.Assert(err, gc.IsNil)

		if tc.expect == "" {
			c.Check(ValidateSchemaTransition(from, to), gc.IsNil)
		} else {
			c.Check(ValidateSchemaTransition(from, to), gc.ErrorMatches, tc.expect)
		}
	}
}

var _ = gc.Suite(&SchemaSuite{})
