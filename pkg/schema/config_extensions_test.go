package schema

import (
	"time"

	"github.com/LiveRamp/gazette/v2/pkg/protocol"
	"github.com/cockroachdb/cockroach/util/encoding"
	gc "github.com/go-check/check"
)

type ConfigSuite struct{}

func (s *ConfigSuite) TestMappingSpecValidationCases(c *gc.C) {
	var spec = MappingSpec{Tag: -123, Name: "invalid name"}

	c.Check(spec.Validate(), gc.ErrorMatches, `invalid Tag \(-123; expected > 0\)`)
	spec.Tag = 123
	c.Check(spec.Validate(), gc.ErrorMatches, `Name: not a valid token \(invalid name\)`)
	spec.Name = "valid-name"
	c.Check(spec.Validate(), gc.IsNil)
}

func (s *ConfigSuite) TestDimensionSpecValidationCases(c *gc.C) {
	var spec = DimensionSpec{Tag: -12, Type: -34, Name: "invalid name"}

	c.Check(spec.Validate(), gc.ErrorMatches, `invalid Tag \(-12; expected > 0\)`)
	spec.Tag = 123
	c.Check(spec.Validate(), gc.ErrorMatches, `Type: invalid DimensionType \(-34\)`)
	spec.Type = DimensionType_VARINT
	c.Check(spec.Validate(), gc.ErrorMatches, `Name: not a valid token \(invalid name\)`)
	spec.Name = "valid-name"
	c.Check(spec.Validate(), gc.IsNil)
}

func (s *ConfigSuite) TestMetricSpecValidationCases(c *gc.C) {
	var spec = MetricSpec{Tag: -12, Type: -34, DimTag: -56, Name: "invalid name"}

	c.Check(spec.Validate(), gc.ErrorMatches, `invalid Tag \(-12; expected > 0\)`)
	spec.Tag = 12
	c.Check(spec.Validate(), gc.ErrorMatches, `Type: invalid MetricType \(-34\)`)
	spec.Type = MetricType_STRING_HLL
	c.Check(spec.Validate(), gc.ErrorMatches, `invalid DimTag \(-56; expected > 0\)`)
	spec.DimTag = 56
	c.Check(spec.Validate(), gc.ErrorMatches, `Name: not a valid token \(invalid name\)`)
	spec.Name = "valid-name"
	c.Check(spec.Validate(), gc.IsNil)
}

func (s *ConfigSuite) TestShapeValidationCases(c *gc.C) {
	var shape = Shape{Dimensions: []DimTag{1, 3, 4, -1}, Metrics: []MetTag{-2, 2, 4, 3}}

	c.Check(shape.Validate(), gc.ErrorMatches, `invalid Dimension Tag \(-1; expected > 0\)`)
	shape.Dimensions[3] = 1
	c.Check(shape.Validate(), gc.ErrorMatches, `duplicated Dimension Tag \(1\)`)
	shape.Dimensions[3] = 2
	c.Check(shape.Validate(), gc.ErrorMatches, `invalid Metric Tag \(-2; expected > 0\)`)
	shape.Metrics[0] = 4
	c.Check(shape.Validate(), gc.ErrorMatches, `duplicated Metric Tag \(4\)`)
	shape.Metrics[2] = 5
	c.Check(shape.Validate(), gc.IsNil)
}

func (s *ConfigSuite) TestRelationValidationCases(c *gc.C) {
	var labels = []protocol.Label{{Name: "invalid label"}}

	var spec = RelationSpec{
		Tag:      -12,
		Name:     "invalid name",
		Selector: protocol.LabelSelector{Include: protocol.LabelSet{Labels: labels}},
		Mapping:  -34,
		Shape:    Shape{Dimensions: []DimTag{1, 2, 1}},
		Retention: &RelationSpec_Retention{
			RemoveAfter: time.Second,
			RelativeTo:  33,
		},
	}

	c.Check(spec.Validate(), gc.ErrorMatches, `invalid Tag \(-12; expected > 0\)`)
	spec.Tag = 12
	c.Check(spec.Validate(), gc.ErrorMatches, `Name: not a valid token \(invalid name\)`)
	spec.Name = "valid-name"
	c.Check(spec.Validate(), gc.ErrorMatches, `Selector.Include.Labels\[0\].Name: not a valid token \(invalid label\)`)
	labels[0].Name = "valid-label"
	c.Check(spec.Validate(), gc.ErrorMatches, `invalid Mapping \(-34; expected > 0\)`)
	spec.Mapping = 34
	c.Check(spec.Validate(), gc.ErrorMatches, `Shape: duplicated Dimension Tag \(1\)`)
	spec.Shape.Dimensions[2] = 3
	c.Check(spec.Validate(), gc.ErrorMatches, `Retention: invalid RemoveAfter \(1s; expected >= 1m\)`)
	spec.Retention.RemoveAfter = time.Minute
	c.Check(spec.Validate(), gc.ErrorMatches, `Retention: RelativeTo not a Relation Dimension \(33\)`)
	spec.Retention.RelativeTo = 3

	c.Check(spec.Validate(), gc.IsNil)
	spec.Retention = nil // Retention may be omitted.
	c.Check(spec.Validate(), gc.IsNil)
}

func (s *ConfigSuite) TestRangeFlattenCases(c *gc.C) {
	var cases = []struct {
		spec   DimensionSpec
		ranges []Range
		expect [][2][]byte
		err    string
	}{
		// Basic encoding cases of each dimension type.
		{
			spec: DimensionSpec{Type: DimensionType_VARINT},
			ranges: []Range{
				{Int: &Range_Int{End: 110}},
				{Int: &Range_Int{Begin: 120, End: 130}},
				{Int: &Range_Int{Begin: 150}},
			},
			expect: [][2][]byte{
				{nil, encoding.EncodeVarintAscending(nil, 110)},
				{encoding.EncodeVarintAscending(nil, 120), encoding.EncodeVarintAscending(nil, 130)},
				{encoding.EncodeVarintAscending(nil, 150), nil},
			},
		},
		{
			spec:   DimensionSpec{Type: DimensionType_VARINT},
			ranges: []Range{{}},
			err:    `Dimension tag [\d]+ expected Range_Int`,
		},
		{
			spec: DimensionSpec{Type: DimensionType_FLOAT},
			ranges: []Range{
				{Float: &Range_Float{End: 110}},
				{Float: &Range_Float{Begin: 120, End: 130}},
				{Float: &Range_Float{Begin: 150}},
			},
			expect: [][2][]byte{
				{nil, encoding.EncodeFloatAscending(nil, 110)},
				{encoding.EncodeFloatAscending(nil, 120), encoding.EncodeFloatAscending(nil, 130)},
				{encoding.EncodeFloatAscending(nil, 150), nil},
			},
		},
		{
			spec:   DimensionSpec{Type: DimensionType_FLOAT},
			ranges: []Range{{}},
			err:    `Dimension tag [\d]+ expected Range_Float`,
		},
		{
			spec: DimensionSpec{Type: DimensionType_STRING},
			ranges: []Range{
				{String_: &Range_String{End: "110"}},
				{String_: &Range_String{Begin: "120", End: "130"}},
				{String_: &Range_String{Begin: "150"}},
			},
			expect: [][2][]byte{
				{nil, encoding.EncodeStringAscending(nil, "110")},
				{encoding.EncodeStringAscending(nil, "120"), encoding.EncodeStringAscending(nil, "130")},
				{encoding.EncodeStringAscending(nil, "150"), nil},
			},
		},
		{
			spec:   DimensionSpec{Type: DimensionType_STRING},
			ranges: []Range{{}},
			err:    `Dimension tag [\d]+ expected Range_String`,
		},
		{
			spec: DimensionSpec{Type: DimensionType_TIMESTAMP},
			ranges: []Range{
				{Timestamp: &Range_Timestamp{EndUnix: 110}},
				{Timestamp: &Range_Timestamp{BeginUnix: 120, EndUnix: 130}},
				{Timestamp: &Range_Timestamp{BeginUnix: 150}},
			},
			expect: [][2][]byte{
				{nil, encoding.EncodeTimeAscending(nil, time.Unix(110, 0))},
				{encoding.EncodeTimeAscending(nil, time.Unix(120, 0)),
					encoding.EncodeTimeAscending(nil, time.Unix(130, 0))},
				{encoding.EncodeTimeAscending(nil, time.Unix(150, 0)), nil},
			},
		},
		{
			spec:   DimensionSpec{Type: DimensionType_TIMESTAMP},
			ranges: []Range{{}},
			err:    `Dimension tag [\d]+ expected Range_Timestamp`,
		},

		{ // Invalid order within a Range.
			spec:   DimensionSpec{Type: DimensionType_VARINT},
			ranges: []Range{{Int: &Range_Int{Begin: 456, End: 123}}},
			err:    `Dimension tag [\d]+ invalid range order int:<begin:456 end:123 > `,
		},
		{ // Invalid order across Ranges.
			spec: DimensionSpec{Type: DimensionType_VARINT},
			ranges: []Range{
				{Int: &Range_Int{Begin: 123, End: 456}},
				{Int: &Range_Int{Begin: 455, End: 789}},
			},
			err: `Dimension tag [\d]+ invalid range order @1 int:.*`,
		},
		{ // Invalid order (first Range is open-ended).
			spec: DimensionSpec{Type: DimensionType_VARINT},
			ranges: []Range{
				{Int: &Range_Int{Begin: 123}},
				{Int: &Range_Int{Begin: 455}},
			},
			err: `Dimension tag [\d]+ invalid range order @1 int:.*`,
		},
	}
	for _, tc := range cases {
		var out, err = flattenRangeSpecs(tc.spec, tc.ranges)
		if tc.err != "" {
			c.Check(err, gc.ErrorMatches, tc.err)
		}
		c.Check(out, gc.DeepEquals, tc.expect)
	}
}

var _ = gc.Suite(&ConfigSuite{})
