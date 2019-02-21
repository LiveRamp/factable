package factable

import (
	"time"

	"github.com/cockroachdb/cockroach/util/encoding"
	gc "github.com/go-check/check"
)

func (s *SchemaSuite) TestFilterFlattenCases(c *gc.C) {
	// Fix time at 151 seconds past unix epoch.
	defer func(fn func() time.Time) { timeNow = fn }(timeNow)
	timeNow = func() time.Time { return time.Unix(151, 0) }

	var cases = []struct {
		spec   DimensionSpec
		filter QuerySpec_Filter
		expect []ResolvedQuery_Filter_Range
		err    string
	}{
		// Basic encoding cases of each dimension type.
		{
			spec: DimensionSpec{Type: DimensionType_VARINT},
			filter: QuerySpec_Filter{
				Ints: []QuerySpec_Filter_Int{
					{End: 110},
					{Begin: 120, End: 130},
					{Begin: 150},
				},
			},
			expect: []ResolvedQuery_Filter_Range{
				{nil, encoding.EncodeVarintAscending(nil, 110)},
				{encoding.EncodeVarintAscending(nil, 120), encoding.EncodeVarintAscending(nil, 130)},
				{encoding.EncodeVarintAscending(nil, 150), nil},
			},
		},
		{
			spec:   DimensionSpec{Type: DimensionType_VARINT},
			filter: QuerySpec_Filter{},
			err:    `expected Ints`,
		},
		{
			spec: DimensionSpec{Type: DimensionType_FLOAT},
			filter: QuerySpec_Filter{
				Floats: []QuerySpec_Filter_Float{
					{End: 110},
					{Begin: 120, End: 130},
					{Begin: 150},
				},
			},
			expect: []ResolvedQuery_Filter_Range{
				{nil, encoding.EncodeFloatAscending(nil, 110)},
				{encoding.EncodeFloatAscending(nil, 120), encoding.EncodeFloatAscending(nil, 130)},
				{encoding.EncodeFloatAscending(nil, 150), nil},
			},
		},
		{
			spec:   DimensionSpec{Type: DimensionType_FLOAT},
			filter: QuerySpec_Filter{},
			err:    `expected Floats`,
		},
		{
			spec: DimensionSpec{Type: DimensionType_STRING},
			filter: QuerySpec_Filter{
				Strings: []QuerySpec_Filter_String{
					{End: "110"},
					{Begin: "120", End: "130"},
					{Begin: "150"},
				},
			},
			expect: []ResolvedQuery_Filter_Range{
				{nil, encoding.EncodeStringAscending(nil, "110")},
				{encoding.EncodeStringAscending(nil, "120"), encoding.EncodeStringAscending(nil, "130")},
				{encoding.EncodeStringAscending(nil, "150"), nil},
			},
		},
		{
			spec:   DimensionSpec{Type: DimensionType_STRING},
			filter: QuerySpec_Filter{},
			err:    `expected Strings`,
		},
		{
			spec: DimensionSpec{Type: DimensionType_TIMESTAMP},
			filter: QuerySpec_Filter{
				Times: []QuerySpec_Filter_Time{
					{End: time.Unix(110, 0)},
					{Begin: time.Unix(120, 0), End: time.Unix(130, 0)},
					{Begin: time.Unix(150, 0)},
				},
			},
			expect: []ResolvedQuery_Filter_Range{
				{nil, encoding.EncodeTimeAscending(nil, time.Unix(110, 0))},
				{encoding.EncodeTimeAscending(nil, time.Unix(120, 0)),
					encoding.EncodeTimeAscending(nil, time.Unix(130, 0))},
				{encoding.EncodeTimeAscending(nil, time.Unix(150, 0)), nil},
			},
		},
		{
			spec: DimensionSpec{Type: DimensionType_TIMESTAMP},
			filter: QuerySpec_Filter{
				Times: []QuerySpec_Filter_Time{
					{RelativeEnd: -time.Second * 41},
					{RelativeBegin: -time.Second * 31, RelativeEnd: -time.Second * 21},
					{RelativeBegin: -time.Second},
				},
			},
			expect: []ResolvedQuery_Filter_Range{
				{nil, encoding.EncodeTimeAscending(nil, time.Unix(110, 0))},
				{encoding.EncodeTimeAscending(nil, time.Unix(120, 0)),
					encoding.EncodeTimeAscending(nil, time.Unix(130, 0))},
				{encoding.EncodeTimeAscending(nil, time.Unix(150, 0)), nil},
			},
		},
		{
			spec:   DimensionSpec{Type: DimensionType_TIMESTAMP},
			filter: QuerySpec_Filter{},
			err:    `expected Times`,
		},

		{ // Invalid order within a Range.
			spec: DimensionSpec{Type: DimensionType_VARINT},
			filter: QuerySpec_Filter{
				Ints: []QuerySpec_Filter_Int{
					{Begin: 456, End: 123},
				},
			},
			err: `invalid range order \(ints:<begin:456 end:123 > \)`,
		},
		{ // Invalid order across Ranges.
			spec: DimensionSpec{Type: DimensionType_VARINT},
			filter: QuerySpec_Filter{
				Ints: []QuerySpec_Filter_Int{
					{Begin: 123, End: 456},
					{Begin: 455, End: 789},
				},
			},
			err: `invalid range order \(ints:<begin:123 end:456 > ints:<begin:455 end:789 > \)`,
		},
		{ // Invalid order (first Range is open-ended).
			spec: DimensionSpec{Type: DimensionType_VARINT},
			filter: QuerySpec_Filter{
				Ints: []QuerySpec_Filter_Int{
					{Begin: 123},
					{Begin: 456},
				},
			},
			err: `invalid range order \(ints:<begin:123 > ints:<begin:456 > \)`,
		},
	}
	for _, tc := range cases {
		var out, err = flattenQueryFilter(tc.spec, tc.filter)
		if tc.err != "" {
			c.Check(err, gc.ErrorMatches, tc.err)
		}
		c.Check(out, gc.DeepEquals, tc.expect)
	}
}

func (s *SchemaSuite) TestQueryResolutionCases(c *gc.C) {
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

	var schema, err = NewSchema(nil, buildCfg())
	c.Assert(err, gc.IsNil)

	var spec = QuerySpec{
		MaterializedView: "unknown-view",
		View: ViewSpec{
			Dimensions: []string{"unknown-dim", dimAStr},
			Metrics:    []string{"unknown-metric", metAStrUniq},
		},
		Filters: []QuerySpec_Filter{
			{
				Dimension: "unknown-filter-dim",
				Ints:      []QuerySpec_Filter_Int{{Begin: 456, End: 123}},
			},
			{Dimension: dimAnInt},
		},
	}

	_, err = schema.ResolveQuery(spec)
	c.Check(err, gc.ErrorMatches, `no such MaterializedView \(unknown-view\)`)
	spec.MaterializedView = mvTest

	_, err = schema.ResolveQuery(spec)
	c.Check(err, gc.ErrorMatches, `View.Dimensions\[0\]: no such Dimension \(unknown-dim\)`)
	spec.View.Dimensions[0] = dimAnIntTwo

	_, err = schema.ResolveQuery(spec)
	c.Check(err, gc.ErrorMatches, `View.Metrics\[0\]: no such Metric \(unknown-metric\)`)
	spec.View.Metrics[0] = metOtherStrUniq

	_, err = schema.ResolveQuery(spec)
	c.Check(err, gc.ErrorMatches, `View.Dimensions\[0\]: not part of MaterializedView \(dimAnIntTwo\)`)
	spec.View.Dimensions[0] = dimAStr

	_, err = schema.ResolveQuery(spec)
	c.Check(err, gc.ErrorMatches, `View.Dimensions\[1\]: duplicated Dimension \(dimAStr\)`)
	spec.View.Dimensions = spec.View.Dimensions[:1]

	_, err = schema.ResolveQuery(spec)
	c.Check(err, gc.ErrorMatches, `Filters\[0\]: no such Dimension \(unknown-filter-dim\)`)
	spec.Filters[0].Dimension = dimAnIntTwo

	_, err = schema.ResolveQuery(spec)
	c.Check(err, gc.ErrorMatches, `Filters\[0\]: Dimension is not part of MaterializedView \(dimAnIntTwo\)`)
	spec.Filters[0].Dimension = dimAnInt

	_, err = schema.ResolveQuery(spec)
	c.Check(err, gc.ErrorMatches, `Filters\[0\]: invalid range order \(.*\)`)
	spec.Filters[0].Ints = []QuerySpec_Filter_Int{{Begin: 123, End: 456}}

	_, err = schema.ResolveQuery(spec)
	c.Check(err, gc.ErrorMatches, `Filters\[1\]: duplicated Dimension \(dimAnInt\)`)
	spec.Filters = spec.Filters[:1]

	_, err = schema.ResolveQuery(spec)
	c.Check(err, gc.ErrorMatches, `View.Metrics\[0\]: not part of MaterializedView \(metOtherStrUniq\)`)
	spec.View.Metrics[0] = metAStrUniq

	_, err = schema.ResolveQuery(spec)
	c.Check(err, gc.ErrorMatches, `View.Metrics\[1\]: duplicated Metric \(metAStrUniq\)`)
	spec.View.Metrics = spec.View.Metrics[:1]

	out, err := schema.ResolveQuery(spec)
	c.Check(err, gc.IsNil)

	// Verify ResolvedQuery has expected shape.
	c.Check(out, gc.DeepEquals, ResolvedQuery{
		MvTag: mvTestTag,
		View: ResolvedView{
			DimTags: []DimTag{dimAStrTag},
			MetTags: []MetTag{metAStrUniqTag},
		},
		Filters: []ResolvedQuery_Filter{
			{
				DimTag: dimAnIntTag,
				Ranges: []ResolvedQuery_Filter_Range{
					{
						Begin: encoding.EncodeVarintAscending(nil, 123),
						End:   encoding.EncodeVarintAscending(nil, 456),
					},
				},
			},
		},
	})

}
