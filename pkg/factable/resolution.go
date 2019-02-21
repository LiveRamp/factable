package factable

import (
	"bytes"
	"time"

	pb "github.com/LiveRamp/gazette/v2/pkg/protocol"
	"github.com/cockroachdb/cockroach/util/encoding"
)

// ResolveQuery maps a QuerySpec through the Schema into a ResolvedQuery.
func (schema *Schema) ResolveQuery(spec QuerySpec) (ResolvedQuery, error) {
	var out ResolvedQuery
	var ok bool
	var err error

	if out.MvTag, ok = schema.MVTags[spec.MaterializedView]; !ok {
		return out, pb.NewValidationError("no such MaterializedView (%s)", spec.MaterializedView)
	}
	var mvSpec = schema.Views[out.MvTag]

	if out.View, err = schema.resolveView(spec.View); err != nil {
		return out, pb.ExtendContext(err, "View")
	}
	// Index dimension names of the materialized view.
	var _, names = indexStrings(mvSpec.View.Dimensions)

	// Verify query dimensions against the materialized view.
	for i, name := range spec.View.Dimensions {
		if marked, ok := names[name]; !ok {
			err = pb.NewValidationError("not part of MaterializedView (%s)", name)
		} else if marked {
			err = pb.NewValidationError("duplicated Dimension (%s)", name)
		} else {
			names[name] = true // Mark to detect duplicates.
		}
		if err != nil {
			return out, pb.ExtendContext(err, "View.Dimensions[%d]", i)
		}
	}

	// Clear marks. We'll use again to detect filter duplicates.
	for name := range names {
		names[name] = false
	}
	// Resolve and flatten Filters, verifying each against the view.
	for i, f := range spec.Filters {
		var rqf ResolvedQuery_Filter

		if rqf.DimTag, err = schema.resolveDimension(f.Dimension); err != nil {
			// Pass.
		} else if marked, ok := names[f.Dimension]; !ok {
			err = pb.NewValidationError("Dimension is not part of MaterializedView (%s)", f.Dimension)
		} else if marked {
			err = pb.NewValidationError("duplicated Dimension (%s)", f.Dimension)
		} else if rqf.Ranges, err = flattenQueryFilter(schema.Dimensions[rqf.DimTag], f); err != nil {
			// Pass.
		}

		if err != nil {
			return out, pb.ExtendContext(err, "Filters[%d]", i)
		}
		out.Filters = append(out.Filters, rqf)
		names[f.Dimension] = true // Mark to detect duplicates.
	}
	// Index metric names of the materialized view.
	_, names = indexStrings(mvSpec.View.Metrics)

	for i, name := range spec.View.Metrics {
		if marked, ok := names[name]; !ok {
			err = pb.NewValidationError("not part of MaterializedView (%s)", name)
		} else if marked {
			err = pb.NewValidationError("duplicated Metric (%s)", name)
		} else {
			names[name] = true // Mark to detect duplicates.
		}
		if err != nil {
			return out, pb.ExtendContext(err, "View.Metrics[%d]", i)
		}
	}

	return out, nil
}

func (schema *Schema) resolveView(spec ViewSpec) (ResolvedView, error) {
	var out = ResolvedView{
		DimTags: nil,
		MetTags: make([]MetTag, len(spec.Metrics)),
	}
	var err error

	if out.DimTags, err = schema.resolveDimensions(spec.Dimensions); err != nil {
		return out, err
	}
	for i, name := range spec.Metrics {
		var ok bool

		if out.MetTags[i], ok = schema.MetTags[name]; !ok {
			return out, pb.ExtendContext(
				pb.NewValidationError("no such Metric (%s)", name),
				"Metrics[%d]", i)
		}
	}
	return out, nil
}

func (schema *Schema) resolveDimensions(names []string) ([]DimTag, error) {
	var out = make([]DimTag, len(names))
	var err error

	for i, name := range names {
		if out[i], err = schema.resolveDimension(name); err != nil {
			return nil, pb.ExtendContext(err, "Dimensions[%d]", i)
		}
	}
	return out, nil
}

func (schema *Schema) resolveDimension(name string) (DimTag, error) {
	if tag, ok := schema.DimTags[name]; !ok {
		return 0, pb.NewValidationError("no such Dimension (%s)", name)
	} else {
		return tag, nil
	}
}

func flattenQueryFilter(dim DimensionSpec, filter QuerySpec_Filter) ([]ResolvedQuery_Filter_Range, error) {
	var ranges []ResolvedQuery_Filter_Range

	switch dim.Type {
	case DimensionType_VARINT:
		if filter.Ints == nil {
			return nil, pb.NewValidationError("expected Ints")
		}
		for _, fi := range filter.Ints {
			var rng ResolvedQuery_Filter_Range

			if fi.Begin != 0 {
				rng.Begin = encoding.EncodeVarintAscending(nil, fi.Begin)
			}
			if fi.End != 0 {
				rng.End = encoding.EncodeVarintAscending(nil, fi.End)
			}
			ranges = append(ranges, rng)
		}
	case DimensionType_FLOAT:
		if filter.Floats == nil {
			return nil, pb.NewValidationError("expected Floats")
		}
		for _, fi := range filter.Floats {
			var rng ResolvedQuery_Filter_Range

			if fi.Begin != 0 {
				rng.Begin = encoding.EncodeFloatAscending(nil, fi.Begin)
			}
			if fi.End != 0 {
				rng.End = encoding.EncodeFloatAscending(nil, fi.End)
			}
			ranges = append(ranges, rng)
		}
	case DimensionType_STRING:
		if filter.Strings == nil {
			return nil, pb.NewValidationError("expected Strings")
		}
		for _, fi := range filter.Strings {
			var rng ResolvedQuery_Filter_Range

			if fi.Begin != "" {
				rng.Begin = encoding.EncodeStringAscending(nil, fi.Begin)
			}
			if fi.End != "" {
				rng.End = encoding.EncodeStringAscending(nil, fi.End)
			}
			ranges = append(ranges, rng)
		}
	case DimensionType_TIMESTAMP:
		if filter.Times == nil {
			return nil, pb.NewValidationError("expected Times")
		}
		for _, fi := range filter.Times {
			var rng ResolvedQuery_Filter_Range

			if !fi.Begin.IsZero() {
				rng.Begin = encoding.EncodeTimeAscending(nil, fi.Begin)
			} else if fi.RelativeBegin != 0 {
				rng.Begin = encoding.EncodeTimeAscending(nil, timeNow().Add(fi.RelativeBegin))
			}

			if !fi.End.IsZero() {
				rng.End = encoding.EncodeTimeAscending(nil, fi.End)
			} else if fi.RelativeEnd != 0 {
				rng.End = encoding.EncodeTimeAscending(nil, timeNow().Add(fi.RelativeEnd))
			}
			ranges = append(ranges, rng)
		}
	default:
		panic("unexpected dimension type")
	}

	for i, rng := range ranges {
		// We expect all ordered [Begin, End) ranges to be monotonically increasing.
		// Return an error if a range is inconsistent with itself, or with the range which came before.
		if (rng.End != nil && bytes.Compare(rng.Begin, rng.End) > 0) ||
			(i != 0 && (ranges[i-1].End == nil || bytes.Compare(ranges[i-1].End, rng.Begin) != -1)) {
			return nil, pb.NewValidationError("invalid range order (%s)", filter.String())
		}
	}
	return ranges, nil
}

var timeNow = time.Now
