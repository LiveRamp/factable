package schema

import (
	"bytes"
	"time"

	pb "github.com/LiveRamp/gazette/v2/pkg/protocol"
	"github.com/cockroachdb/cockroach/util/encoding"
	"github.com/pkg/errors"
)

func (x DimensionType) Validate() error {
	if _, ok := DimensionType_name[int32(x)]; !ok || x == DimensionType_INVALID_DIM_TYPE {
		return pb.NewValidationError("invalid DimensionType (%s)", x)
	}
	return nil
}

func (x MetricType) Validate() error {
	if _, ok := MetricType_name[int32(x)]; !ok || x == MetricType_INVALID_METRIC_TYPE {
		return pb.NewValidationError("invalid MetricType (%s)", x)
	}
	return nil
}

func (x MetricType) DimensionType() DimensionType {
	switch x {
	case MetricType_VARINT_SUM, MetricType_VARINT_GUAGE:
		return DimensionType_VARINT
	case MetricType_FLOAT_SUM:
		return DimensionType_FLOAT
	case MetricType_STRING_HLL:
		return DimensionType_STRING
	default:
		panic(x) // Invalid MetricType
	}
}

func (m *MappingSpec) Validate() error {
	if m.Tag <= 0 {
		return pb.NewValidationError("invalid Tag (%d; expected > 0)", m.Tag)
	} else if err := pb.ValidateToken(m.Name, 2, 128); err != nil {
		return pb.ExtendContext(err, "Name")
	}
	return nil
}

func (m *DimensionSpec) Validate() error {
	if m.Tag <= 0 {
		return pb.NewValidationError("invalid Tag (%d; expected > 0)", m.Tag)
	} else if err := m.Type.Validate(); err != nil {
		return pb.ExtendContext(err, "Type")
	} else if err := pb.ValidateToken(m.Name, 2, 128); err != nil {
		return pb.ExtendContext(err, "Name")
	}
	return nil
}

func (m *MetricSpec) Validate() error {
	if m.Tag <= 0 {
		return pb.NewValidationError("invalid Tag (%d; expected > 0)", m.Tag)
	} else if err := m.Type.Validate(); err != nil {
		return pb.ExtendContext(err, "Type")
	} else if m.DimTag <= 0 {
		return pb.NewValidationError("invalid DimTag (%d; expected > 0)", m.DimTag)
	} else if err := pb.ValidateToken(m.Name, 2, 128); err != nil {
		return pb.ExtendContext(err, "Name")
	}
	return nil
}

func (m *Shape) Validate() error {
	var dims, mets = make(map[DimTag]struct{}), make(map[MetTag]struct{})

	for _, t := range m.Dimensions {
		if t <= 0 {
			return pb.NewValidationError("invalid Dimension Tag (%d; expected > 0)", t)
		} else if _, ok := dims[t]; ok {
			return pb.NewValidationError("duplicated Dimension Tag (%d)", t)
		}
		dims[t] = struct{}{}
	}
	for _, t := range m.Metrics {
		if t <= 0 {
			return pb.NewValidationError("invalid Metric Tag (%d; expected > 0)", t)
		} else if _, ok := mets[t]; ok {
			return pb.NewValidationError("duplicated Metric Tag (%d)", t)
		}
		mets[t] = struct{}{}
	}
	return nil
}

func (m *RelationSpec) Validate() error {
	if m.Tag <= 0 {
		return pb.NewValidationError("invalid Tag (%d; expected > 0)", m.Tag)
	} else if err := pb.ValidateToken(m.Name, 2, 128); err != nil {
		return pb.ExtendContext(err, "Name")
	} else if err := m.Selector.Validate(); err != nil {
		return pb.ExtendContext(err, "Selector")
	} else if m.Mapping <= 0 {
		return pb.NewValidationError("invalid Mapping (%d; expected > 0)", m.Mapping)
	} else if err := m.Shape.Validate(); err != nil {
		return pb.ExtendContext(err, "Shape")
	} else if m.Retention != nil {
		if m.Retention.RemoveAfter < time.Minute {
			return pb.NewValidationError("Retention: invalid RemoveAfter (%v; expected >= 1m)",
				m.Retention.RemoveAfter)
		}
		var found bool
		for _, t := range m.Shape.Dimensions {
			if t == m.Retention.RelativeTo {
				found = true
			}
		}
		if !found {
			return pb.NewValidationError("Retention: RelativeTo not a Relation Dimension (%v)",
				m.Retention.RelativeTo)
		}
	}
	return nil
}

func (m *QuerySpec) Validate() error {
	if err := m.Shape.Validate(); err != nil {
		return pb.ExtendContext(err, "Shape")
	}

	// Filters are validated by flattenRangeSpecs, which has access to the DimensionSpec.
	return nil
}

func flattenRangeSpecs(dim DimensionSpec, specs []Range) ([][2][]byte, error) {
	var ranges [][2][]byte

	for i, spec := range specs {
		var begin, end []byte

		switch dim.Type {
		case DimensionType_VARINT:
			if spec.Int == nil {
				return nil, errors.Errorf("Dimension tag %d expected Range_Int", dim.Tag)
			}
			if spec.Int.Begin != 0 {
				begin = encoding.EncodeVarintAscending(nil, spec.Int.Begin)
			}
			if spec.Int.End != 0 {
				end = encoding.EncodeVarintAscending(nil, spec.Int.End)
			}
		case DimensionType_FLOAT:
			if spec.Float == nil {
				return nil, errors.Errorf("Dimension tag %d expected Range_Float", dim.Tag)
			}
			if spec.Float.Begin != 0 {
				begin = encoding.EncodeFloatAscending(nil, spec.Float.Begin)
			}
			if spec.Float.End != 0 {
				end = encoding.EncodeFloatAscending(nil, spec.Float.End)
			}
		case DimensionType_STRING:
			if spec.String_ == nil {
				return nil, errors.Errorf("Dimension tag %d expected Range_String", dim.Tag)
			}
			if spec.String_.Begin != "" {
				begin = encoding.EncodeStringAscending(nil, spec.String_.Begin)
			}
			if spec.String_.End != "" {
				end = encoding.EncodeStringAscending(nil, spec.String_.End)
			}
		case DimensionType_TIMESTAMP:
			if spec.Timestamp == nil {
				return nil, errors.Errorf("Dimension tag %d expected Range_Timestamp", dim.Tag)
			}

			if spec.Timestamp.BeginUnix != 0 && spec.Timestamp.Begin.IsZero() {
				spec.Timestamp.Begin = time.Unix(spec.Timestamp.BeginUnix, 0)
			}
			if spec.Timestamp.EndUnix != 0 && spec.Timestamp.End.IsZero() {
				spec.Timestamp.End = time.Unix(spec.Timestamp.EndUnix, 0)
			}
			if !spec.Timestamp.Begin.IsZero() {
				begin = encoding.EncodeTimeAscending(nil, spec.Timestamp.Begin)
			}
			if !spec.Timestamp.End.IsZero() {
				end = encoding.EncodeTimeAscending(nil, spec.Timestamp.End)
			}
		default:
			panic("unexpected dimension type")
		}

		if end != nil && bytes.Compare(begin, end) > 0 {
			return nil, errors.Errorf("Dimension tag %d invalid range order %s",
				dim.Tag, spec.String())
		}
		if i != 0 && (ranges[i-1][1] == nil || bytes.Compare(ranges[i-1][1], begin) != -1) {
			return nil, errors.Errorf("Dimension tag %d invalid range order @%d %s, %s",
				dim.Tag, i, specs[i-1].String(), spec.String())
		}

		ranges = append(ranges, [2][]byte{begin, end})
	}
	return ranges, nil
}
