package factable

import (
	"bytes"
	"fmt"
	"time"

	pb "github.com/LiveRamp/gazette/v2/pkg/protocol"
	"github.com/cockroachdb/cockroach/util/encoding"
	"github.com/pkg/errors"
)

// Validate returns an error if the DimensionType is malformed.
func (x DimensionType) Validate() error {
	if _, ok := DimensionType_name[int32(x)]; !ok || x == DimensionType_INVALID_DIM_TYPE {
		return pb.NewValidationError("invalid DimensionType (%s)", x)
	}
	return nil
}

// MarshalYAML returns the string DimensionType name.
func (x DimensionType) MarshalYAML() (interface{}, error) {
	return x.String(), nil
}

// UnmarshalYAML decodes the string DimensionType name.
func (x *DimensionType) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var str string

	if err := unmarshal(&str); err != nil {
		return err
	} else if tag, ok := DimensionType_value[str]; !ok {
		return fmt.Errorf("%q is not a valid DimensionType (options are %v)", str, DimensionType_value)
	} else {
		*x = DimensionType(tag)
		return nil
	}
}

// Validate returns an error if the MetricType is malformed.
func (x MetricType) Validate() error {
	if _, ok := MetricType_name[int32(x)]; !ok || x == MetricType_INVALID_METRIC_TYPE {
		return pb.NewValidationError("invalid MetricType (%s)", x)
	}
	return nil
}

// DimensionType which corresponds to the MetricType.
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

// MarshalYAML returns the string MetricType name.
func (x MetricType) MarshalYAML() (interface{}, error) {
	return x.String(), nil
}

// UnmarshalYAML decodes the string MetricType name.
func (x *MetricType) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var str string

	if err := unmarshal(&str); err != nil {
		return err
	} else if tag, ok := MetricType_value[str]; !ok {
		return fmt.Errorf("%q is not a valid MetricType (options are %v)", str, MetricType_value)
	} else {
		*x = MetricType(tag)
		return nil
	}
}

// Validate returns an error if the MappingSpec is malformed.
func (m *MappingSpec) Validate() error {
	if m.Tag <= 0 {
		return pb.NewValidationError("invalid Tag (%d; expected > 0)", m.Tag)
	} else if err := pb.ValidateToken(m.Name, 2, 128); err != nil {
		return pb.ExtendContext(err, "Name")
	}
	return nil
}

// Validate returns an error if the DimensionSpec is malformed.
func (m *DimensionSpec) Validate() error {
	if m.Tag <= 0 {
		return pb.NewValidationError("invalid Tag (%d; expected > 0)", m.Tag)
	} else if err := m.Type.Validate(); err != nil {
		return pb.ExtendContext(err, "Type")
	} else if err = pb.ValidateToken(m.Name, 2, 128); err != nil {
		return pb.ExtendContext(err, "Name")
	}
	return nil
}

// Validate returns an error if the MetricSpec is malformed.
func (m *MetricSpec) Validate() error {
	if m.Tag <= 0 {
		return pb.NewValidationError("invalid Tag (%d; expected > 0)", m.Tag)
	} else if err := m.Type.Validate(); err != nil {
		return pb.ExtendContext(err, "Type")
	} else if m.DimTag <= 0 {
		return pb.NewValidationError("invalid DimTag (%d; expected > 0)", m.DimTag)
	} else if err = pb.ValidateToken(m.Name, 2, 128); err != nil {
		return pb.ExtendContext(err, "Name")
	}
	return nil
}

// Validate returns an error if the ViewSpec is malformed.
func (m *ViewSpec) Validate() (err error) {
	if m.RelTag <= 0 {
		return pb.NewValidationError("invalid RelTag (%d; expected > 0)", m.RelTag)
	}
	for i, t := range m.Dimensions {
		if t <= 0 {
			err = pb.NewValidationError("invalid Dimension Tag (%d; expected > 0)", t)
		} else if findDim(t, m.Dimensions[:i]) {
			err = pb.NewValidationError("duplicated Dimension Tag (%d)", t)
		}
		if err != nil {
			return pb.ExtendContext(err, "Dimensions[%d]", i)
		}
	}
	for i, t := range m.Metrics {
		if t <= 0 {
			err = pb.NewValidationError("invalid Metric Tag (%d; expected > 0)", t)
		} else if findMet(t, m.Metrics[:i]) {
			err = pb.NewValidationError("duplicated Metric Tag (%d)", t)
		}
		if err != nil {
			return pb.ExtendContext(err, "Metrics[%d]", i)
		}
	}
	return nil
}

// Validate returns an error if the RelationSpec is malformed.
func (m *RelationSpec) Validate() (err error) {
	if m.Tag <= 0 {
		return pb.NewValidationError("invalid Tag (%d; expected > 0)", m.Tag)
	} else if err = pb.ValidateToken(m.Name, 2, 128); err != nil {
		return pb.ExtendContext(err, "Name")
	} else if err = m.Selector.Validate(); err != nil {
		return pb.ExtendContext(err, "Selector")
	} else if m.Mapping <= 0 {
		return pb.NewValidationError("invalid Mapping (%d; expected > 0)", m.Mapping)
	}
	for i, t := range m.Dimensions {
		if t <= 0 {
			err = pb.NewValidationError("invalid Dimension Tag (%d; expected > 0)", t)
		} else if findDim(t, m.Dimensions[:i]) {
			err = pb.NewValidationError("duplicated Dimension Tag (%d)", t)
		}
		if err != nil {
			return pb.ExtendContext(err, "Dimensions[%d]", i)
		}
	}
	return nil
}

// Validate returns an error if the MaterializedViewSpec is malformed.
func (m *MaterializedViewSpec) Validate() error {
	if m.Tag <= 0 {
		return pb.NewValidationError("invalid Tag (%d; expected > 0)", m.Tag)
	} else if err := pb.ValidateToken(m.Name, 2, 128); err != nil {
		return pb.ExtendContext(err, "Name")
	} else if err = m.View.Validate(); err != nil {
		return pb.ExtendContext(err, "View")
	}

	if m.Retention != nil {
		if m.Retention.RemoveAfter < time.Minute {
			return pb.NewValidationError("Retention: invalid RemoveAfter (%v; expected >= 1m)",
				m.Retention.RemoveAfter)
		} else if !findDim(m.Retention.RelativeTo, m.View.Dimensions) {
			return pb.NewValidationError("Retention: RelativeTo not a Relation Dimension (%v)",
				m.Retention.RelativeTo)
		}
	}
	return nil
}

// Validate returns an error if the QuerySpec is malformed.
func (m *QuerySpec) Validate() error {
	if err := m.View.Validate(); err != nil {
		return pb.ExtendContext(err, "View")
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
			if spec.Str == nil {
				return nil, errors.Errorf("Dimension tag %d expected Range_String", dim.Tag)
			}
			if spec.Str.Begin != "" {
				begin = encoding.EncodeStringAscending(nil, spec.Str.Begin)
			}
			if spec.Str.End != "" {
				end = encoding.EncodeStringAscending(nil, spec.Str.End)
			}
		case DimensionType_TIMESTAMP:
			if spec.Time == nil {
				return nil, errors.Errorf("Dimension tag %d expected Range_Time", dim.Tag)
			}
			if !spec.Time.Begin.IsZero() {
				begin = encoding.EncodeTimeAscending(nil, spec.Time.Begin)
			}
			if !spec.Time.End.IsZero() {
				end = encoding.EncodeTimeAscending(nil, spec.Time.End)
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

// Validate returns an error if the QueryRequest is malformed.
func (m *QueryRequest) Validate() error {
	if m.View <= 0 {
		return pb.NewValidationError("invalid MaterializedView (%d; expected > 0)", m.View)
	} else if err := m.Query.Validate(); err != nil {
		return pb.ExtendContext(err, "Query")
	}
	if m.Shard != "" {
		if err := m.Shard.Validate(); err != nil {
			return pb.ExtendContext(err, "Shard")
		}
	}
	if m.Header != nil {
		if err := m.Header.Validate(); err != nil {
			return pb.ExtendContext(err, "Header")
		} else if m.Shard == "" {
			return pb.NewValidationError("expected Shard with Header")
		}
	}
	return nil
}

// Validate returns an error if the QueryResponse is malformed.
func (m *QueryResponse) Validate() error {
	if m.Header != nil {
		if err := m.Header.Validate(); err != nil {
			return pb.ExtendContext(err, "Header")
		} else if m.Content != nil {
			return pb.NewValidationError("unexpected Content")
		}
	} else if m.Content == nil {
		return pb.NewValidationError("expected Header or Content")
	}
	return nil
}

func findDim(dim DimTag, in []DimTag) bool {
	for _, d := range in {
		if dim == d {
			return true
		}
	}
	return false
}

func findMet(met MetTag, in []MetTag) bool {
	for _, m := range in {
		if met == m {
			return true
		}
	}
	return false
}
