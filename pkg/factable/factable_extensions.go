package factable

import (
	"fmt"

	pb "github.com/LiveRamp/gazette/v2/pkg/protocol"
)

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

// Validate returns an error if the QueryRequest is malformed.
func (m *ExecuteQueryRequest) Validate() error {
	if m.Header != nil {
		if err := m.Header.Validate(); err != nil {
			return pb.ExtendContext(err, "Header")
		} else if m.Shard == "" {
			return pb.NewValidationError("expected Shard with Header")
		}
	}
	if m.Shard != "" {
		if err := m.Shard.Validate(); err != nil {
			return pb.ExtendContext(err, "Shard")
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
