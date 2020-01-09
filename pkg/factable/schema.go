//go:generate protoc -I . -I ../../vendor  --gogo_out=plugins=grpc:. factable.proto
package factable

import (
	"time"

	"go.gazette.dev/core/message"
	pb "go.gazette.dev/core/consumer/protocol"
)

// Field is a simple data type (eg, int64, float64, time.Time, string). A
// Dimension is always a Field, as is an Aggregate which has been flattened.
type Field interface{}

// Aggregate captures the inner state of a partially-reduced Metric.
// For example, a Metric computing an Average might Flatten into a float64,
// but internally would retain both the numerator and denominator as its
// Aggregate, enabling future reduction with other Aggregate instances.
type Aggregate interface{}

type (
	// MapTag uniquely identifies a MappingSpec.
	MapTag uint64
	// DimTag uniquely identifies a DimensionSpec.
	DimTag uint64
	// MetTag uniquely identifies a MetricSpec.
	MetTag uint64
	// RelTag uniquely identifies a RelationSpec.
	RelTag uint64
	// MVTag uniquely identifies a MaterializedViewSpec.
	MVTag uint64
)

// RelationRow is a user-defined record type which a Mapping extractor produces,
// and over which which a Dimension extractor operates.
type RelationRow []interface{}

// ExtractFns are user-defined functions which map a message to InputRecords,
// or InputRecords to an extracted Dimension. Each extractor is keyed by its
// corresponding configured tag. The configured dimension type of the tag must
// match that of the extractor registered herein.
type ExtractFns struct {
	// NewMessage supplies a user Message instance for the input JournalSpec.
	NewMessage func(*pb.JournalSpec) (message.Message, error)
	// Mapping of decoded messages to zero or more RelationRows.
	Mapping map[MapTag]func(message.Envelope) []RelationRow
	// Dimension extractors for each DimensionType.
	// Registered tags must be non-overlapping.
	Int    map[DimTag]func(r RelationRow) int64
	Float  map[DimTag]func(r RelationRow) float64
	String map[DimTag]func(r RelationRow) string
	Time   map[DimTag]func(r RelationRow) time.Time
}

// Schema composes ExtractFns with a validated, indexed SchemaSpec.
type Schema struct {
	Extract ExtractFns
	Spec    SchemaSpec

	Mappings       map[MapTag]MappingSpec
	Dimensions     map[DimTag]DimensionSpec
	Metrics        map[MetTag]MetricSpec
	Relations      map[RelTag]RelationSpec
	ReservedMVTags map[MVTag]struct{}
	Views          map[MVTag]MaterializedViewSpec

	MapTags map[string]MapTag
	DimTags map[string]DimTag
	MetTags map[string]MetTag
	RelTags map[string]RelTag
	MVTags  map[string]MVTag
}

// NewSchema returns a Schema over the given Spec and optional ExtractFns.
// Deep checking of specification referential integrity is performed, and if
// ExtractFns are provided, specifications are checked for consistency against
// registered extractors as well.
func NewSchema(optionalExtractors *ExtractFns, spec SchemaSpec) (Schema, error) {
	// Build up mappings of tags to associated specifications. As we process
	// portions of the Spec, we'll use these indexes to validate referential
	// integrity of the holistic, composed Spec & ExtractFns.
	var schema = Schema{
		Spec: spec,

		Mappings:       make(map[MapTag]MappingSpec),
		Dimensions:     make(map[DimTag]DimensionSpec),
		Metrics:        make(map[MetTag]MetricSpec),
		Relations:      make(map[RelTag]RelationSpec),
		ReservedMVTags: make(map[MVTag]struct{}),
		Views:          make(map[MVTag]MaterializedViewSpec),

		MapTags: make(map[string]MapTag),
		DimTags: make(map[string]DimTag),
		MetTags: make(map[string]MetTag),
		RelTags: make(map[string]RelTag),
		MVTags:  make(map[string]MVTag),
	}

	if optionalExtractors != nil {
		schema.Extract = *optionalExtractors
	}

	schema.Dimensions[DimMVTag] = DimensionSpec{
		Tag:  0,
		Type: DimensionType_VARINT,
	}

	if optionalExtractors != nil {
		if schema.Extract = *optionalExtractors; schema.Extract.NewMessage == nil {
			return Schema{}, pb.NewValidationError("ExtractFns: NewMessage not defined")
		}
	}
	for i, spec := range spec.Mappings {
		if err := schema.addMapping(spec); err != nil {
			return Schema{}, pb.ExtendContext(err, "Mappings[%d]", i)
		}
	}
	for i, spec := range spec.Dimensions {
		if err := schema.addDimension(spec); err != nil {
			return Schema{}, pb.ExtendContext(err, "Dimensions[%d]", i)
		}
	}
	for i, spec := range spec.Metrics {
		if err := schema.addMetric(spec); err != nil {
			return Schema{}, pb.ExtendContext(err, "Metrics[%d]", i)
		}
	}
	for i, spec := range spec.Relations {
		if err := schema.addRelation(spec); err != nil {
			return Schema{}, pb.ExtendContext(err, "Relations[%d]", i)
		}
	}
	for i, spec := range spec.ReservedViewTags {
		if err := schema.addReservedMVTag(spec); err != nil {
			return Schema{}, pb.ExtendContext(err, "ReservedViewTags[%d]", i)
		}
	}
	for i, spec := range spec.Views {
		if err := schema.addMaterializedView(spec); err != nil {
			return Schema{}, pb.ExtendContext(err, "Views[%d]", i)
		}
	}
	return schema, nil
}

func (schema *Schema) addMapping(spec MappingSpec) error {
	if err := schema.validateName(spec.Name); err != nil {
		return err
	} else if _, ok := schema.Mappings[spec.Tag]; ok {
		return pb.NewValidationError("MapTag already specified (%d)", spec.Tag)
	} else if _, ok = schema.Extract.Mapping[spec.Tag]; !ok && schema.Extract.Mapping != nil {
		return pb.NewValidationError("extractor not registered (%d)", spec.Tag)
	}

	schema.Mappings[spec.Tag] = spec
	schema.MapTags[spec.Name] = spec.Tag
	return nil
}

func (schema *Schema) addDimension(spec DimensionSpec) error {
	if err := schema.validateName(spec.Name); err != nil {
		return err
	} else if _, ok := schema.Dimensions[spec.Tag]; ok {
		return pb.NewValidationError("DimTag already specified (%d)", spec.Tag)
	}

	var ok bool
	switch spec.Type {
	case DimensionType_VARINT:
		_, ok = schema.Extract.Int[spec.Tag]
	case DimensionType_FLOAT:
		_, ok = schema.Extract.Float[spec.Tag]
	case DimensionType_STRING:
		_, ok = schema.Extract.String[spec.Tag]
	case DimensionType_TIMESTAMP:
		_, ok = schema.Extract.Time[spec.Tag]
	default:
		return pb.NewValidationError("invalid DimensionType (%s)", spec.Type)
	}
	if !ok && schema.Extract.Mapping != nil {
		return pb.NewValidationError("extractor not registered (%d; type %s)", spec.Tag, spec.Type)
	}

	schema.Dimensions[spec.Tag] = spec
	schema.DimTags[spec.Name] = spec.Tag
	return nil
}

func (schema *Schema) addMetric(spec MetricSpec) error {
	if err := schema.validateName(spec.Name); err != nil {
		return err
	} else if _, ok := schema.Metrics[spec.Tag]; ok {
		return pb.NewValidationError("MetTag already specified (%d)", spec.Tag)
	} else if spec.DimTag, err = schema.resolveDimension(spec.Dimension); err != nil {
		return err
	}

	var dimType DimensionType
	switch spec.Type {
	case MetricType_VARINT_SUM, MetricType_VARINT_GAUGE:
		dimType = DimensionType_VARINT
	case MetricType_FLOAT_SUM:
		dimType = DimensionType_FLOAT
	case MetricType_STRING_HLL:
		dimType = DimensionType_STRING
	default:
		return pb.NewValidationError("invalid MetricType (%s)", spec.Type)
	}

	if dimSpec := schema.Dimensions[spec.DimTag]; dimSpec.Type != dimType {
		return pb.NewValidationError("Metric Type mismatch (%s has type %v; expected %v)",
			dimSpec.Name, dimSpec.Type, dimType)
	}

	schema.Metrics[spec.Tag] = spec
	schema.MetTags[spec.Name] = spec.Tag
	return nil
}

func (schema *Schema) addRelation(spec RelationSpec) error {
	if err := schema.validateName(spec.Name); err != nil {
		return err
	} else if _, ok := schema.Relations[spec.Tag]; ok {
		return pb.NewValidationError("RelTag already specified (%d)", spec.Tag)
	} else if err = spec.Selector.Validate(); err != nil {
		return pb.ExtendContext(err, "Selector")
	} else if spec.MapTag, ok = schema.MapTags[spec.Mapping]; !ok {
		return pb.NewValidationError("no such Mapping (%s)", spec.Mapping)
	} else if spec.DimTags, err = schema.resolveDimensions(spec.Dimensions); err != nil {
		return err
	} else if ind, _ := indexStrings(spec.Dimensions); ind != 0 {
		return pb.ExtendContext(
			pb.NewValidationError("duplicated Dimension (%s)", spec.Dimensions[ind]),
			"Dimensions[%d]", ind)
	}

	schema.Relations[spec.Tag] = spec
	schema.RelTags[spec.Name] = spec.Tag
	return nil
}

func (schema *Schema) addReservedMVTag(spec ReservedMVTagSpec) error {
	if _, ok := schema.ReservedMVTags[spec.Tag]; ok {
		return pb.NewValidationError("MVTag already reserved (%d)", spec.Tag)
	} else if _, ok := schema.Views[spec.Tag]; ok {
		return pb.NewValidationError("MVTag in use and cannot be reserved (%d)", spec.Tag)
	}

	schema.ReservedMVTags[spec.Tag] = struct{}{}
	return nil
}

func (schema *Schema) addMaterializedView(spec MaterializedViewSpec) error {
	if err := schema.validateName(spec.Name); err != nil {
		return err
	} else if _, ok := schema.ReservedMVTags[spec.Tag]; ok {
		return pb.NewValidationError("MVTag reserved (%d)", spec.Tag)
	} else if _, ok := schema.Views[spec.Tag]; ok {
		return pb.NewValidationError("MVTag already specified (%d)", spec.Tag)
	} else if spec.RelTag, ok = schema.RelTags[spec.Relation]; !ok {
		return pb.NewValidationError("no such Relation (%s)", spec.Relation)
	} else if spec.ResolvedView, err = schema.resolveView(spec.View); err != nil {
		return err
	}

	// Verify all spec.Dimensions are constituents of the Relation.
	var _, names = indexStrings(schema.Relations[spec.RelTag].Dimensions)

	for i, s := range spec.View.Dimensions {
		var err error
		if marked, ok := names[s]; !ok {
			err = pb.NewValidationError("not part of Relation (%s)", s)
		} else if marked {
			err = pb.NewValidationError("duplicated Dimension (%s)", s)
		} else {
			names[s] = true // Mark to detect duplicates.
		}
		if err != nil {
			return pb.ExtendContext(err, "Dimensions[%d]", i)
		}
	}

	// Verify Retention spec, and that the RelativeTo Dimension is a View constituent.
	if spec.Retention != nil {
		var err error

		// Copy to resolve RelativeTo without modifying input Spec.
		var cpy = new(MaterializedViewSpec_Retention)
		*cpy = *spec.Retention
		spec.Retention = cpy

		if spec.Retention.RelativeToTag, err = schema.resolveDimension(spec.Retention.RelativeTo); err != nil {
			// Pass.
		} else if marked := names[spec.Retention.RelativeTo]; !marked {
			err = pb.NewValidationError("not a View Dimension (%s)", spec.Retention.RelativeTo)
		} else if dimSpec := schema.Dimensions[spec.Retention.RelativeToTag]; dimSpec.Type != DimensionType_TIMESTAMP {
			err = pb.NewValidationError("mismatched Type (%v; expected TIMESTAMP)", dimSpec.Type)
		}
		if err != nil {
			err = pb.ExtendContext(err, "RelativeTo")
		}
		if spec.Retention.RemoveAfter < time.Minute {
			err = pb.NewValidationError("invalid RemoveAfter (%v; expected >= 1m)", spec.Retention.RemoveAfter)
		}
		if err != nil {
			return pb.ExtendContext(err, "Retention")
		}
	}

	// Verify all Dimensions of spec.Metrics are constituents of the Relation.
	for i, tag := range spec.ResolvedView.MetTags {
		var metSpec = schema.Metrics[tag]
		var err error

		if _, ok := names[metSpec.Dimension]; !ok {
			err = pb.NewValidationError("Dimension not part of Relation (%s; metric %s)", metSpec.Dimension, metSpec.Name)
		} else if marked, _ := names[metSpec.Name]; marked {
			err = pb.NewValidationError("duplicated Metric (%s)", metSpec.Name)
		} else {
			names[metSpec.Name] = true // Mark for duplicates.
		}
		if err != nil {
			return pb.ExtendContext(err, "Metrics[%d]", i)
		}
	}

	schema.Views[spec.Tag] = spec
	schema.MVTags[spec.Name] = spec.Tag
	return nil
}

// validateName returns an error if |name| is not a valid token or is duplicated
// by another Schema entity.
func (schema *Schema) validateName(name string) error {
	if err := pb.ValidateToken(name, 2, 128); err != nil {
		return pb.ExtendContext(err, "Name")
	} else if _, ok := schema.MapTags[name]; ok {
	} else if _, ok = schema.DimTags[name]; ok {
	} else if _, ok = schema.MetTags[name]; ok {
	} else if _, ok = schema.RelTags[name]; ok {
	} else if _, ok = schema.MVTags[name]; ok {
	} else {
		return nil
	}
	return pb.NewValidationError("duplicated Name (%s)", name)
}

// ValidateSchemaTransition returns an error if the transition from |from|
// to |to| would alter an immutable property of the Schema.
func ValidateSchemaTransition(from, to Schema) (err error) {
	// Confirm no DimensionSpec.Types were changed.
	for i := range to.Spec.Dimensions {
		var dimTo = to.Dimensions[to.Spec.Dimensions[i].Tag]

		if dimFrom, ok := from.Dimensions[dimTo.Tag]; !ok {
			continue // Added.
		} else if dimFrom.Type != dimTo.Type {
			err = pb.NewValidationError("cannot alter immutable Type (%v != %v)", dimFrom.Type, dimTo.Type)
		}
		if err != nil {
			return pb.ExtendContext(err, "Dimensions[%d]", i)
		}
	}
	// Confirm no MetricSpec.Types or DimTags changed.
	for i := range to.Spec.Metrics {
		var metTo = to.Metrics[to.Spec.Metrics[i].Tag]

		if metFrom, ok := from.Metrics[metTo.Tag]; !ok {
			continue // Added.
		} else if metFrom.Type != metTo.Type {
			err = pb.NewValidationError("cannot alter immutable Type (%v != %v)", metFrom.Type, metTo.Type)
		} else if metFrom.DimTag != metTo.DimTag {
			err = pb.NewValidationError("cannot alter immutable DimTag (%d != %d)", metFrom.DimTag, metTo.DimTag)
		}
		if err != nil {
			return pb.ExtendContext(err, "Metrics[%d]", i)
		}
	}
	// Confirm no RelationSpec.Mappings have changed.
	for i := range to.Spec.Relations {
		var relTo = to.Relations[to.Spec.Relations[i].Tag]

		if relFrom, ok := from.Relations[relTo.Tag]; !ok {
			continue // Added.
		} else if relFrom.MapTag != relTo.MapTag {
			err = pb.NewValidationError("cannot alter immutable MapTag (%d != %d)", relFrom.MapTag, relTo.MapTag)
		}
		if err != nil {
			return pb.ExtendContext(err, "Relations[%d]", i)
		}
	}
	// Confirm no MaterializedViewSpec.Views have changed.
	for i := range to.Spec.Views {
		var mvTo = to.Views[to.Spec.Views[i].Tag]

		if mvFrom, ok := from.Views[mvTo.Tag]; !ok {
			continue // Added.
		} else if veErr := mvFrom.ResolvedView.VerboseEqual(&mvTo.ResolvedView); veErr != nil {
			err = pb.NewValidationError("cannot alter immutable View: %s", veErr.Error())
		}
		if err != nil {
			return pb.ExtendContext(err, "Views[%d]", i)
		}
	}
	return nil
}

// indexStrings adds all |strings| to |m|, or returns the index of the first duplicated string.
func indexStrings(strings []string) (int, map[string]bool) {
	var m = make(map[string]bool, len(strings))

	for i, s := range strings {
		if _, ok := m[s]; ok {
			return i, nil
		} else {
			m[s] = false
		}
	}
	return 0, m
}
