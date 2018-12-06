//go:generate protoc -I . -I ../../vendor  --gogo_out=plugins=grpc:. schema.proto
package schema

import (
	"errors"
	"time"

	"git.liveramp.net/jgraet/factable/pkg/hll"
	"github.com/LiveRamp/gazette/v2/pkg/message"
	pb "github.com/LiveRamp/gazette/v2/pkg/protocol"
	"github.com/cockroachdb/cockroach/util/encoding"
)

// Field is an instance of a Dimension value. Fields are simple types
// (eg, int64, float64, time.Time, string, []byte).
type Field interface{}

// Aggregate captures the inner state of a partially-reduced Metric.
// For example, a Metric computing an Average might Flatten into a float64,
// but internally would retain both the numerator and denominator as its
// Aggregate, enabling future reduction with other Aggregate instances.
type Aggregate interface{}

type (
	MapTag int64
	DimTag int64
	MetTag int64
	RelTag int64
)

// InputRecord is a user-defined record type over which a Dimension
// extractor operates.
type InputRecord []interface{}

// Extractors are user-defined functions which map a message to InputRecords,
// or InputRecords to an extracted Dimension. Each extractor is keyed by its
// corresponding configured tag. The configured dimension type of the tag must
// match that of the extractor registered herein.
type Extractors struct {
	// NewMessage supplies a user Message instance for the input JournalSpec.
	NewMessage func(*pb.JournalSpec) (message.Message, error)
	// Mapping of decoded messages to zero or more InputRecords.
	Mapping map[MapTag]func(message.Envelope) []InputRecord
	// Dimension extractors for each DimensionType.
	// Registered tags must be non-overlapping.
	Int    map[DimTag]func(r InputRecord) int64
	Float  map[DimTag]func(r InputRecord) float64
	String map[DimTag]func(r InputRecord) string
	Time   map[DimTag]func(r InputRecord) time.Time
}

// Schema composes Extractors with validated Mapping, Dimension, Metric,
// and Relation specifications.
type Schema struct {
	Extractors

	Mappings   map[MapTag]MappingSpec
	Dimensions map[DimTag]DimensionSpec
	Metrics    map[MetTag]MetricSpec
	Relations  map[RelTag]RelationSpec
}

// NewSchema returns a Schema over the given Config and optional Extractors.
// Deep checking of specification referential integrity is performed, and if
// Extractors are provided, specifications are checked for consistency against
// registered extractors as well.
func NewSchema(optionalExtractors *Extractors, cfg Config) (Schema, error) {
	// Build up mappings of tags to associated specifications. As we process
	// portions of the Config, we'll use these indexes to validate referential
	// integrity of the holistic, composed Config & Extractors.
	var (
		ext        Extractors
		mappings   = make(map[MapTag]MappingSpec)
		dimensions = make(map[DimTag]DimensionSpec)
		metrics    = make(map[MetTag]MetricSpec)
		relations  = make(map[RelTag]RelationSpec)
		names      = make(map[string]struct{})
	)

	var onName = func(n string) error {
		if _, ok := names[n]; ok {
			return pb.NewValidationError("duplicated Name (%s)", n)
		} else {
			names[n] = struct{}{}
			return nil
		}
	}
	var onMapping = func(spec MappingSpec) error {
		if err := spec.Validate(); err != nil {
			return err
		} else if _, ok := ext.Mapping[spec.Tag]; !ok && optionalExtractors != nil {
			return pb.NewValidationError("Mapping extractor not registered (%d)", spec.Tag)
		} else if _, ok := mappings[spec.Tag]; ok {
			return pb.NewValidationError("Mapping already specified (%d)", spec.Tag)
		} else if err = onName(spec.Name); err != nil {
			return err
		} else {
			mappings[spec.Tag] = spec
			return nil
		}
	}

	var onDimension = func(spec DimensionSpec) error {
		if err := spec.Validate(); err != nil {
			return err
		}

		var ok bool
		switch spec.Type {
		case DimensionType_VARINT:
			_, ok = ext.Int[spec.Tag]
		case DimensionType_FLOAT:
			_, ok = ext.Float[spec.Tag]
		case DimensionType_STRING:
			_, ok = ext.String[spec.Tag]
		case DimensionType_TIMESTAMP:
			_, ok = ext.Time[spec.Tag]
		default:
			panic("not reached")
		}

		if !ok && optionalExtractors != nil {
			return pb.NewValidationError("Dimension extractor not registered (%d; type %s)",
				spec.Tag, spec.Type)
		} else if _, ok := dimensions[spec.Tag]; ok {
			return pb.NewValidationError("Dimension already specified (%d)", spec.Tag)
		} else if err := onName(spec.Name); err != nil {
			return err
		} else {
			dimensions[spec.Tag] = spec
			return nil
		}
	}

	var onMetric = func(spec MetricSpec) error {
		if err := spec.Validate(); err != nil {
			return err
		} else if dimSpec, ok := dimensions[spec.DimTag]; !ok {
			return pb.NewValidationError("Metric DimTag is not specified (%d)", spec.DimTag)
		} else if dimSpec.Type != spec.Type.DimensionType() {
			return pb.NewValidationError("Metric Type mismatch (DimTag %d has type %v; expected %v)",
				dimSpec.Tag, dimSpec.Type, spec.Type.DimensionType())
		} else if _, ok := metrics[spec.Tag]; ok {
			return pb.NewValidationError("Metric already specified (%d)", spec.Tag)
		} else if err = onName(spec.Name); err != nil {
			return err
		} else {
			metrics[spec.Tag] = spec
			return nil
		}
	}

	var onRelation = func(spec RelationSpec) error {
		if err := spec.Validate(); err != nil {
			return err
		} else if _, ok := mappings[spec.Mapping]; !ok {
			return pb.NewValidationError("Mapping is not specified (%d)", spec.Mapping)
		}
		for i, tag := range spec.Shape.Dimensions {
			if _, ok := dimensions[tag]; !ok {
				return pb.ExtendContext(pb.NewValidationError("Dimension not specified (%d)", tag),
					"Shape.Dimensions[%d]", i)
			}
		}
		for i, tag := range spec.Shape.Metrics {
			if _, ok := metrics[tag]; !ok {
				return pb.ExtendContext(
					pb.NewValidationError("Metric not specified (%d)", tag),
					"Shape.Metrics[%d]", i)
			}
		}
		if spec.Retention != nil {
			if dimSpec := dimensions[spec.Retention.RelativeTo]; dimSpec.Type != DimensionType_TIMESTAMP {
				return pb.ExtendContext(pb.NewValidationError(
					"Dimension Type mismatch (%v; expected TIMESTAMP)", dimSpec.Type),
					"Retention.RelativeTo")
			}
		}
		if _, ok := relations[spec.Tag]; ok {
			return pb.NewValidationError("Relation already specified (%d)", spec.Tag)
		} else if err := onName(spec.Name); err != nil {
			return err
		} else {
			relations[spec.Tag] = spec
			return nil
		}
	}

	if optionalExtractors != nil {
		ext = *optionalExtractors
		if ext.NewMessage == nil {
			return Schema{}, pb.NewValidationError("Extractors: NewMessage not defined")
		}
	}
	for i := range cfg.Mappings {
		if err := onMapping(cfg.Mappings[i]); err != nil {
			return Schema{}, pb.ExtendContext(err, "Mappings[%d]", i)
		}
	}
	for i := range cfg.Dimensions {
		if err := onDimension(cfg.Dimensions[i]); err != nil {
			return Schema{}, pb.ExtendContext(err, "Dimensions[%d]", i)
		}
	}
	for i := range cfg.Metrics {
		if err := onMetric(cfg.Metrics[i]); err != nil {
			return Schema{}, pb.ExtendContext(err, "Metrics[%d]", i)
		}
	}
	for i := range cfg.Relations {
		if err := onRelation(cfg.Relations[i]); err != nil {
			return Schema{}, pb.ExtendContext(err, "Relations[%d]", i)
		}
	}
	return Schema{
		Extractors: ext,
		Mappings:   mappings,
		Dimensions: dimensions,
		Metrics:    metrics,
		Relations:  relations,
	}, nil
}

// ExtractDimension extracts dimension |dim| from the Tuple and appends its
// encoding to |b|, returning the result.
func (schema *Schema) ExtractAndEncodeDimension(b []byte, dim DimTag, t InputRecord) []byte {
	switch schema.Dimensions[dim].Type {
	case DimensionType_VARINT:
		b = encoding.EncodeVarintAscending(b, schema.Extractors.Int[dim](t))
	case DimensionType_FLOAT:
		b = encoding.EncodeFloatAscending(b, schema.Extractors.Float[dim](t))
	case DimensionType_STRING:
		b = encoding.EncodeStringAscending(b, schema.Extractors.String[dim](t))
	case DimensionType_TIMESTAMP:
		b = encoding.EncodeTimeAscending(b, schema.Extractors.Time[dim](t))
	default:
		panic("unexpected dimension type")
	}
	return b
}

// DequeEncodedDimension of DimTag |dim| from |b|, returning the remainder.
func (schema *Schema) DequeEncodedDimension(b []byte, dim DimTag) (rem []byte, err error) {
	var tmp [64]byte
	switch schema.Dimensions[dim].Type {
	case DimensionType_VARINT:
		rem, _, err = encoding.DecodeVarintAscending(b)
	case DimensionType_FLOAT:
		rem, _, err = encoding.DecodeFloatAscending(b)
	case DimensionType_STRING:
		rem, _, err = encoding.DecodeBytesAscending(b, tmp[:0])
	case DimensionType_TIMESTAMP:
		rem, _, err = encoding.DecodeTimeAscending(b)
	default:
		panic("unexpected dimension type")
	}
	return
}

// DequeEncodedMetric of MetTag |met| from |b|, returning the remainder.
func (schema *Schema) DequeEncodedMetric(b []byte, met MetTag) (rem []byte, err error) {
	switch schema.Metrics[met].Type {
	case MetricType_VARINT_SUM, MetricType_VARINT_GUAGE:
		rem, _, err = encoding.DecodeVarintAscending(b)
	case MetricType_FLOAT_SUM:
		rem, _, err = encoding.DecodeFloatAscending(b)
	case MetricType_STRING_HLL:
		rem, _, err = decodeMaybePrefixedBytes(b)
	default:
		panic("unexpected metric type")
	}
	return
}

// InitMetric returns a suitable Aggregate for metric |met|. If |agg| is non-nil
// it must have been previously initialized for |met|, and will be zero'd for reuse.
func (schema *Schema) InitMetric(met MetTag, agg Aggregate) Aggregate {
	switch schema.Metrics[met].Type {
	case MetricType_VARINT_SUM, MetricType_VARINT_GUAGE:
		if ptr, ok := agg.(*int64); ok {
			*ptr = 0
		} else {
			agg = new(int64)
		}
	case MetricType_FLOAT_SUM:
		if ptr, ok := agg.(*float64); ok {
			*ptr = 0
		} else {
			agg = new(float64)
		}
	case MetricType_STRING_HLL:
		if agg == nil {
			agg = new(uniqueAggregate)
		}
		agg.(*uniqueAggregate).p = hll.Init(agg.(*uniqueAggregate).p)
	default:
		panic("unexpected metric type")
	}
	return agg
}

// EncodeMetric appends the encoding of |agg| (which must be of metric type |met|)
// to |b|, returning the result.
func (schema *Schema) EncodeMetric(b []byte, met MetTag, agg Aggregate) []byte {
	switch schema.Metrics[met].Type {
	case MetricType_VARINT_SUM, MetricType_VARINT_GUAGE:
		b = encoding.EncodeVarintAscending(b, *agg.(*int64))
	case MetricType_FLOAT_SUM:
		b = encoding.EncodeFloatAscending(b, *agg.(*float64))
	case MetricType_STRING_HLL:
		var ua = *agg.(*uniqueAggregate)
		b = encoding.EncodeUvarintAscending(b, uint64(len(ua.p)))
		b = append(b, ua.p...)
	default:
		panic("unexpected metric type")
	}
	return b
}

// FoldMetric extracts metric |met| from the InputRecord and folds it into
// Aggregate |agg|, which must have already been initialized.
func (schema *Schema) FoldMetric(met MetTag, agg Aggregate, t InputRecord) {
	switch spec := schema.Metrics[met]; spec.Type {
	case MetricType_VARINT_SUM:
		*agg.(*int64) += schema.Extractors.Int[spec.DimTag](t)
	case MetricType_VARINT_GUAGE:
		*agg.(*int64) = schema.Extractors.Int[spec.DimTag](t)
	case MetricType_FLOAT_SUM:
		*agg.(*float64) += schema.Extractors.Float[spec.DimTag](t)
	case MetricType_STRING_HLL:
		if s := schema.Extractors.String[spec.DimTag](t); len(s) != 0 {
			var hash = hll.MurmurSum64([]byte(s)) // Doesn't escape => no copy.
			var reg, count = hll.RegisterRhoRetailNext(hash)
			var ua = agg.(*uniqueAggregate)
			var err error

			if ua.p, ua.tmp, _, err = hll.Add(ua.p, ua.tmp, reg, count); err != nil {
				panic(err)
			}
		}
	default:
		panic("unexpected metric type")
	}
}

// ReduceMetric decodes an Aggregate of metric |met| from |b|, and reduces
// it into |agg| (which must have already been initialized). The remainder of
// |b| is returned.
func (schema *Schema) ReduceMetric(b []byte, met MetTag, agg Aggregate) (rem []byte, err error) {
	switch spec := schema.Metrics[met]; spec.Type {
	case MetricType_VARINT_SUM:
		var v int64
		if rem, v, err = encoding.DecodeVarintAscending(b); err == nil {
			*agg.(*int64) += v
		}
	case MetricType_VARINT_GUAGE:
		var v int64
		if rem, v, err = encoding.DecodeVarintAscending(b); err == nil {
			*agg.(*int64) = v
		}
	case MetricType_FLOAT_SUM:
		var v float64
		if rem, v, err = encoding.DecodeFloatAscending(b); err == nil {
			*agg.(*float64) += v
		}
	case MetricType_STRING_HLL:
		var ua = agg.(*uniqueAggregate)
		var dec []byte

		if rem, dec, err = decodeMaybePrefixedBytes(b); err == nil {
			ua.p, ua.tmp, err = hll.Reduce(ua.p, dec, ua.tmp)
		}
	default:
		panic("unexpected dimension type")
	}
	return
}

func decodeMaybePrefixedBytes(b []byte) (rem []byte, dec []byte, err error) {
	// Magic byte from the `encoding` package for representing ascending bytes.
	const bytesAscendingMarker byte = 0x12

	// Is this a legacy ascending bytes encoding?
	// TODO(johnny): Remove support, replacing with test & panic.
	if b[0] == bytesAscendingMarker {
		rem, dec, err = encoding.DecodeBytesAscending(b, nil)
		return
	}

	// This is the newer, varint prefix encoding.
	var l uint64
	if rem, l, err = encoding.DecodeUvarintAscending(b); err != nil {
		return
	} else if int(l) > len(rem) {
		err = errors.New("invalid byte length")
	} else {
		dec, rem = rem[:l], rem[l:]
	}
	return
}

type uniqueAggregate struct{ p, tmp []byte }
