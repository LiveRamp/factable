//go:generate protoc -I . -I ../../vendor  --gogo_out=plugins=grpc:. factable.proto
package factable

import (
	"fmt"
	"time"

	"git.liveramp.net/jgraet/factable/pkg/hll"
	"github.com/LiveRamp/gazette/v2/pkg/message"
	pb "github.com/LiveRamp/gazette/v2/pkg/protocol"
	"github.com/cockroachdb/cockroach/util/encoding"
	"github.com/pkg/errors"
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
	MapTag int64
	// DimTag uniquely identifies a DimensionSpec.
	DimTag int64
	// MetTag uniquely identifies a MetricSpec.
	MetTag int64
	// RelTag uniquely identifies a RelationSpec.
	RelTag int64
	// MVTag uniquely identifies a MaterializedViewSpec.
	MVTag int64
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

	Mappings   map[MapTag]MappingSpec
	Dimensions map[DimTag]DimensionSpec
	Metrics    map[MetTag]MetricSpec
	Relations  map[RelTag]RelationSpec
	Views      map[MVTag]MaterializedViewSpec
}

// NewSchema returns a Schema over the given Spec and optional ExtractFns.
// Deep checking of specification referential integrity is performed, and if
// ExtractFns are provided, specifications are checked for consistency against
// registered extractors as well.
func NewSchema(optionalExtractors *ExtractFns, spec SchemaSpec) (Schema, error) {
	// Build up mappings of tags to associated specifications. As we process
	// portions of the Spec, we'll use these indexes to validate referential
	// integrity of the holistic, composed Spec & ExtractFns.
	var (
		ext        ExtractFns
		mappings   = make(map[MapTag]MappingSpec)
		dimensions = make(map[DimTag]DimensionSpec)
		metrics    = make(map[MetTag]MetricSpec)
		relations  = make(map[RelTag]RelationSpec)
		views      = make(map[MVTag]MaterializedViewSpec)
		names      = make(map[string]struct{})
	)

	dimensions[DimMVTag] = DimensionSpec{
		Tag:  0,
		Type: DimensionType_VARINT,
		Name: "MVTag",
		Desc: "Placeholder DimensionSpec for MVTag",
	}

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
		} else if _, ok = mappings[spec.Tag]; ok {
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
		} else if _, ok = dimensions[spec.Tag]; ok {
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
		} else if _, ok = metrics[spec.Tag]; ok {
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
		for i, tag := range spec.Dimensions {
			if _, ok := dimensions[tag]; !ok {
				return pb.ExtendContext(pb.NewValidationError("Dimension not specified (%d)", tag),
					"Dimensions[%d]", i)
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

	var onMaterializedView = func(spec MaterializedViewSpec) error {
		if err := spec.Validate(); err != nil {
			return err
		} else if _, ok := relations[spec.View.RelTag]; !ok {
			return pb.NewValidationError("View.RelTag is not specified (%d)", spec.View.RelTag)
		}
		for i, tag := range spec.View.Dimensions {
			if !findDim(tag, relations[spec.View.RelTag].Dimensions) {
				return pb.ExtendContext(
					pb.NewValidationError("Dimension not part of Relation (%d; RelTag %d)", tag, spec.View.RelTag),
					"View.Dimensions[%d]", i)
			}
		}
		for i, tag := range spec.View.Metrics {
			if metSpec, ok := metrics[tag]; !ok {
				return pb.ExtendContext(
					pb.NewValidationError("Metric not specified (%d)", tag),
					"View.Metrics[%d]", i)
			} else if !findDim(metSpec.DimTag, relations[spec.View.RelTag].Dimensions) {
				return pb.ExtendContext(
					pb.NewValidationError("Metric Dimension not part of Relation (%d; DimTag %d; RelTag %d)",
						tag, metSpec.DimTag, spec.View.RelTag),
					"View.Metrics[%d]", i)
			}
		}
		if spec.Retention != nil {
			if dimSpec := dimensions[spec.Retention.RelativeTo]; dimSpec.Type != DimensionType_TIMESTAMP {
				return pb.ExtendContext(pb.NewValidationError(
					"Dimension Type mismatch (%v; expected TIMESTAMP)", dimSpec.Type),
					"Retention.RelativeTo")
			}
			// Note that spec.Validate() already asserts RelativeTo is a member of Dimensions.
		}
		if _, ok := views[spec.Tag]; ok {
			return pb.NewValidationError("MaterializedView already specified (%d)", spec.Tag)
		} else if err := onName(spec.Name); err != nil {
			return err
		} else {
			views[spec.Tag] = spec
			return nil
		}
	}

	if optionalExtractors != nil {
		ext = *optionalExtractors
		if ext.NewMessage == nil {
			return Schema{}, pb.NewValidationError("ExtractFns: NewMessage not defined")
		}
	}
	for i := range spec.Mappings {
		if err := onMapping(spec.Mappings[i]); err != nil {
			return Schema{}, pb.ExtendContext(err, "Mappings[%d]", i)
		}
	}
	for i := range spec.Dimensions {
		if err := onDimension(spec.Dimensions[i]); err != nil {
			return Schema{}, pb.ExtendContext(err, "Dimensions[%d]", i)
		}
	}
	for i := range spec.Metrics {
		if err := onMetric(spec.Metrics[i]); err != nil {
			return Schema{}, pb.ExtendContext(err, "Metrics[%d]", i)
		}
	}
	for i := range spec.Relations {
		if err := onRelation(spec.Relations[i]); err != nil {
			return Schema{}, pb.ExtendContext(err, "Relations[%d]", i)
		}
	}
	for i := range spec.Views {
		if err := onMaterializedView(spec.Views[i]); err != nil {
			return Schema{}, pb.ExtendContext(err, "Views[%d]", i)
		}
	}
	return Schema{
		Extract:    ext,
		Spec:       spec,
		Mappings:   mappings,
		Dimensions: dimensions,
		Metrics:    metrics,
		Relations:  relations,
		Views:      views,
	}, nil
}

// ExtractAndMarshalDimension extracts dimension |dim| from the RelationRow and
// appends its encoding to |b|, returning the result.
func (schema *Schema) ExtractAndMarshalDimension(b []byte, dim DimTag, row RelationRow) []byte {
	switch schema.Dimensions[dim].Type {
	case DimensionType_VARINT:
		b = encoding.EncodeVarintAscending(b, schema.Extract.Int[dim](row))
	case DimensionType_FLOAT:
		b = encoding.EncodeFloatAscending(b, schema.Extract.Float[dim](row))
	case DimensionType_STRING:
		b = encoding.EncodeStringAscending(b, schema.Extract.String[dim](row))
	case DimensionType_TIMESTAMP:
		b = encoding.EncodeTimeAscending(b, schema.Extract.Time[dim](row))
	default:
		panic("unexpected dimension type")
	}
	return b
}

// UnmarshalDimension of DimTag |dim| from |b|, returning the un-marshaled Field and byte remainder.
func (schema *Schema) UnmarshalDimension(b []byte, dim DimTag) (rem []byte, field Field, err error) {
	if dim == DimMVTag {
		// Special case: this is the RelTag which prefixes the row.
		rem, field, err = encoding.DecodeVarintAscending(b)
		return
	}
	switch schema.Dimensions[dim].Type {
	case DimensionType_VARINT:
		rem, field, err = encoding.DecodeVarintAscending(b)
	case DimensionType_FLOAT:
		rem, field, err = encoding.DecodeFloatAscending(b)
	case DimensionType_STRING:
		rem, field, err = encoding.DecodeStringAscending(b, nil)
	case DimensionType_TIMESTAMP:
		rem, field, err = encoding.DecodeTimeAscending(b)
	default:
		panic("unexpected dimension type")
	}
	return
}

// UnmarshalDimensions of DimTags |dims| from |b|, invoking |fn| for each
// un-marshaled Field. It may return a decoding error, an error returned by
// |fn|, or an error if the input |b| has any unconsumed remainder.
func (schema *Schema) UnmarshalDimensions(b []byte, dims []DimTag, fn func(Field) error) error {
	var (
		field Field
		err   error
	)
	for _, tag := range dims {
		if b, field, err = schema.UnmarshalDimension(b, tag); err != nil {
			return err
		} else if err = fn(field); err != nil {
			return err
		}
	}
	if len(b) != 0 {
		return errors.Errorf("unexpected []byte remainder (%x)", b)
	}
	return nil
}

// dequeDimension of DimTag |dim| from |b|, returning the remainder.
// dequeDimension is like UnmarshalDimension, but avoids unnecessary interface{}
// allocations.
func (schema *Schema) dequeDimension(b []byte, dim DimTag) (rem []byte, err error) {
	if dim == DimMVTag {
		// Special case: this is the MVTag which prefixes the row.
		rem, _, err = encoding.DecodeVarintAscending(b)
		return
	}
	var tmp [64]byte // Does not escape.
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

// MarshalMetric appends the encoding of |agg| (which must be of metric type |met|)
// to |b|, returning the result.
func (schema *Schema) MarshalMetric(b []byte, met MetTag, agg Aggregate) []byte {
	switch schema.Metrics[met].Type {
	case MetricType_VARINT_SUM, MetricType_VARINT_GUAGE:
		b = encoding.EncodeVarintAscending(b, *agg.(*int64))
	case MetricType_FLOAT_SUM:
		b = encoding.EncodeFloatAscending(b, *agg.(*float64))
	case MetricType_STRING_HLL:
		var ua = agg.(*uniqueAggregate)
		b = encoding.EncodeUvarintAscending(b, uint64(len(ua.p)))
		b = append(b, ua.p...)
	default:
		panic("unexpected metric type")
	}
	return b
}

// UnmarshalMetric of MetTag |met| from |b|, returning the un-marshaled Aggregate and byte remainder.
func (schema *Schema) UnmarshalMetric(b []byte, met MetTag) (rem []byte, agg Aggregate, err error) {
	switch schema.Metrics[met].Type {
	case MetricType_VARINT_SUM, MetricType_VARINT_GUAGE:
		var v = new(int64)
		rem, *v, err = encoding.DecodeVarintAscending(b)
		agg = v
	case MetricType_FLOAT_SUM:
		var v = new(float64)
		rem, *v, err = encoding.DecodeFloatAscending(b)
		agg = v
	case MetricType_STRING_HLL:
		rem, agg, err = decodeMaybePrefixedBytes(b)
		agg = &uniqueAggregate{p: agg.([]byte)}
	default:
		panic("unexpected metric type")
	}
	return
}

// UnmarshalMetrics of MetTags |mets| from |b|, invoking |fn| for each
// un-marshaled Aggregate. It may return a decoding error, an error returned by
// |fn|, or an error if the input |b| has any unconsumed remainder.
func (schema *Schema) UnmarshalMetrics(b []byte, mets []MetTag, fn func(Aggregate) error) error {
	var (
		agg Aggregate
		err error
	)
	for _, tag := range mets {
		if b, agg, err = schema.UnmarshalMetric(b, tag); err != nil {
			return err
		} else if err = fn(agg); err != nil {
			return err
		}
	}
	if len(b) != 0 {
		return errors.Errorf("unexpected []byte remainder (%x)", b)
	}
	return nil
}

// dequeMetric of MetTag |met| from |b|, returning the remainder. dequeMetric is
// like UnmarshalMetric, but avoids unnecessary interface{} allocations and does
// not fail if |b| is empty.
func (schema *Schema) dequeMetric(b []byte, met MetTag) (rem []byte, err error) {
	if len(b) == 0 {
		// Ignore Aggregates not actually present in |b|. This allows
		// new Metrics to be appended onto an existing Relation.
		return
	}
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

// FoldMetric extracts metric |met| from the RelationRow and folds it into
// Aggregate |agg|, which must have already been initialized.
func (schema *Schema) FoldMetric(met MetTag, agg Aggregate, row RelationRow) {
	switch spec := schema.Metrics[met]; spec.Type {
	case MetricType_VARINT_SUM:
		*agg.(*int64) += schema.Extract.Int[spec.DimTag](row)
	case MetricType_VARINT_GUAGE:
		*agg.(*int64) = schema.Extract.Int[spec.DimTag](row)
	case MetricType_FLOAT_SUM:
		*agg.(*float64) += schema.Extract.Float[spec.DimTag](row)
	case MetricType_STRING_HLL:
		if s := schema.Extract.String[spec.DimTag](row); len(s) != 0 {
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
	if len(b) == 0 {
		return
	}
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
		panic("unexpected metric type")
	}
	return
}

// ValidateSchemaTransition returns an error if the transition from |from|
// to |to| would alter an immutable property of the Schema.
func ValidateSchemaTransition(from, to Schema) (err error) {
	// Confirm no DimensionSpec.Types were changed.
	for i, dimTo := range to.Spec.Dimensions {
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
	for i, metTo := range to.Spec.Metrics {
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
	for i, relTo := range to.Spec.Relations {
		if relFrom, ok := from.Relations[relTo.Tag]; !ok {
			continue // Added.
		} else if relFrom.Mapping != relTo.Mapping {
			err = pb.NewValidationError("cannot alter immutable Mapping (%d != %d)", relFrom.Mapping, relTo.Mapping)
		}
		if err != nil {
			return pb.ExtendContext(err, "Relations[%d]", i)
		}
	}
	// Confirm no MaterializedViewSpec.Views have changed.
	for i, mvTo := range to.Spec.Views {
		if mvFrom, ok := from.Views[mvTo.Tag]; !ok {
			continue // Added.
		} else if veErr := mvFrom.View.VerboseEqual(&mvTo.View); veErr != nil {
			err = pb.NewValidationError("cannot alter immutable View: %s", veErr.Error())
		}
		if err != nil {
			return pb.ExtendContext(err, "Views[%d]", i)
		}
	}
	return nil
}

// Flatten Aggregate into a corresponding simple Field type.
func Flatten(agg Aggregate) Field {
	switch a := agg.(type) {
	case *int64:
		return *a
	case *float64:
		return *a
	case *uniqueAggregate:
		if cnt, err := hll.Count(a.p); err != nil {
			panic(err)
		} else {
			return int64(cnt)
		}
	default:
		panic(a)
	}
}

// BuildStrHLL constructs a HLL Aggregate initialized with the given strings.
// It's primarily useful in the construction of test fixtures.
func BuildStrHLL(strs ...string) Aggregate {
	var agg = new(uniqueAggregate)
	agg.p = hll.Init(agg.p)

	for _, s := range strs {
		var hash = hll.MurmurSum64([]byte(s))
		var reg, count = hll.RegisterRhoRetailNext(hash)
		var err error

		if agg.p, agg.tmp, _, err = hll.Add(agg.p, agg.tmp, reg, count); err != nil {
			panic(err)
		}
	}
	return agg
}

// PackKey encodes a Schema key having the given dimensions. It's primarily
// useful in the construction of test fixtures.
func PackKey(dims ...interface{}) []byte {
	var b = []byte{}

	for i := range dims {
		switch d := dims[i].(type) {
		case int:
			b = encoding.EncodeVarintAscending(b, int64(d))
		case MVTag:
			b = encoding.EncodeVarintAscending(b, int64(d))
		case int64:
			b = encoding.EncodeVarintAscending(b, d)
		case float64:
			b = encoding.EncodeFloatAscending(b, d)
		case string:
			b = encoding.EncodeStringAscending(b, d)
		case time.Time:
			b = encoding.EncodeTimeAscending(b, d)
		default:
			panic(dims[i])
		}
	}
	return b
}

// PackValue encodes a Schema value having the given aggregates. It's primarily
// useful in the construction of test fixtures.
func PackValue(aggs ...interface{}) []byte {
	var b = []byte{}

	for i := range aggs {
		switch a := aggs[i].(type) {

		case int:
			b = encoding.EncodeVarintAscending(b, int64(a))
		case int64:
			b = encoding.EncodeVarintAscending(b, a)
		case float64:
			b = encoding.EncodeFloatAscending(b, a)
		case *uniqueAggregate:
			b = encoding.EncodeUvarintAscending(b, uint64(len(a.p)))
			b = append(b, a.p...)
		default:
			panic(aggs[i])
		}
	}
	return b
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

		if _, err = hll.Count(dec); err != nil {
			panic(fmt.Sprintf("len %d : %s", len(dec), err))
		}
	}
	return
}

type uniqueAggregate struct{ p, tmp []byte }

func (ua *uniqueAggregate) String() string {
	var cnt, err = hll.Count(ua.p)
	if err != nil {
		panic(err)
	}
	return fmt.Sprintf("hll(%d)", cnt)
}

// DimMVTag is a non-validating DimTag which stands-in for the MVTag
// encoded as the first field of every row key serialized in a VTable.
const DimMVTag DimTag = 0
