package factable

import (
	"fmt"
	"time"

	"git.liveramp.net/jgraet/factable/pkg/hll"
	"github.com/cockroachdb/cockroach/util/encoding"
	"github.com/pkg/errors"
)

// ExtractAndMarshalDimension extracts ordered dimensions |dims| from the
// RelationRow and appends their encoding to |b|, returning the result.
func (schema *Schema) ExtractAndMarshalDimensions(b []byte, dims []DimTag, row RelationRow) []byte {
	for _, dim := range dims {
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
	}
	return b
}

// UnmarshalDimensions of DimTags |dims| from |b|, invoking |fn| for each
// un-marshaled Field. It may return a decoding error, an error returned by
// |fn|, or an error if the input |b| has any unconsumed remainder.
func (schema *Schema) UnmarshalDimensions(b []byte, dims []DimTag, fn func(Field) error) error {
	var (
		field Field
		err   error
	)
	for _, dim := range dims {

		if dim == DimMVTag {
			// Special case: this is a MVTag which prefixes the row.
			b, field, err = encoding.DecodeVarintAscending(b)
		} else {
			switch schema.Dimensions[dim].Type {
			case DimensionType_VARINT:
				b, field, err = encoding.DecodeVarintAscending(b)
			case DimensionType_FLOAT:
				b, field, err = encoding.DecodeFloatAscending(b)
			case DimensionType_STRING:
				b, field, err = encoding.DecodeStringAscending(b, nil)
			case DimensionType_TIMESTAMP:
				b, field, err = encoding.DecodeTimeAscending(b)
			default:
				panic("unexpected dimension type")
			}
		}

		if err != nil {
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
func (schema *Schema) dequeDimension(b []byte, dim DimTag) (rem []byte, err error) {
	if dim == DimMVTag {
		// Special case: this is a MVTag which prefixes the row.
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
func (schema *Schema) MarshalMetrics(b []byte, mets []MetTag, aggs []Aggregate) []byte {
	for i, met := range mets {
		switch schema.Metrics[met].Type {
		case MetricType_VARINT_SUM, MetricType_VARINT_GAUGE:
			b = encoding.EncodeVarintAscending(b, *aggs[i].(*int64))
		case MetricType_FLOAT_SUM:
			b = encoding.EncodeFloatAscending(b, *aggs[i].(*float64))
		case MetricType_STRING_HLL:
			var ua = aggs[i].(*uniqueAggregate)
			b = encoding.EncodeUvarintAscending(b, uint64(len(ua.p)))
			b = append(b, ua.p...)
		default:
			panic("unexpected metric type")
		}
	}
	return b
}

// UnmarshalMetrics of MetTags |mets| from |b|, invoking |fn| for each
// un-marshaled Aggregate. It may return a decoding error, an error returned by
// |fn|, or an error if the input |b| has any unconsumed remainder.
func (schema *Schema) UnmarshalMetrics(b []byte, mets []MetTag, fn func(Aggregate) error) error {
	var (
		agg Aggregate
		err error
	)
	for _, met := range mets {

		switch schema.Metrics[met].Type {
		case MetricType_VARINT_SUM, MetricType_VARINT_GAUGE:
			var v = new(int64)
			b, *v, err = encoding.DecodeVarintAscending(b)
			agg = v
		case MetricType_FLOAT_SUM:
			var v = new(float64)
			b, *v, err = encoding.DecodeFloatAscending(b)
			agg = v
		case MetricType_STRING_HLL:
			b, agg, err = decodePrefixedBytes(b)
			agg = &uniqueAggregate{p: agg.([]byte)}
		default:
			panic("unexpected metric type")
		}

		if err != nil {
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
	case MetricType_VARINT_SUM, MetricType_VARINT_GAUGE:
		rem, _, err = encoding.DecodeVarintAscending(b)
	case MetricType_FLOAT_SUM:
		rem, _, err = encoding.DecodeFloatAscending(b)
	case MetricType_STRING_HLL:
		rem, _, err = decodePrefixedBytes(b)
	default:
		panic("unexpected metric type")
	}
	return
}

// InitAggregates |aggs| in-place for each of metrics |met|. If an index
// aggs[i] is non-nil it must have been previously initialized for
// mets[i], and will be zero'd for reuse.
func (schema *Schema) InitAggregates(mets []MetTag, aggs []Aggregate) {
	if len(mets) != len(aggs) {
		panic("mets and aggs must be same length")
	}

	for i, met := range mets {
		switch schema.Metrics[met].Type {
		case MetricType_VARINT_SUM, MetricType_VARINT_GAUGE:
			if ptr, ok := aggs[i].(*int64); ok {
				*ptr = 0
			} else {
				aggs[i] = new(int64)
			}
		case MetricType_FLOAT_SUM:
			if ptr, ok := aggs[i].(*float64); ok {
				*ptr = 0
			} else {
				aggs[i] = new(float64)
			}
		case MetricType_STRING_HLL:
			if aggs[i] == nil {
				aggs[i] = new(uniqueAggregate)
			}
			aggs[i].(*uniqueAggregate).p = hll.Init(aggs[i].(*uniqueAggregate).p)
		default:
			panic("unexpected metric type")
		}
	}
}

// FoldMetrics extracts metrics |mets| from the RelationRow and folds it into
// Aggregates |aggs|, which must have already been initialized.
func (schema *Schema) FoldMetrics(mets []MetTag, aggs []Aggregate, row RelationRow) {
	if len(mets) != len(aggs) {
		panic("mets and aggs must be same length")
	}

	for i, met := range mets {
		switch spec := schema.Metrics[met]; spec.Type {
		case MetricType_VARINT_SUM:
			*aggs[i].(*int64) += schema.Extract.Int[spec.DimTag](row)
		case MetricType_VARINT_GAUGE:
			*aggs[i].(*int64) = schema.Extract.Int[spec.DimTag](row)
		case MetricType_FLOAT_SUM:
			*aggs[i].(*float64) += schema.Extract.Float[spec.DimTag](row)
		case MetricType_STRING_HLL:
			if s := schema.Extract.String[spec.DimTag](row); len(s) != 0 {
				var hash = hll.MurmurSum64([]byte(s)) // Doesn't escape => no copy.
				var reg, count = hll.RegisterRhoRetailNext(hash)
				var ua = aggs[i].(*uniqueAggregate)
				var err error

				if ua.p, ua.tmp, _, err = hll.Add(ua.p, ua.tmp, reg, count); err != nil {
					panic(err)
				}
			}
		default:
			panic("unexpected metric type")
		}
	}
}

// ReduceMetrics decodes Aggregates of metrics |mets| from |b|, and reduces
// each into |aggs| (which must have already been initialized). The remainder of
// |b| is returned.
func (schema *Schema) ReduceMetrics(b []byte, mets []MetTag, aggs []Aggregate) ([]byte, error) {
	if len(mets) != len(aggs) {
		panic("mets and aggs must be same length")
	}
	for i, tag := range mets {
		var err error
		if b, err = schema.reduceMetric(b, tag, aggs[i]); err != nil {
			return nil, err
		}
	}
	return b, nil
}

func (schema *Schema) reduceMetric(b []byte, met MetTag, agg Aggregate) (rem []byte, err error) {
	if len(b) == 0 {
		return
	}
	switch spec := schema.Metrics[met]; spec.Type {
	case MetricType_VARINT_SUM:
		var v int64
		if rem, v, err = encoding.DecodeVarintAscending(b); err == nil {
			*agg.(*int64) += v
		}
	case MetricType_VARINT_GAUGE:
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

		if rem, dec, err = decodePrefixedBytes(b); err == nil {
			ua.p, ua.tmp, err = hll.Reduce(ua.p, dec, ua.tmp)
		}
	default:
		panic("unexpected metric type")
	}
	return
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

// PackKey encodes a key having the given dimensions. It's primarily
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

// PackValue encodes a value having the given aggregates. It's primarily
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

func decodePrefixedBytes(b []byte) (rem []byte, dec []byte, err error) {
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
