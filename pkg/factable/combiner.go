package factable

import (
	"bytes"
	"fmt"

	"github.com/pkg/errors"
)

// Combiner combines ordered keys and values of a relation on behalf of a
// QuerySpec. It applies query filters, identifies opportunities to seek forward
// in the input key space due to current filters, and produces grouped output
// keys and aggregates.
type Combiner struct {
	Schema

	input struct {
		dimension struct {
			// Ordered dimension tags of each input row.
			tags []DimTag
			// A range is specified as [2][]byte, which are inclusive begin & end values
			// for a range of acceptable dimension field encodings. |ranges| is an
			// ordered, non-overlapping set of such ranges.
			ranges [][][2][]byte
		}
		metric struct {
			// Ordered metric tags of each input row.
			tags []MetTag
			// Reorder is the Metric's index in the grouped output. If -1,
			// the metric does not appear in the output and aggregation
			// may be skipped.
			reorder []int
		}
		// Current accepted row, and the next row being considered.
		cur, next inputRowState
	}

	output struct {
		// For each output dimension, its index within the input dimensions.
		dimOrder []int
		// Ordered metric tags of each output row.
		metrics []MetTag
		// Previously flushed output row, and the next row being combined over.
		flushed, next struct {
			key    []byte      // Grouped output key.
			aggs   []Aggregate // Grouped aggregates, in |output.metrics| order.
			sawRow bool        // Is at least one input row reflected by the grouping?
		}
	}
}

// NewCombiner returns a Combiner over rows having the |input| ViewSpec, and which
// emits rows & aggregates according to the Query ViewSpec and applicable filters.
// It returns an error if the |input| ViewSpec or |query| are not well-formed with
// respect to each other, and to the Schema.
func NewCombiner(schema Schema, input ViewSpec, query QuerySpec) (*Combiner, error) {
	// Index dimension tags by their order within the input ViewSpec.
	var dimIndex = make(map[DimTag]int)
	for d, tag := range input.Dimensions {
		if _, ok := schema.Dimensions[tag]; !ok {
			return nil, errors.Errorf("Input Dimension tag %d not in Schema", tag)
		} else if _, ok = dimIndex[tag]; ok {
			return nil, errors.Errorf("Input Dimension tag %d appears more than once", tag)
		}
		dimIndex[tag] = d
	}
	// Mark dimensions included in the output shape, and their order w.r.t the input dimensions.
	var groupedDims []int
	var dimRanges = make([][][2][]byte, len(input.Dimensions))

	for _, tag := range query.View.Dimensions {
		if ind, ok := dimIndex[tag]; !ok {
			return nil, errors.Errorf("Query Dimension tag %d not in input ViewSpec (%v)", tag, input.Dimensions)
		} else if dimRanges[ind] != nil {
			return nil, errors.Errorf("Query Dimension tag %d appears more than once", tag)
		} else {
			groupedDims = append(groupedDims, ind)
			// Use a zero-valued but non-nil slice placeholder to mark this
			// as an output dimension which must be captured.
			dimRanges[ind] = [][2][]byte{}
		}
	}
	// For each Query_Filter, verify its Dimension and collect its flattened byte ranges.
	for _, f := range query.Filters {
		if ind, ok := dimIndex[f.Dimension]; !ok {
			return nil, errors.Errorf("Filter Dimension tag %d not in input ViewSpec (%v)", f.Dimension, input.Dimensions)
		} else if len(dimRanges[ind]) != 0 {
			return nil, errors.Errorf("Filter Dimension tag %d appears more than once", f.Dimension)
		} else if rng, err := flattenRangeSpecs(schema.Dimensions[f.Dimension], f.Ranges); err != nil {
			return nil, err
		} else {
			dimRanges[ind] = rng
		}
	}
	// Drop extra trailing dimensions of the input which we don't need
	// (because they have no filter ranges, and they're not part of the output).
	for len(dimRanges) != 0 && dimRanges[len(dimRanges)-1] == nil {
		dimRanges = dimRanges[:len(dimRanges)-1]
	}
	// Fill remaining, empty |dimRanges| with an unconstrained, open-ended range.
	for d, rng := range dimRanges {
		if len(rng) == 0 {
			dimRanges[d] = [][2][]byte{{}}
		}
	}

	// Index metric tags by their order within the Query ViewSpec.
	var metInvIndex = make(map[MetTag]int)
	for m, tag := range query.View.Metrics {
		if _, ok := schema.Metrics[tag]; !ok {
			return nil, errors.Errorf("Query Metric tag %d not in Schema", tag)
		} else if _, ok = metInvIndex[tag]; ok {
			return nil, errors.Errorf("Query Metric tag %d appears more than once", tag)
		}
		metInvIndex[tag] = m
	}
	// Build the mapping of input metrics to their respective output order.
	var reorderedMetrics []int
	for _, tag := range input.Metrics {
		if _, ok := schema.Metrics[tag]; !ok {
			return nil, errors.Errorf("Input Metric tag %d not in Schema", tag)
		} else if ind, ok := metInvIndex[tag]; ok {
			reorderedMetrics = append(reorderedMetrics, ind)
			delete(metInvIndex, tag)
		} else {
			reorderedMetrics = append(reorderedMetrics, -1)
		}
	}
	if len(metInvIndex) != 0 {
		return nil, errors.Errorf("Query Metrics not in Input ViewSpec (%v)", metInvIndex)
	}

	// Drop extra trailing metrics of the input which we don't need.
	for len(reorderedMetrics) != 0 && reorderedMetrics[len(reorderedMetrics)-1] == -1 {
		reorderedMetrics = reorderedMetrics[:len(reorderedMetrics)-1]
	}

	var cb = &Combiner{Schema: schema}
	cb.input.dimension.tags = input.Dimensions[:len(dimRanges)]
	cb.input.dimension.ranges = dimRanges
	cb.input.metric.tags = input.Metrics[:len(reorderedMetrics)]
	cb.input.metric.reorder = reorderedMetrics
	cb.input.cur = inputRowState{
		offsets:  make([]int, len(dimRanges)+1),
		rangeInd: make([]int, len(dimRanges)),
	}
	cb.input.next = inputRowState{
		offsets: make([]int, len(dimRanges)+1),
	}
	cb.output.dimOrder = groupedDims
	cb.output.metrics = query.View.Metrics

	_ = cb.Flush()
	return cb, nil
}

// Combine folds the ordered input key & value into the Combiner's state.
// |seek| is returned if the Combiner did not accept |key|, and will not process
// further input keys less than the key returned by Seek(). |flushed| is
// returned if the Combiner determined that a previous grouped key has been fully
// aggregated, and Flushed it. Only one of |flushed| or |seek| will be set.
func (cb *Combiner) Combine(key, value []byte) (flushed, seek bool, err error) {
	if seek, err = cb.transition(key); err != nil || seek {
		return
	}
	// Did the output key change? If so, we must flush.
	for _, d := range cb.output.dimOrder {
		if cb.input.cur.updated[d] {
			flushed = cb.Flush()
			break
		}
	}
	// Reduce value aggregates under our next grouped output key.
	for m, tag := range cb.input.metric.tags {
		if ind := cb.input.metric.reorder[m]; ind != -1 {
			value, err = cb.Schema.ReduceMetric(value, tag, cb.output.next.aggs[ind])
		} else {
			value, err = cb.Schema.dequeMetric(value, tag)
		}
		if err != nil {
			return
		}
	}
	cb.output.next.sawRow = true
	return
}

// Seek appends the next potential key or key prefix which will be
// considered by the Combiner to |b|. If Seek instead returns nil,
// then the valid input range has been exhausted and iteration may halt.
func (cb *Combiner) Seek(b []byte) []byte {
	return buildSeekKey(b, cb.input.next, cb.input.dimension.ranges)
}

// Flush the key and aggregates currently being grouped. It returns false iff
// the Combiner has no current key & aggregates to be flushed (eg, because the
// Combiner is in an initialized state, or every key supplied to Combine was
// filtered).
func (cb *Combiner) Flush() bool {
	// Swap |next| & |flushed|, and re-init |next|.
	cb.output.next, cb.output.flushed = cb.output.flushed, cb.output.next // Swap.

	// Build the next output key.
	cb.output.next.key = cb.output.next.key[:0]
	for _, d := range cb.output.dimOrder {
		var (
			input = cb.input.cur
			field = input.key[input.offsets[d]:input.offsets[d+1]]
		)
		cb.output.next.key = append(cb.output.next.key, field...)
	}
	// Initialize Aggregates.
	if cb.output.next.aggs == nil {
		cb.output.next.aggs = make([]Aggregate, len(cb.output.metrics))
	}
	for m, tag := range cb.output.metrics {
		cb.output.next.aggs[m] = cb.Schema.InitMetric(tag, cb.output.next.aggs[m])
	}
	cb.output.next.sawRow = false

	return cb.output.flushed.sawRow
}

// Key returns the last Flushed key.
func (cb *Combiner) Key() []byte { return cb.output.flushed.key }

// Value returns the last Flushed value, which is appended to |b|.
func (cb *Combiner) Value(b []byte) []byte {
	for m, tag := range cb.output.metrics {
		b = cb.Schema.MarshalMetric(b, tag, cb.output.flushed.aggs[m])
	}
	return b
}

// Aggregates returns the last Flushed Aggregates.
func (cb *Combiner) Aggregates() []Aggregate { return cb.output.flushed.aggs }

type inputRowState struct {
	key      []byte // Raw input row key.
	offsets  []int  // Begin and end byte offsets of each field within the key. Has len(dims)+1.
	rangeInd []int  // Index of each field within the set of allowed field ranges.
	updated  []bool // True if the field has been updated.
}

func (cb *Combiner) transition(key []byte) (rejected bool, err error) {
	// Re-init for this |key|.
	cb.input.next = inputRowState{
		key:      append(cb.input.next.key[:0], key...),
		offsets:  append(cb.input.next.offsets[:0], 0),
		rangeInd: cb.input.next.rangeInd[:0],
		updated:  cb.input.next.updated[:0],
	}
	var restartRanges bool

	for d := 0; d != len(cb.input.dimension.tags) && !rejected; d++ {
		var (
			pBeg, pEnd = cb.input.cur.offsets[d], cb.input.cur.offsets[d+1]
			pField     = cb.input.cur.key[pBeg:pEnd]
			nBeg       = cb.input.next.offsets[d]
			nEnd       = min(nBeg+len(pField), len(cb.input.next.key))
		)
		// Compare with raw bytes of the last field to determine relative
		// ordering. This is safe to do because variable-length encoded fields
		// always include a trailing termination marker, which means a field can
		// never encode as a byte prefix of another field unless it is strictly
		// equal to it.
		if cmp := bytes.Compare(pField, cb.input.next.key[nBeg:nEnd]); !restartRanges && cmp > 0 {
			panic(fmt.Sprintf(
				"key ordering invariant violated (pField %v; cur.key %v, nField %v, next.key %v)",
				pField,
				cb.input.cur.key,
				cb.input.next.key[nBeg:],
				cb.input.next.key,
			))
		} else if cmp == 0 && len(pField) != 0 {
			// Dimension didn't change from |cur| to |next|.
			cb.input.next.offsets = append(cb.input.next.offsets, nEnd)
			cb.input.next.updated = append(cb.input.next.updated, false)
			cb.input.next.rangeInd = append(cb.input.next.rangeInd, cb.input.cur.rangeInd[d])
			continue
		}

		// Dimension |d| changed from key |cur| to |next|.
		//
		// We must determine dimension |d|'s actual encoded length, and then
		// determine whether and where the field indexes within the dimension's
		// predicate ranges.
		if rem, err := cb.dequeDimension(cb.input.next.key[nBeg:], cb.input.dimension.tags[d]); err != nil {
			return true, errors.Wrapf(err, "decoding dimension %d of %v", d, cb.input.next)
		} else {
			nEnd = len(cb.input.next.key) - len(rem)
		}

		var (
			nField   = cb.input.next.key[nBeg:nEnd]
			rangeInd int
		)
		if !restartRanges {
			// This is the first altered dimension we've encountered. Start
			// evaluating from where |cur| left off. All dimensions which follow
			// should restart range iteration.
			rangeInd = cb.input.cur.rangeInd[d]
			restartRanges = true
		}
		// Increment |rangeInd| until we find a range which |nField| falls
		// within, or we exhaust allowed ranges.
		for !rejected {
			if rangeInd == len(cb.input.dimension.ranges[d]) {
				rejected = true // We're beyond all accepted ranges.
				break
			}
			var rng [2][]byte = cb.input.dimension.ranges[d][rangeInd]

			// Is |nField| ordered *after* rng[1]? An empty filter is
			// treated as greater than any field value.
			if len(rng[1]) != 0 && bytes.Compare(nField, rng[1]) > 0 {
				rangeInd++ // This range is exhausted. Increment to the next.
				continue
			}
			// Is |nField| ordered *before* rng[0]? An empty filter is
			// treated as less than any field value.
			if bytes.Compare(nField, rng[0]) < 0 {
				rejected = true
			} else {
				// |nField| is within |rng|.
			}
			break
		}

		cb.input.next.offsets = append(cb.input.next.offsets, nEnd)
		cb.input.next.updated = append(cb.input.next.updated, true)
		cb.input.next.rangeInd = append(cb.input.next.rangeInd, rangeInd)
	}
	if !rejected {
		cb.input.next, cb.input.cur = cb.input.cur, cb.input.next // Transition to |next|.
	}
	return rejected, nil
}

func buildSeekKey(b []byte, r inputRowState, ranges [][][2][]byte) []byte {
	var (
		d        = len(r.rangeInd) - 1
		ind      = r.rangeInd[d]
		incByOne bool
	)
	for ind == len(ranges[d]) {
		if d == 0 {
			return nil
		}
		d, ind = d-1, r.rangeInd[d-1]

		// Determine the dimension's field order w.r.t. the current range end.
		// Treat a nil range-end as being open-ended.
		var (
			field = r.key[r.offsets[d]:r.offsets[d+1]]
			cmp   int
		)
		if ranges[d][ind][1] == nil {
			cmp = -1
		} else {
			cmp = bytes.Compare(field, ranges[d][ind][1])
		}

		switch cmp {
		case 0:
			// We have exhausted the current range. There may be another range of
			// this dimension. Otherwise we'll pop this dimension and check the next.
			ind++
		case -1:
			// Increment by one to obtain the next possible value within the range.
			incByOne = true
		case 1:
			panic("invalid range order")
		}
	}
	// The prefix up to dimension |d| remains the same.
	b = append(b, r.key[:r.offsets[d]]...)

	if incByOne {
		// Seek to the current value of dimension |d|, incremented by one.
		b = append(b, r.key[r.offsets[d]:r.offsets[d+1]]...)
		incrementByOne(b)
		d, ind = d+1, 0
	}

	// Append the next seek'd dimension range, as well as successive,
	// contiguous dimensions also having non-open begin ranges.
	for d != len(ranges) && ranges[d][ind][0] != nil {
		b = append(b, ranges[d][ind][0]...)
		d, ind = d+1, 0
	}
	return b
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func incrementByOne(b []byte) {
	for i := len(b) - 1; i >= 0; i-- {
		if b[i]++; b[i] != 0 {
			return // b[i] was not already 0xff.
		}
	}
	panic("b is already max-value")
}
