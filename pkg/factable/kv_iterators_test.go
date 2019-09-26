package factable

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"math/rand"

	"github.com/cockroachdb/cockroach/util/encoding"
	gc "github.com/go-check/check"
)

type IteratorsSuite struct{}

func (s *IteratorsSuite) TestMergeIteratorCases(c *gc.C) {
	var seq = buildTestKeyValueSequence()

	// Case: Merging a single iterator is an identity transform.
	var it = NewMergeIterator(NewSliceIterator(seq...))
	verify(c, it, NewSliceIterator(seq...), false, false)

	// Randomly split |seq| across multiple key/value sets, preserving order.
	// |kvsOne|, |kvsTwo|, and |kvsThree| are fully disjoint and collectively
	// hold the entire |seq|. |kvsFour| duplicates portions of the sequence.
	var rnd = rand.New(rand.NewSource(0xfeedbeef))
	var kvsOne, kvsTwo, kvsThree, kvsFour [][2][]byte

	for _, kv := range seq {
		switch rnd.Intn(5) {
		case 0:
			kvsOne = append(kvsOne, kv)
		case 1:
			kvsTwo = append(kvsTwo, kv)
		case 2:
			kvsThree = append(kvsThree, kv)
		case 3:
			kvsOne = append(kvsOne, kv)
			kvsFour = append(kvsFour, kv)
		case 4:
			kvsTwo = append(kvsTwo, kv)
			kvsFour = append(kvsFour, kv)
		}
	}

	// Case: |kvsFour| is not included. Expect the iterator produces |seq|, exactly.
	it = NewMergeIterator(
		NewSliceIterator(kvsOne...),
		NewSliceIterator(kvsTwo...),
		NewSliceIterator(kvsThree...),
	)
	verify(c, it, NewSliceIterator(seq...), false, false)

	// Case: Now include |kvsFour|. Expect the iterator produces |seq|, with repeats.
	var it1, it2, it3, it4 = NewSliceIterator(kvsOne...),
		NewSliceIterator(kvsTwo...),
		NewSliceIterator(kvsThree...),
		NewSliceIterator(kvsFour...)
	it = NewMergeIterator(it1, it2, it3, it4)
	verify(c, it, NewSliceIterator(seq...), true, false)

	// Expect Close also closes all subordinate iterators.
	c.Check(it.Close(), gc.IsNil)
	for _, sit := range []*sliceIter{it1, it2, it3, it4} {
		c.Check(sit.closed, gc.Equals, true)
	}

	// Case: Expect errors are prioritized and returned.
	it = NewMergeIterator(NewSliceIterator(kvsOne...), errIterator{})
	var _, _, err = it.Next()
	c.Check(err, gc.ErrorMatches, "next error")
	c.Check(it.Close(), gc.ErrorMatches, "close error")
}

func (s *IteratorsSuite) TestCombinerIteratorCases(c *gc.C) {
	var schema, err = NewSchema(makeExtractors(), makeTestConfig())
	c.Assert(err, gc.IsNil)

	var (
		input = ResolvedView{
			DimTags: []DimTag{dimAnIntTag, dimAnIntTwoTag, dimAnIntThreeTag},
			MetTags: []MetTag{metAnIntSumTag, metAnIntGaugeTag},
		}
		query = ResolvedQuery{
			View: input,
			Filters: []ResolvedQuery_Filter{
				{DimTag: dimAnIntTag, Ranges: []ResolvedQuery_Filter_Range{
					{End: encoding.EncodeVarintAscending(nil, startAt+4)},
				}},
			},
		}
	)
	var seq = buildTestKeyValueSequence()

	// Case: query filter limits output to first half of the sequence.
	it, err := NewCombinerIterator(NewSliceIterator(seq...), schema, input, query)
	c.Assert(err, gc.IsNil)
	verify(c, it, NewSliceIterator(seq[:len(seq)/2]...), false, false)

	// Case: no filter. All input is consumed.
	query.Filters = nil

	it, err = NewCombinerIterator(NewSliceIterator(seq...), schema, input, query)
	c.Assert(err, gc.IsNil)
	verify(c, it, NewSliceIterator(seq...), false, false)

	// Case: all input rows combined into a single output row.
	query.View.DimTags = nil

	it, err = NewCombinerIterator(NewSliceIterator(seq...), schema, input, query)
	c.Assert(err, gc.IsNil)

	key, val, err := it.Next()
	c.Check(err, gc.IsNil)
	c.Check(key, gc.DeepEquals, []byte(nil))
	c.Check(val, gc.DeepEquals, PackValue(1000, 9)) // 1k row sum; 9 is last row gauge.

	_, _, err = it.Next()
	c.Check(err, gc.Equals, KVIteratorDone)

	// Case: all input is filtered.
	query.Filters = []ResolvedQuery_Filter{
		{DimTag: dimAnIntTag, Ranges: []ResolvedQuery_Filter_Range{
			{End: encoding.EncodeVarintAscending(nil, startAt-1)},
		}},
	}

	var sliceIt = NewSliceIterator(seq...)
	it, err = NewCombinerIterator(sliceIt, schema, input, query)
	c.Assert(err, gc.IsNil)

	_, _, err = it.Next()
	c.Check(err, gc.Equals, KVIteratorDone)

	// Close also closes the wrapped iterator.
	c.Check(it.Close(), gc.IsNil)
	c.Check(sliceIt.closed, gc.Equals, true)

	// Case: invalid QuerySpec returns an error.
	it, err = NewCombinerIterator(nil, schema, ResolvedView{}, query)
	c.Check(err, gc.ErrorMatches, `Filter Dimension tag \d+ not in input ViewSpec .*`)

	// Case: Wrapped iterator errors are passed through.
	it, err = NewCombinerIterator(errIterator{}, schema, input, query)
	c.Check(err, gc.IsNil)

	_, _, err = it.Next()
	c.Check(err, gc.ErrorMatches, "next error")
	c.Check(it.Close(), gc.ErrorMatches, "close error")
}

func (s *IteratorsSuite) TestSortingIterator(c *gc.C) {
	var schema, err = NewSchema(makeExtractors(), makeTestConfig())
	c.Assert(err, gc.IsNil)

	var (
		input = ResolvedView{
			DimTags: []DimTag{dimAnIntTag, dimAnIntTwoTag, dimAnIntThreeTag},
			MetTags: []MetTag{metAnIntSumTag, metAnIntGaugeTag},
		}
		query = ResolvedQuery{
			View: ResolvedView{
				DimTags: []DimTag{dimAnIntThreeTag, dimAnIntTwoTag, dimAnIntTag}, // Invert order.
				MetTags: []MetTag{metAnIntSumTag},
			},
		}
	)
	var seq = buildTestKeyValueSequence()

	var sliceIt = NewSliceIterator(seq...)
	combIt, err := NewCombinerIterator(sliceIt, schema, input, query)
	c.Assert(err, gc.IsNil)

	// Expect that, after sorting, order matches that of |seq|.
	var it = NewSortingIterator(combIt, 1<<11, 4)
	verify(c, it, NewSliceIterator(seq...), false, true)

	// Expect that closing the sorting iterator cascades to wrapped iterators.
	c.Check(it.Close(), gc.IsNil)
	c.Check(sliceIt.closed, gc.Equals, true)

	// Expect wrapped iterator errors are passed through.
	it = NewSortingIterator(errIterator{}, 1<<11, 4)

	_, _, err = it.Next()
	c.Check(err, gc.ErrorMatches, "next error")
	c.Check(it.Close(), gc.ErrorMatches, "close error")
}

func (s *IteratorsSuite) TestStreamIteratorCases(c *gc.C) {
	// Build a fixture with two keys & values.
	var kv []byte
	{
		var buf bytes.Buffer
		var bw = bufio.NewWriter(&buf)
		c.Check(MarshalBinaryKeyValue(bw, []byte("key 1"), []byte("value one")), gc.IsNil)
		c.Check(MarshalBinaryKeyValue(bw, []byte("key two"), []byte("value 2")), gc.IsNil)
		c.Check(bw.Flush(), gc.IsNil)
		kv = buf.Bytes()
	}
	// readKV verifies the |kv| fixture is read from |it|.
	var readKV = func(it *streamIter) {
		var k, v, err = it.Next()
		c.Check(err, gc.IsNil)
		c.Check(k, gc.DeepEquals, []byte("key 1"))
		c.Check(v, gc.DeepEquals, []byte("value one"))

		k, v, err = it.Next()
		c.Check(err, gc.IsNil)
		c.Check(k, gc.DeepEquals, []byte("key two"))
		c.Check(v, gc.DeepEquals, []byte("value 2"))

		_, _, err = it.Next()
		c.Check(err, gc.Equals, KVIteratorDone)
	}

	var buildRecv = func(payloads ...[]byte) func(interface{}) error {
		return func(qr interface{}) error {
			if l := len(payloads); l != 0 {
				qr.(*QueryResponse).Content = payloads[0]
				payloads = payloads[1:]
				return nil
			} else {
				return io.EOF
			}
		}
	}

	// Case: Single frame with length prefix & content.
	readKV(NewStreamIterator(buildRecv(kv)))
	// Case: Length prefix is split across frames.
	readKV(NewStreamIterator(buildRecv(kv[:3], kv[3:])))
	// Case: Content suffix is split across frames.
	readKV(NewStreamIterator(buildRecv(kv[:12], kv[12:])))
	// Case: Empty iterator.
	var _, _, err = NewStreamIterator(buildRecv()).Next()
	c.Check(err, gc.Equals, KVIteratorDone)
	// Case: Unexpected EOF reading length prefix.
	_, _, err = NewStreamIterator(buildRecv(kv[:7])).Next()
	c.Check(err, gc.Equals, io.ErrUnexpectedEOF)
	// Case: Unexpected EOF reading content suffix.
	_, _, err = NewStreamIterator(buildRecv(kv[:12])).Next()
	c.Check(err, gc.Equals, io.ErrUnexpectedEOF)
	// Case: Unreasonably large key/value length.
	_, _, err = NewStreamIterator(buildRecv([]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff})).Next()
	c.Check(err, gc.ErrorMatches, `read prefix is too large: \d+`)

	// Case: Expect Close()ing the iterator fully drains it.
	var it = NewStreamIterator(buildRecv(kv))
	c.Check(it.Close(), gc.IsNil)
	_, _, err = it.Next()
	c.Check(err, gc.Equals, KVIteratorDone)
}

func (s *IteratorsSuite) TestHexIteratorRoundTrip(c *gc.C) {
	var (
		buf bytes.Buffer
		seq            = buildTestKeyValueSequence()
		it  KVIterator = NewSliceIterator(seq...)
		bw             = bufio.NewWriter(&buf)
		hw             = NewHexEncoder(bw)
	)
	// Encode |seq| into |buf|.
	for {
		var key, value, err = it.Next()
		if err == KVIteratorDone {
			break
		}
		c.Assert(err, gc.IsNil)
		c.Assert(hw.Encode(key, value), gc.IsNil)
	}
	c.Assert(bw.Flush(), gc.IsNil)

	it = NewHexIterator(bufio.NewReader(&buf))
	verify(c, it, NewSliceIterator(seq...), false, false)
}

func (s *IteratorsSuite) TestHexIteratorBufferFull(c *gc.C) {
	var (
		buf bytes.Buffer
		seq            = buildFatValueSequence()
		it  KVIterator = NewSliceIterator(seq...)
		bw             = bufio.NewWriter(&buf)
		hw             = NewHexEncoder(bw)
	)
	// Encode |seq| into |buf|.
	for {
		var key, value, err = it.Next()
		if err == KVIteratorDone {
			break
		}
		c.Assert(err, gc.IsNil)
		c.Assert(hw.Encode(key, value), gc.IsNil)
	}
	c.Assert(bw.Flush(), gc.IsNil)

	it = NewHexIterator(bufio.NewReaderSize(&buf, 16))
	verify(c, it, NewSliceIterator(seq...), false, false)
}

func buildFatValueSequence() (kvs [][2][]byte) {
	// value will fit into 16 byte buffer (including '\n' byte suffix).
	var fit = [2][]byte{{0x01, 0x10}, {0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06}}
	// value won't fit into 16 byte buffer.
	var noFit = [2][]byte{{0x01, 0x11}, {0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}}
	kvs = append(kvs, fit)
	kvs = append(kvs, noFit)
	return
}

func buildTestKeyValueSequence() (kvs [][2][]byte) {
	var arena = make(kvsArena, 1<<12)
	var value = encoding.EncodeVarintAscending(nil, 1)

	for i := 0; i != 10; i++ {
		for j := 0; j != 10; j++ {
			for k := 0; k != 10; k++ {
				kvs = append(kvs, arena.add(
					buildKey(i, j, k),
					encoding.EncodeVarintAscending(value, int64(k)),
				))
			}
		}
	}
	return
}

func verify(c *gc.C, it KVIterator, expect KVIterator, allowRepeats bool, useValuePrefix bool) {
	var notFirst bool
	var expectKey, expectValue []byte
	for {
		var key, value, err = it.Next()

		if allowRepeats && notFirst && err == nil &&
			bytes.Compare(key, expectKey) == 0 &&
			bytes.Compare(value, expectValue) == 0 {
			continue
		}
		notFirst = true

		var expectErr error
		expectKey, expectValue, expectErr = expect.Next()
		c.Check(err, gc.Equals, expectErr)

		if err != nil {
			break
		}

		c.Check(key, gc.DeepEquals, expectKey)

		if useValuePrefix {
			c.Check(bytes.HasPrefix(expectValue, value), gc.Equals, true)
		} else {
			c.Check(value, gc.DeepEquals, expectValue)
		}
	}
}

type errIterator struct{}

func (errIterator) Next() ([]byte, []byte, error) { return nil, nil, errors.New("next error") }
func (errIterator) Close() error                  { return errors.New("close error") }

var _ = gc.Suite(&IteratorsSuite{})
