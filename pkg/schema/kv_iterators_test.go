package schema

import (
	"bytes"
	"io/ioutil"
	"math/rand"
	"os"

	"github.com/cockroachdb/cockroach/util/encoding"
	gc "github.com/go-check/check"
	"github.com/tecbot/gorocksdb"
)

type IteratorsSuite struct{}

func (s *IteratorsSuite) TestMergeIteratorCases(c *gc.C) {
	var seq = buildTestKeyValueSequence()
	var rnd = rand.New(rand.NewSource(0xfeedbeef))

	// Randomly split |seq| across multiple key/value sets, preserving order.
	// |kvsOne|, |kvsTwo|, and |kvsThree| are fully disjoint and collectively
	// hold the entire |seq|. |kvsFour| duplicates portions of the sequence.
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

	// |kvsFour| is not included. Expect the iterator produces |seq|, exactly.
	var it = NewMergeIterator(
		NewSliceIterator(kvsOne...),
		NewSliceIterator(kvsTwo...),
		NewSliceIterator(kvsThree...),
	)
	verify(c, it, NewSliceIterator(seq...), false, false)

	// Now include |kvsFour|. Expect the iterator produces |seq|, with repeats.
	it = NewMergeIterator(
		NewSliceIterator(kvsOne...),
		NewSliceIterator(kvsTwo...),
		NewSliceIterator(kvsThree...),
		NewSliceIterator(kvsFour...),
	)
	verify(c, it, NewSliceIterator(seq...), true, false)
}

func (s *IteratorsSuite) TestCombinerIteratorCases(c *gc.C) {
	var schema, err = NewSchema(makeExtractors(), makeTestConfig())
	c.Assert(err, gc.IsNil)

	var (
		input = Shape{
			Dimensions: []DimTag{dimAnInt, dimAnIntTwo, dimAnIntThree},
			Metrics:    []MetTag{metAnIntSum, metAnIntGauge},
		}
		query = QuerySpec{
			Shape: input,
			Filters: []QuerySpec_Filter{
				{Dimension: dimAnInt, Ranges: []Range{{Int: &Range_Int{End: startAt + 4}}}},
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
	query.Dimensions = nil

	it, err = NewCombinerIterator(NewSliceIterator(seq...), schema, input, query)
	c.Assert(err, gc.IsNil)

	c.Check(it.Next(), gc.IsNil)
	c.Check(it.Key(), gc.DeepEquals, []byte(nil))
	c.Check(it.Value(), gc.DeepEquals, encoding.EncodeVarintAscending(
		encoding.EncodeVarintAscending(nil, 1000), 9)) // 1k row sum; 9 is last row gauge.
	c.Check(it.Next(), gc.Equals, KVIteratorDone)
}

func (s *IteratorsSuite) TestSortingIterator(c *gc.C) {
	var schema, err = NewSchema(makeExtractors(), makeTestConfig())
	c.Assert(err, gc.IsNil)

	var (
		input = Shape{
			Dimensions: []DimTag{dimAnInt, dimAnIntTwo, dimAnIntThree},
			Metrics:    []MetTag{metAnIntSum, metAnIntGauge},
		}
		query = QuerySpec{
			Shape: Shape{
				Dimensions: []DimTag{dimAnIntThree, dimAnIntTwo, dimAnInt}, // Invert order.
				Metrics:    []MetTag{metAnIntSum},
			},
		}
	)
	var seq = buildTestKeyValueSequence()

	combIt, err := NewCombinerIterator(NewSliceIterator(seq...), schema, input, query)
	c.Assert(err, gc.IsNil)

	// Expect that, after sorting, order matches that of |seq|.
	var it = NewSortingIterator(combIt, 1<<11, 4)
	verify(c, it, NewSliceIterator(seq...), false, true)
}

func (s *IteratorsSuite) TestRocksIterator(c *gc.C) {
	var dir, err = ioutil.TempDir("", "iters-suite")
	c.Assert(err, gc.IsNil)
	defer func() { c.Assert(os.RemoveAll(dir), gc.IsNil) }()

	var (
		opts = gorocksdb.NewDefaultOptions()
		wo   = gorocksdb.NewDefaultWriteOptions()
		ro   = gorocksdb.NewDefaultReadOptions()
	)
	defer opts.Destroy()
	defer wo.Destroy()
	defer ro.Destroy()

	opts.SetCreateIfMissing(true)
	db, err := gorocksdb.OpenDb(opts, dir)
	c.Assert(err, gc.IsNil)
	defer db.Close()

	var seq = buildTestKeyValueSequence()
	for _, kv := range seq {
		c.Assert(db.Put(wo, kv[0], kv[1]), gc.IsNil)
	}

	// Case: no range restriction given.
	var it = NewRocksDBIterator(db, ro, nil, nil)
	verify(c, it, NewSliceIterator(seq...), false, false)

	// Case: begin half-way through the sequence.
	it = NewRocksDBIterator(db, ro, buildKey(5), nil)
	verify(c, it, NewSliceIterator(seq[len(seq)/2:]...), false, false)

	// Case: end half-way through the sequence.
	it = NewRocksDBIterator(db, ro, nil, buildKey(5))
	verify(c, it, NewSliceIterator(seq[:len(seq)/2]...), false, false)

	// Case: range produces a single key.
	it = NewRocksDBIterator(db, ro, buildKey(5), buildKey(5, 0, 1))
	verify(c, it, NewSliceIterator(seq[len(seq)/2:1+len(seq)/2]...), false, false)
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
	for {
		var err = it.Next()

		if allowRepeats && notFirst && err == nil &&
			bytes.Compare(it.Key(), expect.Key()) == 0 &&
			bytes.Compare(it.Value(), expect.Value()) == 0 {
			continue
		}
		notFirst = true

		c.Check(err, gc.Equals, expect.Next())

		if err != nil {
			break
		}

		c.Check(it.Key(), gc.DeepEquals, expect.Key())

		if useValuePrefix {
			c.Check(bytes.HasPrefix(expect.Value(), it.Value()), gc.Equals, true)
		} else {
			c.Check(it.Value(), gc.DeepEquals, expect.Value())
		}

	}
}

func newKV(key, value string) [2][]byte { return [2][]byte{[]byte(key), []byte(value)} }

var _ = gc.Suite(&IteratorsSuite{})
