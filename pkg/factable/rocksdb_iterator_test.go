// +build rocksdb

package factable

import (
	"io/ioutil"
	"os"

	"github.com/cockroachdb/cockroach/util/encoding"
	gc "github.com/go-check/check"
	"github.com/tecbot/gorocksdb"
)

func (s *IteratorsSuite) TestRocksIteratorCases(c *gc.C) {
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
	var it KVIterator = NewRocksDBIterator(db, ro, nil, nil)
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

	// Case: layer on a NewCombinerIterator. Expect that iterator uses the Seeker
	// interface provided by RocksDB.
	var (
		input = ResolvedView{
			DimTags: []DimTag{dimAnIntTag, dimAnIntTwoTag, dimAnIntThreeTag},
			MetTags: []MetTag{metAnIntSumTag, metAnIntGaugeTag},
		}
		query = ResolvedQuery{
			View: input,
			Filters: []ResolvedQuery_Filter{
				{DimTag: dimAnIntTag, Ranges: []ResolvedQuery_Filter_Range{
					{Begin: encoding.EncodeVarintAscending(nil, startAt+5)}}},
			},
		}
	)
	schema, err := NewSchema(makeExtractors(), makeTestConfig())
	c.Assert(err, gc.IsNil)

	// Case: query filter limits output to first half of the sequence.
	it, err = NewCombinerIterator(NewRocksDBIterator(db, ro, nil, nil), schema, input, query)
	c.Assert(err, gc.IsNil)
	verify(c, it, NewSliceIterator(seq[len(seq)/2:]...), false, false)

	// Case: all input rows combined into a single output row.
	query.View.DimTags = nil

	it, err = NewCombinerIterator(NewRocksDBIterator(db, ro, nil, nil), schema, input, query)
	c.Assert(err, gc.IsNil)

	key, value, err := it.Next()
	c.Check(err, gc.IsNil)
	c.Check(key, gc.DeepEquals, []byte(nil))
	c.Check(value, gc.DeepEquals, PackValue(500, 9)) // 500 row sum; 9 is last row gauge.

	_, _, err = it.Next()
	c.Check(err, gc.Equals, KVIteratorDone)

	// Case: all input is filtered.
	query.Filters = []ResolvedQuery_Filter{
		{DimTag: dimAnIntTag, Ranges: []ResolvedQuery_Filter_Range{
			{End: encoding.EncodeVarintAscending(nil, startAt-1)},
		}},
	}

	it, err = NewCombinerIterator(NewRocksDBIterator(db, ro, nil, nil), schema, input, query)
	c.Assert(err, gc.IsNil)

	_, _, err = it.Next()
	c.Check(err, gc.Equals, KVIteratorDone)
}
