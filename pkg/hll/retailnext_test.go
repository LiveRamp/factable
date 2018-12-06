package hll

import (
	gc "github.com/go-check/check"
	retailnext "github.com/retailnext/hllpp"
)

type RNSuite struct{}

func (s *RNSuite) TestRNSparseReader(c *gc.C) {
	var q = retailnext.New()

	q.AddHash(uint64(123<<(64-P)) | 0xffff)
	q.AddHash(uint64(456<<(64-P)) | 0x1ffff)
	q.AddHash(uint64(789<<(64-P)) | 0x2ffffff)

	var r = newSparseRNReader(q.Marshal())

	c.Check(r.cursor.register, gc.Equals, Register(123))
	c.Check(r.count, gc.Equals, uint8(35))

	c.Check(r.done(), gc.Equals, false)
	r.next()

	c.Check(r.cursor.register, gc.Equals, Register(456))
	c.Check(r.count, gc.Equals, uint8(34))

	c.Check(r.done(), gc.Equals, false)
	r.next()

	c.Check(r.cursor.register, gc.Equals, Register(789))
	c.Check(r.count, gc.Equals, uint8(25))

	c.Check(r.done(), gc.Equals, false)
	r.next()

	c.Check(r.done(), gc.Equals, true)
}

func (s *RNSuite) TestReduceSparseSparseRN(c *gc.C) {
	var q = retailnext.New()

	runReduceTestCase(c, reduceTestCase{
		iterations: 1000,
		itemMax:    1 << 16,
		p:          Init(nil),
		e:          Init(nil),
		addQ: func(tmp []byte, _ Register, _ uint8, h uint64) (_ []byte, err error) {
			q.AddHash(h)
			return tmp, nil
		},
		countQ: func() (int, error) { return int(q.Count()), nil },
		bytesQ: func() []byte { return q.Marshal() },
	})
}

func (s *RNSuite) TestReduceDenseSparseRN(c *gc.C) {
	var q = retailnext.New()

	runReduceTestCase(c, reduceTestCase{
		iterations: 1000,
		itemMax:    1 << 17,
		p:          InitDense(nil),
		e:          InitDense(nil),
		addQ: func(tmp []byte, _ Register, _ uint8, h uint64) (_ []byte, err error) {
			q.AddHash(h)
			return tmp, nil
		},
		countQ: func() (int, error) { return int(q.Count()), nil },
		bytesQ: func() []byte { return q.Marshal() },
	})
}

func (s *RNSuite) TestReduceDenseDenseRN5bits(c *gc.C) {
	var q = retailnext.New()

	runReduceTestCase(c, reduceTestCase{
		iterations: 100000,
		itemMax:    1 << 17,
		p:          InitDense(nil),
		e:          InitDense(nil),
		addQ: func(tmp []byte, _ Register, _ uint8, h uint64) (_ []byte, err error) {
			q.AddHash(h)
			return tmp, nil
		},
		countQ: func() (int, error) { return int(q.Count()), nil },
		bytesQ: func() []byte { return q.Marshal() },
	})
}

func (s *RNSuite) TestReduceDenseDenseRN6bits(c *gc.C) {
	var q = retailnext.New()
	var e = InitDense(nil)

	// Coerce a 6-bit HLL by adding a hash with a count requiring 6 bits.
	var hash = uint64(12345<<(64-P)) | 0xffff
	var reg, count = RegisterRhoRetailNext(hash)
	c.Assert(reg, gc.Equals, Register(12345))
	c.Assert(count, gc.Equals, uint8(35))

	q.AddHash(hash)
	e, _, _, _ = Add(e, nil, reg, count)

	runReduceTestCase(c, reduceTestCase{
		iterations: 100000,
		itemMax:    1 << 17,
		p:          InitDense(nil),
		e:          e,
		addQ: func(tmp []byte, _ Register, _ uint8, h uint64) (_ []byte, err error) {
			q.AddHash(h)
			return tmp, nil
		},
		countQ: func() (int, error) { return int(q.Count()), nil },
		bytesQ: func() []byte { return q.Marshal() },
	})
}

var _ = gc.Suite(&RNSuite{})
