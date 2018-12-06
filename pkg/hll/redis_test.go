package hll

import (
	"encoding/binary"
	"math/rand"
	"testing"
	"time"

	gc "github.com/go-check/check"
)

type RedisSuite struct{}

func (s *RedisSuite) TestAppendSparseSpanCases(c *gc.C) {
	// Zero cases of decreasing bytes.
	c.Check(dumpOpcodes(appendSparseSpan(initEmpty(nil), opcodeXZEROMaxRun, 0)),
		gc.DeepEquals, []string{"XZERO(16384) [0, 16384)"})
	c.Check(dumpOpcodes(appendSparseSpan(initEmpty(nil), opcodeZEROMaxRun+1, 0)),
		gc.DeepEquals, []string{"XZERO(65) [0, 65)"})
	c.Check(dumpOpcodes(appendSparseSpan(initEmpty(nil), opcodeZEROMaxRun, 0)),
		gc.DeepEquals, []string{"ZERO(64) [0, 64)"})
	c.Check(dumpOpcodes(appendSparseSpan(initEmpty(nil), opcodeZEROMaxRun-1, 0)),
		gc.DeepEquals, []string{"ZERO(63) [0, 63)"})
	c.Check(dumpOpcodes(appendSparseSpan(initEmpty(nil), 1, 0)),
		gc.DeepEquals, []string{"ZERO(1) [0, 1)"})

	// Single-byte value cases.
	c.Check(dumpOpcodes(appendSparseSpan(initEmpty(nil), 1, 32)),
		gc.DeepEquals, []string{"VALUE(32 over 1) [0, 1)"})
	c.Check(dumpOpcodes(appendSparseSpan(initEmpty(nil), 2, 31)),
		gc.DeepEquals, []string{"VALUE(31 over 2) [0, 2)"})
	c.Check(dumpOpcodes(appendSparseSpan(initEmpty(nil), 3, 30)),
		gc.DeepEquals, []string{"VALUE(30 over 3) [0, 3)"})
	c.Check(dumpOpcodes(appendSparseSpan(initEmpty(nil), 4, 29)),
		gc.DeepEquals, []string{"VALUE(29 over 4) [0, 4)"})

	// Multi-byte value cases.
	c.Check(dumpOpcodes(appendSparseSpan(initEmpty(nil), 5, 28)),
		gc.DeepEquals, []string{
			"VALUE(28 over 4) [0, 4)",
			"VALUE(28 over 1) [4, 5)"})
	c.Check(dumpOpcodes(appendSparseSpan(initEmpty(nil), 10, 27)),
		gc.DeepEquals, []string{
			"VALUE(27 over 4) [0, 4)",
			"VALUE(27 over 4) [4, 8)",
			"VALUE(27 over 2) [8, 10)"})
	c.Check(dumpOpcodes(appendSparseSpan(initEmpty(nil), 7, 26)),
		gc.DeepEquals, []string{
			"VALUE(26 over 4) [0, 4)",
			"VALUE(26 over 3) [4, 7)"})
	c.Check(dumpOpcodes(appendSparseSpan(initEmpty(nil), 8, 1)),
		gc.DeepEquals, []string{
			"VALUE(1 over 4) [0, 4)",
			"VALUE(1 over 4) [4, 8)"})
}

func (s *RedisSuite) TestFindCoveringCases(c *gc.C) {
	var p = initEmpty(nil)

	p = appendSparseSpan(p, 30, 0)
	p = appendSparseSpan(p, 20, 0)
	p = appendSparseSpan(p, 1, 19)
	p = appendSparseSpan(p, 1, 19)
	p = appendSparseSpan(p, 1, 0)
	p = appendSparseSpan(p, 3, 5)
	p = appendSparseSpan(p, 10, 0)
	p = appendSparseSpan(p, 30, 0)

	c.Assert(dumpOpcodes(p), gc.DeepEquals, []string{
		"ZERO(30) [0, 30)",
		"ZERO(20) [30, 50)",
		"VALUE(19 over 1) [50, 51)",
		"VALUE(19 over 1) [51, 52)",
		"ZERO(1) [52, 53)",
		"VALUE(5 over 3) [53, 56)",
		"ZERO(10) [56, 66)",
		"ZERO(30) [66, 96)",
	})

	var cases = []struct {
		register          Register
		prev, cover, next span
	}{
		{
			register: 29, // In first ZERO opcode.
			prev:     span{},
			cover: span{
				begin: cursor{index: HeaderSize},
				end:   cursor{index: HeaderSize + 2, register: 50},
			},
			next: span{
				begin: cursor{index: HeaderSize + 2, register: 50},
				end:   cursor{index: HeaderSize + 4, register: 52},
				count: 19,
			},
		},
		{
			register: 51, // In second VALUE(19) opcode.
			prev: span{
				begin: cursor{index: HeaderSize},
				end:   cursor{index: HeaderSize + 2, register: 50},
			},
			cover: span{
				begin: cursor{index: HeaderSize + 2, register: 50},
				end:   cursor{index: HeaderSize + 4, register: 52},
				count: 19,
			},
			next: span{
				begin: cursor{index: HeaderSize + 4, register: 52},
				end:   cursor{index: HeaderSize + 5, register: 53},
			},
		},
		{
			register: 52, // In third ZERO(1) opcode.
			prev: span{
				begin: cursor{index: HeaderSize + 2, register: 50},
				end:   cursor{index: HeaderSize + 4, register: 52},
				count: 19,
			},
			cover: span{
				begin: cursor{index: HeaderSize + 4, register: 52},
				end:   cursor{index: HeaderSize + 5, register: 53},
			},
			next: span{
				begin: cursor{index: HeaderSize + 5, register: 53},
				end:   cursor{index: HeaderSize + 6, register: 56},
				count: 5,
			},
		},
		{
			register: 55, // In third VALUE(5) opcode.
			prev: span{
				begin: cursor{index: HeaderSize + 4, register: 52},
				end:   cursor{index: HeaderSize + 5, register: 53},
			},
			cover: span{
				begin: cursor{index: HeaderSize + 5, register: 53},
				end:   cursor{index: HeaderSize + 6, register: 56},
				count: 5,
			},
			next: span{
				begin: cursor{index: HeaderSize + 6, register: 56},
				end:   cursor{index: HeaderSize + 8, register: 96},
			},
		},
		{
			register: 95, // In final ZERO(30) opcode.
			prev: span{
				begin: cursor{index: HeaderSize + 5, register: 53},
				end:   cursor{index: HeaderSize + 6, register: 56},
				count: 5,
			},
			cover: span{
				begin: cursor{index: HeaderSize + 6, register: 56},
				end:   cursor{index: HeaderSize + 8, register: 96},
			},
			// Expect |next| is empty span at input end.
			next: span{
				begin: cursor{index: HeaderSize + 8, register: 96},
				end:   cursor{index: HeaderSize + 8, register: 96},
			},
		},
	}

	for _, tc := range cases {
		var prev, cover, next, err = findCoveringSpan(p, tc.register)

		c.Check(err, gc.IsNil)
		c.Check(prev, gc.Equals, tc.prev)
		c.Check(cover, gc.Equals, tc.cover)
		c.Check(next, gc.Equals, tc.next)
	}
}

func (s *RedisSuite) TestFindCoveringSingleTerminatingOpcode(c *gc.C) {
	var p = appendSparseSpan(initEmpty(nil), 16380, 0)
	p = appendSparseSpan(p, 2, 21)
	p = appendSparseSpan(p, 2, 31)

	// A single terminating opcode requires performing two span shifts
	// in one loop iteration.
	c.Assert(dumpOpcodes(p), gc.DeepEquals, []string{
		"XZERO(16380) [0, 16380)",
		"VALUE(21 over 2) [16380, 16382)",
		"VALUE(31 over 2) [16382, 16384)",
	})

	var prev, cover, next, err = findCoveringSpan(p, 16383)

	c.Check(err, gc.IsNil)
	c.Check(prev, gc.Equals, span{
		begin: cursor{index: HeaderSize + 2, register: 16380},
		end:   cursor{index: HeaderSize + 3, register: 16382},
		count: 21,
	})
	c.Check(cover, gc.Equals, span{
		begin: cursor{index: HeaderSize + 3, register: 16382},
		end:   cursor{index: HeaderSize + 4, register: 16384},
		count: 31,
	})
	c.Check(next, gc.Equals, span{
		begin: cursor{index: HeaderSize + 4, register: 16384},
		end:   cursor{index: HeaderSize + 4, register: 16384},
		count: 31, // Not cleared.
	})
}

func (s *RedisSuite) TestSparseAddCases(c *gc.C) {
	type setting struct {
		reg   Register
		value uint8
		added bool
	}

	var cases = []struct {
		seq    []setting
		expect []string
	}{
		// Case D only: Split and update VALUE.
		{[]setting{
			{59, 22, true},
			{59, 21, false},
			{59, 23, true},
		}, []string{
			"ZERO(59) [0, 59)",
			"VALUE(23 over 1) [59, 60)",
			"XZERO(16324) [60, 16384)"},
		},
		// Case D only: Split and update ZERO.
		{[]setting{
			{59, 2, true},
			{61, 4, true},
			{60, 3, true},
		}, []string{
			"ZERO(59) [0, 59)",
			"VALUE(2 over 1) [59, 60)",
			"VALUE(3 over 1) [60, 61)",
			"VALUE(4 over 1) [61, 62)",
			"XZERO(16322) [62, 16384)"},
		},
		// Case D & B: Merge with previous span.
		{[]setting{
			{59, 21, true},
			{60, 21, true},
		}, []string{
			"ZERO(59) [0, 59)",
			"VALUE(21 over 2) [59, 61)",
			"XZERO(16323) [61, 16384)"},
		},
		// Case D & B: Merge with previous span, removing covering span.
		{[]setting{
			{59, 21, true},
			{61, 20, true},
			{60, 21, true},
		}, []string{
			"ZERO(59) [0, 59)",
			"VALUE(21 over 2) [59, 61)",
			"VALUE(20 over 1) [61, 62)",
			"XZERO(16322) [62, 16384)"},
		},
		// Case D & C: Merge with next span.
		{[]setting{
			{60, 21, true},
			{59, 21, true},
		}, []string{
			"ZERO(59) [0, 59)",
			"VALUE(21 over 2) [59, 61)",
			"XZERO(16323) [61, 16384)"},
		},
		// Case D & C: Merge with next span, removing covering span.
		{[]setting{
			{59, 20, true},
			{61, 21, true},
			{60, 21, true},
		}, []string{
			"ZERO(59) [0, 59)",
			"VALUE(20 over 1) [59, 60)",
			"VALUE(21 over 2) [60, 62)",
			"XZERO(16322) [62, 16384)"},
		},
		// Case A: Grouping both forward and backward.
		{[]setting{
			{61, 21, true},
			{59, 21, true},
			{60, 21, true},
		}, []string{
			"ZERO(59) [0, 59)",
			"VALUE(21 over 3) [59, 62)",
			"XZERO(16322) [62, 16384)"},
		},
	}

	for _, tc := range cases {
		var p = Init(nil)
		var added bool
		var err error

		// Precondition: All registers are zeroed.
		c.Assert(dumpOpcodes(p), gc.DeepEquals, []string{"XZERO(16384) [0, 16384)"})

		for _, s := range tc.seq {
			p, added, err = sparseAdd(p, s.reg, s.value)
			c.Check(err, gc.IsNil)
			c.Check(added, gc.Equals, s.added)
		}
		c.Check(dumpOpcodes(p), gc.DeepEquals, tc.expect)
	}
}

func (s *RedisSuite) TestReduceSparseSparseCases(c *gc.C) {
	// Loop twice to test reduction order invariance.
	for i := 0; i != 2; i++ {
		var p, q, e = initEmpty(nil), initEmpty(nil), initEmpty(nil)

		// Notation: -'s are single-opcode runs, and ,'s are distinct opcodes.
		// P: [1,3,2-2,0,3,3,2].
		p = appendSparseSpan(p, 1, 1)
		p = appendSparseSpan(p, 1, 3)
		p = appendSparseSpan(p, 2, 2)
		p = appendSparseSpan(p, 1, 0)
		p = appendSparseSpan(p, 1, 3)
		p = appendSparseSpan(p, 1, 3)
		p = appendSparseSpan(p, 1, 2)

		// Q: [2,2-2,1.1,2,2,3].
		q = appendSparseSpan(q, 1, 2)
		q = appendSparseSpan(q, 2, 2)
		q = appendSparseSpan(q, 2, 1)
		q = appendSparseSpan(q, 1, 2)
		q = appendSparseSpan(q, 1, 2)
		q = appendSparseSpan(q, 1, 3)

		// Expected union: [2,3,2-2,1,3-3-3].
		e = appendSparseSpan(e, 1, 2)
		e = appendSparseSpan(e, 1, 3)
		e = appendSparseSpan(e, 2, 2)
		e = appendSparseSpan(e, 1, 1)
		e = appendSparseSpan(e, 3, 3)

		if i == 1 {
			// Expect the result is invariant to P, Q order.
			p, q = q, p
		}

		var err error
		p, _, err = reduceSparseSparse(p, q, nil)
		c.Check(err, gc.IsNil)
		c.Check(p, gc.DeepEquals, e)
	}
}

func (s *RedisSuite) TestReduceSparseSparse(c *gc.C) {
	var q = Init(nil)

	runReduceTestCase(c, reduceTestCase{
		iterations: 1000,
		itemMax:    1 << 16,
		p:          Init(nil),
		e:          Init(nil),
		addQ: func(tmp []byte, r Register, c uint8, _ uint64) (_ []byte, err error) {
			q, tmp, _, err = Add(q, tmp, r, c)
			return tmp, err
		},
		countQ: func() (int, error) { return Count(q) },
		bytesQ: func() []byte { return q },
	})
}

func (s *RedisSuite) TestReduceDenseDense(c *gc.C) {
	var q = InitDense(nil)

	runReduceTestCase(c, reduceTestCase{
		iterations: 100000,
		itemMax:    1 << 17,
		p:          InitDense(nil),
		e:          InitDense(nil),
		addQ: func(tmp []byte, r Register, c uint8, _ uint64) (_ []byte, err error) {
			q, tmp, _, err = Add(q, tmp, r, c)
			return tmp, err
		},
		countQ: func() (int, error) { return Count(q) },
		bytesQ: func() []byte { return q },
	})
}

func (s *RedisSuite) TestReduceDenseSparse(c *gc.C) {
	var q = Init(nil)

	runReduceTestCase(c, reduceTestCase{
		iterations: 1000,
		itemMax:    1 << 16,
		p:          InitDense(nil),
		e:          InitDense(nil),
		addQ: func(tmp []byte, r Register, c uint8, _ uint64) (_ []byte, err error) {
			q, tmp, _, err = Add(q, tmp, r, c)
			return tmp, err
		},
		countQ: func() (int, error) { return Count(q) },
		bytesQ: func() []byte { return q },
	})
}

type reduceTestCase struct {
	iterations int
	itemMax    int32

	p, e []byte

	addQ   func([]byte, Register, uint8, uint64) ([]byte, error)
	countQ func() (int, error)
	bytesQ func() []byte
}

func runReduceTestCase(c *gc.C, tc reduceTestCase) {
	var seed = time.Now().UnixNano()
	c.Log("seed: ", seed)
	rand.Seed(seed)

	var tmp []byte
	var err error

	for i := 0; i != tc.iterations; i++ {
		var b [4]byte
		binary.LittleEndian.PutUint32(b[:], uint32(rand.Int31n(tc.itemMax)))
		var hash = MurmurSum64(b[:])
		var reg, count = RegisterRhoRetailNext(hash)

		if i%2 == 0 {
			tc.p, tmp, _, err = Add(tc.p, tmp, reg, count)
			c.Assert(err, gc.IsNil)
			tc.e, tmp, _, err = Add(tc.e, tmp, reg, count)
			c.Assert(err, gc.IsNil)
		} else {
			tmp, err = tc.addQ(tmp, reg, count, hash)
			c.Assert(err, gc.IsNil)

			tc.e, tmp, _, err = Add(tc.e, tmp, reg, count)
			c.Assert(err, gc.IsNil)
		}
	}

	// Precondition: P & Q are disjoint sets.
	cp, errp := Count(tc.p)
	cq, errq := tc.countQ()
	ce, erre := Count(tc.e)

	c.Check(erre, gc.IsNil)
	c.Check(errp, gc.IsNil)
	c.Check(errq, gc.IsNil)

	c.Check(cp, gc.Not(gc.Equals), ce)
	c.Check(cq, gc.Not(gc.Equals), ce)

	// Reducing Q into P gives equal count & register bits to the expected union set.
	tc.p, tmp, errp = Reduce(tc.p, tc.bytesQ(), tmp)
	c.Check(errp, gc.IsNil)

	cp, errp = Count(tc.p)
	c.Check(errp, gc.IsNil)
	c.Check(cp, gc.Equals, ce)

	// Verify bit equality.
	c.Check(tc.p, gc.DeepEquals, tc.e)
}

var _ = gc.Suite(&RedisSuite{})

func Test(t *testing.T) { gc.TestingT(t) }
