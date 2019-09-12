package blackbox

import (
	"math/rand"
	"testing"
	"time"

	"github.com/LiveRamp/factable/pkg/hll"
	gc "github.com/go-check/check"
)

// Magic constant used by Redis source for Murmur hashing.
const hashSeed = 0xadc83b19

type BlackboxSuite struct{}

func (s *BlackboxSuite) TestPatternLengthCompare(c *gc.C) {
	var compare = func(input []byte) {
		var hash = wrapMurmurHash64A(input, hashSeed)

		var reg, cnt = hll.RegisterRhoRedis(hash)
		var wreg, wcnt = wrapPatLen(input)

		c.Check(reg, gc.Equals, wreg)
		c.Check(cnt, gc.Equals, wcnt)
	}

	compare([]byte(nil))
	compare([]byte("foo"))
	compare([]byte("bar"))
	compare([]byte("baz"))
	compare([]byte("a funny thing happened on the way to the forum"))
}

func (s *BlackboxSuite) TestDenseAddAndSumCompare(c *gc.C) {
	var seed = time.Now().UnixNano()
	c.Log("seed: ", seed)
	rand.Seed(seed)

	var whll = wrapCreateHLL()
	wrapSparseToDense(whll)

	var p = hll.InitDense(nil)
	var added bool
	var err error

	for i := 0; i != 100000; i++ {
		var b [2]byte
		rand.Read(b[:]) // Select a random item from [0, 65536).

		var reg, count = hll.RegisterRhoRedis(wrapMurmurHash64A(b[:], hashSeed))

		p, _, added, err = hll.Add(p, nil, reg, count)
		c.Check(err, gc.IsNil)
		c.Check(added, gc.Equals, wrapDenseAdd(whll, b[:]))
	}

	// Verify bit-equality of underlying registers. Note the trailing byte
	// of |p| is not a register (it's extra to avoid range checks on lookup).
	c.Check(p[hll.HeaderSize:len(p)-1], gc.DeepEquals, wrapRegisters(whll))

	// Verify exact equality of computed sums.
	var e, ez = hll.DenseSum(p)
	var we, wez = wrapDenseSum(whll)
	c.Check(e, gc.Equals, we)
	c.Check(ez, gc.Equals, wez)

	count, err := hll.Count(p)
	c.Check(err, gc.IsNil)
	c.Check(count, gc.Equals, wrapCount(whll))
}

func (s *BlackboxSuite) TestSparseAddSumAndPromoteCompare(c *gc.C) {
	var seed = time.Now().UnixNano()
	c.Log("seed: ", seed)
	rand.Seed(seed)

	var whll = wrapCreateHLL()
	var p = hll.Init(nil)
	var added bool
	var err error

	for i := 0; i != 1000; i++ {
		var b [2]byte
		rand.Read(b[:]) // Select a random item from [0, 65536).
		var reg, count = hll.RegisterRhoRedis(wrapMurmurHash64A(b[:], hashSeed))

		p, _, added, err = hll.Add(p, nil, reg, count)
		c.Check(err, gc.IsNil)
		c.Check(added, gc.Equals, wrapSparseAdd(whll, b[:]))
	}

	// Verify bit-equality of underlying registers.
	c.Check(p[hll.HeaderSize:], gc.DeepEquals, wrapRegisters(whll))

	// Verify exact equality of computed sums.
	e, ez, err := hll.SparseSum(p)
	var we, wez = wrapSparseSum(whll)

	c.Check(e, gc.Equals, we)
	c.Check(ez, gc.Equals, wez)
	c.Check(err, gc.IsNil)

	count, err := hll.Count(p)
	c.Check(err, gc.IsNil)
	c.Check(count, gc.Equals, wrapCount(whll))

	// Promote both to dense representation.
	p, _, err = hll.SparseToDense(p, nil)
	c.Check(err, gc.IsNil)
	wrapSparseToDense(whll)

	// Verify continued bit-equality of underlying registers.
	c.Check(p[hll.HeaderSize:len(p)-1], gc.DeepEquals, wrapRegisters(whll))

	// Verified continued equality of computed sums.
	e, ez = hll.DenseSum(p)
	c.Check(e, gc.Equals, we)
	c.Check(ez, gc.Equals, wez)

	count, err = hll.Count(p)
	c.Check(err, gc.IsNil)
	c.Check(count, gc.Equals, wrapCount(whll))
}

var _ = gc.Suite(&BlackboxSuite{})

func Test(t *testing.T) { gc.TestingT(t) }
