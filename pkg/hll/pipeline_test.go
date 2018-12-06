package hll

import (
	gc "github.com/go-check/check"
)

type PipelineSuite struct{}

func (s *PipelineSuite) TestEncodeSparseHLL(c *gc.C) {
	var p, _, err = sparseAdd(Init(nil), 1234, 5)
	c.Assert(err, gc.IsNil)

	var out []byte
	out, err = EncodePipelineFormat(nil, p)
	c.Assert(err, gc.IsNil)

	c.Check(out, gc.DeepEquals, []byte{
		'S',           // Encoding.
		0x0, 0x0, 0x0, // Unused.
		0x1, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, // Cardinality.
		P,             // Precision.
		0x0, 0x0, 0x0, // Unused.
		0x5, 0x0, 0x0, 0x0, // Register byte length.
		0x44, 0xd1, 0x90, 0x7b, 0x2c, // Sparse encoding.
	})
}

func (s *PipelineSuite) TestEncodeDenseHLL(c *gc.C) {
	var p = InitDense(nil)
	setDenseRegister(p, 1, 5)

	var out, err = EncodePipelineFormat(nil, p)
	c.Assert(err, gc.IsNil)

	c.Check(out[:30], gc.DeepEquals, []byte{
		'D',           // Encoding.
		0x0, 0x0, 0x0, // Unused.
		0x1, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, // Cardinality.
		P,             // Precision.
		0x0, 0x0, 0x0, // Unused.
		0x1, 0x30, 0x0, 0x0, // Register byte length.
		0x40, 0x1, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, // Registers.
	})
}

var _ = gc.Suite(&PipelineSuite{})
