package hll

import (
	"encoding/binary"
	"fmt"
)

const (
	HeaderSizeRetailNext = 15
)

// RegisterRhoRedis returns the Register and rho (count) for |hash| with an
// identical implementation to the RetailNext HLLPP implementation.
func RegisterRhoRetailNext(hash uint64) (Register, uint8) {
	// Mask to the leading P bits of |hash|. This is the register.
	const mask = ((1 << P) - 1) << (64 - P)

	var reg = Register((hash & mask) >> (64 - P))

	// Count leading zeros in |hash| after the register.
	var bit = uint64(1 << (63 - P))
	var count uint8 = 1

	// Use a goto to allow this function to inline (for loops won't be).
	hash |= 1
loop:

	if hash&bit == 0 {
		bit >>= 1
		count++
		goto loop
	}
	return reg, count
}

// DecodeRetailNextHeader verifies that |p| is a serialized RetailNext HLL,
// and extracts key properties of the representation.
func DecodeRetailNextHeader(p []byte) (sparse bool, bpr int, err error) {
	// [0,2): Version. Only value is 0x1.
	if p[0] != 0x00 || p[1] != 0x1 {
		err = fmt.Errorf("not a marshalled retailnext HLLPP")
		return
	}
	// [2,6): Representation byte length.
	if hlen := int(binary.BigEndian.Uint32(p[2:])); hlen != len(p) {
		var tlen = hlen // Escapes to heap.
		err = fmt.Errorf("length mismatch: %d vs %d", tlen, len(p))
		return
	}
	// [6,8): Flags. Only valid flag is 0x1 (is sparse).
	if p[6] == 0 && p[7] == 0x1 {
		sparse = true
	} else if p[6] != 0 || p[7] != 0 {
		err = fmt.Errorf("unknown flags: %v", p[6:8])
		return
	}
	// [8,9): Configured dense precision.
	// [9,10): Configured sparse precision.
	if prec, sprec := p[8], p[9]; prec != P || sprec != 20 {
		var tprec, tsprec = prec, sprec // Escapes to heap.
		err = fmt.Errorf("unsupported precision P=%d P'=%d", tprec, tsprec)
		return
	}
	// [10,14): Sparse byte length; ignored.

	// [14,15): Bits per register.
	bpr = int(p[14])
	if bpr != 0 && bpr != 5 && bpr != 6 {
		var tbpr = bpr // Escapes to heap.
		err = fmt.Errorf("unsupported bits per register %d", tbpr)
		return
	}
	// [15, ...): Data.

	return
}

// sparseRNReader is a utility for reading the RetailNext sparse format.
type sparseRNReader struct {
	cursor
	count uint8
	p     []byte
	plen  int
	accum uint64
	n     int
}

func newSparseRNReader(p []byte) sparseRNReader {
	var r = sparseRNReader{
		p:      p,
		plen:   len(p),
		cursor: cursor{index: HeaderSizeRetailNext},
	}
	if r.plen != HeaderSizeRetailNext {
		r.next()
	}
	return r
}

func (r *sparseRNReader) done() bool { return r.cursor.index == r.plen }

func (r *sparseRNReader) next() {
	// Increment by the previous opcode length, and halt if at EOF.
	if r.cursor.index, r.n = r.cursor.index+r.n, 0; r.cursor.index == r.plen {
		return
	}

	var delta uint64
	delta, r.n = binary.Uvarint(r.p[r.cursor.index:])
	r.accum += delta

	if r.accum&0x1 == 0 {
		r.cursor.register, r.count = RegisterRhoRetailNext(r.accum << (63 - 20))
	} else {
		r.cursor.register = Register(r.accum & 0x7ffe000 >> 13)
		r.count = uint8(r.accum&0x7e>>1) + 6
	}

	if r.count > 64 {
		panic("count too large")
	}
}

func reduceDenseDenseRN6bits(p, q []byte) {
	var lenq = len(q)
	var r Register

	for i := HeaderSizeRetailNext; i != lenq; i, r = i+3, r+4 {
		// Unroll to process 4 registers each iteration.
		var c1 uint8 = (q[i+0]) >> 2                 // 11111100
		var c2 uint8 = (q[i+0]&0x3)<<4 | (q[i+1])>>4 // 00000011 11110000
		var c3 uint8 = (q[i+1]&0xf)<<2 | (q[i+2])>>6 // 00001111 11000000
		var c4 uint8 = (q[i+2] & 0x3f)               // 00111111

		if getDenseRegister(p, r+0) < c1 {
			setDenseRegister(p, r+0, c1)
		}
		if getDenseRegister(p, r+1) < c2 {
			setDenseRegister(p, r+1, c2)
		}
		if getDenseRegister(p, r+2) < c3 {
			setDenseRegister(p, r+2, c3)
		}
		if getDenseRegister(p, r+3) < c4 {
			setDenseRegister(p, r+3, c4)
		}
	}
}

func reduceDenseDenseRN5bits(p, q []byte) {
	var lenq = len(q)
	var r Register

	for i := HeaderSizeRetailNext; i != lenq; i, r = i+5, r+8 {
		// Unroll to process 8 registers each iteration.
		var c0 uint8 = (q[i+0]) >> 3                   // 11111000
		var c1 uint8 = (q[i+0]&0x7)<<2 | (q[i+1] >> 6) // 00000111 11000000
		var c2 uint8 = (q[i+1] & 0x3e) >> 1            // 00111110
		var c3 uint8 = (q[i+1]&0x1)<<4 | (q[i+2] >> 4) // 00000001 11110000
		var c4 uint8 = (q[i+2]&0xf)<<1 | (q[i+3] >> 7) // 00001111 10000000
		var c5 uint8 = (q[i+3] & 0x7c) >> 2            // 01111100
		var c6 uint8 = (q[i+3]&0x3)<<3 | (q[i+4] >> 5) // 00000011 11100000
		var c7 uint8 = (q[i+4] & 0x1f)                 // 00111111

		if getDenseRegister(p, r+0) < c0 {
			setDenseRegister(p, r+0, c0)
		}
		if getDenseRegister(p, r+1) < c1 {
			setDenseRegister(p, r+1, c1)
		}
		if getDenseRegister(p, r+2) < c2 {
			setDenseRegister(p, r+2, c2)
		}
		if getDenseRegister(p, r+3) < c3 {
			setDenseRegister(p, r+3, c3)
		}
		if getDenseRegister(p, r+4) < c4 {
			setDenseRegister(p, r+4, c4)
		}
		if getDenseRegister(p, r+5) < c5 {
			setDenseRegister(p, r+5, c5)
		}
		if getDenseRegister(p, r+6) < c6 {
			setDenseRegister(p, r+6, c6)
		}
		if getDenseRegister(p, r+7) < c7 {
			setDenseRegister(p, r+7, c7)
		}
	}
}

func reduceDenseSparseRN(p, q []byte) {
	for qr := newSparseRNReader(q); !qr.done(); qr.next() {
		if getDenseRegister(p, qr.cursor.register) < qr.count {
			setDenseRegister(p, qr.cursor.register, qr.count)
		}
	}
}

func reduceSparseSparseRN(p, q, tmp []byte) (pout, tmpout []byte, err error) {
	var pr = newSparseReader(p)
	var qr = newSparseRNReader(q)

	var unsorted = Init(unsortedPool.Get().([]byte))
	defer unsortedPool.Put(unsorted)

	// Initialize with header but no sparse opcodes.
	tmp = initEmpty(tmp)

	// Step 1: |q| is mostly sorted. Perform a parallel walk of |p| and the
	// sorted portions of |q|, merging into |tmp|. Hold onto the unsorted
	// items of |q| to be added later.
	var qspan, merge span

	for !pr.done() || !qr.done() {
		if m := max(pr.span.count, qspan.count); merge.count != m {
			tmp = appendSparseSpan(tmp, merge.run(), merge.count)
			merge.begin, merge.count = merge.end, m
		}

		if pr.span.end.register < qspan.end.register {
			merge.end = pr.span.end
			pr.next()
			continue // Don't step |q|.
		} else if pr.span.end.register > qspan.end.register {
			merge.end = qspan.end
		} else {
			// Span end registers are equal. Step both.
			merge.end = pr.span.end
			pr.next()
		}

		if qspan.end.register < qr.cursor.register {
			// |q| is zeros between the current end and next cursor.
			qspan.begin, qspan.end, qspan.count = qspan.end, qr.cursor, 0
		} else if qr.done() {
			qspan.begin, qspan.end, qspan.count =
				qspan.end, cursor{register: Registers}, 0
		} else if qspan.end.register > qr.cursor.register {
			unsorted, _, _, err = Add(unsorted, nil, qr.cursor.register, qr.count)
			if err != nil {
				return nil, nil, err
			}
			qr.next()
		} else {
			qspan.begin, qspan.end, qspan.count =
				qspan.end, cursor{register: qspan.end.register + 1}, qr.count

			// Step |q|. The encoding may have multiple contiguous opcodes for the
			// same register. Step through these, retaining the maximum count.
			for qr.next(); !qr.done() &&
				qspan.begin.register == qr.cursor.register; qr.next() {
				qspan.count = max(qspan.count, qr.count)
			}
		}
	}
	// Append the final span to complete the pass.
	tmp = appendSparseSpan(tmp, merge.run(), merge.count)

	if merge.end.register != Registers {
		panic("insufficient registers")
	}

	// Reduce the unsorted component of |q| back in.
	return Reduce(tmp, unsorted, p[:0])
}
