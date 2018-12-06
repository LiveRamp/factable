/* Package `hll` implements a HyperLogLog++ probabilistic cardinality
 * estimator, operating directly over []byte slices of the Redis wire format.
 *
 * The package includes functions for directly reducing serialized HLLs in
 * the RetailNext dense & sparse formats, and for emitting HLLs in the
 * PipelineDB format.
 *
 * Copyright (c) 2017, LiveRamp / Acxiom Inc. All rights reserved.
 * Portions of this package Copyright (c) 2014, Salvatore Sanfilippo
 * <antirez at gmail dot com> All rights reserved.
 */

/* hyperloglog.c - Redis HyperLogLog probabilistic cardinality approximation.
 * This file implements the algorithm and the exported Redis commands.
 *
 * Copyright (c) 2014, Salvatore Sanfilippo <antirez at gmail dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
package hll

import (
	"fmt"
	"math"
	"sync"
)

const (
	P         = 14     // The greater P is, the smaller the error.
	Registers = 1 << P // With P=14, 16384 registers.

	CountBits = 6 // Enough to count up to 63 leading zeros.
	CountMax  = (1 << CountBits) - 1

	HeaderSize         = 16
	DenseRegistersSize = (Registers*CountBits+7)/8 + 1 + HeaderSize

	alpha = 0.7213 / (1 + 1.079/Registers)

	denseMarker  = 0 // Dense encoding marker.
	sparseMarker = 1 // Sparse encoding marker.

	sparseMaxBytes = 2500
)

type Register uint

// Layout of header, 16 bytes total:
// 'H', 'Y', 'L', 'L' (magic prefix).
// encoding    (uint8, 0x0 for dense, 0x1 for sparse).
// _           ([3]uint8, not used and must be zero).
// cardinality ([8]uint8, cached cardinality in little endian)
const (
	encodingOffset    = 5
	cardinalityOffset = 8
)

// Init returns an empty sparse HLL.
func Init(p []byte) []byte {
	return append(initEmpty(p),
		((Registers-1)>>8)&0xff|opcodeXZERO, (Registers-1)&0xff, // XZERO(Registers).
	)
}

// InitDense returns an empty dense HLL.
func InitDense(p []byte) []byte {
	if cap(p) < DenseRegistersSize {
		p = make([]byte, DenseRegistersSize)
	} else {
		p = p[:DenseRegistersSize]

		for i := range p {
			p[i] = 0 // Compiler optimizes to memclr.
		}
	}
	_ = append(p[:0],
		'H', 'Y', 'L', 'L',
		0x0,           // Dense.
		0x0, 0x0, 0x0, // Unused.
		0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, // Cached cardinality of zero.
	)
	return p
}

// IsRedisFormat returns whether the HLL is in the standard Redis wire format.
func IsRedisFormat(p []byte) bool {
	return p[0] == 'H' && p[1] == 'Y' && p[2] == 'L' && p[3] == 'L'
}

func initEmpty(p []byte) []byte {
	if p == nil {
		p = make([]byte, 0, 1024)
	}
	return append(p[:0],
		'H', 'Y', 'L', 'L',
		0x1,           // Sparse flag.
		0x0, 0x0, 0x0, // Unused.
		0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, // Cached cardinality of zero.
	)
}

// The cached cardinality MSB is used to signal validity of the cached value.
func invalidateCache(p []byte) { p[cardinalityOffset+7] |= (1 << 7) }
func validCache(p []byte) bool { return p[cardinalityOffset+7]&(1<<7) == 0 }

// Retrieve the value of the Register at |index|.
func getDenseRegister(p []byte, reg Register) (value uint8) {
	var _byte = HeaderSize + (reg*CountBits)/8
	var _fb = (reg * CountBits) & 7
	var _fb8 = 8 - _fb
	var b0, b1 = p[_byte], p[_byte+1]

	return ((b0 >> _fb) | (b1 << _fb8)) & CountMax
}

// Set the value of the Register at |reg| to |value|.
func setDenseRegister(p []byte, reg Register, value uint8) {
	var _byte = HeaderSize + (reg*CountBits)/8
	var _fb = (reg * CountBits) & 7
	var _fb8 = 8 - _fb

	p[_byte] &= ^(CountMax << _fb)
	p[_byte] |= value << _fb
	p[_byte+1] &= ^(CountMax >> _fb8)
	p[_byte+1] |= value >> _fb8
}

// RegisterRhoRedis returns the Register and rho (count) for |hash| with an
// identical implementation to Redis.
func RegisterRhoRedis(hash uint64) (Register, uint8) {
	var reg = Register(hash & (Registers - 1)) // Register index.
	hash |= (1 << 63)                          // Make sure the loop terminates.

	var bit uint64 = Registers // First bit not used to address the Register.
	var count uint8 = 1        // Initialized to 1 since we count the "00000...1" pattern.

	for (hash & bit) == 0 {
		count++
		bit <<= 1
	}
	return reg, count
}

const (
	opcodeMask  = 0xc0 // 11000000
	opcodeZERO  = 0x00 // 00xxxxxx
	opcodeXZERO = 0x40 // 01xxxxxx
	opcodeVALUE = 0x80 // 1xxxxxxx

	opcodeVALUEMaxCount = 32 // 5-bit register
	opcodeVALUEMaxRun   = 4  // Can encode up to four contiguous registers.

	opcodeZEROMaxRun  = 64
	opcodeXZEROMaxRun = 16384
)

func sparseZeroLen(p []byte, i int) Register {
	// Extract 00xxxxxx + 1.
	return Register(p[i]&0x3f) + 1
}
func sparseXZeroLen(p []byte, i int) Register {
	// Extract (00xxxxxx yyyyyyyy) + 1.
	return (Register(p[i]&0x3f)<<8 | Register(p[i+1])) + 1
}
func sparseValLen(p []byte, i int) Register {
	// Extract 000000xx + 1.
	return Register(p[i]&0x3) + 1
}
func sparseValValue(p []byte, i int) uint8 {
	// Extract 0xxxxx00 + 1.
	return (p[i]>>2)&0x1f + 1
}

func appendSparseSpan(b []byte, run Register, value uint8) []byte {
	if run == 0 {
		return b
	}
	if value == 0 {
		if r := run - 1; run <= opcodeZEROMaxRun {
			return append(b, byte(r))
		} else if run <= opcodeXZEROMaxRun {
			return append(b, byte(r>>8)|opcodeXZERO, byte(r))
		} else {
			// A single ZXERO opcode should span all Registers.
			panic("zero run too large")
		}
	}

	// Append one or more VALUE opcodes, as needed to fill |run|.
	// Each VALUE opcode can span up to 4 bits.
	for run != 0 {
		if run >= opcodeVALUEMaxRun {
			b = append(b, ((value-1)<<2)|(opcodeVALUEMaxRun-1)|opcodeVALUE)
			run -= opcodeVALUEMaxRun
		} else {
			b = append(b, ((value-1)<<2)|byte(run-1)|opcodeVALUE)
			run = 0
		}
	}
	return b
}

func dumpOpcodes(p []byte) []string {
	var out []string
	var reg Register

	for i, plen := HeaderSize, len(p); i != plen; {
		switch p[i] & opcodeMask {
		case opcodeZERO:
			var run = sparseZeroLen(p, i)
			out = append(out, fmt.Sprintf("ZERO(%d) [%d, %d)", run, reg, reg+run))
			i, reg = i+1, reg+run
		case opcodeXZERO:
			var run = sparseXZeroLen(p, i)
			out = append(out, fmt.Sprintf("XZERO(%d) [%d, %d)", run, reg, reg+run))
			i, reg = i+2, reg+run
		default: // opcodeVALUE.
			var run, value = sparseValLen(p, i), sparseValValue(p, i)
			out = append(out,
				fmt.Sprintf("VALUE(%d over %d) [%d, %d)", value, run, reg, reg+run))
			i, reg = i+1, reg+run
		}
	}
	return out
}

// Cursor locates a Register in |p| and its covering opcode index.
type cursor struct {
	index    int      // byte index of cursor into |p|.
	register Register // first register of the opcode at |index|.
}

// Span is a [begin, end) range of cursors in |p| having the same |count|.
type span struct {
	begin, end cursor
	count      uint8
}

// Run returns the number of registers covered by this span.
func (s *span) run() Register { return s.end.register - s.begin.register }

// findCoveringSpan locates a previous, covering, and next span for
// register |reg| in opcode buffer |p|.
func findCoveringSpan(p []byte, reg Register) (prev, cover, next span, err error) {
	for sr := newSparseReader(p); !sr.done(); {
		sr.next()

		if reg < cover.end.register {
			return // We've found our covering span, and |next| is fully read. All done.
		} else {
			prev, cover, next = cover, next, sr.span
		}
	}
	if reg >= cover.end.register {
		var tr, tc = reg, cover // Escapes to heap.
		err = fmt.Errorf("did not find covering span for %v (best is %v)", tr, tc)
	}
	return
}

// Add folds |reg| and |count| into the HLL encoded by |p|. |tmp| may be used
// to reduce required allocation.
func Add(p, tmp []byte, reg Register, count uint8) (pout, tmpout []byte, added bool, err error) {
	if len(tmp) != 0 {
		return p, tmp, false, fmt.Errorf("tmp is non-empty")
	}

	var dense = len(p) == DenseRegistersSize

	// If |count| can't be represented with 5 registers, or if the representation
	// is already large, then we must promote.
	if !dense && (count > opcodeVALUEMaxCount || len(p) > sparseMaxBytes) {
		if p, tmp, err = SparseToDense(p, tmp); err != nil {
			return p, tmp, false, err
		}
		dense = true
	}

	if dense {
		if count > getDenseRegister(p, reg) {
			setDenseRegister(p, reg, count)
			return p, tmp, true, nil
		}
		return p, tmp, false, nil
	}

	p, added, err = sparseAdd(p, reg, count)
	return p, tmp, added, err
}

func sparseAdd(p []byte, reg Register, count uint8) ([]byte, bool, error) {
	// Locate previous, covering, and next opcode spans.
	var prev, cover, next, err = findCoveringSpan(p, reg)
	if err != nil {
		return p, false, err
	}

	// If the covering span already has at least this count, no update needed.
	if cover.count >= count {
		return p, false, nil
	}

	// Build up buffer |sb| of opcodes to splice into |p|, replacing range |splice|.
	var stackbuffer [12]byte
	var sbuf = stackbuffer[:0]
	var splice span

	// Case A: |prev| and |next| have |count|, and |cover| is one register wide.
	// We merge all three into a single span.
	if prev.count == count &&
		next.count == count &&
		cover.begin.register+1 == cover.end.register {

		sbuf = appendSparseSpan(sbuf, next.end.register-prev.begin.register, count)
		splice = span{begin: prev.begin, end: next.end}

		// Case B: Extend |prev| by one to include |reg|, and reduce |cover| by one.
	} else if prev.count == count && prev.end.register == reg {
		sbuf = appendSparseSpan(sbuf, prev.run()+1, count)
		sbuf = appendSparseSpan(sbuf, cover.run()-1, cover.count)
		splice = span{begin: prev.begin, end: cover.end}

		// Case C: Extend |next| by one to include |reg|, and reduce |cover| by one.
	} else if next.count == count && next.begin.register == reg+1 {
		sbuf = appendSparseSpan(sbuf, cover.run()-1, cover.count)
		sbuf = appendSparseSpan(sbuf, next.run()+1, count)
		splice = span{begin: cover.begin, end: next.end}

		// Case D: Break up |cover| to insert |reg|.
	} else {
		sbuf = appendSparseSpan(sbuf, reg-cover.begin.register, cover.count)
		sbuf = appendSparseSpan(sbuf, 1, count)
		sbuf = appendSparseSpan(sbuf, cover.end.register-reg-1, cover.count)
		splice = cover
	}

	// Grow or shrink |p| as required to splice |sbuf| in at |splice|.
	switch delta := len(sbuf) - (splice.end.index - splice.begin.index); {
	case delta > 0:
		// Extend and shift trailing bytes in |p|.
		p = append(p, sbuf[:delta]...)
		copy(p[splice.end.index+delta:], p[splice.end.index:])
	case delta < 0:
		// Shift and drop trailing bytes in |p|.
		copy(p[splice.end.index+delta:], p[splice.end.index:])
		p = p[:len(p)+delta]
	}
	copy(p[splice.begin.index:], sbuf)

	return p, true, nil
}

func SparseToDense(p, tmp []byte) (pout, tmpout []byte, err error) {
	var reg Register

	tmp = InitDense(tmp)

	for i, plen := HeaderSize, len(p); i != plen; {
		switch p[i] & opcodeMask {
		case opcodeZERO:
			i, reg = i+1, reg+sparseZeroLen(p, i)
		case opcodeXZERO:
			i, reg = i+2, reg+sparseXZeroLen(p, i)
		default: // opcodeVALUE.
			var run, value = sparseValLen(p, i), sparseValValue(p, i)

			for end := reg + run; reg != end; reg++ {
				if reg >= Registers {
					var treg = reg // Escapes to heap.
					return nil, nil, fmt.Errorf("large register (%v)", treg)
				}
				setDenseRegister(tmp, reg, value)
			}
			i++
		}
	}

	if reg != Registers {
		var treg = reg // Escapes to heap.
		return nil, nil, fmt.Errorf("SparseToDense: wrong # of registers (got %v)", treg)
	}
	return tmp, p[:0], err
}

// Count returns the cardinality of an HLL.
func Count(p []byte) (int, error) {
	var E, ez float64
	var err error
	var iez int

	if len(p) == DenseRegistersSize {
		E, iez = DenseSum(p)
	} else {
		E, iez, err = SparseSum(p)
	}
	ez = float64(iez)

	/* Apply loglog-beta to the raw estimate. See:
	 * "LogLog-Beta and More: A New Algorithm for Cardinality Estimation
	 * Based on LogLog Counting" Jason Qin, Denys Kim, Yumei Tung
	 * arXiv:1612.02284 */
	var zl = math.Log(ez + 1)
	var beta = -0.370393911*ez +
		0.070471823*zl +
		0.17393686*math.Pow(zl, 2.0) +
		0.16339839*math.Pow(zl, 3.0) +
		-0.09237745*math.Pow(zl, 4.0) +
		0.03738027*math.Pow(zl, 5.0) +
		-0.005384159*math.Pow(zl, 6.0) +
		0.00042419*math.Pow(zl, 7.0)

	return int(math.Floor(alpha*Registers*(Registers-ez)*(1/(E+beta)) + 0.5)), err
}

/* Compute SUM(2^-reg) in the dense representation.
 * PE is an array with a pre-computer table of values 2^-reg indexed by reg.
 * As a side effect the integer pointed by 'ezp' is set to the number
 * of zero registers. */
func DenseSum(p []byte) (E float64, ez int) {
	var r = p[HeaderSize:]

	/* Redis default is to use 16384 registers 6 bits each. The code works
	 * with other values by modifying the defines, but for our target value
	 * we take a faster path with unrolled loops. */
	if Registers == 16384 && CountBits == 6 {
		var r0, r1, r2, r3, r4, r5, r6, r7, r8, r9,
			r10, r11, r12, r13, r14, r15 uint8
		for j := 0; j < 1024; j++ {
			/* Handle 16 registers per iteration. */
			r0 = r[0] & 63
			r1 = (r[0]>>6 | r[1]<<2) & 63
			r2 = (r[1]>>4 | r[2]<<4) & 63
			r3 = (r[2] >> 2) & 63
			r4 = r[3] & 63
			r5 = (r[3]>>6 | r[4]<<2) & 63
			r6 = (r[4]>>4 | r[5]<<4) & 63
			r7 = (r[5] >> 2) & 63
			r8 = r[6] & 63
			r9 = (r[6]>>6 | r[7]<<2) & 63
			r10 = (r[7]>>4 | r[8]<<4) & 63
			r11 = (r[8] >> 2) & 63
			r12 = r[9] & 63
			r13 = (r[9]>>6 | r[10]<<2) & 63
			r14 = (r[10]>>4 | r[11]<<4) & 63
			r15 = (r[11] >> 2) & 63

			if r0 == 0 {
				ez++
			}
			if r1 == 0 {
				ez++
			}
			if r2 == 0 {
				ez++
			}
			if r3 == 0 {
				ez++
			}
			if r4 == 0 {
				ez++
			}
			if r5 == 0 {
				ez++
			}
			if r6 == 0 {
				ez++
			}
			if r7 == 0 {
				ez++
			}
			if r8 == 0 {
				ez++
			}
			if r9 == 0 {
				ez++
			}
			if r10 == 0 {
				ez++
			}
			if r11 == 0 {
				ez++
			}
			if r12 == 0 {
				ez++
			}
			if r13 == 0 {
				ez++
			}
			if r14 == 0 {
				ez++
			}
			if r15 == 0 {
				ez++
			}

			/* Additional parens will allow the compiler to optimize the
			 * code more with a loss of precision that is not very relevant
			 * here (floating point math is not commutative!). */
			E += (PE[r0] + PE[r1]) + (PE[r2] + PE[r3]) + (PE[r4] + PE[r5]) +
				(PE[r6] + PE[r7]) + (PE[r8] + PE[r9]) + (PE[r10] + PE[r11]) +
				(PE[r12] + PE[r13]) + (PE[r14] + PE[r15])
			r = r[12:]
		}
	} else {
		for j := Register(0); j < Registers; j++ {

			if val := getDenseRegister(p, j); val == 0 {
				ez++
				/* Increment E at the end of the loop. */
			} else {
				E += PE[val] /* Precomputed 2^(-reg[j]). */
			}
		}
		E += float64(ez) /* Add 2^0 'ez' times. */
	}
	return
}

func SparseSum(p []byte) (E float64, ez int, err error) {
	var reg, rez Register

	for i, plen := HeaderSize, len(p); i != plen; {
		switch p[i] & opcodeMask {
		case opcodeZERO:
			var run = sparseZeroLen(p, i)
			rez += run
			i, reg = i+1, reg+run
		case opcodeXZERO:
			var run = sparseXZeroLen(p, i)
			rez += run
			i, reg = i+2, reg+run
		default: // opcodeVALUE.
			var run, value = sparseValLen(p, i), sparseValValue(p, i)
			E += PE[value] * float64(run)
			i, reg = i+1, reg+run
		}
	}

	E += float64(rez) // Add 2^0 'ez' times.
	ez = int(rez)

	if reg != Registers {
		err = fmt.Errorf("SparseSum: insufficient number of sparse registers")
	}
	return
}

// Reduce reduces HLL |q| into HLL |p|. |tmp| may be used to reduce memory allocation.
// |p| and |tmp| may be modified and are returned. |q| is never modified.
func Reduce(p, q, tmp []byte) (pout, tmpout []byte, err error) {
	if len(tmp) != 0 {
		return p, tmp, fmt.Errorf("tmp is non-empty")
	}
	if !IsRedisFormat(p) {
		return p, tmp, fmt.Errorf("p is not Redis format")
	}

	if IsRedisFormat(q) {

		var shouldPromote = (len(q) == DenseRegistersSize) ||
			(len(p)+len(q) > sparseMaxBytes)

		if len(p) != DenseRegistersSize && shouldPromote {
			if p, tmp, err = SparseToDense(p, tmp); err != nil {
				return p, tmp, err
			}
		}

		if len(p) == DenseRegistersSize {
			if len(q) == DenseRegistersSize {
				reduceDenseDense(p, q)
				return p, tmp, nil
			}
			// |q| is sparse.
			reduceDenseSparse(p, q)
			return p, tmp, nil
		}

		return reduceSparseSparse(p, q, tmp)
	}

	// Expect |q| is in RetailNext format.
	qsparse, qbpr, err := DecodeRetailNextHeader(q)
	if err != nil {
		return p, tmp, err
	}

	if !qsparse {
		if len(p) != DenseRegistersSize {
			if p, tmp, err = SparseToDense(p, tmp); err != nil {
				return p, tmp, err
			}
		}

		switch qbpr {
		case 5:
			reduceDenseDenseRN5bits(p, q)
		case 6:
			reduceDenseDenseRN6bits(p, q)
		default:
			var tbpr = qbpr // Escapes to heap.
			return p, tmp, fmt.Errorf("unsupported bits per register: %v", tbpr)
		}
		return p, tmp, nil
	}
	// |q| is sparse.

	if len(p) != DenseRegistersSize && len(p)+len(q) > sparseMaxBytes {
		if p, tmp, err = SparseToDense(p, tmp); err != nil {
			return p, tmp, err
		}
	}

	if len(p) == DenseRegistersSize {
		reduceDenseSparseRN(p, q)
		return p, tmp, nil
	}

	// |p| is also sparse.
	return reduceSparseSparseRN(p, q, tmp)
}

// sparseReader is a utility for reading the standard Redis sparse wire format.
type sparseReader struct {
	cursor
	cnt  uint8
	p    []byte
	plen int
	span
}

func newSparseReader(p []byte) sparseReader {
	return sparseReader{
		cursor: cursor{index: HeaderSize},
		span:   span{begin: cursor{index: HeaderSize}, end: cursor{index: HeaderSize}},
		p:      p,
		plen:   len(p),
	}
}

func (r *sparseReader) done() bool { return r.span.begin.index == r.plen }

func (r *sparseReader) next() {
	r.span.begin, r.span.end, r.span.count = r.span.end, r.cursor, r.cnt

	// `for` loops are considered "hairy" by the inliner.
	// Use a goto + label to achieve the same result.
loop:

	if r.cursor.index == r.plen {
		return
	}

	switch r.p[r.cursor.index] & opcodeMask {
	case opcodeZERO:
		r.cursor, r.cnt = cursor{
			index:    r.cursor.index + 1,
			register: r.cursor.register + sparseZeroLen(r.p, r.cursor.index),
		}, 0
	case opcodeXZERO:
		r.cursor, r.cnt = cursor{
			index:    r.cursor.index + 2,
			register: r.cursor.register + sparseXZeroLen(r.p, r.cursor.index),
		}, 0
	default: // opcodeVALUE.
		r.cursor, r.cnt = cursor{
			index:    r.cursor.index + 1,
			register: r.cursor.register + sparseValLen(r.p, r.cursor.index),
		}, sparseValValue(r.p, r.cursor.index)
	}

	if r.cnt == r.span.count {
		// Opcode continues the current span.
		r.span.end = r.cursor
		goto loop
	}
}

func reduceSparseSparse(p, q, tmp []byte) (pout, tmpout []byte, err error) {
	if len(p) == HeaderSize+2 {
		// If |p| is an empty sparse representation, just copy |q|.
		return append(p[:HeaderSize], q[HeaderSize:]...), tmpout, nil
	}

	var pr, qr = newSparseReader(p), newSparseReader(q)
	var merge span

	// Initialize with header but no sparse opcodes.
	tmp = initEmpty(tmp)

	for !pr.done() {
		if m := max(pr.span.count, qr.span.count); merge.count != m {
			// The max value over the next register range has changed,
			// implying that the previous span being merged is now complete.
			tmp = appendSparseSpan(tmp, merge.run(), merge.count)
			merge.begin, merge.count = merge.end, m
		}

		// Extend |merge| to the minimum extent of |pr.span| & |qr.span|.
		if pr.span.end.register < qr.span.end.register {
			merge.end = pr.span.end
			pr.next()
		} else if pr.span.end.register > qr.span.end.register {
			merge.end = qr.span.end
			qr.next()
		} else {
			// Span end registers are equal. Step both.
			merge.end = pr.span.end
			pr.next()
			qr.next()
		}
	}

	if !qr.done() {
		err = fmt.Errorf("unequal number of Registers")
	}

	// Append final span and return.
	return appendSparseSpan(tmp, merge.run(), merge.count), p[:0], err
}

func reduceDenseDense(p, q []byte) {
	for i := Register(0); i != Registers; i++ {
		if cp, cq := getDenseRegister(p, i), getDenseRegister(q, i); cp < cq {
			setDenseRegister(p, i, cq)
		}
	}
}

func reduceDenseSparse(p, q []byte) {
	var reg Register

	for i, lenq := HeaderSize, len(q); i != lenq; {
		switch q[i] & opcodeMask {
		case opcodeZERO:
			i, reg = i+1, reg+sparseZeroLen(q, i)
		case opcodeXZERO:
			i, reg = i+2, reg+sparseXZeroLen(q, i)
		default: // opcodeVALUE.
			var run, value = sparseValLen(q, i), sparseValValue(q, i)

			for end := reg + run; reg != end; reg++ {
				if getDenseRegister(p, reg) < value {
					setDenseRegister(p, reg, value)
				}
			}
			i++
		}
	}
}

var PE [64]float64

func init() {
	PE[0] = 1 /* 2^(-reg[j]) is 1 when m is 0. */
	for j := uint(1); j < 64; j++ {
		/* 2^(-reg[j]) is the same as 1/2^reg[j]. */
		PE[j] = 1.0 / float64((uint(1) << j))
	}
}

func max(a, b uint8) uint8 {
	if a > b {
		return a
	} else {
		return b
	}
}

var unsortedPool = sync.Pool{
	New: func() interface{} { return []byte(nil) },
}
