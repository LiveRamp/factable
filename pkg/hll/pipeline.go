package hll

import (
	"encoding/binary"
)

const (
	PipelineDenseDirty  = 'd'
	PipelineDenseClean  = 'D'
	PipelineSparseClean = 'S'
	PipelineSparseDirty = 's'
)

// EncodePipelineFormat appends the HLL |p|, in Redis format, to |b| as a
// PipelineDB-formatted HLL. The representations share the same register
// wire format, but have slightly differing headers.
func EncodePipelineFormat(b, p []byte) ([]byte, error) {
	var cnt [8]byte
	if c, err := Count(p); err != nil {
		return b, err
	} else {
		binary.LittleEndian.PutUint64(cnt[:], uint64(c))
	}

	var encoding byte
	if len(p) == DenseRegistersSize {
		encoding = PipelineDenseClean
	} else {
		encoding = PipelineSparseClean
	}

	var blen [4]byte
	binary.LittleEndian.PutUint32(blen[:], uint32(len(p)-HeaderSize))

	// Append PipelineDB HLL header, then register bytes. Pipeline uses the same
	// wire format for dense and sparse representations.
	b = append(b,
		encoding,      // Encoding.
		0x0, 0x0, 0x0, // Unused.
		cnt[0], cnt[1], cnt[2], cnt[3], cnt[4], cnt[5], cnt[6], cnt[7], // Cardinality.
		P,             // Precision.
		0x0, 0x0, 0x0, // Unused.
		blen[0], blen[1], blen[2], blen[3], // Register byte length.
	)
	return append(b, p[HeaderSize:]...), nil
}
