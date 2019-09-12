package blackbox

/*
#cgo LDFLAGS: -lm

#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include "sds.h"

// Internal declaration lifted from hyperloglog.c
struct hllhdr {
    char magic[4];      // "HYLL"
    uint8_t encoding;   // HLL_DENSE or HLL_SPARSE.
    uint8_t notused[3]; // Reserved for future use, must be zero.
    uint8_t card[8];    // Cached cardinality, little endian.
    uint8_t registers;  // Data bytes.
};

// Stripped down redisObject definition from server.h
typedef struct redisObject {
    void *ptr;
} robj;

// Functions from hyperloglog.c, declared here for CGO wrapping.
uint64_t MurmurHash64A (const void * key, int len, unsigned int seed);
int hllPatLen(unsigned char *ele, size_t elesize, long *regp);
int hllDenseAdd(uint8_t *registers, unsigned char *ele, size_t elesize);
double hllDenseSum(uint8_t *registers, double *PE, int *ezp);
int hllSparseToDense(robj *o);

int hllSparseAdd(robj *o, unsigned char *ele, size_t elesize);
double hllSparseSum(uint8_t *sparse, int sparselen, double *PE, int *ezp, int *invalid);

double hllRawSum(uint8_t *registers, double *PE, int *ezp);
uint64_t hllCount(struct hllhdr *hdr, int *invalid);
int hllAdd(robj *o, unsigned char *ele, size_t elesize);
int hllMerge(uint8_t *max, robj *hll);
robj *createHLLObject(void);

*/
import "C"
import (
	"unsafe"

	"github.com/LiveRamp/factable/pkg/hll"
)

func wrapMurmurHash64A(input []byte, seed int) uint64 {
	var b = C.CBytes(input)
	defer C.free(b)

	return uint64(C.MurmurHash64A(b, C.int(len(input)), C.unsigned(seed)))
}

func wrapPatLen(input []byte) (r hll.Register, count uint8) {
	var b = C.CBytes(input)
	defer C.free(b)

	var cr C.long
	var ccnt = C.hllPatLen((*C.uchar)(b), C.size_t(len(input)), &cr)

	return hll.Register(cr), uint8(ccnt)
}

func wrapCreateHLL() *C.robj {
	return C.createHLLObject()
}

func wrapSparseToDense(o *C.robj) {
	if C.hllSparseToDense(o) != 0 {
		panic("failed to convert")
	}
}

func wrapDenseAdd(o *C.robj, input []byte) bool {
	var b = C.CBytes(input)
	defer C.free(b)

	var hdr = (*C.struct_hllhdr)(o.ptr)

	return C.hllDenseAdd(&hdr.registers, (*C.uchar)(b), C.size_t(len(input))) == 1
}

func wrapDenseSum(o *C.robj) (E float64, ez int) {
	var pe [64]C.double
	for i, v := range hll.PE {
		pe[i] = C.double(v)
	}

	var cez C.int
	var hdr = (*C.struct_hllhdr)(o.ptr)

	var cE = C.hllDenseSum(&hdr.registers, &pe[0], &cez)

	return float64(cE), int(cez)
}

func wrapSparseAdd(o *C.robj, input []byte) bool {
	var b = C.CBytes(input)
	defer C.free(b)

	return C.hllSparseAdd(o, (*C.uchar)(b), C.size_t(len(input))) == 1
}

func wrapSparseSum(o *C.robj) (E float64, ez int) {
	var pe [64]C.double
	for i, v := range hll.PE {
		pe[i] = C.double(v)
	}

	var invalid C.int
	var cez C.int
	var hdr = (*C.struct_hllhdr)(o.ptr)

	var cE = C.hllSparseSum(
		&hdr.registers,
		C.int(C.sdslen((C.sds)(o.ptr))-16), // 16 == HLL_HDR_SIZE
		&pe[0],
		&cez,
		&invalid,
	)

	if invalid != 0 {
		panic("invalid")
	}
	return float64(cE), int(cez)
}

func wrapCount(o *C.robj) int {
	var invalid C.int
	var hdr = (*C.struct_hllhdr)(o.ptr)

	var ccnt = C.hllCount(hdr, &invalid)

	if invalid != 0 {
		panic("invalid")
	}
	return int(ccnt)
}

func wrapRegisters(o *C.robj) []byte {
	var hdr = (*C.struct_hllhdr)(o.ptr)
	return C.GoBytes(unsafe.Pointer(&hdr.registers),
		C.int(C.sdslen((C.sds)(o.ptr))-16))
}
