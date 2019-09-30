package factable

import (
	"bufio"
	"bytes"
	"container/heap"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"sort"
	"sync"
)

// KVIterator is an iterator over keys and their values.
type KVIterator interface {
	// Next returns the next |key| and |value| of the KVIterator. It returns
	// KVIteratorDone if no items remain in the sequence, or any encountered
	// error (which also invalidates the iterator). The returned |key| and
	// |value| may be retained only until the next call to Next.
	Next() (key, value []byte, err error)
	// Close the KVIterator, releasing associated resources.
	Close() error
}

// KVIteratorSeeker is an optional interface of KVIterator which will
// seek to |key|, which must be lexicographically greater-than the last
// returned key. If the sequence ends prior to the sought |key|,
// a subsequent Next will return KVIteratorDone.
type KVIteratorSeeker interface {
	Seek(key []byte)
}

// KVIteratorDone is returned by Next when the sequence is complete.
var KVIteratorDone = errors.New("iterator done")

// NewMergeIterator accepts a variable number of KVIterators, and returns a
// merging iterator which produces the combined sequence of keys across all
// iterators. If all input KVIterators enumerate Keys in sorted order, than
// the sequence produced by NewMergeIterator is also sorted. |iterators| must
// be non-empty or NewMergeIterator panics.
func NewMergeIterator(iterators ...KVIterator) *mergeIter {
	if len(iterators) == 0 {
		panic("no iterators provided")
	}
	var h = make([]mergeIterItem, len(iterators))
	for i, it := range iterators {
		h[i].it = it
	}
	return &mergeIter{heap: h}
}

type mergeIter struct {
	cur  mergeIterItem
	heap []mergeIterItem
}
type mergeIterItem struct {
	it       KVIterator
	key, val []byte
	err      error
}

func (m *mergeIter) Next() ([]byte, []byte, error) {
	// On our first invocation (only), call Next on every iterator and
	// initialize a heap over their ordered Keys.
	if m.cur.it == nil {
		var queue = m.heap
		m.heap = m.heap[:0]

		for _, item := range queue {
			item.key, item.val, item.err = item.it.Next()
			heap.Push(m, item)
		}
	} else {
		// Step the last-returned iterator.
		m.cur.key, m.cur.val, m.cur.err = m.cur.it.Next()
		heap.Push(m, m.cur)
	}
	// Pop next ordered iterator.
	m.cur = heap.Pop(m).(mergeIterItem)
	return m.cur.key, m.cur.val, m.cur.err
}

func (m *mergeIter) Close() (err error) {
	// Close all iterators, retaining the first-encountered error.
	for _, item := range m.heap {
		if err2 := item.it.Close(); err2 != nil && err == nil {
			err = err2
		}
	}
	if m.cur.it != nil {
		if err2 := m.cur.it.Close(); err2 != nil && err == nil {
			err = err2
		}
	}
	return err
}

// heap.Heap implementation.
func (m mergeIter) Len() int            { return len(m.heap) }
func (m mergeIter) Swap(i, j int)       { m.heap[i], m.heap[j] = m.heap[j], m.heap[i] }
func (m *mergeIter) Push(x interface{}) { m.heap = append(m.heap, x.(mergeIterItem)) }

func (m *mergeIter) Pop() interface{} {
	old := m.heap
	n := len(old)
	x := old[n-1]
	m.heap = old[0 : n-1]
	return x
}

func (m mergeIter) Less(i, j int) bool {
	var ii, jj = m.heap[i], m.heap[j]

	if ii.err == nil && jj.err == nil {
		return bytes.Compare(m.heap[i].key, m.heap[j].key) < 0
	} else if ii.err != nil {
		if ii.err == KVIteratorDone {
			return false // KVIteratorDone is not less than anything.
		} else {
			// Other errors order before everything else.
			return jj.err == nil || jj.err == KVIteratorDone
		}
	} else /* jj.err != nil */ {
		// We order before KVIteratorDone, and after any other error.
		return jj.err == KVIteratorDone
	}
}

// NewCombinerIterator runs the |input| iterator through a Combiner represented
// by the |inputShape| and |query|. The returned iterator steps over keys & values
// emitted by the Combiner, driving the |input| iterator forward as required.
func NewCombinerIterator(it KVIterator, schema Schema, input ResolvedView, query ResolvedQuery) (*combIter, error) {
	var cb, err = NewCombiner(schema, input, query)
	if err != nil {
		return nil, err
	}
	var seeker, _ = it.(KVIteratorSeeker)
	return &combIter{cb: *cb, it: it, seeker: seeker}, nil
}

type combIter struct {
	cb     Combiner
	it     KVIterator
	seeker KVIteratorSeeker
	tmp    []byte
}

func (m *combIter) Next() ([]byte, []byte, error) {
	for {
		if key, value, err := m.it.Next(); err == KVIteratorDone {
			if m.cb.Flush() {
				break
			}
			return nil, nil, KVIteratorDone
		} else if err != nil {
			return nil, nil, err
		} else if flushed, shouldSeek, err := m.cb.Combine(key, value); err != nil {
			return nil, nil, err
		} else if flushed {
			break
		} else if shouldSeek && m.seeker != nil {
			if m.tmp = m.cb.Seek(m.tmp[:0]); m.tmp == nil {
				if m.cb.Flush() {
					break
				}
				return nil, nil, KVIteratorDone
			}
			m.seeker.Seek(m.tmp)
		}
	}
	m.tmp = m.cb.Value(m.tmp[:0])
	return m.cb.Key(), m.tmp, nil
}

func (m *combIter) Close() error { return m.it.Close() }

// NewSliceIterator returns a KVIterator which ranges over the input slice |s|.
// The input slice is directly referenced by the iterator and must not change.
func NewSliceIterator(s ...[2][]byte) *sliceIter {
	return &sliceIter{i: -1, s: s}
}

type sliceIter struct {
	i      int
	s      [][2][]byte
	closed bool
}

func (s *sliceIter) Next() ([]byte, []byte, error) {
	if s.closed {
		panic("Next of closed iterator")
	} else if s.i++; s.i >= len(s.s) {
		return nil, nil, KVIteratorDone
	}
	return s.s[s.i][0], s.s[s.i][1], nil
}

func (s *sliceIter) Close() error {
	s.closed = true
	return nil
}

// NewSortingIterator returns an KVIterator which, on its first call to Next,
// fully consumes the |input| KVIterator sequence and sorts it. Subsequent
// invocations of Next step over the sorted range. The iterator uses a block-
// based parallel sort-and-merge strategy: input keys are buffered into multiple
// blocks of size |arenaSize|, sorted in parallel, and finally merged. Up to
// |maxArenas| blocks will be used to buffer input: if more would be required,
// ErrResultSetTooLarge is returned.
func NewSortingIterator(it KVIterator, arenaSize, maxArenas int) *sortIter {
	return &sortIter{
		it:        it,
		arenaSize: arenaSize,
		arenas:    maxArenas,
	}
}

type sortIter struct {
	it                KVIterator
	arenaSize, arenas int
	*mergeIter
}

func (s *sortIter) Next() ([]byte, []byte, error) {
	if s.mergeIter != nil {
		return s.mergeIter.Next()
	}

	var (
		arena  = make(kvsArena, s.arenaSize)
		block  [][2][]byte
		blocks []KVIterator
		wg     sync.WaitGroup
	)

	for {
		var key, value, err = s.it.Next()

		if err == KVIteratorDone {
			break
		} else if err != nil {
			return nil, nil, err
		} else if len(arena) == 0 {
			if s.arenas--; s.arenas == 0 {
				return nil, nil, ErrResultSetTooLarge
			}

			// Copy |block| into a new, allocated |tmp|. Retain a SliceIterator
			// built around it, and sort it in the background.
			var tmp = append(make([][2][]byte, 0, len(block)), block...)
			blocks = append(blocks, NewSliceIterator(tmp...))

			wg.Add(1)
			go func(sorter kvsSorter) {
				sort.Sort(sorter)
				wg.Done()
			}(kvsSorter(tmp))

			// Begin a new |block| backed by a new arena.
			block = block[:0]
			arena = make(kvsArena, s.arenaSize)
		}
		block = append(block, arena.add(key, value))
	}

	// Sort final |block|, and wait for all background sorting to complete.
	sort.Sort(kvsSorter(block))
	blocks = append(blocks, NewSliceIterator(block...))
	wg.Wait()

	s.mergeIter = NewMergeIterator(blocks...)
	return s.mergeIter.Next()
}

func (s *sortIter) Close() (err error) {
	if s.mergeIter != nil {
		err = s.mergeIter.Close()
	}
	if err2 := s.it.Close(); err == nil {
		err = err2
	}
	return
}

// sort.Interface implementation.
type kvsSorter [][2][]byte

func (kvs kvsSorter) Len() int           { return len(kvs) }
func (kvs kvsSorter) Swap(i, j int)      { kvs[i], kvs[j] = kvs[j], kvs[i] }
func (kvs kvsSorter) Less(i, j int) bool { return bytes.Compare(kvs[i][0], kvs[j][0]) == -1 }

// kvsArena builds key/value instances out of a pre-allocated storage arena.
type kvsArena []byte

func (b *kvsArena) add(key, val []byte) [2][]byte {
	key = append((*b)[:0], key...)
	*b = (*b)[min(len(key), len(*b)):]
	val = append((*b)[:0], val...)
	*b = (*b)[min(len(val), len(*b)):]

	return [2][]byte{key, val}
}

// MarshalBinaryKeyValue marshals the row defined by |key| & |value| into the
// bufio.Writer, using the same wire encoding as NewStreamIterator.
func MarshalBinaryKeyValue(bw *bufio.Writer, key, value []byte) (err error) {
	var blen [8]byte
	binary.BigEndian.PutUint32(blen[0:4], uint32(len(key)))
	binary.BigEndian.PutUint32(blen[4:8], uint32(len(value)))

	_, _ = bw.Write(blen[0:8])
	_, _ = bw.Write(key)
	_, err = bw.Write(value)
	return
}

// NewHexIterator returns a KVIterator which wraps a bufio.Reader of hex-encoded
// lines of keys & values, eg "HEX(key)\tHEX(value)\n".
func NewHexIterator(br *bufio.Reader) *hexIter { return &hexIter{br: br} }

type hexIter struct {
	br       *bufio.Reader
	key, val []byte
}

func (h *hexIter) Next() ([]byte, []byte, error) {
	var raw []byte
	var err error

	// Read hex-encoded row key.
	if raw, err = readUntil(h.br, '\t'); err == io.EOF {
		if len(raw) == 0 {
			return nil, nil, KVIteratorDone
		} else {
			return nil, nil, io.ErrUnexpectedEOF
		}
	} else if err != nil{
		return nil, nil, err
	}

	var l = len(raw) - 1 // Drop \t.
	raw = raw[:l]

	if dl := hex.DecodedLen(l); dl > cap(h.key) {
		h.key = make([]byte, dl, dl*2)
	} else {
		h.key = h.key[:dl]
	}
	if _, err = hex.Decode(h.key, raw); err != nil {
		return nil, nil, err
	}

	// Read hex-encoded row value.
	if raw, err = readUntil(h.br, '\n'); err == io.EOF {
		return nil, nil, io.ErrUnexpectedEOF
	} else if err != nil {
		return nil, nil, err
	}

	l = len(raw) - 1
	raw = raw[:l] // Drop '\n'.

	if dl := hex.DecodedLen(len(raw)); dl > cap(h.val) {
		h.val = make([]byte, dl, dl*2)
	} else {
		h.val = h.val[:dl]
	}
	if _, err = hex.Decode(h.val, raw); err != nil {
		return nil, nil, err
	}

	return h.key, h.val, nil
}

func (h *hexIter) Close() error { return nil } // No-op.

// NewHexEncoder returns a HexEncoder which encodes hexadecimal keys & values to |w|.
// See also NewHexIterator.
func NewHexEncoder(bw *bufio.Writer) *HexEncoder { return &HexEncoder{bw: bw, hw: hex.NewEncoder(bw)} }

type HexEncoder struct {
	bw *bufio.Writer
	hw io.Writer
}

func (e *HexEncoder) Encode(key, value []byte) error {
	_, _ = e.hw.Write(key)
	_ = e.bw.WriteByte('\t')
	_, _ = e.hw.Write(value)
	return e.bw.WriteByte('\n')
}

func readUntil(br *bufio.Reader, delim byte) ([]byte, error) {
	var b, err = br.ReadSlice(delim)
	if err == bufio.ErrBufferFull {
		var full, rest []byte
		// Preserve read contents as the reader overwrites `b` in subsequent
		// reads with contents of its internal buffer.
		full = append(full, b...)

		rest, err = br.ReadBytes(delim)
		full = append(full, rest...)
		b = full
	}
	return b, err
}

// NewStreamIterator returns a KVIterator which wraps a stream. |recvFn| reads
// the next *QueryResponse of the stream, and should be initialized with a
// Factable_QueryClient instance's RecvMsg function closure.
func NewStreamIterator(recvFn func(interface{}) error) *streamIter {
	return &streamIter{recvFn: recvFn}
}

type streamIter struct {
	recvFn func(interface{}) error
	resp   QueryResponse
	rem    []byte
}

func (rd *streamIter) Next() ([]byte, []byte, error) {
	var b, err = rd.fetch(8)
	if err == io.EOF {
		if len(b) == 0 {
			return nil, nil, KVIteratorDone
		}
		return nil, nil, io.ErrUnexpectedEOF
	} else if err != nil {
		return nil, nil, err
	}

	var keyLen = binary.BigEndian.Uint32(b[0:4])
	var valLen = binary.BigEndian.Uint32(b[4:8])
	var n = int(keyLen + valLen)

	if b, err = rd.fetch(n); err == io.EOF {
		return nil, nil, io.ErrUnexpectedEOF
	} else if err != nil {
		return nil, nil, err
	}
	return b[0:keyLen], b[keyLen : keyLen+valLen], nil
}

func (rd *streamIter) Close() error {
	// Drain the stream, blocking until we read a stream error or closure.
	// To do otherwise would leak the gRPC stream resource. It's on the caller
	// to wire up & cancel an applicable Context in order to prematurely
	// abort the RPC.
	for {
		var _, _, err = rd.Next()

		switch err {
		case KVIteratorDone, context.Canceled, io.ErrUnexpectedEOF:
			return nil
		case nil:
			continue
		default:
			return err
		}
	}
}

func (rd *streamIter) fetch(n int) (b []byte, err error) {
	// Sanity check: refuse to allocate very large buffers.
	if n > 1<<29 { // 512MB
		return nil, fmt.Errorf("read prefix is too large: %d", n)
	}
	for n > len(rd.rem)+len(rd.resp.Content) {
		// We must read more to satisfy |n|. Append remaining content into
		// |rem|, and read the next frame.
		rd.rem = append(rd.rem, rd.resp.Content...)

		if err = rd.recvFn(&rd.resp); err != nil {
			return rd.rem, err
		}
	}
	if l := len(rd.rem); l != 0 {
		// Shift remainder of peek'd content to |rem|, such that the full peek
		// is served out of it.
		rd.rem = append(rd.rem, rd.resp.Content[:n-l]...)
		rd.resp.Content = rd.resp.Content[n-l:]

		b, rd.rem = rd.rem, rd.rem[:0]
	} else {
		b, rd.resp.Content = rd.resp.Content[:n], rd.resp.Content[n:]
	}
	return
}

var ErrResultSetTooLarge = errors.New("result set is too large for buffering")
