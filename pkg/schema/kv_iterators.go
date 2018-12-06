package schema

import (
	"bytes"
	"container/heap"
	"fmt"
	"sort"
	"sync"

	"github.com/tecbot/gorocksdb"
)

// KVIterator is an iterator over keys and their values.
type KVIterator interface {
	// Next steps the KVIterator to the next key & value, returning nil on
	// success. It returns KVIteratorDone if the iterator sequence is complete,
	// or any encountered error.
	Next() error
	// Key returns the current iterator Key. It's invalidated by a call to Next.
	Key() []byte
	// Value of the current Key. It's also invalidated by a call to Next.
	Value() []byte
	// Close the KVIterator, returning an encountered error.
	Close() error
}

// KVIteratorSeeker is an optional interface of KVIterator which will
// seek to |key|, which must be lexicographically greater-than the last
// returned Key.
type KVIteratorSeeker interface {
	Seek(key []byte)
}

// KVIteratorDone is returned by KVIterator.Next when the sequence is complete.
var KVIteratorDone = fmt.Errorf("KVIterator done")

// NewMergeIterator accepts a variable number of KVIterators, and returns a
// merging iterator which produces the combined sequence of keys across all
// iterators. If all input KVIterators enumerate Keys in sorted order, than the
// sequence produced by NewMergeIterator is also sorted. NewMergeIterator will
// modify the input |iterators| slice.
func NewMergeIterator(iterators ...KVIterator) *mergeIter {
	return &mergeIter{heap: iterators}
}

type mergeIter struct {
	KVIterator
	heap []KVIterator
}

func (m *mergeIter) Next() error {
	// On our first invocation (only), call Next on every iterator and
	// initialize a heap over their ordered Keys.
	if m.KVIterator == nil {
		var queue = m.heap
		m.heap = m.heap[:0]

		for _, it := range queue {
			if err := it.Next(); err == KVIteratorDone {
				// Pass.
			} else if err != nil {
				return err
			} else {
				heap.Push(m, it)
			}
		}
	} else {
		// Step the last-returned iterator. Re-heap it if non-empty, and otherwise Close.
		if err := m.KVIterator.Next(); err == KVIteratorDone {
			if err = m.KVIterator.Close(); err != nil {
				return err
			}
		} else if err != nil {
			return err
		} else {
			heap.Push(m, m.KVIterator)
		}
	}

	if len(m.heap) == 0 {
		return KVIteratorDone
	}

	// Pop next ordered iterator.
	m.KVIterator = heap.Pop(m).(KVIterator)
	return nil
}

func (m *mergeIter) Close() (err error) {
	// Close all remaining iterators, retaining the first-encountered error.
	for _, it := range m.heap {
		if err2 := it.Close(); err2 != nil && err == nil {
			err = err2
		}
	}
	return err
}

// heap.Heap implementation.
func (m mergeIter) Len() int            { return len(m.heap) }
func (m mergeIter) Swap(i, j int)       { m.heap[i], m.heap[j] = m.heap[j], m.heap[i] }
func (m *mergeIter) Push(x interface{}) { m.heap = append(m.heap, x.(KVIterator)) }

func (m *mergeIter) Pop() interface{} {
	old := m.heap
	n := len(old)
	x := old[n-1]
	m.heap = old[0 : n-1]
	return x
}

func (m mergeIter) Less(i, j int) bool {
	return bytes.Compare(m.heap[i].Key(), m.heap[j].Key()) < 0
}

// NewCombinerIterator runs the |input| iterator through a Combiner represented
// by the |inputShape| and |query|. The returned iterator steps over keys & values
// emitted by the Combiner, driving the |input| iterator forward as required.
func NewCombinerIterator(input KVIterator, schema Schema, inputShape Shape, query QuerySpec) (*combIter, error) {
	var cb, err = NewCombiner(schema, inputShape, query)
	if err != nil {
		return nil, err
	}
	var seeker, _ = input.(KVIteratorSeeker)
	return &combIter{cb: *cb, input: input, seeker: seeker}, nil
}

type combIter struct {
	cb     Combiner
	input  KVIterator
	seeker KVIteratorSeeker
	tmp    []byte
}

func (m *combIter) closeAndFlush() error {
	if err := m.input.Close(); err != nil {
		return err
	}
	m.input = nil
	m.cb.Flush()
	return nil
}

func (m *combIter) Next() error {
	if m.input == nil {
		return KVIteratorDone
	}

	for {
		if err := m.input.Next(); err == KVIteratorDone {
			if err = m.closeAndFlush(); err != nil {
				return err
			}
			break
		} else if err != nil {
			return err
		}

		if flushed, shouldSeek, err := m.cb.Combine(m.input.Key(), m.input.Value()); err != nil {
			return err
		} else if flushed {
			break
		} else if shouldSeek {
			if m.tmp = m.cb.Seek(m.tmp[:0]); m.tmp == nil {
				// No further keys will be accepted. We're done.
				if err = m.closeAndFlush(); err != nil {
					return err
				}
				break
			} else if m.seeker != nil {
				m.seeker.Seek(m.tmp)
			}
		}
	}
	return nil
}

func (m *combIter) Key() []byte { return m.cb.Key() }
func (m *combIter) Value() []byte {
	m.tmp = m.cb.Value(m.tmp[:0])
	return m.tmp
}

func (m *combIter) Close() error {
	if m.input != nil {
		return m.input.Close()
	}
	return nil
}

// NewSliceIterator returns a KVIterator which ranges over the input slice |s|.
// The input slice is directly referenced by the iterator and must not change.
func NewSliceIterator(s ...[2][]byte) *sliceIter {
	return &sliceIter{i: -1, s: s}
}

type sliceIter struct {
	i int
	s [][2][]byte
}

func (s *sliceIter) Next() error {
	if s.i++; s.i == len(s.s) {
		return KVIteratorDone
	}
	return nil
}

func (s *sliceIter) Key() []byte   { return s.s[s.i][0] }
func (s *sliceIter) Value() []byte { return s.s[s.i][1] }
func (s *sliceIter) Close() error  { return nil }

// NewRocksDBIterator returns an iterator over |db| using ReadOptions |ro|. If
// |from| is non-nil, iteration starts with a key which is equal or greater. If
// |to| is non-nil, iteration halts with a key which is equal or greater (eg,
// exclusive of |to|).
func NewRocksDBIterator(db *gorocksdb.DB, ro *gorocksdb.ReadOptions, from, to []byte) *rocksIter {
	return &rocksIter{db: db, ro: ro, from: from, to: to}
}

type rocksIter struct {
	db       *gorocksdb.DB
	ro       *gorocksdb.ReadOptions
	it       *gorocksdb.Iterator
	from, to []byte
}

func (r *rocksIter) Next() error {
	if r.it == nil {
		r.it = r.db.NewIterator(r.ro)

		if r.from != nil {
			r.it.Seek(r.from)
		} else {
			r.it.Seek([]byte{0x01}) // Seek the first non-metadata key of the DB.
		}
	} else {
		r.it.Next()
	}

	if r.it.Err() != nil {
		return r.it.Err()
	} else if !r.it.Valid() {
		return KVIteratorDone
	} else if r.to != nil && bytes.Compare(r.to, r.it.Key().Data()) <= 0 {
		return KVIteratorDone
	} else {
		return nil
	}
}

func (r *rocksIter) Close() error {
	if r.it != nil {
		r.it.Close()
	}
	return nil
}

func (r *rocksIter) Seek(b []byte) { r.it.Seek(b) }
func (r *rocksIter) Key() []byte   { return r.it.Key().Data() }
func (r *rocksIter) Value() []byte { return r.it.Value().Data() }

// NewSortingIterator returns an KVIterator which, on its first call to Next,
// fully consumes the |input| KVIterator sequence and sorts it. Subsequent
// invocations of Next step over the sorted range. The iterator uses a block-
// based parallel sort-and-merge strategy: input keys are buffered into multiple
// blocks of size |arenaSize|, sorted in parallel, and finally merged. Up to
// |maxArenas| blocks will be used to buffer input: if more would be required,
// ErrResultSetTooLarge is returned.
func NewSortingIterator(input KVIterator, arenaSize, maxArenas int) *sortIter {
	return &sortIter{
		input:     input,
		arenaSize: arenaSize,
		arenas:    maxArenas,
	}
}

type sortIter struct {
	input             KVIterator
	arenaSize, arenas int
	*mergeIter
}

func (s *sortIter) Next() error {
	if s.mergeIter != nil {
		return s.mergeIter.Next()
	}

	var (
		input  = s.input
		arena  = make(kvsArena, s.arenaSize)
		block  [][2][]byte
		blocks []KVIterator
		wg     sync.WaitGroup
	)
	s.input = nil

	var err error
	for err = input.Next(); err == nil; err = input.Next() {
		if len(arena) == 0 {
			if s.arenas--; s.arenas == 0 {
				_ = input.Close()
				return ErrResultSetTooLarge
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
		block = append(block, arena.add(input.Key(), input.Value()))
	}

	if err != KVIteratorDone {
		_ = input.Close()
		return err
	} else if err = input.Close(); err != nil {
		return err
	}

	// Sort final |block|, and wait for all background sorting to complete.
	sort.Sort(kvsSorter(block))
	blocks = append(blocks, NewSliceIterator(block...))
	wg.Wait()

	s.mergeIter = NewMergeIterator(blocks...)
	return s.mergeIter.Next()
}

func (s *sortIter) Close() error {
	if s.input != nil {
		return s.input.Close()
	}
	return s.mergeIter.Close()
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

var ErrResultSetTooLarge = fmt.Errorf("result set is too large for buffering")
