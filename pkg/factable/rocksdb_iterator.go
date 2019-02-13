// +build rocksdb

package factable

import (
	"bytes"

	"github.com/tecbot/gorocksdb"
)

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
	seeked   bool
}

func (r *rocksIter) Next() ([]byte, []byte, error) {
	if r.it == nil {
		r.it = r.db.NewIterator(r.ro)

		if r.from != nil {
			r.it.Seek(r.from)
		} else {
			r.it.Seek([]byte{0x01}) // Seek the first non-metadata key of the DB.
		}
		r.seeked = true
	}

	if !r.seeked {
		r.it.Next()
	} else {
		r.seeked = false
	}

	if r.it.Err() != nil {
		return nil, nil, r.it.Err()
	} else if !r.it.Valid() {
		return nil, nil, KVIteratorDone
	} else if r.to != nil && bytes.Compare(r.to, r.it.Key().Data()) <= 0 {
		return nil, nil, KVIteratorDone
	} else {
		return r.it.Key().Data(), r.it.Value().Data(), nil
	}
}

func (r *rocksIter) Close() error {
	if r.it != nil {
		r.it.Close()
	}
	return nil
}

func (r *rocksIter) Seek(b []byte) {
	r.it.Seek(b)
	r.seeked = true
}
