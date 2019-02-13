package internal

import (
	"git.liveramp.net/jgraet/factable/pkg/factable"
	"git.liveramp.net/jgraet/factable/pkg/testing/quotes"
	"github.com/cockroachdb/cockroach/util/encoding"
	gc "github.com/go-check/check"
)

type DeltasSuite struct{}

func (s *DeltasSuite) TestTransactionTransitions(c *gc.C) {
	var txn = Transactions{
		Extractor: make(map[string]Transactions_State),
	}
	var cases = []struct {
		row    bool
		seqNo  int64
		commit int
		err    string
	}{
		{row: true, seqNo: 1}, // Row delta.
		{row: true, seqNo: 2},
		{seqNo: 2, commit: 2}, // Commit.
		{row: true, seqNo: 3},
		{row: true, seqNo: 5},
		{seqNo: 6, commit: 2}, // Commit.
		{row: true, seqNo: 9},
		{row: true, seqNo: 10},
		{seqNo: 6}, // Rollback. No rows committed.
		{row: true, seqNo: 7},
		{seqNo: 7, commit: 1}, // Commit.
		{seqNo: 7},            // Commit replay. Noop.

		// Error cases:
		// Prime with additional pending commit fixtures.
		{row: true, seqNo: 8},
		{row: true, seqNo: 9},
		{row: true, seqNo: 10},

		// Replay of a row-delta.
		{row: true, seqNo: 8, err: `repeat SeqNo \(extractor a, seqNo 8\)`},

		// Invalid commit seqNos.
		{seqNo: 9, err: `unexpected commit SeqNo \(seqNo: 9, last event: 10, last commit: 7\)`},
		{seqNo: 6, err: `unexpected commit SeqNo \(seqNo: 6, last event: 10, last commit: 7\)`},
	}
	for _, tc := range cases {
		var de = DeltaEvent{Extractor: "a", SeqNo: tc.seqNo}
		if tc.row {
			de.RowKey = []byte("key")
		}
		var committed, err = txn.Apply(de)
		if tc.err != "" {
			c.Check(err, gc.ErrorMatches, tc.err)
		} else {
			c.Check(err, gc.IsNil)
		}
		c.Check(committed, gc.HasLen, tc.commit)
	}
}

func (s *DeltasSuite) TestRowToRelationSpec(c *gc.C) {
	var cfg = quotes.BuildSchemaSpec()
	var schema, _ = factable.NewSchema(nil, quotes.BuildSchemaSpec())

	var cases = []struct {
		row  []byte
		err  string
		spec factable.MaterializedViewSpec
	}{
		{
			row:  encoding.EncodeVarintAscending(nil, int64(quotes.MVWordStats)),
			spec: cfg.Views[0],
		},
		{
			row:  encoding.EncodeVarintAscending(nil, int64(quotes.MVQuoteStats)),
			spec: cfg.Views[1],
		},
		{
			row: []byte("garbage"),
			err: `decoding view tag: insufficient bytes to decode uvarint value: arbage`,
		},
		{
			row: encoding.EncodeVarintAscending(nil, 9999),
			err: `MVTag 9999: view not found`,
		},
	}
	for _, tc := range cases {
		var de = DeltaEvent{RowKey: tc.row}
		var spec, err = de.ViewSpec(&schema)

		if tc.err != "" {
			c.Check(err, gc.ErrorMatches, tc.err)
		} else {
			c.Check(err, gc.IsNil)
		}
		c.Check(spec, gc.DeepEquals, tc.spec)
	}
}

var _ = gc.Suite(&DeltasSuite{})
