//go:generate protoc -I . -I ../../vendor  --gogo_out=plugins=grpc:. deltas.proto
package internal

import (
	"git.liveramp.net/jgraet/factable/pkg/factable"
	"github.com/LiveRamp/gazette/v2/pkg/message"
	"github.com/cockroachdb/cockroach/util/encoding"
	"github.com/pkg/errors"
)

// ViewSpec maps the aggregated DeltaEvent to its MaterializedViewSpec.
func (m *DeltaEvent) ViewSpec(s *factable.Schema) (factable.MaterializedViewSpec, error) {
	return ViewSpecOfRow(m.RowKey, s)
}

func (m Transactions) Apply(e DeltaEvent) (committed []DeltaEvent, err error) {
	var (
		state = m.Extractor[e.Extractor]
		l     = len(state.Events)
	)
	if l == 0 {
		// Initialization case. Append a DeltaEvent which trivially
		// represents commit of SeqNo 0.
		state.Events, l = append(state.Events, DeltaEvent{}), 1
	}

	// Invariants:
	// - Events[0] is the last commit acknowledgement DeltaEvent.
	// - All other Events[1:] are row-deltas.
	// - Events is ordered monotonically on SeqNo.

	if e.RowKey == nil {
		// Event is a commit acknowledgement.
		if e.SeqNo == state.Events[0].SeqNo {
			// Acknowledgement represents a roll-back. Discard events read since.
			state.Events = state.Events[:1]
		} else if e.SeqNo >= state.Events[l-1].SeqNo {
			// Event acknowledges that deltas through SeqNo were fully committed.
			// Return |committed| row-deltas, and reset for the next transaction.
			committed, state.Events = state.Events[1:], state.Events[:1]
			state.Events[0] = e
		} else {
			err = errors.Errorf("unexpected commit SeqNo (seqNo: %d, last event: %d, last commit: %d)",
				e.SeqNo, state.Events[l-1].SeqNo, state.Events[0].SeqNo)
		}
	} else if e.SeqNo > state.Events[l-1].SeqNo {
		// Event is a properly sequenced row DeltaEvent.
		state.Events = append(state.Events, e)
	} else {
		err = errors.Errorf("repeat SeqNo (extractor %s, seqNo %d)", e.Extractor, e.SeqNo)
	}

	m.Extractor[e.Extractor] = state
	return
}

// ViewSpecOfRow maps a row key to its MaterializedViewSpec.
func ViewSpecOfRow(key []byte, s *factable.Schema) (factable.MaterializedViewSpec, error) {
	var _, mvTag, err = encoding.DecodeVarintAscending(key)
	if err != nil {
		return factable.MaterializedViewSpec{}, errors.Wrap(err, "decoding view tag")
	}
	var spec, ok = s.Views[factable.MVTag(mvTag)]
	if !ok {
		return factable.MaterializedViewSpec{}, errors.Wrapf(ErrViewNotFound, "MVTag %d", mvTag)
	}
	return spec, nil
}

// MustViewSpecOfRow maps a row key to its MaterializedViewSpec, and panics on error.
func MustViewSpecOfRow(key []byte, s *factable.Schema) factable.MaterializedViewSpec {
	if spec, err := ViewSpecOfRow(key, s); err != nil {
		panic(err)
	} else {
		return spec
	}
}

// DeltaMapping returns a ModuloMapping of DeltaEvent on RowKey.
func DeltaMapping(partsFn message.PartitionsFunc) message.MappingFunc {
	return message.ModuloMapping(
		func(msg message.Message, b []byte) []byte { return msg.(*DeltaEvent).RowKey },
		partsFn,
	)
}

// ErrViewNotFound indicates the MVTag is not in the Schema.
var ErrViewNotFound = errors.New("view not found")
