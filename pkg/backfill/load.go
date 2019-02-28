package backfill

import (
	"bufio"
	"context"
	"io"
	"time"

	"git.liveramp.net/jgraet/factable/pkg/factable"
	"git.liveramp.net/jgraet/factable/pkg/internal"
	"github.com/LiveRamp/gazette/v2/pkg/client"
	"github.com/LiveRamp/gazette/v2/pkg/message"
	pb "github.com/LiveRamp/gazette/v2/pkg/protocol"
	"github.com/pkg/errors"
)

// Load a Reader of hex-encoded key/values into DeltaEvent partitions identified
// by |selector|. Each DeltaEvent row is sequenced under extractor |name| and
// written atomically with its commit acknowledgement. This ensures that multiple
// calls to Load with interspersed failures will not result into double-counting
// of rows by the VTable service, so long as each invocation uses the same |name|
// and reads identical input sequences from the Reader.
func Load(ctx context.Context, r io.Reader, rjc pb.RoutedJournalClient, selector pb.LabelSelector, name string) error {
	var mapping message.MappingFunc

	var partitions, err = client.NewPolledList(ctx, rjc, time.Minute, pb.ListRequest{Selector: selector})
	if err != nil {
		return errors.WithMessage(err, "fetching deltas partitions")
	} else {
		mapping = internal.DeltaMapping(partitions.List)
	}

	var (
		delta = &internal.DeltaEvent{
			Extractor: name,
			SeqNo:     1,
		}
		as = client.NewAppendService(ctx, rjc)
		it = factable.NewHexIterator(bufio.NewReaderSize(r, 32*1024))
	)
	for {
		delta.RowKey, delta.RowValue, err = it.Next()
		if err == factable.KVIteratorDone {
			break
		} else if err != nil {
			return errors.WithMessagef(err, "reading at SeqNo %d", delta.SeqNo)
		}

		// Map the DeltaEvent to its journal & framing.
		journal, framing, err := mapping(delta)
		if err != nil {
			return errors.WithMessagef(err, "failed to map DeltaEvent")
		}
		// Atomically write the DeltaEvent and its acknowledgement.
		var aa = as.StartAppend(journal)
		aa.Require(framing.Marshal(delta, aa.Writer()))

		delta.RowKey, delta.RowValue = nil, nil
		aa.Require(framing.Marshal(delta, aa.Writer()))

		if err = aa.Release(); err != nil {
			return errors.WithMessagef(err, "failed to write DeltaEvents")
		}
		delta.SeqNo += 1
	}

	client.WaitForPendingAppends(as.PendingExcept(""))
	return nil
}
