package vtable

import (
	"bufio"
	"context"
	"fmt"

	"github.com/LiveRamp/factable/pkg/factable"
	"github.com/LiveRamp/gazette/v2/pkg/consumer"
	"github.com/LiveRamp/gazette/v2/pkg/protocol"
	"github.com/cockroachdb/cockroach/util/encoding"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

func (t *VTable) ResolveQuery(ctx context.Context, spec *factable.QuerySpec) (*factable.ResolvedQuery, error) {
	var resolved, err = t.schemaFn().ResolveQuery(*spec)
	return &resolved, err
}

func (t *VTable) ExecuteQuery(req *factable.ExecuteQueryRequest, stream factable.Query_ExecuteQueryServer) error {
	if err := req.Validate(); err != nil {
		return err
	}
	var schema = t.schemaFn()

	if viewSpec, ok := schema.Views[req.Query.MvTag]; !ok {
		return fmt.Errorf("MvTag not found (%d)", req.Query.MvTag)
	} else if req.Shard != "" {
		return t.queryShard(req, stream, schema, viewSpec)
	} else {
		return t.queryTable(req, stream, schema)
	}
}

func (t *VTable) queryTable(req *factable.ExecuteQueryRequest, stream factable.Query_ExecuteQueryServer, schema *factable.Schema) error {
	// Enumerate all VTable shards.
	var shards, err = t.svc.List(stream.Context(), &consumer.ListRequest{})
	if err != nil {
		return err
	} else if shards.Status != consumer.Status_OK {
		return fmt.Errorf(shards.Status.String())
	}
	// Perform fail-fast sanity check that all shards are assigned.
	for _, shard := range shards.Shards {
		if shard.Route.Primary == -1 {
			return fmt.Errorf("shard %s has no assigned primary process", shard.Spec.Id)
		}
	}

	var (
		vtable         = factable.NewQueryClient(t.svc.Loopback)
		ctx, cancel    = context.WithCancel(stream.Context())
		shardIterators []factable.KVIterator
	)

	// Normally all iterators are read to KVIteratorDone and then Closed by
	// sendQueryResponses. If we abort early due to error, we may have a number
	// of client streams which still need to be canceled and drained.
	defer func() {
		cancel()
		for _, it := range shardIterators {
			_ = it.Close()
		}
	}()

	for _, shard := range shards.Shards {
		var shardClient factable.Query_ExecuteQueryClient

		req.Header = &protocol.Header{
			ProcessId: shard.Route.Members[shard.Route.Primary],
			Route:     shard.Route,
			Etcd:      shards.Header.Etcd,
		}
		req.Shard = shard.Spec.Id

		if shardClient, err = vtable.ExecuteQuery(
			protocol.WithDispatchRoute(ctx, shard.Route, req.Header.ProcessId),
			req,
		); err != nil {
			err = errors.WithMessagef(err, "starting proxy stream to %s", req.Header.ProcessId)
			return err
		} else {
			shardIterators = append(shardIterators, factable.NewStreamIterator(shardClient.RecvMsg))
		}
	}

	// We must merge across shardIterators, and combine to unique output rows.
	var it factable.KVIterator = factable.NewMergeIterator(shardIterators...)
	var q = factable.ResolvedQuery{View: req.Query.View}

	if it, err = factable.NewCombinerIterator(it, *schema, q.View, q); err != nil {
		// Pass.
	} else {
		err = sendQueryResponses(stream.Send, &factable.QueryResponse{Header: &shards.Header}, it)
	}
	if err != nil {
		log.WithField("err", err).Error("table scan failed")
	}
	return err
}

func (t *VTable) queryShard(req *factable.ExecuteQueryRequest, stream factable.Query_ExecuteQueryServer,
	schema *factable.Schema, mvSpec factable.MaterializedViewSpec) error {

	var res, err = t.svc.Resolver.Resolve(consumer.ResolveArgs{
		Context:     stream.Context(),
		ShardID:     req.Shard,
		MayProxy:    false,
		ProxyHeader: req.Header,
	})

	if err != nil {
		return err
	} else if res.Status != consumer.Status_OK {
		return fmt.Errorf(res.Status.String())
	}
	defer res.Done()

	// We expect to read KV rows from the underlying DB having the view
	// shape, with the addition of a prefixed MVTag dimension. Tack on a
	// filter which also constrains that dimension to the target view.
	req.Query.Filters = append(req.Query.Filters, factable.ResolvedQuery_Filter{
		DimTag: factable.DimMVTag,
		Ranges: []factable.ResolvedQuery_Filter_Range{
			{
				Begin: encoding.EncodeVarintAscending(nil, int64(req.Query.MvTag)),
				End:   encoding.EncodeVarintAscending(nil, int64(req.Query.MvTag)),
			},
		},
	})
	var prefixView = factable.ResolvedView{
		DimTags: append([]factable.DimTag{factable.DimMVTag}, mvSpec.ResolvedView.DimTags...),
		MetTags: mvSpec.ResolvedView.MetTags,
	}
	var it factable.KVIterator
	var store = res.Store.(*consumer.RocksDBStore)

	// Stage 1: Iterate over raw DB rows.
	it = factable.NewRocksDBIterator(store.DB, store.ReadOptions, nil, nil)
	// Stage 2: Combine over DB rows to filter and emit the desired |req.Query.Shape|.
	if it, err = factable.NewCombinerIterator(it, *schema, prefixView, req.Query); err != nil {
		return err
	}
	// If the |req.Query.View.DimTags| is not a strict prefix of |mvSpec.ResolvedView.DimTags|,
	// than Stage 2 output is not in naturally sorted order, and may included
	// repetitions of rows.
	if !isPrefix(req.Query.View.DimTags, mvSpec.ResolvedView.DimTags) {
		// Stage 3: Sort Stage 2 output, ordered on the desired query shape.
		it = factable.NewSortingIterator(it, t.cfg.Factable.ArenaSize, t.cfg.Factable.MaxArenas)

		// Stage 4: Re-combine the sorted output into unique output rows. This
		// combiner pass accepts and emits the query's Shape, and has no filters.
		var q = factable.ResolvedQuery{View: req.Query.View}
		if it, err = factable.NewCombinerIterator(it, *schema, q.View, q); err != nil {
			return err
		}
	}
	// Send header & query responses.
	if err = sendQueryResponses(stream.Send, &factable.QueryResponse{Header: &res.Header}, it); err != nil {
		log.WithField("err", err).Error("shard scan failed")
	}
	return err
}

func isPrefix(a, b []factable.DimTag) bool {
	if len(a) > len(b) {
		return false
	}
	for i, tag := range a {
		if b[i] != tag {
			return false
		}
	}
	return true
}

func sendQueryResponses(send func(*factable.QueryResponse) error, header *factable.QueryResponse, it factable.KVIterator) error {
	var (
		qsw      = &queryResponseWriter{send: send}
		bw       = bufio.NewWriter(qsw)
		key, val []byte
		err      error
	)
	// Iteratively send each scanned key & value with 4-byte length prefix.
	for key, val, err = it.Next(); err == nil; key, val, err = it.Next() {
		if header != nil {
			// Send header before first response row.
			if err = send(header); err != nil {
				break
			}
			header = nil
		}
		if err = factable.MarshalBinaryKeyValue(bw, key, val); err != nil {
			break
		}
	}
	if err == factable.KVIteratorDone {
		if err = it.Close(); err == nil {
			err = bw.Flush()
		}
	}
	return err
}

// queryResponseWriter adapts a Query_QueryServer to an io.Writer.
type queryResponseWriter struct {
	send func(*factable.QueryResponse) error
	resp factable.QueryResponse
}

func (qsw *queryResponseWriter) Write(p []byte) (int, error) {
	qsw.resp.Content = p
	return len(p), qsw.send(&qsw.resp)
}
