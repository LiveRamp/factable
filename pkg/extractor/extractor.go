package extractor

import (
	"runtime"
	"strconv"
	"time"

	"github.com/cockroachdb/cockroach/util/encoding"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	broker "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/consumer"
	"go.gazette.dev/core/consumer/recoverylog"
	"go.gazette.dev/core/mainboilerplate/runconsumer"
	"go.gazette.dev/core/message"

	"go.gazette.dev/core/broker/protocol"

	"github.com/LiveRamp/factable/pkg/factable"
	. "github.com/LiveRamp/factable/pkg/internal"

	"go.gazette.dev/core/broker/client"
)

// Extractor is a runconsumer.Application which extracts and locally combines
// relation DeltaEvents from events of an input journal.
type Extractor struct {
	Extractors factable.ExtractFns

	// schemaFn returns the current effective Schema.
	schemaFn func() *factable.Schema
	// deltas maps a DeltaEvent to its partition.
	deltas message.MappingFunc
	// txnSemaphoreCh constrains the number of concurrent consumer transactions,
	// encouraging more messages processed per-transaction.
	txnSemaphoreCh chan struct{}
}

// Config utilized by Extractor.
type Config struct {
	Factable CommonConfig `group:"Factable" namespace:"factable"`

	runconsumer.BaseConfig
}

// NewConfig returns a new Spec.
func (Extractor) NewConfig() runconsumer.Config { return new(Config) }

// InitApplication initializes the Extractor.
func (ext *Extractor) InitApplication(args runconsumer.InitArgs) error {
	var cfg = args.Config.(*Config)

	if err := cfg.Factable.Validate(); err != nil {
		return err
	}
	if cfg.Factable.TxnConcurrency == 0 {
		cfg.Factable.TxnConcurrency = uint(runtime.GOMAXPROCS(0))
	}

	// Fetch and watch the shared SchemaSpec from Etcd.
	var ks = NewSchemaKeySpace(cfg.Factable.SchemaKey, &ext.Extractors)

	if err := ks.Load(args.Context, args.Service.Etcd, 0); err != nil {
		return errors.WithMessagef(err, "loading schema KeySpace (%s)", ks.Root)
	}
	var ss = &SchemaService{
		Config:     cfg.Factable,
		KS:         ks,
		Etcd:       args.Service.Etcd,
		ExtractFns: &ext.Extractors,
	}
	go func() {
		if err := ks.Watch(args.Context, args.Service.Etcd); err != nil {
			log.WithField("err", err).Error("schema KeySpace watch failed")
		}
	}()
	ext.schemaFn = ss.Schema
	ext.txnSemaphoreCh = make(chan struct{}, cfg.Factable.TxnConcurrency)
	factable.RegisterSchemaServer(args.Server.GRPCServer, ss)

	// Fetch and watch partitions to which DeltaEvents are to be written.
	if parts, err := client.NewPolledList(args.Context,
		args.Service.Journals,
		time.Minute,
		broker.ListRequest{Selector: cfg.Factable.DeltasSelector()},
	); err != nil {
		return errors.WithMessage(err, "fetching deltas partitions")
	} else {
		ext.deltas = DeltaMapping(parts.List)
	}

	return nil
}

type shardState struct {
	// SeqNo of the next row DeltaEvent. Persisted by JSONStore.
	SeqNo int64
	// Journals written to as part of the current transaction. Persisted by JSONStore.
	TransactionJournals map[protocol.Journal]string

	// Tag of MaterializedViewSpec being served by this Shard.
	mvTag factable.MVTag
	// Schema being used for the current consumer transaction.
	schema *factable.Schema
	// Row-keys and Aggregates which have been folded over during this
	// transaction, and which remain to be flushed to row-deltas.
	pending map[string][]factable.Aggregate
}

// NewStore constructs a Store for |shard| around the recovered local directory
// |dir| and initialized Recorder |rec|.
func (ext *Extractor) NewStore(shard consumer.Shard, rec *recoverylog.Recorder) (consumer.Store, error) {
	var state = &shardState{
		SeqNo:               1,
		TransactionJournals: make(map[protocol.Journal]string),
		pending:             make(map[string][]factable.Aggregate),
	}

	var store, err = consumer.NewJSONFileStore(rec, state)
	if err != nil {
		return nil, err
	}

	// Run BeginTxn to cause the Shard to fail immediately if the `mvTag` label
	// or view are mis-configured. Without this, the Shard would still fail, but
	// only after attempting to process an input message. BeginTxn is otherwise
	// a no-op.
	err = ext.BeginTxn(shard, store)
	var finOp = client.FinishedOperation(err)
	// Ensure acknowledgements of the last committed transaction are written
	// to respective tracked journals. This also informs readers that they
	// should roll back events of a larger SeqNo.

	if err2 := ext.FinishTxn(shard, store, finOp); err != nil {
		return nil, err
	} else if err2 != nil {
		return nil, err2
	}
	return store, nil
}

func (ext *Extractor) NewMessage(spec *broker.JournalSpec) (message.Message, error) {
	return ext.Extractors.NewMessage(spec)
}

func (ext *Extractor) BeginTxn(shard consumer.Shard, store consumer.Store) error {
	var state = store.(*consumer.JSONFileStore).State.(*shardState)
	ext.txnSemaphoreCh <- struct{}{} // Block until we obtain the transaction concurrency semaphore.
	state.schema = ext.schemaFn()    // Used through life of the current transaction.

	// Map the `mvTag` label to a MaterializedViewSpec. Having verified that spec
	// is present, we're then assured that all entities *referenced* by it are
	// also present and consistent.
	if l := shard.Spec().LabelSet.ValuesOf("mvTag"); len(l) != 1 {
		return errors.Errorf(`expected single "mvTag" label (%v)`, l)
	} else if tag, err := strconv.ParseInt(l[0], 10, 64); err != nil {
		return errors.WithMessagef(err, `parsing "mvTag" label value`)
	} else if mv, ok := ext.schemaFn().Views[factable.MVTag(tag)]; !ok {
		return errors.Wrapf(ErrViewNotFound, "mvTag %d", tag)
	} else {
		state.mvTag = mv.Tag
	}

	return nil
}

func (*Extractor) ConsumeMessage(_ consumer.Shard, store consumer.Store, envelope message.Envelope, _ *message.Publisher) error {
	var (
		state   = store.(*consumer.JSONFileStore).State.(*shardState)
		mvSpec  = state.schema.Views[state.mvTag]
		relSpec = state.schema.Relations[mvSpec.RelTag]
	)
	for _, row := range state.schema.Extract.Mapping[relSpec.MapTag](envelope) {
		var (
			tmp [256]byte // Does not escape.
			key = encoding.EncodeVarintAscending(tmp[:0], int64(mvSpec.Tag))
		)

		key = state.schema.ExtractAndMarshalDimensions(key, mvSpec.ResolvedView.DimTags, row)

		var aggs, ok = state.pending[string(key)]
		if !ok {
			// Initialize Aggregates.
			aggs = make([]factable.Aggregate, len(mvSpec.ResolvedView.MetTags))
			state.schema.InitAggregates(mvSpec.ResolvedView.MetTags, aggs)

			state.pending[string(key)] = aggs // Track for remainder of txn.
		}

		state.schema.FoldMetrics(mvSpec.ResolvedView.MetTags, aggs, row)
	}
	return nil
}

func (ext *Extractor) FinalizeTxn(shard consumer.Shard, store consumer.Store, publisher *message.Publisher) error {
	var (
		state  = store.(*consumer.JSONFileStore).State.(*shardState)
		mvSpec = state.schema.Views[state.mvTag]
		delta  = &DeltaEvent{Extractor: shard.Spec().Id.String()}
	)
	for key, aggs := range state.pending {
		// Load |SeqNo|, |key| and |aggs| into |delta| in preparation for marshalling.
		delta.SeqNo = state.SeqNo
		delta.RowKey = append(delta.RowKey[:0], key...)
		delta.RowValue = state.schema.MarshalMetrics(delta.RowValue[:0], mvSpec.ResolvedView.MetTags, aggs)
		delete(state.pending, key)

		// Map and publish the DeltaEvent. This parallels `message.Publish`,
		// except we retain the mapped journal and framing for future use.
		var journal, contentType, err = ext.deltas(delta)
		if err != nil {
			return err
		}

		framing, err := message.FramingByContentType(contentType)
		if err != nil {
			return err
		}

		var ar = broker.AppendRequest{Journal: journal}

		var aa = shard.JournalClient().StartAppend(ar, nil)
		if err = aa.Require(framing.Marshal(delta, aa.Writer())).Release(); err != nil {
			return err
		}
		// Track that |journal| was published to as part of this transaction.
		state.TransactionJournals[journal] = framing.ContentType()
		state.SeqNo++ // Tick for next row DeltaEvent.
	}
	return nil
}

func (ext *Extractor) FinishTxn(shard consumer.Shard, store consumer.Store, op consumer.OpFuture) error {
	defer func() {
		_ = <-ext.txnSemaphoreCh // Release the transaction concurrency semaphore.
	}()

	if op.Err() != nil {
		return nil // Don't write commit acknowledgments if the transaction failed.
	}

	select {
	case <-op.Done():

	}

	var (
		state     = store.(*consumer.JSONFileStore).State.(*shardState)
		commitAck = &DeltaEvent{
			Extractor: shard.Spec().Id.String(),
			SeqNo:     state.SeqNo - 1, // Acknowledge last row DeltaEvent SeqNo.
		}
	)

	var checkpoint, err = store.RestoreCheckpoint(shard)
	if err != nil {
		return err
	}

	var commitOp = store.StartCommit(shard, checkpoint, consumer.OpFutures{op: {}})

	// 2PC: When |barrier| resolves, our transaction has committed. For each
	// Journal of the transaction, queue a "commit acknowledgement" message
	// which informs readers that all messages previously read under the
	// transaction ID may now be applied.
	for journal, contentType := range state.TransactionJournals {
		var framing, err = message.FramingByContentType(contentType)
		if err != nil   {
			return errors.Wrapf(err, "journal %s", journal)
		}

		var ar = broker.AppendRequest{Journal: journal}
		var aa = shard.JournalClient().StartAppend(ar, consumer.OpFutures{commitOp: {}})
		if err = aa.Require(framing.Marshal(commitAck, aa.Writer())).Release(); err != nil {
			return err
		}
		delete(state.TransactionJournals, journal) // Clear for next txn.
	}
	return nil
}
