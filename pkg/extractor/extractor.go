package extractor

import (
	"strconv"
	"time"

	"git.liveramp.net/jgraet/factable/pkg/factable"
	. "git.liveramp.net/jgraet/factable/pkg/internal"
	"github.com/LiveRamp/gazette/v2/pkg/client"
	"github.com/LiveRamp/gazette/v2/pkg/consumer"
	"github.com/LiveRamp/gazette/v2/pkg/mainboilerplate/runconsumer"
	"github.com/LiveRamp/gazette/v2/pkg/message"
	pb "github.com/LiveRamp/gazette/v2/pkg/protocol"
	"github.com/LiveRamp/gazette/v2/pkg/recoverylog"
	"github.com/cockroachdb/cockroach/util/encoding"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

// Extractor is a runconsumer.Application which extracts and locally combines
// relation DeltaEvents from events of an input journal.
type Extractor struct {
	Extractors factable.ExtractFns

	// schemaFn returns the current effective Schema.
	schemaFn func() *factable.Schema
	// deltas maps a DeltaEvent to its partition.
	deltas message.MappingFunc
}

// Config utilized by Extractor.
type Config struct {
	Factable CommonConfig `group:"Factable" namespace:"factable"`

	runconsumer.BaseConfig
}

// NewConfig returns a new Spec.
func (Extractor) NewConfig() runconsumer.Config { return new(Config) }

// InitApplication initializes the Extractor.
func (c *Extractor) InitApplication(args runconsumer.InitArgs) error {
	var cfg = args.Config.(*Config)

	if err := cfg.Factable.Validate(); err != nil {
		return err
	}

	// Fetch and watch the shared SchemaSpec from Etcd.
	var ks = NewSchemaKeySpace(cfg.Factable.SchemaKey, &c.Extractors)

	if err := ks.Load(args.Context, args.Service.Etcd, 0); err != nil {
		return errors.WithMessagef(err, "loading schema KeySpace (%s)", ks.Root)
	}
	var ss = &SchemaService{
		Config:     cfg.Factable,
		KS:         ks,
		Etcd:       args.Service.Etcd,
		ExtractFns: &c.Extractors,
	}
	go func() {
		if err := ks.Watch(args.Context, args.Service.Etcd); err != nil {
			log.WithField("err", err).Error("schema KeySpace watch failed")
		}
	}()
	c.schemaFn = ss.Schema
	factable.RegisterSchemaServer(args.Server.GRPCServer, ss)

	// Fetch and watch partitions to which DeltaEvents are to be written.
	if parts, err := client.NewPolledList(args.Context,
		args.Service.Journals,
		time.Minute,
		pb.ListRequest{Selector: cfg.Factable.DeltasSelector()},
	); err != nil {
		return errors.WithMessage(err, "fetching deltas partitions")
	} else {
		c.deltas = DeltaMapping(parts.List)
	}

	return nil
}

type shardState struct {
	// SeqNo of the next row DeltaEvent. Persisted by JSONStore.
	SeqNo int64
	// Journals written to as part of the current transaction. Persisted by JSONStore.
	TransactionJournals map[pb.Journal]string

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
func (c *Extractor) NewStore(shard consumer.Shard, dir string, rec *recoverylog.Recorder) (consumer.Store, error) {
	var state = &shardState{
		SeqNo:               1,
		TransactionJournals: make(map[pb.Journal]string),
		pending:             make(map[string][]factable.Aggregate),
	}

	var store, err = consumer.NewJSONFileStore(rec, dir, state)
	if err != nil {
		return nil, err
	}
	// Ensure acknowledgements of the last committed transaction are written
	// to respective tracked journals. This also informs readers that they
	// should roll back events of a larger SeqNo.
	if err = c.FinishTxn(shard, store, nil); err != nil {
		return nil, err
	}
	// Run BeginTxn to cause the Shard to fail immediately if the `mvTag` label
	// or view are mis-configured. Without this, the Shard would still fail, but
	// only after attempting to process an input message. BeginTxn is otherwise
	// a no-op.
	return store, c.BeginTxn(shard, store)
}

func (c *Extractor) NewMessage(spec *pb.JournalSpec) (message.Message, error) {
	return c.Extractors.NewMessage(spec)
}

func (c *Extractor) BeginTxn(shard consumer.Shard, store consumer.Store) error {
	var state = store.(*consumer.JSONFileStore).State.(*shardState)
	state.schema = c.schemaFn() // Used through life of the current transaction.

	// Map the `mvTag` label to a MaterializedViewSpec. Having verified that spec
	// is present, we're then assured that all entities *referenced* by it are
	// also present and consistent.
	if l := shard.Spec().LabelSet.ValuesOf("mvTag"); len(l) != 1 {
		return errors.Errorf(`expected single "mvTag" label (%v)`, l)
	} else if tag, err := strconv.ParseInt(l[0], 10, 64); err != nil {
		return errors.WithMessagef(err, `parsing "mvTag" label value`)
	} else if mv, ok := c.schemaFn().Views[factable.MVTag(tag)]; !ok {
		return errors.Wrapf(ErrViewNotFound, "mvTag %d", tag)
	} else {
		state.mvTag = mv.Tag
	}

	return nil
}

func (*Extractor) ConsumeMessage(shard consumer.Shard, store consumer.Store, envelope message.Envelope) error {
	var (
		state   = store.(*consumer.JSONFileStore).State.(*shardState)
		mvSpec  = state.schema.Views[state.mvTag]
		relSpec = state.schema.Relations[mvSpec.View.RelTag]
	)
	for _, record := range state.schema.Extract.Mapping[relSpec.Mapping](envelope) {
		var (
			tmp [256]byte // Does not escape.
			key = encoding.EncodeVarintAscending(tmp[:0], int64(mvSpec.Tag))
		)
		for _, tag := range mvSpec.View.Dimensions {
			key = state.schema.ExtractAndMarshalDimension(key, tag, record)
		}

		var aggs, ok = state.pending[string(key)]
		if !ok {
			// Initialize Aggregates.
			aggs = make([]factable.Aggregate, len(mvSpec.View.Metrics))
			for m, tag := range mvSpec.View.Metrics {
				aggs[m] = state.schema.InitMetric(tag, aggs[m])
			}
			state.pending[string(key)] = aggs // Track for remainder of txn.
		}
		// Fold the |record| into |aggs|.
		for m, tag := range mvSpec.View.Metrics {
			state.schema.FoldMetric(tag, aggs[m], record)
		}
	}
	return nil
}

func (c *Extractor) FinalizeTxn(shard consumer.Shard, store consumer.Store) error {
	var (
		state  = store.(*consumer.JSONFileStore).State.(*shardState)
		mvSpec = state.schema.Views[state.mvTag]
		delta  = &DeltaEvent{Extractor: shard.Spec().Id.String()}
	)
	for key, aggs := range state.pending {
		// Load |SeqNo|, |key| and |aggs| into |delta| in preparation for marshalling.
		delta.SeqNo = state.SeqNo
		delta.RowKey = append(delta.RowKey[:0], key...)
		delta.RowValue = delta.RowValue[:0]

		for m, tag := range mvSpec.View.Metrics {
			delta.RowValue = state.schema.MarshalMetric(delta.RowValue, tag, aggs[m])
		}
		delete(state.pending, key)

		// Map and publish the DeltaEvent. This parallels `message.Publish`,
		// except we retain the mapped journal and framing for future use.
		var journal, framing, err = c.deltas(delta)
		if err != nil {
			return err
		}
		var aa = shard.JournalClient().StartAppend(journal)
		if err = aa.Require(framing.Marshal(delta, aa.Writer())).Release(); err != nil {
			return err
		}
		// Track that |journal| was published to as part of this transaction.
		state.TransactionJournals[journal] = framing.ContentType()
		state.SeqNo++ // Tick for next row DeltaEvent.
	}
	return nil
}

func (*Extractor) FinishTxn(shard consumer.Shard, store consumer.Store, err error) error {
	if err != nil {
		return nil // Don't write commit acknowledgments if the transaction failed.
	}
	var (
		state     = store.(*consumer.JSONFileStore).State.(*shardState)
		barrier   = store.Recorder().WeakBarrier()
		commitAck = &DeltaEvent{
			Extractor: shard.Spec().Id.String(),
			SeqNo:     state.SeqNo - 1, // Acknowledge last row DeltaEvent SeqNo.
		}
	)
	// 2PC: When |barrier| resolves, our transaction has committed. For each
	// Journal of the transaction, queue a "commit acknowledgement" message
	// which informs readers that all messages previously read under the
	// transaction ID may now be applied.
	for journal, contentType := range state.TransactionJournals {
		var framing, err = message.FramingByContentType(contentType)
		if err != nil {
			return errors.Wrapf(err, "journal %s", journal)
		}
		var aa = shard.JournalClient().StartAppend(journal, barrier)
		if err = aa.Require(framing.Marshal(commitAck, aa.Writer())).Release(); err != nil {
			return err
		}
		delete(state.TransactionJournals, journal) // Clear for next txn.
	}
	return nil
}
