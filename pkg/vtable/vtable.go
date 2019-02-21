package vtable

import (
	"git.liveramp.net/jgraet/factable/pkg/factable"
	. "git.liveramp.net/jgraet/factable/pkg/internal"
	"github.com/LiveRamp/gazette/v2/pkg/consumer"
	"github.com/LiveRamp/gazette/v2/pkg/mainboilerplate/runconsumer"
	"github.com/LiveRamp/gazette/v2/pkg/message"
	pb "github.com/LiveRamp/gazette/v2/pkg/protocol"
	"github.com/LiveRamp/gazette/v2/pkg/recoverylog"
	"github.com/cockroachdb/cockroach/util/encoding"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

// VTable is a runconsumer.Application which aggregates DeltaEvents into indexed
// relation rows, and provides gRPC query APIs against those stored relations.
type VTable struct {
	schemaFn  func() *factable.Schema
	arenaSize int
	maxArenas int
	svc       *consumer.Service
}

// Config utilized by VTable.
type Config struct {
	Factable struct {
		CommonConfig

		ArenaSize int `long:"arenaSize" default:"16777216" description:"Byte size of arena buffers used for on-demand, query-time sorts"`
		MaxArenas int `long:"maxArenas" default:"8" description:"Maximum number of arenas an on-demand query may consume before it errors"`
	} `group:"Factable" namespace:"factable"`

	runconsumer.BaseConfig
}

// NewConfig returns a new Spec.
func (*VTable) NewConfig() runconsumer.Config { return new(Config) }

// InitApplication initializes the VTable.
func (t *VTable) InitApplication(args runconsumer.InitArgs) error {
	var cfg = args.Config.(*Config)

	if err := cfg.Factable.Validate(); err != nil {
		return err
	} else if cfg.Factable.ArenaSize < 1024 {
		return errors.New("ArenaSize must be >= 1024 bytes")
	} else if cfg.Factable.MaxArenas < 2 {
		return errors.New("ArenaSize must be >= 2")
	}

	// Fetch and watch the shared SchemaSpec from Etcd.
	var ks = NewSchemaKeySpace(cfg.Factable.SchemaKey, nil)

	if err := ks.Load(args.Context, args.Service.Etcd, 0); err != nil {
		return errors.WithMessagef(err, "loading schema KeySpace (%s)", ks.Root)
	}
	var ss = &SchemaService{
		Config:     cfg.Factable.CommonConfig,
		KS:         ks,
		Etcd:       args.Service.Etcd,
		ExtractFns: nil,
	}
	go func() {
		if err := ks.Watch(args.Context, args.Service.Etcd); err != nil {
			log.WithField("err", err).Error("schema KeySpace watch failed")
		}
	}()

	t.schemaFn = ss.Schema
	t.arenaSize = cfg.Factable.ArenaSize
	t.maxArenas = cfg.Factable.MaxArenas
	t.svc = args.Service

	factable.RegisterSchemaServer(args.Server.GRPCServer, ss)
	factable.RegisterQueryServer(args.Server.GRPCServer, t)

	return nil
}

type shardState struct {
	// Schema being used for the current consumer transaction.
	schema *factable.Schema
	// Transaction states.
	txns Transactions
	// Updated rows & values of this consumer transaction.
	updates map[string][]factable.Aggregate
}

func (t *VTable) NewStore(shard consumer.Shard, dir string, rec *recoverylog.Recorder) (consumer.Store, error) {
	var rdb = consumer.NewRocksDBStore(rec, dir)

	// TODO(johnny): tuning.

	if err := rdb.Open(); err != nil {
		return rdb, err
	}
	// Restore combiner.Transactions persisted to the DB.
	dbVal, err := rdb.DB.Get(rdb.ReadOptions, transactionStatesKey)
	if err != nil {
		return rdb, errors.Wrapf(err, "reading %q", transactionStatesKey)
	}
	defer dbVal.Free()

	var txns = Transactions{
		Extractor: make(map[string]Transactions_State),
	}
	if err = txns.Unmarshal(dbVal.Data()); err != nil {
		return rdb, errors.Wrapf(err, "unmarshal %q", transactionStatesKey)
	}
	rdb.Cache = &shardState{
		txns:    txns,
		updates: make(map[string][]factable.Aggregate),
		schema:  t.schemaFn(),
	}
	return rdb, nil
}

func (*VTable) NewMessage(spec *pb.JournalSpec) (message.Message, error) { return new(DeltaEvent), nil }

func (t *VTable) BeginTxn(shard consumer.Shard, store consumer.Store) error {
	var state = store.(*consumer.RocksDBStore).Cache.(*shardState)
	state.schema = t.schemaFn() // Used through life of the current transaction.
	return nil
}

func (t *VTable) ConsumeMessage(shard consumer.Shard, store consumer.Store, envelope message.Envelope) error {
	var (
		rdb   = store.(*consumer.RocksDBStore)
		cache = rdb.Cache.(*shardState)
		event = envelope.Message.(*DeltaEvent)
	)
	var committed, err = cache.txns.Apply(*event)

	// Apply returns errors on invalid transaction state transitions, which can
	// occur due to duplicated writes into the Journal. Warn but otherwise
	// ignore these.
	if err != nil {
		log.WithFields(log.Fields{
			"err":       err,
			"extractor": event.Extractor,
		}).Warn("DeltaEvent did not apply")
	}
	for _, de := range committed {
		if err = accumulate(cache, rdb, de); err != nil {
			return errors.WithMessage(err, "accumulate")
		}
	}
	return nil
}

func (*VTable) FinalizeTxn(shard consumer.Shard, store consumer.Store) error {
	var (
		rdb   = store.(*consumer.RocksDBStore)
		cache = rdb.Cache.(*shardState)
		wb    = rdb.WriteBatch
	)
	// Persist combiner transaction states.
	var buf, err = cache.txns.Marshal()
	if err != nil {
		return err
	}
	wb.Put(transactionStatesKey, buf)

	// Persist relation rows updated during this transaction.
	var value = buf[:0] // Re-use.
	for keyStr, aggs := range cache.updates {
		var (
			key    = []byte(keyStr)
			mvSpec = MustViewSpecOfRow(key, cache.schema)
		)

		value = cache.schema.MarshalMetrics(value[:0], mvSpec.ResolvedView.MetTags, aggs)

		wb.Put(key, value)
		delete(cache.updates, keyStr)
	}
	return nil
}

func (*VTable) FinishTxn(consumer.Shard, consumer.Store, error) error { return nil /* No-op */ }

func accumulate(cache *shardState, rdb *consumer.RocksDBStore, delta DeltaEvent) error {
	var mvSpec, err = delta.ViewSpec(cache.schema)
	if errors.Cause(err) == ErrViewNotFound {
		return nil // View is no longer configured. Drop on the floor.
	} else if err != nil {
		return err
	}

	var aggs, ok = cache.updates[string(delta.RowKey)]
	if !ok {
		// Initialize Aggregates.
		aggs = make([]factable.Aggregate, len(mvSpec.ResolvedView.MetTags))
		cache.schema.InitAggregates(mvSpec.ResolvedView.MetTags, aggs)

		cache.updates[string(delta.RowKey)] = aggs // Track for remainder of txn.

		// Attempt to fill from database.
		var dbVal, err = rdb.DB.Get(rdb.ReadOptions, delta.RowKey)
		if err != nil {
			return err
		}

		if dbVal.Size() == 0 {
			// Database missed.
		} else {
			if _, err = cache.schema.ReduceMetrics(dbVal.Data(), mvSpec.ResolvedView.MetTags, aggs); err != nil {
				return err
			}
		}
		dbVal.Free()
	}

	if _, err = cache.schema.ReduceMetrics(delta.RowValue, mvSpec.ResolvedView.MetTags, aggs); err != nil {
		return err
	}
	return nil
}

var transactionStatesKey = encoding.EncodeStringAscending(
	encoding.EncodeNullAscending(nil), "_transactionStates")
