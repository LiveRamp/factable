package backfill

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"os"

	"github.com/LiveRamp/factable/pkg/factable"
	"github.com/LiveRamp/factable/pkg/internal"
	"go.gazette.dev/core/mainboilerplate"
	"go.gazette.dev/core/labels"
	mbp "go.gazette.dev/core/mainboilerplate"
	"go.gazette.dev/core/message"
	"github.com/cockroachdb/cockroach/util/encoding"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

type cmdMap struct {
	Log        mbp.LogConfig `group:"Logging" namespace:"log" env-namespace:"LOG"`
	Spec       string        `long:"spec" required:"true" description:"Local path to backfill job specification"`
	MaxPending int           `long:"max-pending" default:"8192" description:"Maximum number of aggregates to accumulate in-memory before flushing"`

	extractFns factable.ExtractFns
}

func (cmd *cmdMap) Execute(args []string) error {
	mbp.InitLog(cmd.Log)

	var spec JobSpec
	{
		specIn, err := os.Open(cmd.Spec)
		mbp.Must(err, "failed to open backfill specification")
		mbp.Must(json.NewDecoder(specIn).Decode(&spec), "failed to decode backfill specification")
	}

	var schema, err = factable.NewSchema(&cmd.extractFns, spec.SchemaSpec)
	mbp.Must(err, "failed to build factable Schema")

	var task = MapTaskSpec{JobSpec: &spec}
	var dec = json.NewDecoder(os.Stdin)
	var bw = bufio.NewWriter(os.Stdout)

	for {
		if err = dec.Decode(&task); err == io.EOF {
			return nil // All done.
		} else if err != nil {
			return err
		}

		log.WithFields(log.Fields{
			"frag":  task.Fragment,
			"views": task.MVTags,
		}).Info("processing")

		var msgCh = make(chan message.Envelope, 1024)

		go func() {
			mbp.Must(decode(task, schema, msgCh), "failed to decode messages")
			close(msgCh)
		}()

		mbp.Must(extract(msgCh, schema, task, cmd.MaxPending, bw), "failed to extract messages")
	}
}

func decode(mt MapTaskSpec, schema factable.Schema, out chan<- message.Envelope) error {
	var journal, ok = mt.Inputs[mt.Fragment.Journal]
	if !ok {
		return errors.Errorf("fragment %v journal not in job Inputs", mt.Fragment)
	}
	var framing, err = message.FramingByContentType(journal.LabelSet.ValueOf(labels.ContentType))
	if err != nil {
		return err
	}
	fr, err := client.OpenFragmentURL(context.Background(), mt.Fragment, mt.Fragment.Begin, mt.URL)
	if err != nil {
		return errors.WithMessage(err, "OpenFragmentURL")
	}

	var br = bufio.NewReaderSize(fr, 32*1024)
	for {
		if b, err := framing.Unpack(br); errors.Cause(err) == io.EOF {
			return nil
		} else if err != nil {
			return errors.WithMessage(err, "unpacking message")
		} else if msg, err := schema.Extract.NewMessage(&journal); err != nil {
			return errors.WithMessage(err, "newMsg")
		} else if err = framing.Unmarshal(b, msg); err != nil {
			log.WithField("err", err).Warn("failed to unmarshal message")
		} else {
			out <- message.Envelope{
				Message:     msg,
				Fragment:    &fr.Fragment,
				JournalSpec: &journal,
				NextOffset:  fr.Offset - int64(br.Buffered()),
			}
		}
	}
}

func extract(in <-chan message.Envelope, schema factable.Schema, mt MapTaskSpec, maxPending int, out *bufio.Writer) error {
	// Flatten views and mappings into slices.
	var views []factable.MaterializedViewSpec
	var mappings []func(message.Envelope) []factable.RelationRow

	for _, tag := range mt.MVTags {
		var viewSpec = schema.Views[tag]
		var relSpec = schema.Relations[viewSpec.RelTag]
		views = append(views, viewSpec)
		mappings = append(mappings, schema.Extract.Mapping[relSpec.MapTag])
	}

	// Row-keys and Aggregates which have been folded over and which remain to be flushed.
	var pending = make(map[string][]factable.Aggregate)

	for envelope := range in {
		for i, mvSpec := range views {
			for _, record := range mappings[i](envelope) {
				// Flush |pending| if it's already too large.
				if len(pending) >= maxPending {
					if err := flushPending(pending, schema, out); err != nil {
						return err
					}
				}
				// Encode row-key.
				var (
					tmp [256]byte // Does not escape.
					key = encoding.EncodeVarintAscending(tmp[:0], int64(mvSpec.Tag))
				)
				key = schema.ExtractAndMarshalDimensions(key, mvSpec.ResolvedView.DimTags, record)

				// Does row-key already have an in-memory aggregate?
				var aggs, ok = pending[string(key)]
				if !ok {
					// Miss. Initialize empty Aggregates tracked under the key.
					aggs = make([]factable.Aggregate, len(mvSpec.View.Metrics))
					schema.InitAggregates(mvSpec.ResolvedView.MetTags, aggs)

					pending[string(key)] = aggs
				}
				// Fold the |record| into |aggs|.
				schema.FoldMetrics(mvSpec.ResolvedView.MetTags, aggs, record)
			}
		}
	}
	return flushPending(pending, schema, out)
}

// flushPending writes encoded keys & aggregates of |pending| to |out|, clearing all entries of |pending|.
func flushPending(pending map[string][]factable.Aggregate, schema factable.Schema, out *bufio.Writer) error {
	var enc = hexAggsEncoder{schema: schema, he: *factable.NewHexEncoder(out)}

	for key, aggs := range pending {
		if err := enc.encode([]byte(key), aggs); err != nil {
			return err
		}
		delete(pending, key)
	}
	return out.Flush()
}

type hexAggsEncoder struct {
	schema factable.Schema
	he     factable.HexEncoder
	tmp    []byte
}

func (hae *hexAggsEncoder) encode(key []byte, aggs []factable.Aggregate) error {
	var mv, err = internal.ViewSpecOfRow(key, &hae.schema)
	if err != nil {
		return err
	}
	hae.tmp = hae.schema.MarshalMetrics(hae.tmp[:0], mv.ResolvedView.MetTags, aggs)

	if err = hae.he.Encode(key, hae.tmp); err != nil {
		return err
	}
	return nil
}
