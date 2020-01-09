package backfill

import (
	"bufio"
	"bytes"
	"encoding/json"
	"os"

	"github.com/LiveRamp/factable/pkg/factable"
	"github.com/LiveRamp/factable/pkg/internal"
	mbp "go.gazette.dev/core/mainboilerplate"
	"github.com/pkg/errors"
)

type cmdCombine struct {
	Log  mbp.LogConfig `group:"Logging" namespace:"log" env-namespace:"LOG"`
	Spec string        `long:"spec" default:"/path/to/backfill/job.spec" description:"Local path to backfill job specification"`
}

func (cmd *cmdCombine) Execute(args []string) error {
	mbp.InitLog(cmd.Log)

	var spec JobSpec
	{
		specIn, err := os.Open(cmd.Spec)
		mbp.Must(err, "failed to open backfill specification")
		mbp.Must(json.NewDecoder(specIn).Decode(&spec), "failed to decode backfill specification")
	}

	var schema, err = factable.NewSchema(nil, spec.SchemaSpec)
	mbp.Must(err, "failed to build factable Schema")

	var br = bufio.NewReaderSize(os.Stdin, 32*1024)
	var bw = bufio.NewWriter(os.Stdout)

	return combine(br, schema, bw)
}

func combine(in *bufio.Reader, schema factable.Schema, out *bufio.Writer) error {
	var (
		it  = factable.NewHexIterator(in)
		enc = hexAggsEncoder{schema: schema, he: *factable.NewHexEncoder(out)}

		// Current key being combined over, along with its partial aggregates & view.
		key  []byte
		aggs []factable.Aggregate
		mv   factable.MaterializedViewSpec
	)
	for {
		var nextKey, value, err = it.Next()

		if err == factable.KVIteratorDone {
			break
		} else if err != nil {
			return err
		}

		if cmp := bytes.Compare(key, nextKey); cmp == -1 {
			// The key we're combining over is changing.

			// Flush the now completed |key|.
			if key != nil {
				if err = enc.encode(key, aggs); err != nil {
					return err
				}
			}

			// Reset for |nextKey|.
			key = append(key[:0], nextKey...)

			// Reset |aggs|, (re)allocating it if the view itself has changed.
			if nextMV, err := internal.ViewSpecOfRow(key, &schema); err != nil {
				return err
			} else if nextMV.Tag != mv.Tag {
				mv = nextMV
				aggs = make([]factable.Aggregate, len(mv.View.Metrics))
			}
			schema.InitAggregates(mv.ResolvedView.MetTags, aggs)
		} else if cmp == 1 {
			return errors.Errorf("invalid key order (key %q vs nextKey %q)", key, nextKey)
		}

		if _, err = schema.ReduceMetrics(value, mv.ResolvedView.MetTags, aggs); err != nil {
			return err
		}
	}

	if err := enc.encode(key, aggs); err != nil {
		return err
	}
	return out.Flush()
}
