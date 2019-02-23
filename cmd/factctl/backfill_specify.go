package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"time"

	"git.liveramp.net/jgraet/factable/pkg/backfill"
	"git.liveramp.net/jgraet/factable/pkg/factable"
	"github.com/LiveRamp/gazette/v2/pkg/client"
	"github.com/LiveRamp/gazette/v2/pkg/consumer"
	mbp "github.com/LiveRamp/gazette/v2/pkg/mainboilerplate"
	pb "github.com/LiveRamp/gazette/v2/pkg/protocol"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

type cmdBackfillSpecify struct {
	cfg *BaseCfg

	Name       string    `long:"name" required:"true" description:"Name of the backfill to write specifications for."`
	MinModTime time.Time `long:"min-mod-time" description:"Minimum mod time of Fragments to process. Fragments with earlier mod times are ignored."`
}

func (cmd *cmdBackfillSpecify) Execute([]string) error {
	mbp.InitLog(cmd.cfg.Log)
	pb.RegisterGRPCDispatcher("")

	var (
		ctx     = pb.WithDispatchDefault(context.Background())
		rjc     = cmd.cfg.Broker.RoutedJournalClient(ctx)
		extConn = cmd.cfg.Extractor.Dial(ctx)
	)

	// Fetch the current Schema.
	schemaSpec, err := factable.NewSchemaClient(extConn).GetSchema(ctx, new(empty.Empty))
	mbp.Must(err, "failed to fetch current schema")

	// Fetch Extractor Shards in need of backfill.
	shards, err := consumer.ListShards(ctx, consumer.NewShardClient(extConn), &consumer.ListRequest{
		Selector: pb.LabelSelector{Include: pb.MustLabelSet(backfillLabel, cmd.Name)}})
	mbp.Must(err, "failed to fetch shards requiring backfill")

	if len(shards.Shards) == 0 {
		return errors.Errorf("found no shards matching backfill name %s", cmd.Name)
	}

	// Index Shards on the journals they're reading, and the offsets to read through of each journal.
	var fillOffsets = make(map[pb.Journal]map[factable.MVTag]int64)

	for _, shard := range shards.Shards {
		var tag, err = strconv.ParseInt(shard.Spec.LabelSet.ValueOf("mvTag"), 10, 64)
		mbp.Must(err, "failed to parse view label", "shard", shard.Spec.Id)

		for _, src := range shard.Spec.Sources {
			if _, ok := fillOffsets[src.Journal]; !ok {
				fillOffsets[src.Journal] = make(map[factable.MVTag]int64)
			}
			fillOffsets[src.Journal][factable.MVTag(tag)] = src.MinOffset
		}
	}

	// Fetch fragments of each journal to be backfilled.
	// These will constitute our map tasks.

	mapTasksOut, err := os.Create(cmd.Name + ".tasks")
	mbp.Must(err, "failed to create MapTasksPath")

	var (
		mapTaskEnc = json.NewEncoder(mapTasksOut)
		urlTTL     = 7 * 24 * time.Hour // One week.
		inputNames pb.LabelSelector
	)
	for journal, offsets := range fillOffsets {
		var req = pb.FragmentsRequest{
			Journal:      journal,
			SignatureTTL: &urlTTL,
		}
		if !cmd.MinModTime.IsZero() {
			req.BeginModTime = cmd.MinModTime.Unix()
		}

		var frags, err = client.ListAllFragments(ctx, rjc, req)
		mbp.Must(err, "failed to fetch fragments", "journal", journal)

		for _, frag := range frags.Fragments {
			var task = &backfill.MapTaskSpec{
				Fragment: frag.Spec,
				URL:      frag.SignedUrl,
			}
			for tag, maxOffset := range offsets {
				if task.Fragment.End <= maxOffset {
					task.MVTags = append(task.MVTags, tag)
				}
			}
			if len(task.MVTags) != 0 {
				mbp.Must(mapTaskEnc.Encode(task), "failed to encode MapTaskSpec")
			}
		}

		inputNames.Include.AddValue("name", journal.String())
	}
	mbp.Must(mapTasksOut.Close(), "failed to close")

	// Fetch specs for all journals being read as backfill inputs.
	// Index under |fillSpec| Inputs.
	listResp, err := client.ListAllJournals(ctx, rjc, pb.ListRequest{Selector: inputNames})
	mbp.Must(err, "failed to list input journals")

	var fillSpec = &backfill.JobSpec{
		SchemaSpec: schemaSpec.Spec,
		Inputs:     make(map[pb.Journal]pb.JournalSpec),
	}
	for _, journal := range listResp.Journals {
		fillSpec.Inputs[journal.Spec.Name] = journal.Spec
	}

	jobSpecOut, err := os.Create(cmd.Name + ".spec")
	mbp.Must(err, "failed to create JobSpecPath")

	var enc = json.NewEncoder(jobSpecOut)
	enc.SetIndent(" ", " ")
	mbp.Must(enc.Encode(&fillSpec), "failed to encode spec")
	mbp.Must(jobSpecOut.Close(), "failed to close")

	log.WithFields(log.Fields{
		"spec":  jobSpecOut.Name(),
		"tasks": mapTasksOut.Name(),
	}).Info("generated backfill specification")

	fmt.Printf(`Test your backfill job specification with:

head --lines=1 %v \
	| my-backfill-binary map --spec %v \
	| sort --stable --key=1,1 \
	| my-backfill-binary combine --spec %v
`, mapTasksOut.Name(), jobSpecOut.Name(), jobSpecOut.Name())

	return nil
}

const backfillLabel = "app.factable.dev/backfill"
