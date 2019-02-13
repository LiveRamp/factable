package main

import (
	"bytes"
	"context"
	"crypto/sha1"
	"fmt"
	"io"
	"math/rand"
	"time"

	"git.liveramp.net/jgraet/factable/pkg/factable"
	"github.com/LiveRamp/gazette/v2/cmd/gazctl/editor"
	"github.com/LiveRamp/gazette/v2/pkg/client"
	"github.com/LiveRamp/gazette/v2/pkg/consumer"
	"github.com/LiveRamp/gazette/v2/pkg/consumer/shardspace"
	"github.com/LiveRamp/gazette/v2/pkg/labels"
	mbp "github.com/LiveRamp/gazette/v2/pkg/mainboilerplate"
	pb "github.com/LiveRamp/gazette/v2/pkg/protocol"
	"github.com/LiveRamp/gazette/v2/pkg/protocol/journalspace"
	"github.com/dustinkirkland/golang-petname"
	"github.com/golang/protobuf/ptypes/empty"
	log "github.com/sirupsen/logrus"
	"gopkg.in/yaml.v2"
)

type cmdSync struct {
	cfg *BaseCfg
}

func (cmd *cmdSync) Execute([]string) error {
	mbp.InitLog(cmd.cfg.Log)
	pb.RegisterGRPCDispatcher("")

	var (
		ctx     = pb.WithDispatchDefault(context.Background())
		rjc     = cmd.cfg.Broker.RoutedJournalClient(ctx)
		extConn = cmd.cfg.Extractor.Dial(ctx)
		vtConn  = cmd.cfg.VTable.Dial(ctx)
	)

	// Fetch the current Schema.
	schemaResp, err := factable.NewSchemaClient(extConn).GetSchema(ctx, new(empty.Empty))
	mbp.Must(err, "failed to fetch current schema")
	schema, err := factable.NewSchema(nil, schemaResp.Spec)
	mbp.Must(err, "failed to build Schema from SchemaSpec")

	// Instance of the Factable release. We identify and label generated Shards
	// and recovery logs Journals on the specific release instance name, which
	// allows multiple Factable/ deployments to trivially co-exist in a single
	// broker cluster and/or shared Etcd.
	var releaseInstance = schemaResp.Instance

	// Fetch input Journals matched by RelationSpec selectors, indexed on RelTag.
	var relInputs = make(map[factable.RelTag]*pb.ListResponse)
	for tag, relSpec := range schema.Relations {
		relInputs[tag], err = client.ListAllJournals(ctx, rjc, pb.ListRequest{Selector: relSpec.Selector})
		mbp.Must(err, "failed to list journals", "selector", relSpec.Selector)
	}

	// Fetch current Extractor Shards, and build a shardspace.
	shardsResp, err := consumer.ListShards(ctx, consumer.NewShardClient(extConn), &consumer.ListRequest{})
	mbp.Must(err, "failed to fetch current shards")

	var extShards = shardspace.FromListResponse(shardsResp)
	if len(extShards.Shards) == 0 {
		// Populate some reasonable defaults, which the user can tweak.
		extShards.Common = consumer.ShardSpec{
			RecoveryLogPrefix: "examples/factable/recovery/extractor",
			HintPrefix:        "/gazette/hints/factable/extractor",
			MaxTxnDuration:    time.Minute,
			MinTxnDuration:    time.Second * 10,
		}
	}

	// Generate a backfill name, which created shards will be labeled with.
	rand.Seed(time.Now().UnixNano())
	var backFillName = petname.Generate(2, "-")

	// Walk each input journal of each view, ensuring a shard exists for each.
	for _, view := range schema.Views {
		for _, journal := range relInputs[view.View.RelTag].Journals {
			var (
				mvStr = fmt.Sprintf("%d", view.Tag)
				sum   = sha1.Sum([]byte(releaseInstance + ":" + mvStr + ":" + journal.Spec.Name.String()))
			)
			// Patch in the expected Shard, creating if it doesn't yet exist.
			var shard = extShards.Patch(shardspace.Shard{
				Spec: consumer.ShardSpec{
					Id: consumer.ShardID(fmt.Sprintf("%x", sum[:])[:24]),
					LabelSet: pb.MustLabelSet(
						labels.Instance, releaseInstance,
						labels.ManagedBy, "factable",
						labels.Tag, "extractor",
						"mvName", view.Name,
						"mvTag", mvStr,
					),
				},
			})

			if shard.Revision == 0 {
				// This Shard is new. Fetch a recent offset from which to begin
				// streaming. Historical |journal| content should be back-filled.
				shard.Spec.Sources = []consumer.ShardSpec_Source{{
					Journal:   journal.Spec.Name,
					MinOffset: determineMinOffset(ctx, rjc, journal.Spec.Name),
				}}
				shard.Spec.LabelSet.SetValue(backfillLabel, backFillName)

				log.WithFields(log.Fields{
					"view":     view.Name,
					"journal":  journal.Spec.Name,
					"id":       shard.Spec.Id,
					"backfill": backFillName,
				}).Info("shard created")
			}
		}
	}
	// Mark shards we *didn't* visit as deleted (we no longer need them).
	extShards.MarkUnpatchedForDeletion()
	extShards.PushDown()

	// Give the user an opportunity to tweak shard configuration.
	var extShardChanges = editShardSpace(&extShards)

	// Fetch current VTable Shards, and build a Set.
	shardsResp, err = consumer.ListShards(ctx, consumer.NewShardClient(vtConn), &consumer.ListRequest{})
	mbp.Must(err, "failed to fetch current shards")

	var vtShards = shardspace.FromListResponse(shardsResp)
	if len(vtShards.Shards) == 0 {
		// Populate some reasonable defaults, which the user can tweak.
		vtShards.Common = consumer.ShardSpec{
			RecoveryLogPrefix: "examples/factable/recovery/vtable",
			HintPrefix:        "/gazette/hints/factable/vtable",
			MaxTxnDuration:    time.Minute,
			MinTxnDuration:    time.Second * 10,
		}
	}

	// Fetch the set of DeltaEvent partitions.
	partsResp, err := client.ListAllJournals(ctx, rjc, pb.ListRequest{Selector: schemaResp.DeltaPartitions})
	mbp.Must(err, "failed to fetch DeltaEvent partitions")

	// For each DeltaEvent partition, ensure a VTable Shard exists.
	for _, part := range partsResp.Journals {
		var sum = sha1.Sum([]byte(releaseInstance + ":" + part.Spec.Name.String()))

		_ = vtShards.Patch(shardspace.Shard{
			Spec: consumer.ShardSpec{
				Id:      consumer.ShardID(fmt.Sprintf("%x", sum[:])[:24]),
				Sources: []consumer.ShardSpec_Source{{Journal: part.Spec.Name}},
				LabelSet: pb.MustLabelSet(
					labels.Instance, releaseInstance,
					labels.ManagedBy, "factable",
					labels.Tag, "vtable",
				),
			},
		})
	}
	// Mark shards we *didn't* visit as deleted (we no longer need them).
	vtShards.MarkUnpatchedForDeletion()
	vtShards.PushDown()

	// Give the user an opportunity to tweak shard configuration.
	var vtShardChanges = editShardSpace(&vtShards)

	// Fetch all journals managed by factable, and build a journalspace.
	journalsResp, err := client.ListAllJournals(ctx, rjc, pb.ListRequest{
		Selector: pb.LabelSelector{
			Include: pb.MustLabelSet(
				labels.Instance, releaseInstance,
				labels.ManagedBy, "factable",
			),
		},
	})
	mbp.Must(err, "failed to fetch factable journals")

	var journalTree = journalspace.FromListResponse(journalsResp)
	if len(journalTree.Children) == 0 {
		// Populate some reasonable defaults, which the user can tweak.
		journalTree.Spec = pb.JournalSpec{
			Replication: 3,
			Fragment: pb.JournalSpec_Fragment{
				Length:           1 << 30, // 1GB.
				CompressionCodec: pb.CompressionCodec_SNAPPY,
				Stores:           []pb.FragmentStore{"replace-with-fragment-store"},
				RefreshInterval:  time.Minute * 5,
			},
		}
	}
	// Walk all implied recovery logs, ensuring a JournalSpec exists for each.
	for _, set := range []shardspace.Set{extShards, vtShards} {
		for _, shard := range set.Shards {
			if shard.Delete != nil && *shard.Delete {
				continue
			}
			_ = journalTree.Patch(journalspace.Node{
				Spec: pb.JournalSpec{
					Name: shard.Spec.RecoveryLog(),
					LabelSet: pb.MustLabelSet(
						labels.Instance, releaseInstance,
						labels.ManagedBy, "factable",
						labels.ContentType, labels.ContentType_RecoveryLog,
					),
				},
			})
		}
	}
	// Mark journals we *didn't* visit for deletion (we no longer need them).
	journalTree.MarkUnpatchedForDeletion()
	journalTree.PushDown()

	// Give the user an opportunity to tweak journal configuration.
	var journalChanges = editJournalSpace(&journalTree)

	// Apply all updated specs.
	_, err = client.ApplyJournals(ctx, rjc, &pb.ApplyRequest{Changes: journalChanges})
	mbp.Must(err, "failed to apply journals")
	_, err = consumer.ApplyShards(ctx, consumer.NewShardClient(vtConn), &consumer.ApplyRequest{Changes: vtShardChanges})
	mbp.Must(err, "failed to apply vtable shards")
	_, err = consumer.ApplyShards(ctx, consumer.NewShardClient(extConn), &consumer.ApplyRequest{Changes: extShardChanges})
	mbp.Must(err, "failed to apply extractor shards")

	return nil
}

func editShardSpace(set *shardspace.Set) []consumer.ApplyRequest_Change {
	var changes []consumer.ApplyRequest_Change

	mbp.Must(editor.EditRetryLoop(editor.RetryLoopArgs{
		FilePrefix: "factctl-shards-",
		SelectFn: func() io.Reader {
			set.Hoist()

			var buf = new(bytes.Buffer)
			mbp.Must(yaml.NewEncoder(buf).Encode(set), "failed to encode shardspace")
			return buf
		},
		ApplyFn: func(b []byte) error {
			*set = shardspace.Set{}
			changes = changes[:0]

			if err := yaml.UnmarshalStrict(b, set); err != nil {
				return err
			}
			set.PushDown()

			// Expect all ShardSpecs validate.
			for i, s := range set.Shards {
				var change = consumer.ApplyRequest_Change{ExpectModRevision: s.Revision}

				if s.Delete != nil && *s.Delete == true {
					change.Delete = s.Spec.Id
				} else if err := s.Spec.Validate(); err != nil {
					return err
				} else {
					change.Upsert = &set.Shards[i].Spec // Note |s| is overwritten each iteration.
				}
				changes = append(changes, change)
			}
			return nil
		},
		AbortIfUnchanged: false,
	}), "failed to edit shards")

	return changes
}

func editJournalSpace(tree *journalspace.Node) []pb.ApplyRequest_Change {
	var changes []pb.ApplyRequest_Change

	mbp.Must(editor.EditRetryLoop(editor.RetryLoopArgs{
		FilePrefix: "factctl-journals-",
		SelectFn: func() io.Reader {
			tree.Hoist()

			var buf = new(bytes.Buffer)
			mbp.Must(yaml.NewEncoder(buf).Encode(tree), "failed to encode journalspace")
			return buf
		},
		ApplyFn: func(b []byte) error {
			*tree = journalspace.Node{}
			changes = changes[:0]

			if err := yaml.UnmarshalStrict(b, &tree); err != nil {
				return err
			}
			tree.PushDown()

			// Expect that all journals validate.
			return tree.WalkTerminalNodes(func(node *journalspace.Node) error {
				var change = pb.ApplyRequest_Change{ExpectModRevision: node.Revision}

				if node.Delete != nil && *node.Delete {
					change.Delete = node.Spec.Name
				} else if err := node.Spec.Validate(); err != nil {
					return err
				} else {
					change.Upsert = &node.Spec
				}
				changes = append(changes, change)
				return nil
			})
		},
		AbortIfUnchanged: false,
	}), "failed to edit journals")

	return changes
}

func determineMinOffset(ctx context.Context, rjc pb.RoutedJournalClient, journal pb.Journal) int64 {
	var (
		req   = &pb.FragmentsRequest{Journal: journal}
		resp  *pb.FragmentsResponse
		frags []pb.FragmentsResponse_SignedFragment
		err   error
	)
	for {
		if resp, err = rjc.Fragments(ctx, req); resp != nil && resp.Status != pb.Status_OK {
			err = fmt.Errorf(resp.Status.String())
		}
		mbp.Must(err, "failed to fetch fragments")

		frags = append(frags, resp.Fragments...)

		if resp.NextPageToken == 0 {
			break
		} else {
			req.NextPageToken = resp.NextPageToken
		}
	}

	var ind = -1
	for {
		if ind+1 == len(frags) || frags[ind+1].BackingStore == "" {
			// Stop walking Fragments if we run out, or if the next fragment
			// has no BackingStore (yet; eg, it's still a broker-local fragment).
			break
		} else {
			ind++
		}
	}

	if ind != -1 {
		return resp.Fragments[ind].End
	} else {
		return 0
	}
}
