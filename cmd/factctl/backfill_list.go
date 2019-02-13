package main

import (
	"context"
	"encoding/json"
	"os"

	"github.com/LiveRamp/gazette/v2/pkg/consumer"
	mbp "github.com/LiveRamp/gazette/v2/pkg/mainboilerplate"
	pb "github.com/LiveRamp/gazette/v2/pkg/protocol"
	log "github.com/sirupsen/logrus"
)

type cmdBackfillList struct {
	cfg *BaseCfg
}

func (cmd *cmdBackfillList) Execute([]string) error {
	mbp.InitLog(cmd.cfg.Log)
	pb.RegisterGRPCDispatcher("")

	var (
		ctx     = pb.WithDispatchDefault(context.Background())
		extConn = cmd.cfg.Extractor.Dial(ctx)
	)

	// Fetch Extractor Shards in need of backfill.
	shardsResp, err := consumer.ListShards(ctx, consumer.NewShardClient(extConn), &consumer.ListRequest{
		Selector: pb.LabelSelector{Include: pb.MustLabelSet(backfillLabel, "")}})
	mbp.Must(err, "failed to fetch shards requiring back-fill")

	if len(shardsResp.Shards) == 0 {
		log.Info("no shards in need of back-fill")
		return nil
	}

	// Index ShardID by backfill name.
	var fills = make(map[string][]consumer.ShardID)
	for _, shard := range shardsResp.Shards {
		var name = shard.Spec.LabelSet.ValueOf(backfillLabel)
		fills[name] = append(fills[name], shard.Spec.Id)
	}

	var enc = json.NewEncoder(os.Stdout)
	enc.SetIndent(" ", "")
	return enc.Encode(fills)
}
