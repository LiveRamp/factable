package main

import (
	"context"
	"fmt"
	"os"

	"github.com/LiveRamp/factable/pkg/backfill"
	"github.com/LiveRamp/factable/pkg/factable"
	mbp "go.gazette.dev/core/mainboilerplate"
	pb "go.gazette.dev/core/consumer/protocol"
	"github.com/golang/protobuf/ptypes/empty"
)

type cmdBackfillLoad struct {
	cfg *BaseCfg

	Name string `long:"name" required:"true" description:"Name of the back-fill which produced the rows to load"`
	ID   int    `long:"id" required:"true" description:"ID of this loader among all loaders of this back-fill. Eg, 0, 1, 2 ... N"`
	Path string `long:"path" required:"true" description:"Local path to reduced rows to load"`
}

func (cmd *cmdBackfillLoad) Execute([]string) error {
	mbp.InitLog(cmd.cfg.Log)
	pb.RegisterGRPCDispatcher("")

	var (
		ctx      = pb.WithDispatchDefault(context.Background())
		rjc      = cmd.cfg.Broker.RoutedJournalClient(ctx)
		extConn  = cmd.cfg.Extractor.Dial(ctx)
		fullName = fmt.Sprintf("%s-%d", cmd.Name, cmd.ID)
	)

	// Fetch the current Schema.
	var schemaSpec, err = factable.NewSchemaClient(extConn).GetSchema(ctx, new(empty.Empty))
	mbp.Must(err, "failed to fetch current schema")

	fin, err := os.Open(cmd.Path)
	mbp.Must(err, "failed to open input")

	err = backfill.Load(ctx, fin, rjc, schemaSpec.DeltaPartitions, fullName)
	mbp.Must(err, "failed to load back-fill")

	return nil
}
