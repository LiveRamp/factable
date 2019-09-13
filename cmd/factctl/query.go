package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"os"

	"github.com/LiveRamp/factable/pkg/factable"
	mbp "github.com/LiveRamp/gazette/v2/pkg/mainboilerplate"
	pb "github.com/LiveRamp/gazette/v2/pkg/protocol"
	"github.com/golang/protobuf/ptypes/empty"
)

type cmdQuery struct {
	Path   string `long:"path" required:"true" description:"Local path to query file to execute"`
	Format string `long:"format" short:"o" choice:"yaml" choice:"json" choice:"proto" default:"yaml" description:"Input format"`

	cfg *BaseCfg
}

func (cmd *cmdQuery) Execute([]string) error {
	mbp.InitLog(cmd.cfg.Log)
	pb.RegisterGRPCDispatcher("")

	var ctx = pb.WithDispatchDefault(context.Background())
	var vtConn = cmd.cfg.VTable.Dial(ctx)

	var spec factable.QuerySpec
	parseFile(cmd.Path, cmd.Format, &spec)

	var out = csv.NewWriter(os.Stdout)
	out.Comma = '\t'

	// Fetch the schema.
	schemaResp, err := factable.NewSchemaClient(vtConn).GetSchema(ctx, new(empty.Empty))
	mbp.Must(err, "failed to fetch current schema")
	schema, err := factable.NewSchema(nil, schemaResp.Spec)
	mbp.Must(err, "failed to build Schema from SchemaSpec")

	query, err := schema.ResolveQuery(spec)
	mbp.Must(err, "failed to resolve query")

	// Execute the query.
	stream, err := factable.NewQueryClient(vtConn).ExecuteQuery(ctx, &factable.ExecuteQueryRequest{Query: query})
	mbp.Must(err, "failed to query table")

	var it = factable.NewStreamIterator(stream.RecvMsg)
	key, val, err := it.Next()

	for ; err == nil; key, val, err = it.Next() {
		var rec []string

		mbp.Must(schema.UnmarshalDimensions(key, query.View.DimTags, func(f factable.Field) error {
			rec = append(rec, fmt.Sprintf("%v", f))
			return nil
		}), "failed to unmarshal dimensions")
		mbp.Must(schema.UnmarshalMetrics(val, query.View.MetTags, func(a factable.Aggregate) error {
			rec = append(rec, fmt.Sprintf("%v", factable.Flatten(a)))
			return nil
		}), "failed to unmarshal metrics")

		mbp.Must(out.Write(rec), "failed to write output record")
	}
	if err != factable.KVIteratorDone {
		mbp.Must(err, "failed to stream key/values")
	}
	out.Flush()

	return nil
}
