package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"git.liveramp.net/jgraet/factable/pkg/factable"
	mbp "github.com/LiveRamp/gazette/v2/pkg/mainboilerplate"
	pb "github.com/LiveRamp/gazette/v2/pkg/protocol"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	log "github.com/sirupsen/logrus"
	"gopkg.in/yaml.v2"
)

type cmdSchemaGet struct {
	Format string `long:"format" short:"o" choice:"yaml" choice:"json" choice:"proto" default:"proto" description:"Output format"`

	cfg *BaseCfg
}

func (cmd *cmdSchemaGet) Execute([]string) error {
	mbp.InitLog(cmd.cfg.Log)
	pb.RegisterGRPCDispatcher("")

	var ctx = pb.WithDispatchDefault(context.Background())

	// Fetch the schema.
	schemaResp, err := factable.NewSchemaClient(cmd.cfg.Extractor.Dial(ctx)).GetSchema(ctx, new(empty.Empty))
	mbp.Must(err, "failed to fetch current schema")

	log.WithField("revision", schemaResp.ModRevision).Info("fetched at ModRevision")

	switch cmd.Format {
	case "yaml":
		var enc = yaml.NewEncoder(os.Stdout)
		mbp.Must(enc.Encode(&schemaResp.Spec), "failed to write schema")
		return enc.Close()
	case "json":
		var enc = json.NewEncoder(os.Stdout)
		enc.SetIndent(" ", " ")
		return enc.Encode(&schemaResp.Spec)
	case "proto":
		return proto.MarshalText(os.Stdout, &schemaResp.Spec)
	default:
		return fmt.Errorf("unknown Format " + cmd.Format)
	}
}
