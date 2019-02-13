package main

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"os"

	"git.liveramp.net/jgraet/factable/pkg/factable"
	mbp "github.com/LiveRamp/gazette/v2/pkg/mainboilerplate"
	pb "github.com/LiveRamp/gazette/v2/pkg/protocol"
	"github.com/gogo/protobuf/proto"
	log "github.com/sirupsen/logrus"
	"gopkg.in/yaml.v2"
)

type cmdSchemaUpdate struct {
	Path     string `long:"path" required:"true" description:"Local path to schema file to apply"`
	Format   string `long:"format" short:"o" choice:"yaml" choice:"json" choice:"proto" default:"proto" description:"Input format"`
	Instance string `long:"instance" description:"Expected Factable release instance which is being updated"`
	Revision int64  `long:"revision" description:"Expected revision of the current schema to be updated"`

	cfg *BaseCfg
}

func (cmd *cmdSchemaUpdate) Execute([]string) error {
	mbp.InitLog(cmd.cfg.Log)
	pb.RegisterGRPCDispatcher("")

	// Parse the local input Schema.
	var updateReq = &factable.UpdateSchemaRequest{
		ExpectInstance:    cmd.Instance,
		ExpectModRevision: cmd.Revision,
	}
	parseFile(cmd.Path, cmd.Format, &updateReq.Update)

	// Apply the update.
	var ctx = pb.WithDispatchDefault(context.Background())

	var _, err = factable.NewSchemaClient(cmd.cfg.Extractor.Dial(ctx)).UpdateSchema(ctx, updateReq)
	mbp.Must(err, "failed to update schema")

	log.Info("successfully updated schema")
	return nil
}

func parseFile(path, format string, into proto.Message) {
	fin, err := os.Open(path)
	mbp.Must(err, "failed to open local schema")
	rawBytes, err := ioutil.ReadAll(fin)
	mbp.Must(err, "failed to read schema")

	switch format {
	case "json":
		mbp.Must(json.Unmarshal(rawBytes, into), "failed to decode schema")
	case "proto":
		mbp.Must(proto.UnmarshalText(string(rawBytes), into), "failed to decode protobuf text format")
	case "yaml":
		mbp.Must(yaml.UnmarshalStrict(rawBytes, into), "failed to decode yaml")
	default:
		log.WithField("format", format).Fatal("invalid format (expected json, proto, or yaml)")
	}
}
