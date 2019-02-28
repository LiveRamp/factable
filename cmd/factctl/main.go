package main

import (
	mbp "github.com/LiveRamp/gazette/v2/pkg/mainboilerplate"
	"github.com/jessevdk/go-flags"
)

type BaseCfg struct {
	Log       mbp.LogConfig    `group:"Logging" namespace:"log" env-namespace:"LOG"`
	Broker    mbp.ClientConfig `group:"Broker" namespace:"broker" env-namespace:"BROKER"`
	Extractor mbp.ClientConfig `group:"Extractor" namespace:"extractor" env-namespace:"EXTRACTOR"`
	VTable    mbp.ClientConfig `group:"VTable" namespace:"vtable" env-namespace:"VTABLE"`
}

func main() {
	var baseCfg = new(BaseCfg)
	var parser = flags.NewParser(baseCfg, flags.Default)

	var _, err = parser.AddCommand("sync",
		"Sync shards with Schema and Journals",
		"foo bar",
		&cmdSync{cfg: baseCfg},
	)
	mbp.Must(err, "failed to add command")

	cmdSchema, err := parser.AddCommand("schema",
		"Fetch or update the Schema",
		"Commands for interacting with the Factable Schema.",
		&struct{}{},
	)
	mbp.Must(err, "failed to add command")

	_, err = cmdSchema.AddCommand("update",
		"Update the Schema.",
		"Update the Schema blah blah",
		&cmdSchemaUpdate{cfg: baseCfg},
	)
	mbp.Must(err, "failed to add command")

	_, err = cmdSchema.AddCommand("get",
		"Get the Schema.",
		"Fetch the current Schema",
		&cmdSchemaGet{cfg: baseCfg},
	)
	mbp.Must(err, "failed to add command")

	_, err = parser.AddCommand("query",
		"Query a relation",
		"query query foo bar",
		&cmdQuery{cfg: baseCfg},
	)
	mbp.Must(err, "failed to add command")

	cmdBackfill, err := parser.AddCommand("backfill",
		"Work with backfills.",
		"Commands for working with backfills of materialized views.",
		&struct{}{},
	)
	mbp.Must(err, "failed to add command")

	_, err = cmdBackfill.AddCommand("list",
		"List outstanding backfills.",
		"List backfills which have been identified but not completed.",
		&cmdBackfillList{cfg: baseCfg},
	)
	mbp.Must(err, "failed to add command")

	_, err = cmdBackfill.AddCommand("specify",
		"Create back-fill job specification.",
		"Create specifications of the named back-fill, generating an output job spec and mapper tasks.",
		&cmdBackfillSpecify{cfg: baseCfg},
	)
	mbp.Must(err, "failed to add command")

	_, err = cmdBackfill.AddCommand("load",
		"Load back-fill results into DeltaEvent partitions.",
		`Load a back-fill results into DeltaEvent partitions.

Hex-encoded key/value rows are read one-by-one from the input, mapped into their
current DeltaEvent partition, then written, until no more rows remain.

Each DeltaEvent message is atomically written with a corresponding 2PC commit
acknowledgement, which is distinguished by on the back-fill name, the loader ID,
and the input line number. This acknowledgement directs the VTable service to
immediately apply each row, and ensures a repetition of a specific row is ignored.
If a loader job fails partway through its input, the protocol ensures it can be
restarted without risk of double-application of DeltaEvent messages--each
message will be processed exactly once. However, it's crucial that all runs of a
specific loader job read identical input sequences every time.

Inputs to load may be stored within a BLOB store like S3 or Google Cloud Storage.
Rather than first copying inputs to the local machine, consider streaming them
to this tool:

    gsutil cp gs://bucket/my/input/one gs://bucket/my/input/two - \
    | factctl backfill load --name precious-penguin --id 0 --path /dev/stdin 
`,
		&cmdBackfillLoad{cfg: baseCfg},
	)
	mbp.Must(err, "failed to add command")

	mbp.AddPrintConfigCmd(parser, iniFilename)
	mbp.MustParseConfig(parser, iniFilename)
}

const iniFilename = "factctl.ini"
