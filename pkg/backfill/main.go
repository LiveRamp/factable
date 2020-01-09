package backfill

import (
	"github.com/LiveRamp/factable/pkg/factable"
	mbp "go.gazette.dev/core/mainboilerplate"
	pb "go.gazette.dev/core/consumer/protocol"
	"github.com/jessevdk/go-flags"
)

// JobSpec defines common specifications of a backfill job.
type JobSpec struct {
	SchemaSpec factable.SchemaSpec
	Inputs     map[pb.Journal]pb.JournalSpec
}

// MapTaskSpec defines a map tasks to be performed as part of a backfill.
type MapTaskSpec struct {
	*JobSpec `json:",ignore"`
	Fragment pb.Fragment
	URL      string
	MVTags   []factable.MVTag
}

func Main(extractFns factable.ExtractFns) {
	var parser = flags.NewParser(nil, flags.Default)

	var _, err = parser.AddCommand("map",
		"Extract over input fragments.",
		"Map input MapTaskSpecs to extracted view row deltas.",
		&cmdMap{extractFns: extractFns},
	)
	mbp.Must(err, "failed to add command")

	_, err = parser.AddCommand("combine",
		"Combine view row deltas.",
		"Combine over (equivalently: reduce) view row deltas.",
		&cmdCombine{},
	)
	mbp.Must(err, "failed to add command")

	/*
		_, err = parser.AddCommand("load",
			"Load reduced row deltas into Factable.",
			"Load a reduced file of view row deltas into Factable.",
			&loadCfg{cfg: baseCfg},
		)
		mbp.Must(err, "failed to add command")
	*/

	mbp.MustParseArgs(parser)
}
