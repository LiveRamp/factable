package main

import (
	"context"
	"os"

	"github.com/jessevdk/go-flags"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
	mbp "go.gazette.dev/core/mainboilerplate"
	"go.gazette.dev/core/metrics"
	"gopkg.in/yaml.v2"

	"go.gazette.dev/core/broker/client"

	"github.com/LiveRamp/factable/pkg/testing/quotes"
)

// cfg is the top-level configuration object of quotes-publisher.
type baseConfig struct {
	Log mbp.LogConfig `group:"Logging" namespace:"log" env-namespace:"LOG"`
}

type publishQuotes struct {
	Begin  int              `long:"begin" default:"0" description:"Index of first quote to publish."`
	End    int              `long:"end" default:"-1" description:"Index of last quote to publish (or -1, to publish all quotes)."`
	Path   string           `long:"quotes" description:"Path to quotes data file."`
	Broker mbp.ClientConfig `group:"Broker" namespace:"broker" env-namespace:"BROKER"`

	base *baseConfig
}

func (cfg publishQuotes) Execute(args []string) error {
	mbp.InitLog(cfg.base.Log)
	prometheus.MustRegister(metrics.GazetteClientCollectors()...)

	var rjc = cfg.Broker.MustRoutedJournalClient(context.Background())
	var as = client.NewAppendService(context.Background(), rjc)

	mbp.Must(quotes.PublishQuotes(cfg.Begin, cfg.End, cfg.Path, as), "failed to publish quotes")

	for op, _ := range as.PendingExcept("") {
		<-op.Done()
	}

	log.Info("done")
	return nil
}

type writeSchema struct {
	base *baseConfig
}

func (cfg writeSchema) Execute(args []string) error {
	mbp.InitLog(cfg.base.Log)

	var enc = yaml.NewEncoder(os.Stdout)
	if err := enc.Encode(quotes.BuildSchemaSpec()); err != nil {
		return err
	}
	return enc.Close()
}

func main() {
	var baseCfg baseConfig
	var parser = flags.NewParser(&baseCfg, flags.Default)

	_, _ = parser.AddCommand("publish", "Publish quotes", `
Publish a collection of input quotes to Gazette, as input to the example
quotes-extractor module.
`, &publishQuotes{base: &baseCfg})

	_, _ = parser.AddCommand("write-schema", "Print example Schema", `
Write the json-encoded Schema used by the "quotes" example. This schema can be
used to initialize the "factable-schema" ConfigMap expected by the service.
`, &writeSchema{base: &baseCfg})

	mbp.MustParseArgs(parser)
}
