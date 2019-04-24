package quotes

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"time"

	"git.liveramp.net/jgraet/factable/pkg/factable"
	"github.com/LiveRamp/gazette/v2/pkg/brokertest"
	"github.com/LiveRamp/gazette/v2/pkg/client"
	"github.com/LiveRamp/gazette/v2/pkg/consumertest"
	"github.com/LiveRamp/gazette/v2/pkg/etcdtest"
	"github.com/LiveRamp/gazette/v2/pkg/mainboilerplate/runconsumer"
	"github.com/LiveRamp/gazette/v2/pkg/message"
	pb "github.com/LiveRamp/gazette/v2/pkg/protocol"
	"github.com/coreos/etcd/clientv3"
	gc "github.com/go-check/check"
)

// TestCase packages boilerplate runtime state used in the writing of tests
// leveraging the "quotes" package.
type TestCase struct {
	C        *gc.C
	Ctx      context.Context
	Etcd     *clientv3.Client
	Broker   *brokertest.Broker
	Journals pb.RoutedJournalClient

	Specs
	SchemaPath string
}

// NewTestCase stands up runtime state for a test using the given configured Specs.
// It returns a cleanup closure which should be invoked on test completion.
func NewTestCase(c *gc.C, specs Specs) (TestCase, func()) {
	var ctx, cancel = context.WithCancel(pb.WithDispatchDefault(context.Background()))
	var etcd = etcdtest.TestClient()

	// Write SchemaSpec fixture to Etcd.
	{
		var fixture = BuildSchemaSpec()
		var val, _ = fixture.Marshal()
		var _, err = etcd.Put(ctx, SchemaSpecKey, string(val))
		c.Assert(err, gc.IsNil)
	}
	// Start a broker & create journal fixtures.
	var broker = brokertest.NewBroker(c, etcd, "local", "broker")
	brokertest.CreateJournals(c, broker, specs.Journals...)

	return TestCase{
			C:        c,
			Ctx:      ctx,
			Etcd:     etcd,
			Broker:   broker,
			Journals: pb.NewRoutedJournalClient(broker.Client(), pb.NoopDispatchRouter{}),
			Specs:    specs,
		}, func() {
			broker.Tasks.Cancel()
			c.Check(broker.Tasks.Wait(), gc.IsNil)

			cancel()
			etcdtest.Cleanup()
		}
}

// StartApplication configures and initializes a Extractor or VTable Application.
func StartApplication(tc TestCase, app runconsumer.Application) *consumertest.Consumer {
	var cfg = app.NewConfig()

	// Extractor and VTable have overlapping subsets of this configuration structure.
	var cfgStr = `{"Factable": {
		"Deltas": "app.gazette.dev/message-type = DeltaEvent",
		"SchemaKey": "` + SchemaSpecKey + `",
		"Instance": "test-instance",
		"ArenaSize": 2048,
		"MaxArenas": 4
	}}`
	tc.C.Assert(json.NewDecoder(strings.NewReader(cfgStr)).Decode(cfg), gc.IsNil)

	var cmr = consumertest.NewConsumer(consumertest.Args{
		C:        tc.C,
		Journals: tc.Journals,
		App:      app,
		Etcd:     tc.Etcd,
		Root:     fmt.Sprintf("/%s", reflect.TypeOf(app).Elem()),
	})
	tc.C.Assert(app.InitApplication(runconsumer.InitArgs{
		Context: tc.Ctx,
		Config:  cfg,
		Server:  cmr.Server,
		Service: cmr.Service,
	}), gc.IsNil)
	go cmr.Tasks.GoRun()

	return cmr
}

func mapQuoteWords(env message.Envelope) []factable.RelationRow {
	var (
		quote = env.Message.(*Quote)
		words = strings.FieldsFunc(strings.ToLower(quote.Text),
			func(r rune) bool {
				switch r {
				case ' ', ',', '.', '?', ';', '"', '(', ')', '!':
					return true
				}
				return false
			},
		)
		wordCounts = make(map[string]int64)
	)
	for _, word := range words {
		wordCounts[word] = wordCounts[word] + 1
	}

	var rows []factable.RelationRow
	for word, count := range wordCounts {
		rows = append(rows, factable.RelationRow{quote, word, count, int64(0)})
	}

	// Order |rows| on |word|, so that the sequence of mapped RelationRows is stable.
	sort.Slice(rows, func(i, j int) bool {
		return rows[i][1].(string) < rows[j][1].(string)
	})

	// Set QuoteCount to 1 for the first RelationRow of the record (only; others are zero).
	rows[0][3] = int64(1)
	return rows
}

// PublishQuotes publishes quotes [begin, end) of the corpus. |relPath| is the
// relative path of the package invoking PublishQuotes with respect to the
// repository `pkg` directory.
func PublishQuotes(begin, end int, relPath string, ajc client.AsyncJournalClient) (err error) {
	var path = filepath.Join(relPath, "testing", "quotes", "testdata", "author-quote.txt")

	fin, err := os.Open(path)
	if err != nil {
		panic(err)
	}
	var br = bufio.NewReader(fin)
	var quote Quote

	var i = 0
	for ; i < begin; i++ {
		if _, err = br.ReadString('\n'); err != nil {
			return
		}
	}
	for ; i != end; i++ {
		if quote.Author, err = br.ReadString('\t'); err == io.EOF {
			return nil
		} else if err != nil {
			return
		}
		if quote.Text, err = br.ReadString('\n'); err != nil {
			return
		}
		quote.Author = quote.Author[:len(quote.Author)-1] // Strip '\t'.
		quote.Text = quote.Text[:len(quote.Text)-1]       // Strip '\n'.
		quote.ID = int64(i)
		quote.Time = time.Now()

		if _, err = message.Publish(ajc, Mapping, &quote); err != nil {
			return
		}
	}

	client.WaitForPendingAppends(ajc.PendingExcept(""))
	return
}

// Mapping is a message.MappingFunc which maps all Quotes to InputJournal.
func Mapping(_ message.Message) (journal pb.Journal, framing message.Framing, e error) {
	return InputJournal, message.JSONFraming, nil
}

const (
	InputJournal  pb.Journal = "examples/factable/quotes/input"
	SchemaSpecKey            = "/path/to/schema/key"
)
