package quotes

import (
	"fmt"
	"time"

	"git.liveramp.net/jgraet/factable/pkg/factable"
	"github.com/LiveRamp/gazette/v2/pkg/brokertest"
	"github.com/LiveRamp/gazette/v2/pkg/consumer"
	"github.com/LiveRamp/gazette/v2/pkg/labels"
	"github.com/LiveRamp/gazette/v2/pkg/message"
	pb "github.com/LiveRamp/gazette/v2/pkg/protocol"
)

const (
	MapQuoteWords           factable.MapTag = 1
	DimQuoteCount           factable.DimTag = iota
	DimQuoteWordCount       factable.DimTag = iota
	DimQuoteID              factable.DimTag = iota
	DimQuoteAuthor          factable.DimTag = iota
	DimQuoteWord            factable.DimTag = iota
	DimQuoteWordTotalCount  factable.DimTag = iota
	MetricSumQuoteCount     factable.MetTag = iota
	MetricSumWordTotalCount factable.MetTag = iota
	MetricSumWordQuoteCount factable.MetTag = iota
	MetricUniqueWords       factable.MetTag = iota
	MetricLastQuoteID       factable.MetTag = iota
	RelQuoteWords           factable.RelTag = iota
	MVWordStats             factable.MVTag  = iota
	MVQuoteStats            factable.MVTag  = iota
)

type Quote struct {
	ID     int64  // Unique ID of the Quote.
	Author string // Language of the Quote.
	Text   string // Text of the Document.
}

func BuildExtractors() factable.ExtractFns {
	return factable.ExtractFns{
		NewMessage: func(spec *pb.JournalSpec) (message message.Message, e error) {
			return new(Quote), nil
		},
		Mapping: map[factable.MapTag]func(message.Envelope) []factable.RelationRow{
			MapQuoteWords: mapQuoteWords, // Emits RelationRow{*Quote, Word, WordCount, QuoteCount}
		},
		Int: map[factable.DimTag]func(r factable.RelationRow) int64{
			DimQuoteWordCount:      func(r factable.RelationRow) int64 { return 1 },
			DimQuoteCount:          func(r factable.RelationRow) int64 { return r[3].(int64) },
			DimQuoteID:             func(r factable.RelationRow) int64 { return int64(r[0].(*Quote).ID) },
			DimQuoteWordTotalCount: func(r factable.RelationRow) int64 { return r[2].(int64) },
		},
		String: map[factable.DimTag]func(r factable.RelationRow) string{
			DimQuoteAuthor: func(r factable.RelationRow) string { return r[0].(*Quote).Author },
			DimQuoteWord:   func(r factable.RelationRow) string { return r[1].(string) },
		},
	}
}

func BuildSchemaSpec() factable.SchemaSpec {
	return factable.SchemaSpec{
		Mappings: []factable.MappingSpec{
			{
				Tag:  MapQuoteWords,
				Name: "mapQuoteWords",
				Desc: "Maps quotes to constituent words, counts, and totals.",
			},
		},
		Dimensions: []factable.DimensionSpec{
			{
				Tag:  DimQuoteWordCount,
				Name: "quoteWordCount",
				Desc: "Returns 1 for each quote word.",
				Type: factable.DimensionType_VARINT,
			},
			{
				Tag:  DimQuoteCount,
				Name: "quoteCount",
				Desc: "Returns the count of 1 for each quote.",
				Type: factable.DimensionType_VARINT,
			},
			{
				Tag:  DimQuoteID,
				Name: "quoteID",
				Desc: "Returns the quote ID.",
				Type: factable.DimensionType_VARINT,
			},
			{
				Tag:  DimQuoteAuthor,
				Name: "quoteAuthor",
				Desc: "Returns the quote Author.",
				Type: factable.DimensionType_STRING,
			},
			{
				Tag:  DimQuoteWord,
				Name: "quoteWord",
				Desc: "Returns the quote word.",
				Type: factable.DimensionType_STRING,
			},
			{
				Tag:  DimQuoteWordTotalCount,
				Name: "quoteWordTotalCount",
				Desc: "Returns the count of the words within the quote.",
				Type: factable.DimensionType_VARINT,
			},
		},
		Metrics: []factable.MetricSpec{
			{
				Tag:    MetricSumQuoteCount,
				Name:   "sumQuoteCounts",
				Desc:   "Sums over number of quotes.",
				Type:   factable.MetricType_VARINT_SUM,
				DimTag: DimQuoteCount,
			},
			{
				Tag:    MetricSumWordQuoteCount,
				Name:   "sumWordQuotes",
				Desc:   "Sums over number of quotes a word appears in.",
				Type:   factable.MetricType_VARINT_SUM,
				DimTag: DimQuoteWordCount,
			},
			{
				Tag:    MetricSumWordTotalCount,
				Name:   "sumWordCount",
				Desc:   "Sums over total word count.",
				Type:   factable.MetricType_VARINT_SUM,
				DimTag: DimQuoteWordTotalCount,
			},
			{
				Tag:    MetricUniqueWords,
				Name:   "uniqueWords",
				Desc:   "Unique quote words.",
				Type:   factable.MetricType_STRING_HLL,
				DimTag: DimQuoteWord,
			},
			{
				Tag:    MetricLastQuoteID,
				Name:   "lastQuoteID",
				Desc:   "Last quote ID observed for the record.",
				Type:   factable.MetricType_VARINT_GUAGE,
				DimTag: DimQuoteID,
			},
		},
		Relations: []factable.RelationSpec{
			{
				Tag:     RelQuoteWords,
				Name:    "quoteWords",
				Desc:    "Quote events, mapped on unique words",
				Mapping: MapQuoteWords,
				Selector: pb.LabelSelector{
					Include: pb.MustLabelSet("name", InputJournal.String()),
				},
				Dimensions: []factable.DimTag{
					DimQuoteCount,
					DimQuoteWordCount,
					DimQuoteID,
					DimQuoteAuthor,
					DimQuoteWord,
					DimQuoteWordTotalCount,
				},
			},
		},
		Views: []factable.MaterializedViewSpec{
			{
				Tag:  MVWordStats,
				Name: "wordStats",
				Desc: "Word and author word frequency and inverse document frequency.",
				View: factable.ViewSpec{
					RelTag:     RelQuoteWords,
					Dimensions: []factable.DimTag{DimQuoteWord, DimQuoteAuthor},
					Metrics:    []factable.MetTag{MetricSumWordQuoteCount, MetricLastQuoteID, MetricSumWordTotalCount},
				},
			},
			{
				Tag:  MVQuoteStats,
				Name: "quoteStats",
				Desc: "Author and quote statistics.",
				View: factable.ViewSpec{
					RelTag:     RelQuoteWords,
					Dimensions: []factable.DimTag{DimQuoteAuthor, DimQuoteID},
					Metrics:    []factable.MetTag{MetricSumQuoteCount, MetricSumWordQuoteCount, MetricSumWordTotalCount, MetricUniqueWords},
				},
			},
		},
	}
}

// Specs bundles JournalSpecs and ShardSpecs for extractor and vtable consumers
// operating against the `quotes` package.
type Specs struct {
	Journals        []*pb.JournalSpec
	ExtractorShards []*consumer.ShardSpec
	VTableShards    []*consumer.ShardSpec
}

// BuildSpecs returns Specs having |parts| delta partitions.
func BuildSpecs(parts int, views ...factable.MVTag) Specs {
	var specs Specs

	specs.Journals = append(specs.Journals,
		brokertest.Journal(pb.JournalSpec{
			Name: InputJournal,
			LabelSet: pb.MustLabelSet(
				labels.MessageType, "Quote",
				labels.ContentType, labels.ContentType_JSONLines,
			),
		}))

	for _, view := range views {
		var shard = &consumer.ShardSpec{
			Id:                consumer.ShardID(fmt.Sprintf("extractor-%d", view)),
			Sources:           []consumer.ShardSpec_Source{{Journal: InputJournal}},
			RecoveryLogPrefix: "recovery/logs",
			HintPrefix:        "/hints/extractor",
			MaxTxnDuration:    time.Second,
			LabelSet:          pb.MustLabelSet("mvTag", fmt.Sprintf("%d", view)),
		}
		specs.ExtractorShards = append(specs.ExtractorShards, shard)

		specs.Journals = append(specs.Journals,
			brokertest.Journal(pb.JournalSpec{
				Name:     shard.RecoveryLog(),
				LabelSet: pb.MustLabelSet(labels.ContentType, labels.ContentType_RecoveryLog),
			}))
	}
	for i := 0; i != parts; i++ {
		var part = pb.Journal(fmt.Sprintf("deltas/part-%03d", i))

		var shard = &consumer.ShardSpec{
			Id:                consumer.ShardID(fmt.Sprintf("vtable-part-%03d", i)),
			Sources:           []consumer.ShardSpec_Source{{Journal: part}},
			RecoveryLogPrefix: "recovery/logs",
			HintPrefix:        "/hints/vtable",
			MaxTxnDuration:    time.Second,
		}
		specs.VTableShards = append(specs.VTableShards, shard)

		specs.Journals = append(specs.Journals,
			brokertest.Journal(pb.JournalSpec{
				Name: part,
				LabelSet: pb.MustLabelSet(
					labels.MessageType, "DeltaEvent",
					labels.ContentType, labels.ContentType_JSONLines,
				),
			}),
			brokertest.Journal(pb.JournalSpec{
				Name:     shard.RecoveryLog(),
				LabelSet: pb.MustLabelSet(labels.ContentType, labels.ContentType_RecoveryLog),
			}),
		)
	}
	return specs
}
