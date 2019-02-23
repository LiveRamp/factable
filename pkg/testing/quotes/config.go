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
	MapQuoteWords           = "MapQuoteWords"
	DimQuoteAuthor          = "DimQuoteAuthor"
	DimQuoteCount           = "DimQuoteCount"
	DimQuoteID              = "DimQuoteID"
	DimQuoteTime            = "DimQuoteTime"
	DimQuoteWord            = "DimQuoteWord"
	DimQuoteWordCount       = "DimQuoteWordCount"
	DimQuoteWordTotalCount  = "DimQuoteWordTotalCount"
	MetricLastQuoteID       = "MetricLastQuoteID"
	MetricSumQuoteCount     = "MetricSumQuoteCount"
	MetricSumWordQuoteCount = "MetricSumWordQuoteCount"
	MetricSumWordTotalCount = "MetricSumWordTotalCount"
	MetricUniqueWords       = "MetricUniqueWords"
	RelQuoteWords           = "RelQuoteWords"
	MVWordStats             = "MVWordStats"
	MVQuoteStats            = "MVQuoteStats"
	MVRecentQuotes          = "MVRecentQuotes"

	MapQuoteWordsTag          factable.MapTag = iota
	DimQuoteAuthorTag         factable.DimTag = iota
	DimQuoteCountTag          factable.DimTag = iota
	DimQuoteIDTag             factable.DimTag = iota
	DimQuoteTimeTag           factable.DimTag = iota
	DimQuoteWordCountTag      factable.DimTag = iota
	DimQuoteWordTag           factable.DimTag = iota
	DimQuoteWordTotalCountTag factable.DimTag = iota
	MVWordStatsTag            factable.MVTag  = iota
	MVQuoteStatsTag           factable.MVTag  = iota
	MVRecentQuotesTag         factable.MVTag  = iota
)

type Quote struct {
	ID     int64     // Unique ID of the Quote.
	Author string    // Language of the Quote.
	Text   string    // Text of the Document.
	Time   time.Time // Timestamp of the Quote.
}

func BuildExtractors() factable.ExtractFns {
	return factable.ExtractFns{
		NewMessage: func(spec *pb.JournalSpec) (message message.Message, e error) {
			return new(Quote), nil
		},
		Mapping: map[factable.MapTag]func(message.Envelope) []factable.RelationRow{
			MapQuoteWordsTag: mapQuoteWords, // Emits RelationRow{*Quote, Word, WordCount, QuoteCount}
		},
		Int: map[factable.DimTag]func(r factable.RelationRow) int64{
			DimQuoteWordCountTag:      func(r factable.RelationRow) int64 { return 1 },
			DimQuoteCountTag:          func(r factable.RelationRow) int64 { return r[3].(int64) },
			DimQuoteIDTag:             func(r factable.RelationRow) int64 { return int64(r[0].(*Quote).ID) },
			DimQuoteWordTotalCountTag: func(r factable.RelationRow) int64 { return r[2].(int64) },
		},
		String: map[factable.DimTag]func(r factable.RelationRow) string{
			DimQuoteAuthorTag: func(r factable.RelationRow) string { return r[0].(*Quote).Author },
			DimQuoteWordTag:   func(r factable.RelationRow) string { return r[1].(string) },
		},
		Time: map[factable.DimTag]func(r factable.RelationRow) time.Time{
			DimQuoteTimeTag: func(r factable.RelationRow) time.Time { return r[0].(*Quote).Time },
		},
	}
}

func BuildSchemaSpec() factable.SchemaSpec {
	return factable.SchemaSpec{
		Mappings: []factable.MappingSpec{
			{
				Name: MapQuoteWords,
				Desc: "Maps quotes to constituent words, counts, and totals.",
				Tag:  MapQuoteWordsTag,
			},
		},
		Dimensions: []factable.DimensionSpec{
			{
				Name: DimQuoteWordCount,
				Type: factable.DimensionType_VARINT,
				Desc: "Returns 1 for each quote word.",
				Tag:  DimQuoteWordCountTag,
			},
			{
				Name: DimQuoteCount,
				Type: factable.DimensionType_VARINT,
				Desc: "Returns the count of 1 for each quote.",
				Tag:  DimQuoteCountTag,
			},
			{
				Name: DimQuoteID,
				Type: factable.DimensionType_VARINT,
				Desc: "Returns the quote ID.",
				Tag:  DimQuoteIDTag,
			},
			{
				Name: DimQuoteAuthor,
				Type: factable.DimensionType_STRING,
				Desc: "Returns the quote Author.",
				Tag:  DimQuoteAuthorTag,
			},
			{
				Name: DimQuoteWord,
				Type: factable.DimensionType_STRING,
				Desc: "Returns the quote word.",
				Tag:  DimQuoteWordTag,
			},
			{
				Name: DimQuoteWordTotalCount,
				Type: factable.DimensionType_VARINT,
				Desc: "Returns the count of the words within the quote.",
				Tag:  DimQuoteWordTotalCountTag,
			},
			{
				Name: DimQuoteTime,
				Type: factable.DimensionType_TIMESTAMP,
				Desc: "Returns the timestamp of the quote.",
				Tag:  DimQuoteTimeTag,
			},
		},
		Metrics: []factable.MetricSpec{
			{
				Name:      MetricSumQuoteCount,
				Dimension: DimQuoteCount,
				Type:      factable.MetricType_VARINT_SUM,
				Desc:      "Sums over number of quotes.",
				Tag:       1,
			},
			{
				Name:      MetricSumWordQuoteCount,
				Dimension: DimQuoteWordCount,
				Type:      factable.MetricType_VARINT_SUM,
				Desc:      "Sums over number of quotes a word appears in.",
				Tag:       2,
			},
			{
				Name:      MetricSumWordTotalCount,
				Dimension: DimQuoteWordTotalCount,
				Type:      factable.MetricType_VARINT_SUM,
				Desc:      "Sums over total word count.",
				Tag:       3,
			},
			{
				Name:      MetricUniqueWords,
				Dimension: DimQuoteWord,
				Type:      factable.MetricType_STRING_HLL,
				Desc:      "Unique quote words.",
				Tag:       4,
			},
			{
				Name:      MetricLastQuoteID,
				Dimension: DimQuoteID,
				Type:      factable.MetricType_VARINT_GAUGE,
				Desc:      "Last quote ID observed for the record.",
				Tag:       5,
			},
		},
		Relations: []factable.RelationSpec{
			{
				Name:    RelQuoteWords,
				Desc:    "Quote events, mapped on unique words",
				Mapping: MapQuoteWords,
				Selector: pb.LabelSelector{
					Include: pb.MustLabelSet("name", InputJournal.String()),
				},
				Dimensions: []string{
					DimQuoteAuthor,
					DimQuoteCount,
					DimQuoteID,
					DimQuoteTime,
					DimQuoteWord,
					DimQuoteWordCount,
					DimQuoteWordTotalCount,
				},
				Tag: 1,
			},
		},
		Views: []factable.MaterializedViewSpec{
			{
				Name:     MVWordStats,
				Relation: RelQuoteWords,
				View: factable.ViewSpec{
					Dimensions: []string{DimQuoteWord, DimQuoteAuthor},
					Metrics:    []string{MetricSumWordQuoteCount, MetricLastQuoteID, MetricSumWordTotalCount},
				},
				Desc: "Word and author word frequency and inverse document frequency.",
				Tag:  MVWordStatsTag,
			},
			{
				Name:     MVQuoteStats,
				Relation: RelQuoteWords,
				View: factable.ViewSpec{
					Dimensions: []string{DimQuoteAuthor, DimQuoteID},
					Metrics:    []string{MetricSumQuoteCount, MetricSumWordQuoteCount, MetricSumWordTotalCount, MetricUniqueWords},
				},
				Desc: "Author and quote statistics.",
				Tag:  MVQuoteStatsTag,
			},
			{
				Name:     MVRecentQuotes,
				Relation: RelQuoteWords,
				View: factable.ViewSpec{
					Dimensions: []string{DimQuoteID, DimQuoteAuthor, DimQuoteTime},
				},
				Desc: "Recent Quote IDs & Authors, with timestamps and a short retention.",
				Retention: &factable.MaterializedViewSpec_Retention{
					RemoveAfter: time.Minute,
					RelativeTo:  DimQuoteTime,
				},
				Tag: MVRecentQuotesTag,
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
