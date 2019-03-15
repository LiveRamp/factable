Overview
========

Factable is a service for reporting over event-sourced application data. It
features real-time updates, unbounded look-back, extremely fast queries, high
availability, and low total cost of ownership.

It's well suited for domains where the structure of queries is known in advance,
and derives its efficiency from continuous pre-aggregation of those structures.
In the nomenclature of traditional data warehousing, Factable maintains online
*summary* tables which are updated with each committed relation row (ie, message).
It is not a general-purpose OLAP system, but works best as a means for cost-
efficient "productionization" of insights discovered by internal analysts.

LiveRamp uses Factable to power structured and limited ad-hoc queries over
systems authoring tens of billions of messages every day, with look-back
windows spanning months and even years.

Background
==========

Event-sourced applications built atop Gazette are often managing large numbers
of granular messages which are stored across a set of Journals, each partitions
of a conceptual "topic".

Application owners often want to report on messages by characterizing a set of
interesting extractable dimensions and metrics, and then aggregating across all
messages of a topic to compute a summary, or "fact" table.

A challenge with streaming applications is that the universe of messages is
unbounded and continuously growing, at each moment changing the summary.
A few strategies have evolved to cope with this dilemma:

- A common strategy is to window messages (eg, by hour or day) and process
  periodic MapReduce batches which crunch fact table updates within the window.
  Processed summary updates are usually much, much smaller than the space of input
  messages, and from there a variety of warehousing options exist for summary
  storage and fast queries. This strategy is cost effective, but has high latency.
  Grouping across windows and handling out-of-window data can also pose problems.

* Several recent systems (BigQuery, RedShift, MemSQL, ClickHouse) focus on fast
  ingest and management of very large table sizes and highly parallel query
  execution. These systems are powerful and flexible for in-house analytics and
  OLAP, with low ingest latency, but require massive storage and compute resource
  commitments for acceptable performance, and can be expensive to operate. Query
  execution can be fast (seconds), but is fundamentally constrained by the
  underlying table size, which is 1:1 with input messages. Retaining very long
  look-backs can be prohibitively expensive, and typically some sort of
  TTL caching layer must be built on top to mitigate query cost & latency in
  support of dash-boarding and client-facing reports.

- Specialized databases (eg time series databases like InfluxDB) place more
  constraints on the structure of messages and queries, in exchange for more
  cost-effective performance. These trade-offs make them well suited for a
  number of use cases, but limit their general applicability.

* `PipelineDB <http://www.pipelinedb.com/>`_ and its "continuous view"
  abstraction shares commonalities with Factable. As a PostgreSQL extension,
  PipelineDB also offers rich query capabilities and full compatibility with its
  ecosystem. However, PipelineDB is by-default limited to a single machine (though
  a paid offering does include clustering support). There is also no capability to
  efficiently back-fill a new continuous view over historical messages.

Factable is a semi-specialized solution that focuses on the problem of building,
storing, and quickly querying summary tables. Summaries are expected to be smaller
than the underlying relation, but may still be quite large, with storage scaled
across many machines. It provides a limited set of query-time features such as
grouping and filtering (including predicate push-down) but does not implement
generalized SQL. Its design intent is to provide only those query capabilities
which having very efficient and low-latency implementations, in keeping with
Factable's intended use-cases of powering fast client reports.

The Gazette consumers framework is leveraged heavily to power Factable's
capabilities for efficient view back-fill, in-memory aggregation, horizontal
scaling, and high availability. Like all Gazette consumers, Factable instances
are ephemeral, require no pre-provisioning or storage management, and deploy
to commodity hardware via an orchestrator such as Kubernetes.

QuickStart
==========

.. code-block:: console

    # Install git submodules so docker can build
    $ git submodule update --init

    # Install required cli tools for quickstart
    $ go install -tags rocksdb ./cmd/...

    # Build and test Factable.
    $ docker build -t liveramp/factable .

    # Bootstrap local Kubernetes, here using the microk8s context,
    # and deploy a Gazette broker cluster with Etcd.
    $ ~/path/to/github.com/LiveRamp/gazette/v2/test/bootstrap_local_kubernetes.sh microk8s
    $ ~/path/to/github.com/LiveRamp/gazette/v2/test/deploy_brokers.sh microk8s default

    # Deploy Factable with the example "quotes-extractor", and using the "Quotes" schema.
    $ ./test/deploy_factable.sh microk8s default

    # Fetch the current schema from the service.
    $ ~/go/bin/factctl schema get
    INFO[0000] fetched at ModRevision                        revision=13
    mappings:
    - name: MapQuoteWords
      desc: Maps quotes to constituent words, counts, and totals.
      tag: 17
    dimensions:
    - name: DimQuoteWordCount
      type: VARINT
      desc: Returns 1 for each quote word.
      tag: 22
    ... etc ...

    # View quotes input journal, and DeltaEvent partitions. Note the fragment store
    # endpoint (we'll need this a bit later).
    $ ~/go/bin/gazctl journals list --stores
    +------------------------------------------+-------------------------------------------------------------------------------------------------+
    |                   NAME                   |                                             STORES                                              |
    +------------------------------------------+-------------------------------------------------------------------------------------------------+
    | examples/factable/quotes/input           | s3://examples/fragments/?profile=minio&endpoint=http%3A%2F%2Fvigilant-crab-minio.default%3A9000 |
    | examples/factable/vtable/deltas/part-000 | s3://examples/fragments/?profile=minio&endpoint=http%3A%2F%2Fvigilant-crab-minio.default%3A9000 |
    | examples/factable/vtable/deltas/part-001 | s3://examples/fragments/?profile=minio&endpoint=http%3A%2F%2Fvigilant-crab-minio.default%3A9000 |
    | examples/factable/vtable/deltas/part-002 | s3://examples/fragments/?profile=minio&endpoint=http%3A%2F%2Fvigilant-crab-minio.default%3A9000 |
    +------------------------------------------+-------------------------------------------------------------------------------------------------+

    # Publish a collection of example quotes to the input journal. Optionally wait
    # a minute to ensure fragments are persisted, in order to test backfill.
    $ ~/go/bin/quotes-publisher publish --broker.address=http://${BROKER_ADDRESS}:80 --quotes=pkg
    INFO[0000] done

    # "Sync" the set of Extractor & VTable shards with the current Schema and
    # DeltaEvent partitioning. Sync will drop you into an editor to review and tweak
    # ShardSpecs and JournalSpecs before application. When editing recoverylogs, we'll
    # need to fill in our Minio fragment store endpoint listed above. 
    $ ~/go/bin/factctl sync
    INFO[0000] listed input journals                         numInputs=1 relation=RelQuoteWords
    INFO[0000] shard created                                 backfill= id=33f40b6c5936b918c98fb7bc journal=examples/factable/quotes/input view=MVWordStats
    INFO[0000] shard created                                 backfill= id=6a57b4e07c9316aa1b98adda journal=examples/factable/quotes/input view=MVQuoteStats
    INFO[0000] shard created                                 backfill= id=1fd39b77d017766553ac97e6 journal=examples/factable/quotes/input view=MVRecentQuotes

    # Query the "MVQuoteStats" view. Note that views caught up with our Quotes,
    # despite being created after they were published.
    $ ~/go/bin/factctl query --path /dev/stdin <<EOF
    materializedview: MVQuoteStats
    view:
        dimensions:
        - DimQuoteAuthor
        - DimQuoteID
        metrics:
        - MetricSumQuoteCount
        - MetricSumWordQuoteCount
        - MetricSumWordTotalCount
        - MetricUniqueWords
    EOF
    e. e. cummings  9473    1       5       5       5
    e. e. cummings  9474    1       9       9       9
    e. e. cummings  9475    1       9       9       9
    e. e. cummings  9476    1       30      35      30
    e. e. cummings  9477    1       4       4       4
    e. e. cummings  9478    1       15      17      15
    e. e. cummings  9479    1       7       7       7

    # Publish the examples again. Expect queries now reflect the new messages.
    $ ~/go/bin/quotes-publisher publish --broker.address=http://${BROKER_ADDRESS}:80 --quotes=pkg
    INFO[0000] done

    # Let's try running a back-fill. First, fetch the schema for editing. Note the returned revision.
    # Edit to add an exact copy of MVQuoteStats (eg, MVQuoteStats2) with a new tag.
    $ ~/go/bin/factctl schema get > schema.yaml
    # Now apply the updated schema. Use your release instance name, and previously fetched revision.
    $ ~/go/bin/factctl schema update --path schema.yaml --instance opulent-wombat --revision 13
     
    # We want to be sure that input journal fragments have been persisted to cloud storage
    # already (eg, Minio). We can either wait 10 minutes (its configured flush interval),
    # or restart broker pods.
     
    # Also, we want to tweak the fragment store used by this journal to use the
    # raw Minio IP rather than the named service. This just lets us read signed
    # URLs returned by Minio directly from our Host, outside of the local
    # Kubernetes environment. Eg, update:
    #   s3://examples/fragments/?profile=minio&endpoint=http%3A%2F%2Fgoodly-echidna-minio.default%3A9000
    # To:
    #   s3://examples/fragments/?profile=minio&endpoint=http%3A%2F%2F10.152.183.198%3A9000
    $ ~/go/bin/gazctl journals edit -l app.gazette.dev/message-type=Quote
    
    # Run sync again, this time asking it to create a back-fill job.
    # Note that this time, we don't have to fill out the recovery log fragment store.
    # The tool infers values for new journals & shards from those that already exist.
    $ ~/go/bin/factctl sync --create-backfill
    INFO[0000] listed input journals                         numInputs=1 relation=RelQuoteWords
    INFO[0000] shard created                                 backfill=sure-pony id=6e740c5e0777300ac155508e journal=examples/factable/quotes/input view=MVQuoteStats2

    # Try running a query against MVQuoteStats2. It returns no results.

    # Extractor shards in need of back-fill are annotated with a label to
    # that effect. List all extractor shards with current back-fill labels.
    $ ~/go/bin/gazctl shards list -l app.factable.dev/backfill -L app.factable.dev/backfill
    +--------------------------+---------+---------------------------+
    |            ID            | STATUS  | APP FACTABLE DEV/BACKFILL |
    +--------------------------+---------+---------------------------+
    | 6e740c5e0777300ac155508e | PRIMARY | sure-pony                 |
    +--------------------------+---------+---------------------------+

    # Create specifications for our backfill job. Require that only fragments
    # 6 hours old or newer should be filled over. The job will read each input
    # fragment just once, and compute all extracted views simultaneously. It is
    # a good idea to bundle related view updates into a single sync & backfill.
    $ ~/go/bin/factctl backfill specify --name sure-pony --max-age 6h
    INFO[0000] generated backfill specification              spec=sure-pony.spec tasks=sure-pony.tasks
    Test your backfill job specification with:

    head --lines=1 sure-pony.tasks \
            | my-backfill-binary map --spec sure-pony.spec \
            | sort --stable --key=1,1 \
            | my-backfill-binary combine --spec sure-pony.spec

    # Locally run the back-fill as a map/reduce.
    $ cat sure-pony.tasks \
      | ~/go/bin/quotes-backfill map --spec sure-pony.spec \
      | sort --stable --key=1,1 \
      | ~/go/bin/quotes-backfill combine --spec sure-pony.spec > sure-pony.results

    # Back-fill jobs produce row keys and aggregates using hex-encoded key/value
    # format intended for compatibility with Hadoop Streaming.
    $ head -n 5 sure-pony.results
    9f12652e20652e2063756d6d696e67730001f72503      899191b548594c4c01000000000000000000000048628441a6844ca28440be804d74804d818440e780416e80416c8449cf
    9f12652e20652e2063756d6d696e67730001f72504      89a6abf048594c4c01000000000000000000000042849443f980439e80415084434c901e8442658044838c40e18441a784404c84413
    9f12652e20652e2063756d6d696e67730001f72505      898c8ca648594c4c010000000000000000000000515e8841eb8062e18441508c487d
    9f12652e20652e2063756d6d696e67730001f72506      899799c548594c4c010000000000000000000000428494468190425684405e804209800c8845f28441b280438d80
    9f12652e20652e2063756d6d696e67730001f72507      898f8faf48594c4c01000000000000000000000042ae845ea48443148442ef8446888043b6804b35804329

    # Load the backfill results into DeltaEvent partitions.
    $ ~/go/bin/factctl backfill load --name sure-pony --id 0 --path sure-pony.results

    # Now query to compare MVQuoteStats and MVQuoteStats2. They return identical results!
    # Try "accidentally" loading the backfill results a second time. The second load
    # is ignored (de-duplicated), and the views continue to return the same results.
    # 
    # Now try publishing Quotes again. Note that both views update with new counts, as expected.
    
    # Clear the backfill, by simply removing the label from its extractor shards.
    $ ~/go/bin/gazctl shards edit -l app.factable.dev/backfill=sure-pony
    INFO[0005] successfully applied                          rev=122


Architecture
============

Schema Model
------------

Factable requires a "Schema" which describes the shape of user relations,
and the specific views Factable is expected to derive. A schema is defined by:

Dimensions
~~~~~~~~~~
Dimensions are fields which may be extracted from journal messages.

:Name: Short, unique name of the Dimension.
:DimensionType: Type of Dimension fields (string, integer, time, etc).
:DimTag: Unique, immutable integer tag identifying the Dimension.

For each Dimension, the user must provide an "extractor" function of the
appropriate type and registered under the corresponding DimTag. Extractor
functions accept mapped messages and return concrete dimension fields.

Metrics
~~~~~~~
Metrics are measures which may be calculated from a dimension (eg, given a
"cost" dimension, a metric might sum over it). Factable metrics included simple
aggregates like sums and gauges, as well as complex sketches like HyperLogLogs
and HyperMinHashes.

:Name: Short, unique name of the Metric.
:Dimension: Name of the Dimension from which this Metric is derived.
:MetricType:
  Type of the Metric (integer-sum, HLL(string), float-sum, int-gauge, etc).
  Must be consistent with the Dimension type.
:MetTag: Unique, immutable integer tag identifying the Metric.

Mappings
~~~~~~~~
For some use cases, messages may be de-normalized with respect to how the
message might be modeled in a traditional data warehouse. For example, a
single PurchaseEvent might capture multiple product SKUs. To build a relation
at the product SKU grain, one first _maps_ the PurchaseEvent into a distinct row
for each (PurcahseEvent, product SKU) tuple.

Factable Mappings similarly define the means of deriving rows from messages.

:Name: Short, unique name of the Mapping.
:MapTag: Unique, immutable integer tag identifying the Mapping.

Like Dimensions, the user must provide a function registered under the MapTag
which converts an input message into zero or more ``[]factable.RelationRow``.

Relations
~~~~~~~~~
Factable leverages the insight that a traditional warehouse "relation" can also
be defined in terms of application messages *already stored* across a set of
Gazette Journals. Journals are immutable, which means relation rows can be
reliably and repeatedly enumerated directly from the source journals.

:Name: Short, unique name of the Relation.
:Selector: Gazette label selector of Journals capturing Relation messages.
:Mapping: Name of the Mapping applied to messages to obtain relation rows.
:Dimensions: List of Dimensions which may be extracted from relation rows.
:RelTag: Unique, immutable integer tag identifying the Relation.

Materialized Views
~~~~~~~~~~~~~~~~~~
Views summarize a Relation, which may have extremely high cardinality, into a
tractable reduced set of dimensions and measures. When a view is created, it
first fills over all historical messages of relation journals until reaching the
present. Thereafter the view is updated continuously by reading messages as they
commit to relation journals, and updating the metric aggregates of affected view
rows. Unless directed otherwise, views in Factable always reflect all messages of
the relation, regardless of when the view was created.

:Name: Short, unique name of the MaterializedView.
:Relation: Name of the Relation materialized by this view.
:Dimensions:
  Ordered Dimensions summarized by the view. Views are indexed by the natural
  sort order of extracted Dimension fields. The chosen order if view fields is
  therefore essential to performance when filtering and grouping over the row.
  Eg, filtering over a leading dimension allows for skipping over large chunks
  of view rows. Filtering a trailing dimension will likely require examining
  each row.

  Similarly, grouping over a strict prefix of a view's dimensions means the
  natural order of the query matches that of the view, and that queries can be
  executed via a linear scan of the view. Grouping over non-prefix dimensions
  is still possible, but requires buffering, sorting, and re-aggregation at
  query-time.

:Metrics: Metrics aggregated by the view.
:Retention:
  Optional retention which describes the policy for view row expiration.
  Eg, rows should be kept for 6 months with respect to a time Dimension
  included in the view.
:MVTag: Unique, immutable integer tag identifying the MaterializedView.

*Discussion*
~~~~~~~~~~~~
A Schema must be referentially consistent with itself--for example, a Metric's
named Dimension must exist, with a type matching that of the Metric--but may
change over time as entities are added, removed, or renamed. An entity's *tag*,
however, is immutable, and plays a role identical to that of Protocol Buffer tags.
Schema transitions are likewise constrained on tags: it is an error, for example,
to specify a Dimension with a new name and new type but using a previously defined
DimTag.

Processing
----------
Factable separates its execution into an *Extrator* service and a *VTable*
service, communicating over a set of *DeltaEvent* journal partitions.

Extractor Consumer
~~~~~~~~~~~~~~~~~~
The *Extrator* consumer maps messages into relation rows, and from there to
extracted Dimension fields and Metric aggregates. Each extractor ShardSpec
composes a source journal to read with a MaterializedView to process. As
each consumer shard manages its own read offsets, this allows multiple views
to read the same relation journal at different byte offsets--including from byte
offset zero, if the journal must be filled over for a newly-created view.

Extractor must be initialized with a "registry" of domain-specific
``ExtractFns``: user-defined functions indexed on tag which implement
mappings over user message type(s), and extraction of user dimensions. Extractor
consumer binaries are compiled by the user for their application. After
instantiating ``ExtractFns``, the binary calls into an ``extractor.Main``
provided by Factable.

Each processed source message is mapped to zero or more ``[]RelationRow``,
and fed into the defined dimension extractors to produce a "row key". The row key
consists of extracted dimension fields of the view, encoded with an order-
preserving byte encoding and prefixed with the view's MVTag. Importantly, the
natural ordering of these ``[]byte`` keys matches that of the unpacked view
fields tuple. Factable relies on this property to index, scan, and filter/seek
over views represented within the flat, ordered key/value space provided by RocksDB.

Within the scope of single consumer transaction, the extractor combines messages
producing the same row key by updating the key's aggregates in place, and in-memory.
Many practical applications exhibit a strong "hot key" effect, where messages
mapping to a particular row key arrive closely in sequence. In these cases especially,
in-memory combining can provide a *significant* (1 to 2 orders of magnitude)
reduction in downstream Gazette writes.

At the close of a current consumer transaction, the extractor emits each row-key
and set of partial aggregates as a DeltaEvent message. DeltaEvents are mapped to
partitions based on row key, meaning that all DeltaEvents of a given row key
produced by any extractor will arrive at the same partition (and no other), and
that partitions (and the VTable database which index them) generally hold non-
overlapping portions of the overall view key-space.

A two-phase commit write protocol is used to guarantee exactly-once processing
of each DeltaEvent message by the VTable service.

No significant state is managed by the extractor consumer--just a small amount
of metadata in support of the 2PC protocol. At the completion of each consumer
transaction, the previously combined and emitted row keys and aggregates are
discarded. As a result, even after combining it's likely to see repetitions of
specific row key DeltaEvents co-occurring closely, both emitted from a single
extractor shard, as well as across different extractor shards. Further
aggregation is done by the VTable consumer.

VTable Consumer
~~~~~~~~~~~~~~~
The *VTable* consumer provides long-term storage and query capabilities of
materialized views, powered by its consumer Shard stores. A single VTable
deployment services *all* views processed by Factable: as each view row-key is
prefixed by its MVTag, view rows are naturally grouped on disk, and at query
time the MVTag is treated as just another dimension.

Each VTable consumer Shard reads row-keys and aggregates from its assigned
DeltaEvents partition, and aggregates updates to row-keys into its local store.
Shard stores are configured to run regular RocksDB compactions, and a compaction
filter is used to enforce view retention policies and the removal of dropped views.
As DeltaEvents are mapped to partitions on row-key when written, each shard store
will generally hold a non-overlapping and uniformly-sized subset of view rows
(with the caveat that changing DeltaEvent partitioning can result in a small
amount of overlap, which is not a concern).

VTable Queries
~~~~~~~~~~~~~~
VTable consumer shards surface a gRPC Query API. Queries are defined by:

:MaterializedView:
  Name of the view to query. In the future, Factable may infer an appropriate
  view from a named relation to query ("aggregate navigation").
:Dimensions:
  Dimensions to query from the view. Query results are always returned in
  sorted order by query dimensions, and only unique rows are returned.
  View dimensions not included as query dimensions are grouped over.

  Where possible, prefer to query dimensions which are a prefix of the view's,
  as this allows the query executor to best leverage the natural index order.
  Queries having this property are extremely efficient for the executor to
  evaluate, up to and including full queries of very large views.

  Queries over non-prefix dimensions are allowed, but require that the executor
  buffer, sort, and re-aggregate query rows before responding. The executor
  will limit the size of result sets it will return.
:Metrics: Metrics to return with each query row.
:Filters:
  Dimensions to filter over, and allowed ranges of field values for each
  dimension (also known as a "predicate").

  The query executor scans the view using a RocksDB iterator, and filters
  are used to "seek" the iterator forward to the next admissible view key
  given filter constraints.

  WHere possible, use filters and prefer to filter over leading dimensions
  of the view. This allows the query executor to efficiently skip large
  portions of the view, reducing disk IO and compute requirements.


As each shard store holds disjoint subsets of the view, VTables serve queries
using a distributed scatter/gather strategy. The VTable instance which receives
the query coordinates its execution, by first scattering it to all VTable shards.
Each shard independently executes the query, applying filter predicates and
grouping to the requested query dimensions and metrics, and streams ordered
query rows back to the coordinator. The coordinator then merges the results of
each shard into final query results. Coordinator merges need only preserve the
sorted ordering produced by each shard, and it can efficiently stream the
result set to the client as the query is evaluated by shards.

Syncing Shards & Backfills
~~~~~~~~~~~~~~~~~~~~~~~~~~
The Extractor maintains a shard for each tuple of (MaterializedView, InputJournal).
Likewise the VTable service maintains a shard for each DeltaEvent partition. As
new MaterializedViews are added or dropped, or the partition of relation journals
change, or the partition of DeltaEvents change, then the set of Extractor and/or
VTable shards can become out-of-sync.

Factable provides a CLI tool (``factctl sync``) which examines the current
Schema, the set of journals matching Relation label selectors, and DeltaEvent
partitions. It determines the sets of ShardSpecs and JournalSpecs to be added or
removed, gives the user an opportunity to inspect or tweak those specifications,
and sets them live against the running service deployments. ``factctl sync``
should be run after each partitioning or schema change.

By default, Extractor ShardSpecs created by ``factctl sync`` begin reading
from input journals at byte zero. In most cases this is ideal, as the Extractor
will quickly crunch through the journal backlog. For very, very large journals,
it may be more efficient to start the Extractor shard at a recent journal offset,
and then separately crunch historical portions of the journal as a large-scale
map/reduce job. For these cases, ``factctl sync --create-backfill`` will define
a backfill job, and ``factctl backfill specify`` will create job specifications
for use with eg Hadoop Streaming.
