# Overview

Factable is a solution for continuous dashboarding and general reporting over
event-sourced applications, featuring real-time updates, unbounded look-back,
extremely fast queries, high availability, and low total cost of ownership.

It's well-suited for domains where the structure of queries is known in advance,
and derives its efficiency from continuous pre-aggregation of those structures.
It is not a general-purpose OLAP system (eg, like BigQuery), but works best as a
means for cost-efficient "productionization" of insights discovered by internal
analysts.

# Background

Event-sourced applications built atop Gazette are typically managing large numbers
of granular messages which are stored across a set of Journals, each partitions
of a conceptual "topic".

For a range of reasons, application owners often want to report on these messages
by characterizing a set of interesting, extractable dimensions and metrics, and
then aggregating across all messages of a topic to compute a summary, or "fact"
table.

A challenge with streaming applications is that the set of input messages grows
continuously, at each moment changing the summary. A few strategies have
evolved to cope with this dilemma:

* A common strategy is to window messages (eg, by hour or day) and perform
regular MapReduce ETLs which crunch fact table updates within the window.
Processed summary updates are usually much, much smaller than the space of input
messages, and from there a variety of warehousing options exist for summary
storage and fast queries. This strategy is cost effective, but high latency.
 
* Several recent systems (BigQuery, RedShift, MemSQL, ClickHouse) focus on fast
ingest and management of very, very large table sizes and highly parallel query
execution. These systems are powerful and flexible for in-house analytics and
OLAP, with low ingest latency, but require massive storage and compute resource
commitments for acceptable performance, and can be expensive to operate. Query
execution can be fast (seconds), but is fundamentally constrained by the
underlying table size. Typically some type of TTL caching layer must be built
on top to mitigate cost in support of dashboarding and client-facing reports.

* Specialized databases (eg time series databases like InfluxDB) place more
constraints on the structure of messages and queries, in exchange for more
cost-effective performance. These trade-offs make them well suited for a
number of use cases, but limit their general applicability.

# Design


# Deploying Examples


.. code-block:: console

    docker build -t liveramp/factable .
    
    
    ./v2/test/bootstrap_local_kubernetes.sh microk8s
    ./v2/test/deploy_brokers.sh microk8s default
    ./test/deploy_factable.sh microk8s default
    
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

    
    $ ~/go/bin/gazctl journals list --stores
    +------------------------------------------+-------------------------------------------------------------------------------------------------+
    |                   NAME                   |                                             STORES                                              |
    +------------------------------------------+-------------------------------------------------------------------------------------------------+
    | examples/factable/quotes/input           | s3://examples/fragments/?profile=minio&endpoint=http%3A%2F%2Fvigilant-crab-minio.default%3A9000 |
    | examples/factable/vtable/deltas/part-000 | s3://examples/fragments/?profile=minio&endpoint=http%3A%2F%2Fvigilant-crab-minio.default%3A9000 |
    | examples/factable/vtable/deltas/part-001 | s3://examples/fragments/?profile=minio&endpoint=http%3A%2F%2Fvigilant-crab-minio.default%3A9000 |
    | examples/factable/vtable/deltas/part-002 | s3://examples/fragments/?profile=minio&endpoint=http%3A%2F%2Fvigilant-crab-minio.default%3A9000 |
    +------------------------------------------+-------------------------------------------------------------------------------------------------+
    
    $ ~/go/bin/quotes-publisher publish --broker.address=http://10.152.183.198:80 --quotes=pkg
    INFO[0000] done                                         
    
    $ ~/go/bin/factctl sync
    INFO[0000] shard created                                 backfill=lenient-redbird id=20baf470ab314589bff6f822 journal=examples/factable/quotes/input view=MVQuoteStats
    INFO[0000] shard created                                 backfill=lenient-redbird id=219141864c4e4ad742b3232e journal=examples/factable/quotes/input view=MVWordStats
    
    $ ~/go/bin/quotes-publisher publish --broker.address=http://10.152.183.198:80 --quotes=pkg
    INFO[0000] done                                         
    
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
    
    $ ~/go/bin/factctl backfill list
    {
     "lenient-redbird": [
     "20baf470ab314589bff6f822",
     "219141864c4e4ad742b3232e"
     ]
     }
    
    $ ~/go/bin/factctl backfill specify --name lenient-redbird
    INFO[0000] generated backfill specification              spec=lenient-redbird.spec tasks=lenient-redbird.tasks
    Test your backfill job specification with:
    
    head --lines=1 lenient-redbird.tasks \
            | my-backfill-binary map --spec lenient-redbird.spec \
            | sort --stable --key=1,1 \
            | my-backfill-binary combine --spec lenient-redbird.spec
    
    // You'll need to ~/go/bin/gazctl journals edit -l "", and replace named minio service with its direct svc IP in order for this to work:
    $ head --lines=1 lenient-redbird.tasks         | ~/go/bin/quotes-backfill map --spec lenient-redbird.spec         | sort --stable --key=1,1         | ~/go/bin/quotes-backfill combine --spec lenient-redbird.spec
    9f12652e20652e2063756d6d696e67730001f72503      899191b548594c4c01000000000000000000000048628441a6844ca28440be804d74804d818440e780416e80416c8449cf
    9f12652e20652e2063756d6d696e67730001f72504      89a6abf048594c4c01000000000000000000000042849443f980439e80415084434c901e8442658044838c40e18441a784404c84413
    9f12652e20652e2063756d6d696e67730001f72505      898c8ca648594c4c010000000000000000000000515e8841eb8062e18441508c487d
    9f12652e20652e2063756d6d696e67730001f72506      899799c548594c4c010000000000000000000000428494468190425684405e804209800c8845f28441b280438d80
    9f12652e20652e2063756d6d696e67730001f72507      898f8faf48594c4c01000000000000000000000042ae845ea48443148442ef8446888043b6804b35804329
