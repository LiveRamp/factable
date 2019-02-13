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
