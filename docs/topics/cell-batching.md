# Cell Batching: From a Cell Miss to a Segment in Cache

*Deep dive into stage 5.3 of [query-lifecycle.md](../query-lifecycle.md): how a
missing cell value becomes a `CellRequest`, how requests are batched and turned
into few SQL statements, and how the results come back as cached `Segment`s.
The SQL text itself is covered in [sql-generation.md](sql-generation.md), the
cache layers in [caching.md](caching.md), and aggregate-table matching in
[aggregate-tables.md](aggregate-tables.md).*

Mondrian never answers a base-measure cell read with its own SQL query.
Instead, the evaluation loop *records* every cell it could not answer, and a
separate machinery satisfies whole populations of recorded requests at once —
from caches where possible, by rolling up existing segments where possible, and
by SQL `GROUP BY` statements otherwise. This document follows one pass of that
machinery end to end:

```
RolapEvaluator ──► FastBatchingCellReader#get          (client thread)
                     │  miss: record CellRequest, return sentinel
                     ▼
                   FastBatchingCellReader#loadAggregations
                     │  BatchLoader.LoadBatchCommand
                     ▼
                   SegmentCacheManager actor ──► BatchLoader#load
                     │  per AggregationKey: cache? rollup? else Batch → SQL
                     ▼
                   SegmentLoader#load ──► sqlExecutor runs GROUP BY
                     │
                     ▼
                   SegmentWithData ──► RolapStar#register (thread-local)
                                   ──► segment index + segment caches (shared)
```

## The recording pass

`FastBatchingCellReader` is created per execution by `RolapResult#<init>` and
is, by its own javadoc, a cell reader that "lies": when asked for a stored
measure's value it may record the fact that the value was asked for instead of
producing it. `FastBatchingCellReader#get` does, in order:

1. **Build a request.** `RolapAggregationManager#makeRequest(evaluator)`
   converts the evaluator's current member context into a `CellRequest` (next
   section). If the result is null or `CellRequest#isUnsatisfiable`, the cell
   provably has no value and `Util.nullValue` is returned immediately — no
   recording, no SQL.
2. **Probe the local cache.** `AggregationManager#getCellFromCache(request,
   pinSet)` delegates to `RolapStar#getCellFromCache`, which scans only the
   *thread-local* segment list (see "Registration and lifetime" below). A hit
   returns the value.
3. **First-miss peek at the shared cache.** If caching is enabled and this
   reader has not yet recorded any miss (`missCount == 0`),
   `SegmentCacheManager#peek(request)` makes a synchronous round-trip to the
   cache manager: if a matching segment already exists in the shared index —
   including the external cache — its body is fetched, converted to a
   `SegmentWithData`, registered locally, and the probe is retried. This is
   what makes a repeat query answer from cache in a single evaluation pass
   instead of always needing at least two.
4. **Record and lie.** Otherwise `FastBatchingCellReader#recordCellRequest`
   appends the request to the reader's list and `get` returns
   `RolapUtil.valueNotReadyException`.

### The sentinel protocol

Two shared singletons carry "not yet" control flow, and both are compared by
**identity**, never by type or message:

- `RolapUtil.valueNotReadyException` is a plain `new Double(0)` constant
  *returned* (not thrown) as a cell value. The evaluation code checks
  `o != RolapUtil.valueNotReadyException` (e.g. in `RolapResult`) to decide
  whether a computed value is real. Any other `Double(0)` is a legitimate zero.
- `CellRequestQuantumExceededException.INSTANCE` is a singleton
  `RuntimeException` *thrown* by `recordCellRequest` every time the pending
  request count reaches a multiple of the cell-request limit
  (`MondrianProperties#CellBatchSize`; values ≤ 0 mean the built-in default of
  100000). Its javadoc says it plainly: "Not really an exception, just a way of
  aborting a process." Throwing it unwinds the current evaluation pass early so
  that `RolapResult#phase` can flush the accumulated requests to the cache
  manager, keeping memory bounded and giving similar requests a chance to be
  answered from freshly loaded segments. `RolapResult` catches it at several
  phase boundaries and simply continues the load/evaluate loop.

This is why code on the evaluation path must never swallow exceptions
generically: catching `RuntimeException` (or returning a default on "error")
around anything that reads cells will either turn "value not loaded yet" into a
wrong answer or break the chunking protocol. Wrappers that must catch broadly
have to rethrow these two sentinels.

## Anatomy of a CellRequest

A `CellRequest` is the atom of the pipeline: *one measure* plus *one predicate
per constraining star column*. Its fields map directly onto the star model
described in [architecture.md](../architecture.md):

- `measure` — a `RolapStar.Measure`, i.e. the fact-table column plus its
  aggregator. Obtained from the evaluator's current measure member
  (`RolapStoredMeasure#getStarMeasure`).
- A sparse `StarColumnPredicate[]`, indexed by the **bit position** of each
  constrained `RolapStar.Column`. Indexing by bit position gives a canonical
  order without sorting; `CellRequest#check` lazily derives the dense
  `getConstrainedColumns()` array from it.
- `constrainedColumnsBitKey` — the `BitKey` with one bit per constrained
  column. This is the currency of the whole subsystem: batch identity, segment
  identity, aggregate-table matching, and rollup candidate search all compare
  bit keys.
- `compoundPredicateMap` — a sorted map from `BitKey` to `StarPredicate` for
  constraints that are *not* expressible as one value per column (see below).
- `unsatisfiable` — set by `CellRequest#addConstrainedColumn` when two
  different value predicates land on the same column (for example two levels
  from different hierarchies mapping to the same physical column with
  contradictory values). Such a request denotes an empty cell and is answered
  with `Util.nullValue` without touching the database.

`RolapAggregationManager#makeRequest` builds the request from
`RolapEvaluator#getNonAllMembers` (the current non-all member of every
hierarchy). The first member must be a `RolapStoredMeasure` — a calculated
measure yields a null request, which is correct because calculated members are
expanded by the evaluator into reads of base cells. Every other member is
constrained through its level: `RolapCubeLevel#getLevelReader` returns the
level's `LevelReader` strategy, whose `constrainRequest` translates the member
into `CellRequest#addConstrainedColumn(column, predicate)` calls — the member's
key column and, for non-unique levels, its ancestors' columns too. Members that
cannot be expressed against this star (for instance a member of a hierarchy
that does not join to the measure's fact table, unless the "ignore unrelated
dimensions" rules apply) make the whole request null.

**(fork PATCH)** `RolapAggregationManager#makeCellRequest` returns a null
request when it encounters a `#null` member (`member.isNull()`) in the
non-drill-through path: such a member's level has no level reader, and upstream
code would fail later when it tried to use one.

### Compound predicates

When the evaluator context aggregates over a *set* rather than a point — a
multi-member (compound) slicer, or an `Aggregate()` over a member list — a
single value per column cannot express the constraint. Those sets travel on the
evaluator as *aggregation lists* (`RolapEvaluator#getAggregationLists`), and
`RolapAggregationManager#applyCompoundPredicates` converts each into a
`CompoundPredicateInfo` (in `mondrian.rolap`): tuples are grouped by the
`BitKey` of the columns they constrain, and each group becomes an
`OR(AND(column = value ...))` `StarPredicate`. The result is attached via
`CellRequest#addAggregateList` together with its SQL string form
(`CompoundPredicateInfo#getPredicateString`), which is what segment headers use
to compare compound contexts cheaply. The slicer's predicate is built once and
cached on the evaluator (`RolapEvaluator#getSlicerPredicateInfo`) rather than
per cell. A compound predicate that cannot be expressed at all
(`CompoundPredicateInfo#isSatisfiable` false) nulls the request.

## Batching: which requests can share a SQL statement

Two requests can live in the same batch when they agree on
**`AggregationKey`**: the same `RolapStar`, the same
`constrainedColumnsBitKey`, and pairwise-equal compound predicate lists. (The
star is part of the key because two virtual-cube requests can have identical
bit keys against different base fact tables.) Note what is *not* in the key:
the measure and the predicate values — a batch accumulates any number of
measures and value combinations, as long as they constrain the same column set
under the same compound context.

Inside the cache manager, `BatchLoader.Batch` does the accumulation
(`Batch#add`): the shared `columns` array, a `measuresList` (deduplicated,
all from one star), one `valueSets` set of `StarColumnPredicate`s per column,
and the raw per-request predicate `tuples`. When the batch finally loads,
`Batch#initPredicates` collapses each column's value set into a sorted
`ListColumnPredicate` — i.e. the batch asks SQL for the *cross product* of the
requested values per column, which is exactly the shape of a segment.

### CompositeBatch and GROUPING SETS

If `MondrianProperties#EnableGroupingSets` is on **and** the dialect reports
`Dialect#supportsGroupingSets` (`BatchLoader#shouldUseGroupingFunction`),
`BatchLoader#groupBatches` tries to consolidate batches into
`BatchLoader.CompositeBatch`es: one **detailed batch** (the one with the widest
column set) plus **summary batches** whose results are coarser groupings of the
same data. `Batch#canBatch` decides whether a batch subsumes another; all of
the following must hold:

- the detailed batch's bit key is a superset of the other's
  (`Batch#hasOverlappingBitKeys`, via `BitKey#isSuperSetOf`);
- constraints match: shared columns have the same value sets and the detailed
  batch's extra columns request *all* values (`Batch#haveSameValues`, with
  distinct-count-specific rules in `Batch#constraintsMatch`);
- identical measure lists, and no distinct-count measure on either side
  (distinct counts cannot be rolled up);
- both would resolve to the same aggregate table (or none) with the same
  rollup flag, on the same star (`Batch#haveSameStarAndAggregation`);
- the same parent-child closure columns (`Batch#haveSameClosureColumns`) —
  rolling up across a closure level double-counts.

A `CompositeBatch#load` runs its member batches through a
`GroupingSetsCollector` (in `mondrian.rolap`) instead of loading each one:
every batch contributes a `GroupingSet` (its segments plus level/measure bit
keys), and one `SegmentLoader#load` call emits a single SQL statement whose
`GROUP BY GROUPING SETS` clause computes the detailed grouping and all
summaries in one scan. Without grouping sets, each batch loads separately —
one SQL statement per batch. See [sql-generation.md](sql-generation.md) for
the statement assembly.

## The SegmentCacheManager actor

All shared segment bookkeeping is serialized through the `SegmentCacheManager`
**actor**: callers enqueue `Command`s (request/response) or `Event`s
(fire-and-forget) via `SegmentCacheManager#execute` /
`SegmentCacheManager.Actor#execute` onto a bounded queue; dedicated daemon
threads dequeue them, and a `Handler` (the `Visitor` for events) applies index
mutations. Because only the actor mutates the `SegmentCacheIndex`, the index
needs no fine-grained locking. Two thread pools hang off the manager for work
that must *not* run on the actor (anything slow): `cacheExecutor` for external
segment-cache I/O and `sqlExecutor` for SQL statements (pool sizes from
`MondrianProperties#SegmentCacheManagerNumberCacheThreads` /
`#SegmentCacheManagerNumberSqlThreads`).

**(fork PATCH)** Upstream has a single actor thread; this fork runs an array of
them (`SegmentCacheManager.threads`, count from the system property
`mondrian.rolap.agg.SegmentCacheManager.actorThreads`, default 1, capped at
100). To keep the single-writer guarantee that the index design relies on, the
`Actor` serializes **per schema**: every `Message` exposes `getSchema()` (fork
PATCH on the `Message` interface), and `Actor#run` claims the message's
`SchemaKey` in a `processingSchemaKeys` concurrent map before processing,
re-queueing the message if another thread already holds that schema. Different
schemas' commands proceed in parallel; commands for one schema never do.

During `FastBatchingCellReader#loadAggregations` the thread split is:

- **Client (query) thread**: builds the `BatchLoader.LoadBatchCommand` and
  blocks on its response; afterwards fetches suggested segment bodies from the
  composite cache, performs suggested in-memory rollups
  (`FastBatchingCellReader#findResidentRollupCandidate` →
  `SegmentBuilder#rollup`), registers segments locally, and waits on SQL
  futures. It also pre-loads column cardinalities
  (`FastBatchingCellReader#preloadColumnCardinality`) before submitting,
  because cardinality SQL fired from the actor thread could deadlock.
- **Actor thread**: runs `BatchLoader#load` — cache/rollup/SQL triage, batch
  grouping, `Batch#loadAggregation` down to `SegmentLoader#load`, which
  registers pending headers in the index and *submits* the SQL work. Also
  applies index updates carried by events and inline commands.
- **sqlExecutor thread**: runs `SegmentLoader.SegmentLoadCommand` — executes
  the statement, builds segment data, writes the shared caches.

`SegmentCacheManager#peek` (the first-miss probe from the recording pass) is
itself a synchronous `PeekCommand` through the same actor queue.

## From batch to Segment

`BatchLoader#load` triages each cell request via
`BatchLoader#recordCellRequest2`, cheapest option first:

1. **Pending/cached headers** (`BatchLoader#loadFromCaches`). If the request
   matches a header already chosen this cycle, nothing more to do. Otherwise
   `SegmentCacheIndex#locate` searches the shared index for headers whose
   column values cover the request. A located header is either *loading right
   now* (another statement's SQL is in flight — `SegmentCacheIndex#getFuture`
   returns the body future to wait on) or *at rest* in a cache (added to the
   headers-to-fetch list). Either way, no new SQL.
2. **Rollup candidates.** If `MondrianProperties#EnableInMemoryRollup` is on
   and the measure's aggregator (and its rollup aggregator) support fast
   re-aggregation from raw values (`Aggregator#supportsFastAggregates` —
   distinct count does not), `SegmentCacheIndex#findRollupCandidates` looks for
   sets of resident segments at finer granularity whose union covers the
   request. Candidates come back as a `BatchLoader.RollupInfo`; the actual
   `SegmentBuilder#rollup` — building a coarser `SegmentHeader`/`SegmentBody`
   by re-aggregating the finer bodies — happens later on the client thread,
   and only if all candidate bodies are actually still in cache
   (`FastBatchingCellReader#findResidentRollupCandidate`). A successful rollup
   is pushed back into the shared index via an inline actor command.
3. **SQL** (`BatchLoader#loadFromSql`): the request joins (or creates) the
   `Batch` for its `AggregationKey`.

For each batch that reaches SQL, `Batch#loadAggregation` splits out
distinct-count measures the dialect cannot combine
(`Dialect#allowsCountDistinct` and friends) and calls
`AggregationManager#loadAggregation`, which:

- gets the statement-local `Aggregation` for the key
  (`RolapStar#lookupOrCreateAggregation`);
- **optimizes predicates** (`Aggregation#optimizePredicates`): computes a
  "bloat" factor per column — requested values relative to column cardinality
  (or to sibling count in the common drill-down case) — and replaces the
  costliest `IN`-lists with "no constraint" until the estimated result is at
  most twice the exactly-requested cell count. A list longer than
  `MondrianProperties#MaxConstraints` (Oracle's 1000-element `IN` limit is the
  canonical reason) is always dropped; other eliminations happen only when
  `MondrianProperties#OptimizePredicates` is true. Fetching a slightly larger
  clean cross-product is usually cheaper than a huge `WHERE`, and the extra
  cells are cached for free;
- creates one `Segment` per measure (`Aggregation#createSegments`, sorted by
  measure bit position so grouping-set column order is deterministic) and
  wraps them in a `GroupingSet`;
- hands off to `SegmentLoader#load` (directly, or via the collector when
  grouping sets are in play).

`SegmentLoader#load` runs on the actor thread: it registers each segment's
header in the `SegmentCacheIndex` as *loading* (so concurrent statements find
the future instead of issuing duplicate SQL) and submits a
`SegmentLoader.SegmentLoadCommand` to the `sqlExecutor`. The worker's
`SegmentLoader#loadImpl` generates and executes the statement
(`SegmentLoader#createExecuteSql` → `AggregationManager#generateSql`, which is
where aggregate tables are considered — see
[aggregate-tables.md](aggregate-tables.md)), gathers distinct axis values and
cell values (`SegmentLoader#processData`), chooses a dense or sparse
`SegmentDataset`, attaches data to each segment
(`SegmentLoader#setDataToSegments`), writes header+body to the composite cache,
and announces completion (`SegmentCacheManager#loadSucceeded`, an event that
makes the actor mark the index entry loaded and unblock waiters).

### Segment anatomy

- **`Segment`** is the *description*: star, measure, constrained `columns`
  with their `predicates` (one per axis), the `constrainedColumnsBitKey`, the
  compound predicate list, and excluded regions (created by partial cache
  flushes). It precomputes its `SegmentHeader` — the immutable, string-based,
  JVM-independent identity used by the shared index and external caches.
- **`SegmentWithData`** extends `Segment` with `SegmentAxis[]` (per axis, the
  sorted array of actual key values plus a key→offset map) and a
  `SegmentDataset` (the cell values, dense array or sparse map keyed by
  `CellKey`). `SegmentBody` is the serializable counterpart used in caches;
  `SegmentBuilder.SegmentConverter` implementations reconstruct a
  `SegmentWithData` from header+body.
- **Reading a cell back**: `RolapStar#getCellFromCache` scans the thread-local
  segments for one with an equal bit key that `Segment#matches` the request's
  aggregation key and measure, then calls `SegmentWithData#getCellValue` with
  the request's single values. Each key is mapped through
  `SegmentAxis#getOffset` into a `CellKey`; a key outside the segment's axis
  but inside its predicate means "covered but no fact rows", which is the null
  *value* (`Util.nullValue`) — distinct from "wrong segment", which returns
  null and lets the scan continue.

## Registration and lifetime

Every `SegmentWithData` that reaches a statement — from the peek, from the
shared cache, from a rollup, from its own SQL, or from another statement's SQL
future — is registered with `RolapStar#register`, which adds a
`SoftReference` to the *thread-local* `RolapStar.Bar`. That makes subsequent
`FastBatchingCellReader#get` probes lock-free memory reads for the rest of the
statement. The durable copies live elsewhere: the per-schema
`SegmentCacheIndex` (headers, converters, in-flight futures) maintained by the
actor, and the header+body caches behind `SegmentCacheManager.compositeCache`
(in-JVM `MemorySegmentCache` plus any external `mondrian.spi.SegmentCache`).
How long each layer keeps a segment, and how flushes constrain or evict them,
is the subject of [caching.md](caching.md).

## Properties and performance notes

| Property | Default | Effect here |
|---|---|---|
| `mondrian.rolap.cellBatchSize` (`CellBatchSize`) | -1 (→ 100000) | Requests recorded before `CellRequestQuantumExceededException` forces an early load phase. Lower = smaller memory footprint and earlier SQL, but more evaluation passes. |
| `mondrian.rolap.groupingsets.enable` (`EnableGroupingSets`) | false | Consolidates subsumable batches into one `GROUPING SETS` statement (dialect permitting). Fewer fact-table scans when a query needs the same data at several granularities. |
| `mondrian.rolap.EnableInMemoryRollup` | true | Allows answering coarse requests by re-aggregating finer cached segments instead of SQL. |
| `mondrian.rolap.maxConstraints` (`MaxConstraints`) | 1000 | Hard cap on `IN`-list length; longer predicate lists are dropped (fetching more, filtering nothing). |
| `mondrian.rolap.aggregates.optimizePredicates` (`OptimizePredicates`) | true | Enables the bloat-based dropping of near-complete `IN`-lists in `Aggregation#optimizePredicates`. |
| `mondrian.rolap.agg.SegmentCacheManager.actorThreads` (system property, fork PATCH) | 1 | Number of cache-manager actor threads; >1 lets independent schemas' segment bookkeeping proceed in parallel. |

The design consistently trades *more data per statement* for *fewer
statements*: batches ask for cross products rather than exact cell lists,
predicate optimization widens requests to whole levels, grouping sets fold
several granularities into one scan, and rollup avoids the database entirely.
The pathological opposite — thousands of tiny segments — usually indicates
requests that refuse to batch: mismatched compound predicates (each distinct
slicer set is its own `AggregationKey`), distinct-count measures (never
consolidated), or high-cardinality cell scatter that defeats the cross-product
shape. When diagnosing, `mondrian.rolap.agg` debug logging prints each batch's
bit key and columns as it forms.
