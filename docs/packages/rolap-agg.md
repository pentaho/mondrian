# Package guide: `mondrian.rolap.agg`

*The cell/aggregation pipeline: turning recorded cell misses into SQL, and SQL
results into cached segments. The narratives live in
[cell-batching](../topics/cell-batching.md) (request → segment flow),
[sql-generation](../topics/sql-generation.md) (query specs, predicates → SQL),
and [caching](../topics/caching.md) (the three segment-cache levels and
flushing). This document is the class map for the package itself.*

The package has ~44 classes in four clusters: **requests**, the **segment
model**, **loading/cache management**, and **predicates**. Note that the
pipeline's driver classes are *not* here — `FastBatchingCellReader` and
`BatchLoader` (both in `FastBatchingCellReader.java`) live in `mondrian.rolap`,
as do the `StarPredicate`/`StarColumnPredicate` interfaces this package
implements. See [Neighbors](#neighbors) below for the full boundary.

## 1. Requests and batching

| Class | Role |
|---|---|
| `CellRequest` | One needed cell: a `RolapStar.Measure` plus per-column `StarColumnPredicate` constraints (sparse array by bit position), a `constrainedColumnsBitKey`, and compound (multi-column) predicates; flags itself `unsatisfiable` on contradictory constraints |
| `DrillThroughCellRequest` | `CellRequest` subclass that additionally lists which columns/measures to return in the drill-through result set |
| `AggregationKey` | Identity of an aggregation workload: {star, constrained-columns `BitKey`, compound predicate list}. Built from a `CellRequest`; the key under which requests batch together and `Aggregation`s are looked up |
| `CellRequestQuantumExceededException` | Sentinel singleton (`INSTANCE`) thrown by `FastBatchingCellReader#recordCellRequest` every `CellBatchSize` requests to force a load phase; caught and swallowed by evaluation loops |
| `QuerySpec` | Interface: everything needed to generate one segment-loading SQL statement (star, columns, measures, per-column predicates) |
| `AbstractQuerySpec` | Shared SQL assembly (`#generateSqlQuery` → `#nonDistinctGenerateSql`/`#distinctGenerateSql`) on top of `SqlQuery` |
| `SegmentArrayQuerySpec` | The normal case: loads a list of segments from the fact table, one measure column each, optionally with GROUPING SETS |
| `AggQuerySpec` | Parallel spec that reads from an `AggStar` (aggregate table) instead of the fact table; *not* an `AbstractQuerySpec` subclass; `rollup` mode wraps measures in their rollup aggregator |
| `DrillThroughQuerySpec` | Generates the non-aggregated row-listing SQL for drill-through |

## 2. Segment model

| Class | Role |
|---|---|
| `Segment` | Metadata of one cached rectangle of cells: {star, measure, columns, axis predicates, bit key, compound predicates}; precomputes its `SegmentHeader` and hash; `#matches(AggregationKey, Measure)` is the cache probe |
| `SegmentWithData` | `Segment` + `SegmentAxis[]` + `SegmentDataset`: a segment you can actually read cells from (`#getCellValue`) |
| `SegmentAxis` | The sorted actual key values for one constrained column, with a key→offset map used to index the dataset |
| `SegmentBuilder` | Static conversion hub: `Segment`↔`SegmentHeader` (`#toHeader`/`#toSegment`), `SegmentWithData`↔`SegmentBody` (`#addData`); `#rollup` builds a coarser segment in memory from resident segments; hosts the `SegmentConverter` implementations (`StarSegmentConverter`, `SegmentConverterImpl`) that rehydrate cache bodies |
| `SegmentDataset` | Interface for the cell-value store of one segment; iterable as `(CellKey, value)` entries |
| `DenseSegmentDataset` / `DenseNativeSegmentDataset` | Dense-storage abstractions (offset-addressed array) |
| `DenseIntSegmentDataset`, `DenseDoubleSegmentDataset` | Dense primitive arrays (`int[]` / `double[]`) with a null-value `BitSet` |
| `DenseObjectSegmentDataset` | Dense `Object[]` for string/long/arbitrary values |
| `SparseSegmentDataset` | `Map<CellKey, Object>` for sparse populations |
| `AbstractSegmentBody` | Base for the serializable `mondrian.spi.SegmentBody` implementations shipped to external caches |
| `DenseIntSegmentBody`, `DenseDoubleSegmentBody`, `DenseObjectSegmentBody`, `SparseSegmentBody` | Body counterparts of the four dataset shapes |

**The dense/sparse choice** is made in `SegmentLoader`, not `SegmentBuilder`:
while reading the JDBC result, `SegmentLoader` computes the axis-key sets,
multiplies axis sizes into the *possible* cell count `n` (integer overflow
forces sparse), then applies `SegmentLoader#useSparse`: sparse iff
`(possible − SparseSegmentCountThreshold) × SparseSegmentDensityThreshold >
actual` (defaults 1000 and 0.5). `Segment#createDataset` then picks the
concrete class: sparse → `SparseSegmentDataset`; dense → by SQL type
(`INT` → int, `DOUBLE`/`DECIMAL` → double, else object). `SegmentBuilder`
reuses the same `SegmentLoader#useSparse` formula when materializing cache
bodies and rollups. `CellKey` itself lives in `mondrian.rolap`, not here.

## 3. Loading and cache management

| Class | Role |
|---|---|
| `AggregationManager` | Singleton facade (extends `mondrian.rolap.RolapAggregationManager`): owns the `SegmentCacheManager`; `#loadAggregation` drives a batch into segments; `#generateSql` picks `AggQuerySpec` vs `SegmentArrayQuerySpec` (via `#findAgg` aggregate-table matching); `#getCellFromCache`, `#getCacheControl` |
| `Aggregation` | All segments loaded for one `AggregationKey` on a star; `#createSegments` (one `Segment` per measure), `#optimizePredicates` (drops oversized IN-lists per `MaxConstraints`/cardinality), `#load` |
| `SegmentLoader` | Executes the segment SQL and populates datasets: `#createExecuteSql` → `RolapUtil.executeQuery` → `#processData`/`#setDataToSegments`; writes results to the cache index and (off-actor) to external caches (`#cacheSegment`) |
| `GroupingSet` | A set of segments expressible as one `GROUP BY GROUPING SETS` grain |
| `GroupingSetsList` | The detailed grain plus its rollup grains for one combined SQL statement; owns the default axes/columns used while reading rows |
| `SegmentCacheManager` | The actor that owns all shared segment-cache state: command queue (`#execute`), `Handler` (visitor dispatching `Command`s/`Event`s), `Actor` runnable, `cacheExecutor` (external-cache I/O pool), `sqlExecutor` (segment SQL pool), `SegmentCacheIndexRegistry` (per-schema `SegmentCacheIndex`), `#peek`/`PeekCommand` (synchronous first-miss probe), `FlushCommand` (region flush) |
| `SegmentCacheManager.CompositeSegmentCache` | Static inner `SegmentCache` that layers the in-JVM `MemorySegmentCache` over any configured external caches |
| `SegmentCacheWorker` | Guarded wrapper around one `mondrian.spi.SegmentCache`: catches SPI exceptions, forbids long calls on the actor thread(s) |

`GroupingSetsCollector` is in `mondrian.rolap`, not this package.
`MemorySegmentCache` (the default in-JVM store, a
`ConcurrentHashMap<SegmentHeader, SoftReference<SegmentBody>>`) and
`SegmentCacheIndex`/`SegmentCacheIndexImpl` live in `mondrian.rolap.cache`;
the `SegmentCache` SPI, `SegmentHeader`, and `SegmentBody` live in
`mondrian.spi`.

## 4. Predicates

The interfaces — `StarPredicate` (multi-column) and `StarColumnPredicate`
(single-column, extends it) — are in `mondrian.rolap`; every implementation is
here. How they render to WHERE clauses is covered in
[sql-generation](../topics/sql-generation.md).

| Class | Role |
|---|---|
| `AbstractColumnPredicate` | Base implementation of `StarColumnPredicate` (column reference, factory inner class) |
| `ValueColumnPredicate` | `column = value` (also the base carrying a comparable key) |
| `MemberColumnPredicate` | A `ValueColumnPredicate` that remembers which `RolapMember` produced it (needed for cache region flushing by member) |
| `ListColumnPredicate` | Union of predicates on one column — renders as `IN (...)` or `OR` |
| `RangeColumnPredicate` | `column > / >= / < / <= bound` or between two bounds |
| `MemberTuplePredicate` | Multi-column `StarPredicate` constraining a (parent…child) column tuple to a member, or a range above/below/between members |
| `LiteralStarPredicate` | Constant `TRUE`/`FALSE` (`FALSE` marks an unsatisfiable constraint) |
| `ListPredicate` | Base for the two multi-predicate combinators (holds child list + combined bit key) |
| `AndPredicate` | Intersection of child predicates |
| `OrPredicate` | Union of child predicates |
| `MinusStarPredicate` | `plus AND NOT minus` — used when re-constraining segments after partial cache flush |
| `StarPredicates` | Static utilities for comparing/normalizing predicates |

## Neighbors

Classes you will meet constantly in this package's code but that live elsewhere:

| Class | Package | Why it matters here |
|---|---|---|
| `FastBatchingCellReader`, `BatchLoader` (+ `Batch`, `CompositeBatch`, `LoadBatchCommand`, `LoadBatchResponse`) | `mondrian.rolap` | Record requests, group them into batches, and drive this package's loading machinery |
| `RolapAggregationManager` | `mondrian.rolap` | Builds `CellRequest`s from evaluator context (`#makeRequest`) |
| `RolapStar` (+ `.Column`, `.Measure`, `.Table`, `.Bar`), `BitKey`, `CellKey`, `GroupingSetsCollector` | `mondrian.rolap` | Physical star model, bit-position currency, dataset addressing |
| `SegmentHeader`, `SegmentBody`, `SegmentCache`, `SegmentColumn` | `mondrian.spi` | Cache-portable segment identity and payload |
| `SegmentCacheIndex`, `SegmentCacheIndexImpl`, `MemorySegmentCache` | `mondrian.rolap.cache` | Shared index of what is cached/loading; default in-JVM store |
| `AggStar`, `AggTableManager` | `mondrian.rolap.aggmatcher` | Aggregate tables that `AggregationManager#findAgg` matches and `AggQuerySpec` reads |
| `SqlQuery`, `SqlStatement`, `Dialect` | `mondrian.rolap.sql`, `mondrian.spi` | SQL assembly and execution |

## Lifecycle in one paragraph

During an evaluation pass, each cell miss becomes a `CellRequest`
(`RolapAggregationManager#makeRequest`); requests sharing an `AggregationKey`
accumulate into a `BatchLoader.Batch`. At the load phase the batch is answered,
in order of preference, from segments already indexed in the
`SegmentCacheIndex` (including in-memory rollups built by
`SegmentBuilder#rollup`) or by SQL: `Aggregation#createSegments` produces one
`Segment` per measure, `AggregationManager#generateSql` chooses an aggregate
table (`AggQuerySpec`) or the fact table (`SegmentArrayQuerySpec`) — possibly
combining grains via `GroupingSet`s — and `SegmentLoader` executes the
statement, chooses dense or sparse datasets, and emits `SegmentWithData`
objects that are registered in the star's thread-local cache, the shared
index, and the `CompositeSegmentCache`. The full walk-through, including the
sentinel protocol and the `SegmentCacheManager` message choreography, is in
[cell-batching](../topics/cell-batching.md).

## Fork changes in this package

The dominant PATCH is the **multi-threaded `SegmentCacheManager` actor**:
upstream's single actor thread becomes a `Thread[]` (count from the
`mondrian.rolap.agg.SegmentCacheManager.actorThreads` system property), with
`Actor.processingSchemaKeys` guaranteeing at most one in-flight message per
schema — `Message#getSchema` was added for this. Supporting changes:
`SegmentCacheWorker` accepts the thread array for its "don't block the actor"
check, and `SegmentCacheIndexRegistry` uses a `ConcurrentHashMap` instead of a
synchronized map. The only other PATCH here is in
`AbstractQuerySpec#nonDistinctGenerateSql`, which adds the fact table to the
FROM clause first for better ClickHouse join performance. Related PATCHes just
outside the package: a null-member guard in
`RolapAggregationManager#makeCellRequest` (`mondrian.rolap`). Full catalog:
[fork-changes](../topics/fork-changes.md).
