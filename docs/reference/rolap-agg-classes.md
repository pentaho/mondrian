# Class reference: `mondrian.rolap.agg` and `mondrian.rolap.aggmatcher`

*Lookup catalog for every type in `mondrian.rolap.agg` (cell requests, segments,
loading, caching, predicates) and `mondrian.rolap.aggmatcher` (aggregate table
recognition). Tier 1 classes get full entries, Tier 2 a paragraph, Tier 3 one
table line. Narratives: [cell-batching](../topics/cell-batching.md),
[caching](../topics/caching.md), [aggregate-tables](../topics/aggregate-tables.md),
[sql-generation](../topics/sql-generation.md); package maps:
[rolap-agg](../packages/rolap-agg.md), [rolap-aggmatcher](../packages/rolap-aggmatcher.md).*

---

## Package `mondrian.rolap.agg`

### Requests and batching

### CellRequest

- **Purpose**: The atom of the cell-loading pipeline — the context needed to
  read one cell from a star: one `RolapStar.Measure` plus one
  `StarColumnPredicate` per constrained star column. Built by
  `RolapAggregationManager#makeRequest` from the evaluator's member context;
  recorded by `FastBatchingCellReader` on a cache miss.
- **Extends / implements**: none (plain class; see `DrillThroughCellRequest`
  for the drill-through subclass).
- **Key collaborators**: `RolapStar.Column` (bit positions), `BitKey`
  (`constrainedColumnsBitKey` — the identity currency of the whole subsystem),
  `StarColumnPredicate`/`StarPredicate` (constraints), `AggregationKey` (built
  from a request), `FastBatchingCellReader` / `BatchLoader.Batch`
  (accumulation).
- **Lifecycle / scope**: One instance per missed cell per evaluation pass;
  accumulated in the reader's list until the load phase, then discarded.
- **Threading**: Not thread-safe; confined to the creating query thread.
  Mutation phase (`#addConstrainedColumn`, `#addAggregateList`) must finish
  before the first `#getConstrainedColumns` call — `#check` lazily builds and
  freezes the dense `columnsCache` / `columnBitPositions` arrays.
- **Notes**: Predicates live in `sparseColumnPredicateList`, a sparse array
  indexed by column *bit position* (canonical order without sorting — creating
  cell requests is a documented hotspot). Compound (multi-column) constraints
  go to `compoundPredicateMap`, a lazily created `SortedMap<BitKey,
  StarPredicate>`. `#addConstrainedColumn` sets the `unsatisfiable` flag when
  two different single values land on the same column — such a request denotes
  a provably empty cell and never reaches SQL. Public flags `extendedContext`
  and `drillThrough` alter column handling for drill-through. **(fork PATCH,
  adjacent)** the null-member guard that returns a null request lives in
  `RolapAggregationManager#makeCellRequest` (`mondrian.rolap`), not here.

### AggregationKey

- **Purpose**: Identity of an aggregation workload — which requests may share
  one `Aggregation`/batch: {`RolapStar`, `constrainedColumnsBitKey`, list of
  compound `StarPredicate`s}. Deliberately excludes the measure and the
  predicate *values*.
- **Extends / implements**: none.
- **Key collaborators**: Built from a `CellRequest`
  (`AggregationKey(CellRequest)`); consumed by
  `RolapStar#lookupOrCreateAggregation`, `BatchLoader` (batch map key),
  `Segment` (which reuses `AggregationKey.computeHashCode` for its own
  `matches` probe).
- **Lifecycle / scope**: Transient value object, created per request during
  batching.
- **Threading**: Effectively immutable; the hash code is computed lazily on
  first use (`hashCode == 0` sentinel) — benign race.
- **Notes**: The star is part of the key because two virtual-cube requests can
  have identical bit keys against different base fact tables (see the class
  javadoc). `compoundPredicateList` is kept in `BitKey`-sorted order so
  comparisons and generated SQL are deterministic.

**QuerySpec family** — `QuerySpec` (interface) declares everything needed to
generate one segment-loading SQL statement: the star, the columns and their
predicates, and the measures. `AbstractQuerySpec` implements the shared SQL
assembly on top of `SqlQuery` (`#generateSqlQuery` dispatching to
`#nonDistinctGenerateSql` or `#distinctGenerateSql`); **(fork PATCH)**
`AbstractQuerySpec#nonDistinctGenerateSql` adds the fact table to the FROM
clause first, which improves ClickHouse join performance.
`SegmentArrayQuerySpec` (package-private, extends `AbstractQuerySpec`) is the
normal case: one statement loading a list of segments from the fact table,
optionally with a `GROUP BY GROUPING SETS` clause driven by a
`GroupingSetsList`. `DrillThroughQuerySpec` (extends `AbstractQuerySpec`)
generates the non-aggregated row-listing SQL for drill-through from a
`DrillThroughCellRequest`. `AggQuerySpec` (package-private) is the aggregate
table counterpart — a *parallel implementation*, deliberately **not** an
`AbstractQuerySpec` subclass — reading `FROM` an `AggStar`'s fact table,
joining `DimTable`s only as needed, and in `rollup` mode wrapping each measure
in its rollup aggregator (`AggStar.FactTable.Measure#generateRollupString`)
with a `GROUP BY`. See [sql-generation](../topics/sql-generation.md).

| Type (Tier 3) | One line |
|---|---|
| `DrillThroughCellRequest` | `CellRequest` subclass that additionally lists which columns (`#addDrillThroughColumn`) and measures (`#addDrillThroughMeasure`) to return in the drill-through result set. |
| `CellRequestQuantumExceededException` | Singleton sentinel (`INSTANCE`, private constructor) thrown by `FastBatchingCellReader#recordCellRequest` every `CellBatchSize` requests to abort the evaluation pass and force a load phase; caught around `RolapResult#phase` loops and compared by identity — never wrap or swallow it. |

### Segment model

### Segment

- **Purpose**: The *description* of one cached rectangle of cells: a measure
  over a cross-product of (column, value-set) pairs — {star, measure, columns,
  one `StarColumnPredicate` per axis, `constrainedColumnsBitKey`, compound
  predicate list, excluded regions}. Metadata only; the readable variant is
  `SegmentWithData`.
- **Extends / implements**: none; subclassed by `SegmentWithData`.
- **Key collaborators**: `SegmentBuilder#toHeader` (called from the
  constructor — every segment precomputes its immutable `SegmentHeader`, the
  JVM-independent identity used by the shared index and external caches);
  `Aggregation#createSegments` (creation); `RolapStar#getCellFromCache`
  (probing via `Segment#matches(AggregationKey, RolapStar.Measure)`, which
  reuses the precomputed `aggregationKeyHashCode`).
- **Lifecycle / scope**: Created per measure per batch at load time
  (`Aggregation#createSegments`), or rehydrated from a cached
  header+body (`SegmentBuilder#toSegment`). The `SegmentHeader` outlives it in
  the index and caches.
- **Threading**: Immutable after construction; safely shared.
- **Notes**: Inner interface `Segment.ExcludedRegion` models the rectangular
  "holes" created by partial cache-region flushes — cells physically present
  but to be ignored when reading. `Segment#createDataset(axes, sparse, type,
  size)` picks the concrete `SegmentDataset` (sparse map, or dense
  int/double/object array by SQL type). The `id` field is a debug counter.

### SegmentWithData

- **Purpose**: A `Segment` you can read cells from: adds `SegmentAxis[]` (the
  actual key values per constrained column) and a `SegmentDataset` (the cell
  values). `#getCellValue(Object[] keys)` maps each key through
  `SegmentAxis#getOffset` into a `CellKey` and reads the dataset; a key inside
  the predicate but outside the axis means "covered, no fact rows" →
  `Util.nullValue` (a real null cell, distinct from "wrong segment").
- **Extends / implements**: extends `Segment`.
- **Key collaborators**: Built by `SegmentBuilder#addData(Segment,
  SegmentBody)` and `SegmentLoader#setDataToSegments`; registered into the
  thread-local `RolapStar.Bar` via `RolapStar#register`; converted to a
  serializable `mondrian.spi.SegmentBody` for caching.
- **Lifecycle / scope**: Lives in the thread-local `Bar` as a `SoftReference`
  for the duration of a statement; the durable form is header+body in the
  caches.
- **Threading**: Immutable once constructed (the constructor asserts it never
  wraps another `SegmentWithData`); safe to share.

### SegmentBuilder

- **Purpose**: Static conversion hub between the four segment representations:
  `#toHeader(Segment)` → `SegmentHeader`; `#toSegment(header, ...)` →
  `Segment`; `#addData(Segment, SegmentBody)` → `SegmentWithData`; and
  `#rollup(Map<SegmentHeader, SegmentBody>, ...)` → `Pair<SegmentHeader,
  SegmentBody>`, building a coarser segment in memory by re-aggregating finer
  resident bodies (the in-memory rollup path of
  [cell-batching](../topics/cell-batching.md)).
- **Extends / implements**: none; all static.
- **Key collaborators**: `SegmentHeader`/`SegmentBody` (`mondrian.spi`),
  `SegmentCacheIndex#findRollupCandidates` (supplies rollup inputs),
  `FastBatchingCellReader#findResidentRollupCandidate` (drives `#rollup` on
  the client thread), `SegmentLoader` (shares the `#useSparse` dense/sparse
  formula when materializing rollups).
- **Lifecycle / scope**: Stateless utility class.
- **Threading**: All methods thread-safe (no shared mutable state).
- **Notes**: Hosts the `SegmentBuilder.SegmentConverter` interface — how a
  cached header+body is rehydrated — with two implementations:
  `SegmentConverterImpl` (generic, from a previously seen segment) and
  `StarSegmentConverter` (from a star measure + compound predicates; used when
  registering fresh loads). Private helpers `ExcludedRegionList`,
  `ColumnValues`, and the local class `AxisInfo` support rollup axis math.

**SegmentAxis** — the sorted collection of *actual* key values for one
constrained column of a `SegmentWithData`: the axis `predicate` (never null),
the `Comparable[] keys` array, and a `mapKeyToOffset` map used by
`SegmentWithData#getCellValue` to translate a cell's key into a dataset offset.
Built by `SegmentLoader` from SQL results and by `SegmentBuilder#addData` from
a cached `SegmentBody`'s axis value sets. Immutable after construction.

**SegmentDataset implementations** — the cell-value store of one segment.
`SegmentDataset` (interface, `Iterable<Map.Entry<CellKey, Object>>`) is the
contract; `DenseSegmentDataset` (abstract) addresses a flat array by axis
offsets; `DenseNativeSegmentDataset` (abstract) adds a `BitSet nullValues`
distinguishing genuine nulls from primitive zeros; the concrete classes are
`DenseIntSegmentDataset` (`int[]`), `DenseDoubleSegmentDataset` (`double[]`),
`DenseObjectSegmentDataset` (`Object[]`, for strings/longs/arbitrary values),
and `SparseSegmentDataset` (`Map<CellKey, Object>`, for thinly populated
segments). **Selection heuristic**: `SegmentLoader#useSparse` — sparse iff
`(possibleCount − SparseSegmentCountThreshold) × SparseSegmentDensityThreshold
> actualCount` (property defaults 1000 and 0.5), with integer overflow of the
possible-cell product forcing sparse; `Segment#createDataset` then picks the
concrete dense type from the measure's SQL type. `SegmentBuilder` applies the
same formula when materializing rollups and cache bodies.

| Type (Tier 3) | One line |
|---|---|
| `AbstractSegmentBody` | Base class for the serializable `mondrian.spi.SegmentBody` implementations shipped to segment caches; holds the axis value sets and null-axis flags. |
| `DenseIntSegmentBody` | Body counterpart of `DenseIntSegmentDataset`: dense primitive `int[]` plus a null-value `BitSet`. |
| `DenseDoubleSegmentBody` | Body counterpart of `DenseDoubleSegmentDataset`: dense primitive `double[]` plus a null-value `BitSet`. |
| `DenseObjectSegmentBody` | Body counterpart of `DenseObjectSegmentDataset`: dense `Object[]`. |
| `SparseSegmentBody` | Body counterpart of `SparseSegmentDataset`: the sparse cell map, serialized as parallel key/value arrays. |

### Loading and cache management

### Aggregation

- **Purpose**: The workload container for one `AggregationKey` on a star:
  turns a batch's columns/predicates/measures into segments and drives their
  load. Created via `RolapStar#lookupOrCreateAggregation` during a statement's
  load phase.
- **Extends / implements**: none.
- **Key collaborators**: `AggregationKey` (constructor argument — copies star,
  bit key, compound predicates), `Segment` (private `#createSegments` makes
  one per measure, sorted by measure bit position so grouping-set column order
  is deterministic), `SegmentLoader` (does the SQL), `SegmentCacheManager`
  (passed through `#load`), `MondrianProperties#MaxConstraints`.
- **Lifecycle / scope**: Statement-scoped: lives in the thread-local
  `RolapStar.Bar`'s aggregation map, cleared around each query by
  `RolapStar#clearCachedAggregations`. Records a `creationTimestamp` used by
  cache-flush comparisons.
- **Threading**: Confined to the loading thread; not shared across statements.
- **Notes**: `#optimizePredicates(columns, predicates)` implements the
  "fetch more, filter less" policy: a per-column bloat factor (requested
  values vs. cardinality, or vs. sibling count in the drill-down case,
  computed by the private `ValuePruner`) replaces the costliest `IN`-lists
  with "no constraint" until the estimated result is at most twice the exact
  cell count; any list longer than `MaxConstraints` is always dropped, other
  eliminations only when `OptimizePredicates` is true. Private
  `ConstraintComparator` orders columns for pruning.

### AggregationManager

- **Purpose**: The facade of the whole package: creates the
  `SegmentCacheManager`, drives batch loads, generates segment SQL, matches
  aggregate tables, and hands out `CacheControl`.
- **Extends / implements**: extends `mondrian.rolap.RolapAggregationManager`
  (which owns request construction — `#makeRequest` and friends).
- **Key collaborators**: `SegmentCacheManager` (`cacheMgr`, created in the
  constructor), `Aggregation`/`SegmentLoader` (via static `#loadAggregation`),
  `AggStar` (static `#findAgg` — smallest-first scan of
  `RolapStar#getAggStars` using `AggStar#superSetMatch`/`#select`, with
  `#expandLevelBitKey` adding parent-level bits first), `AggQuerySpec` /
  `SegmentArrayQuerySpec` (static `#generateSql` chooses between them),
  `CacheControlImpl` (`#getCacheControl` returns an anonymous subclass whose
  `flush` routes through the cache manager).
- **Lifecycle / scope**: One per `MondrianServer`
  (`MondrianServerImpl#<init>` calls `new AggregationManager(this)`); the
  deprecated static `#getInstance` delegates to the default
  `MondrianServer.forId(null)`, so embedded use effectively sees a process
  singleton. `#shutdown` stops the cache manager.
- **Threading**: Stateless apart from `cacheMgr`; the static entry points are
  called from actor and client threads alike.
- **Notes**: `#getCellFromCache(request[, pinSet])` deliberately probes *only*
  the thread-local `RolapStar.Bar` (lock-free hot path);
  `#getCellFromAllCaches` is the wider probe used by the first-miss peek.
  `#getDrillThroughSql` builds a `DrillThroughQuerySpec`. `#generateSql`
  consults aggregate tables only when `UseAggregates` is on and the request
  carries no compound predicates.

### SegmentCacheManager

- **Purpose**: Active object owning all *shared* segment-cache state: the
  per-schema `SegmentCacheIndex` registry, the composite of in-JVM and
  external segment caches, and the executors for cache I/O and segment SQL.
  All index mutations arrive as messages on its queue, so the index needs no
  fine-grained locking.
- **Extends / implements**: none.
- **Key collaborators**: `AggregationManager` (creates and owns it),
  `BatchLoader` (`mondrian.rolap` — its `LoadBatchCommand` runs here),
  `SegmentLoader` (submits SQL to `sqlExecutor`, announces `#loadSucceeded`),
  `SegmentCacheWorker` / `MemorySegmentCache` / external
  `mondrian.spi.SegmentCache`s (behind `compositeCache`),
  `SegmentCacheIndexImpl` (`mondrian.rolap.cache`).
- **Lifecycle / scope**: One per `MondrianServer` (created by
  `AggregationManager#<init>`); daemon actor threads run until
  `ShutdownCommand`.
- **Threading**: The actor model — see the sub-entries. Two thread pools hang
  off it for work that must not run on actor threads: `cacheExecutor`
  (external-cache I/O; size `SegmentCacheManagerNumberCacheThreads`, rejection
  → `SegmentCacheLimitReached`) and `sqlExecutor` (segment SQL; size
  `SegmentCacheManagerNumberSqlThreads`, rejection → `SqlQueryLimitReached`).
- **Notes**: `#peek(CellRequest)` is the synchronous first-miss probe
  (a `PeekCommand` round-trip returning a `SegmentWithData`); `#execute`
  submits any `Command<T>`; `#loadSucceeded`/`#loadFailed`/`#remove` post
  events; `#loadCacheForStar` syncs pre-existing external-cache headers
  (tracked in `starFactTablesToSync`) into a star's index on first use.

  **`SegmentCacheManager.Actor`** *(sub-entry)* — private static `Runnable`
  holding the bounded `eventQueue` (`ArrayBlockingQueue`, capacity 1000) and
  the `responseMap` that blocked callers wait on. A message is either a
  `Command<?>` (call, put result/exception in `responseMap`) or an `Event`
  (dispatch to the `Handler` visitor, no response). **(fork PATCH)** Upstream
  runs one actor thread; this fork runs a `Thread[]` (count from system
  property `mondrian.rolap.agg.SegmentCacheManager.actorThreads`, default 1,
  clamped to 1..100). To keep the single-writer index guarantee, `Actor#run`
  claims each message's `SchemaKey` in the `processingSchemaKeys`
  `ConcurrentHashMap` before processing and re-queues messages whose schema is
  already being processed — per-schema serialization, cross-schema
  parallelism. Shutdown fans a `ShutdownCommand` out to the sibling threads
  (guarded by an `AtomicBoolean shuttingDown`).

  **Other inner types** *(sub-entries)* — `Visitor` (public interface over the
  five event types) and `Handler` (the private `Visitor` implementation that
  applies index mutations and emits monitor events); `Message` (marker
  interface; **(fork PATCH)** gained `getSchema()` so the actor can serialize
  per schema); `Command<T>` (abstract message with a `Locus`; **(fork PATCH)**
  its `getSchema()` reads the schema from the locus) and `Event` (abstract
  fire-and-forget message); `FlushCommand` (region flush — intersects headers
  via `SegmentCacheIndex#intersectRegion`, then constrains
  (`SegmentHeader#constrain`) or removes each; returns a `FlushResult` whose
  external-cache tasks the caller runs on `cacheExecutor`); `PeekCommand` /
  `PeekResponse` (first-miss probe); `PrintCacheStateCommand` (diagnostics for
  `CacheControl`); `ShutdownCommand` + private `PleaseShutdownException`
  (poison pill); the five events `SegmentLoadSucceededEvent`,
  `SegmentLoadFailedEvent`, `SegmentRemoveEvent`,
  `ExternalSegmentCreatedEvent`, `ExternalSegmentDeletedEvent` (each **(fork
  PATCH)** implements `getSchema()`); `AsyncCacheListener` (translates another
  JVM's external-cache `ENTRY_CREATED`/`ENTRY_DELETED` events into
  index-updating commands); `SegmentCacheIndexRegistry` (one
  `SegmentCacheIndexImpl` per `SchemaKey`; **(fork PATCH)** a
  `ConcurrentHashMap` via `computeIfAbsent` instead of a synchronized map);
  `AbortException` (public runtime exception used to abort an in-flight
  segment load; `SegmentLoader` checks for it among a failure's causes).

**`SegmentCacheManager.CompositeSegmentCache`** — static inner `SegmentCache`
that unions all `SegmentCacheWorker`s (the in-JVM `MemorySegmentCache` worker
plus any external caches): `get` returns the first hit in worker order,
`put`/`remove` go to every worker, `supportsRichIndex` is true only if all
workers support it. This is the single cache object the rest of the engine
talks to (`SegmentCacheManager.compositeCache`).

### SegmentLoader

- **Purpose**: Executes the segment-populating SQL and turns the result set
  into `SegmentWithData`: `#load` registers each segment's header in the
  `SegmentCacheIndex` as *loading* (with a `StarSegmentConverter`) and submits
  a `SegmentLoadCommand` to the cache manager's `sqlExecutor`; the worker's
  `#loadImpl` runs `#createExecuteSql` → `#processData` (gathering axis value
  sets and rows) → dense/sparse dataset choice (`#useSparse`) →
  `#setDataToSegments`, then `#cacheSegment` writes header+body to the
  composite cache off the actor thread.
- **Extends / implements**: none.
- **Key collaborators**: `SegmentCacheManager` (constructor argument;
  executors, index, `#loadSucceeded` announcement), `GroupingSetsList` (drives
  the optional `GROUP BY GROUPING SETS` statement — more than one
  `GroupingSet` in the list means grouping sets are used),
  `AggregationManager#generateSql` (statement text, aggregate-table choice),
  `RolapUtil#executeQuery`, `SegmentBuilder` (shared sparse formula,
  converters).
- **Lifecycle / scope**: Cheap object created per load call (`new
  SegmentLoader(cacheMgr)`); no retained state beyond the manager reference.
- **Threading**: `#load` runs on the actor thread; the submitted
  `SegmentLoadCommand` runs on an `sqlExecutor` thread; `#cacheSegment`
  submits external-cache writes to `cacheExecutor`.
- **Notes**: Inner types: `SegmentLoadCommand` (the `Callable` unit of SQL
  work), `RowList` (column-typed row buffer with inner `Column` hierarchy —
  `ObjectColumn`, `NativeColumn`, `IntColumn`, `LongColumn`, `DoubleColumn` —
  and a `Handler` interface), `SegmentRollupWrapper`, `BooleanComparator`.
  Skips index registration entirely when `DisableCaching` is set.

**GroupingSet / GroupingSetsList** — `GroupingSet` bundles the segments that
share one GROUP BY grain: the segment list, level and measure `BitKey`s, the
axis predicates and columns (`segment0` is the representative).
`GroupingSetsList` (package-private) assembles the detailed grain plus its
rollup grains for one combined `GROUP BY GROUPING SETS` statement: the first
element must be the detailed (widest) grouping set; it derives the rollup
columns, per-set `BitKey`s, and the grouping-function column indexes that
`SegmentLoader#processData` uses to route each result row to the right
segments. With a single-element list, plain GROUP BY SQL is generated. The
collector that accumulates `GroupingSet`s during a composite batch —
`GroupingSetsCollector` — lives in `mondrian.rolap`, not here.

**SegmentCacheWorker** — guarded wrapper around one `mondrian.spi.SegmentCache`
(final class): catches and logs SPI exceptions so a broken cache cannot break
queries, records `supportsRichIndex` once, and asserts that potentially
long-running cache calls never run on a cache-manager actor thread. **(fork
PATCH)** the guard takes the actor `Thread[]` instead of a single thread.
Static `#initCache` instantiates external caches from the
`mondrian.rolap.SegmentCache` property, Java service discovery, and the
`SegmentCacheInjector`.

**MemorySegmentCache** — *lives in `mondrian.rolap.cache`, not this package*
(listed here because it is this subsystem's default in-JVM store): a
`SegmentCache` backed by a `ConcurrentHashMap<SegmentHeader,
SoftReference<SegmentBody>>`, registered by `SegmentCacheManager#<init>` unless
`DisableLocalSegmentCache`/`DisableCaching` is set. Soft references let the GC
evict bodies under memory pressure; `#get` prunes cleared entries. See
[caching](../topics/caching.md) §3.

### Predicates

The interfaces — `StarPredicate` (multi-column) and `StarColumnPredicate`
(single-column) — live in `mondrian.rolap`; every implementation is here. How
each renders to SQL is in [sql-generation](../topics/sql-generation.md).

**AbstractColumnPredicate** — base implementation of `StarColumnPredicate`:
holds the constrained `RolapStar.Column`, lazily derives the single-bit
`BitKey`, and provides default `and`/`or`/`toString` plumbing. Inner
`AbstractColumnPredicate.Factory` builds value predicates and lists.

**ValueColumnPredicate / MemberColumnPredicate** — `ValueColumnPredicate` is
the workhorse `column = value` constraint (implements `Comparable` so
predicate lists can be sorted into canonical order); `MemberColumnPredicate`
extends it, remembering the `RolapMember` that produced the value — needed by
cache-region flushing, which matches segments by member.

**ListColumnPredicate** — union of predicates on one column; renders as
`column IN (...)` (or `OR` of children when value lists don't suffice) via
`#toSql`, and is what `BatchLoader.Batch#initPredicates` collapses each
column's value set into. Caches child bit keys for fast overlap tests.

**RangeColumnPredicate** — constrains one column to `> / >=` a lower bound
and/or `< / <=` an upper bound (either side may be open); bounds are
`ValueColumnPredicate`s.

**MemberTuplePredicate** — multi-column `StarPredicate` constraining a
(parent, ..., child) column tuple to a member, a range above/below a member,
or a range between two members; built from `RolapMember`s against a base cube.
Inner `Bound` and `RelOp` (private enum) encode the comparison endpoints.

**LiteralStarPredicate** — constant `TRUE`/`FALSE` predicate. `TRUE` means
"column unconstrained" (wildcard in segment axes); `FALSE` marks an
unsatisfiable constraint.

**ListPredicate / AndPredicate / OrPredicate** — `ListPredicate` is the
abstract base for the two multi-predicate combinators, holding the child list
and the union of child bit keys; `AndPredicate` is the intersection,
`OrPredicate` the union (the shape compound slicer predicates take:
`OR(AND(column = value ...))`).

**MinusStarPredicate** — `plus AND NOT minus`: true where its first child
(a `StarColumnPredicate`) holds and its second does not; used when
re-constraining segments after a partial cache flush.

| Type (Tier 3) | One line |
|---|---|
| `StarPredicates` | Static utilities for `StarPredicate`s/`StarColumnPredicate`s; its only method, `#optimize`, is currently disabled by an `&& false` guard and returns the predicate unchanged. |

---

## Package `mondrian.rolap.aggmatcher`

### AggTableManager

- **Purpose**: Orchestrates aggregate-table discovery for one schema:
  snapshot JDBC metadata, annotate each star's fact-table columns with usages,
  run excludes and recognizers over every candidate table, and register
  survivors on their stars.
- **Extends / implements**: none.
- **Key collaborators**: `RolapSchema` (owns exactly one manager and calls
  `#initialize(connectInfo)` at the end of schema load), `JdbcSchema`
  (`makeDB` snapshot; `#bindToStar` marks fact columns `MEASURE` /
  `FOREIGN_KEY`), `ExplicitRules` (per-cube include/exclude declarations),
  `DefaultRules` (naming conventions, only when `ReadAggregates` is on),
  `AggStar#makeAggStar` + `RolapStar#addAggStar` (registration, sorted
  smallest-first by `AggStar#getSize`), `mondrian.recorder.MessageRecorder`.
- **Lifecycle / scope**: One per `RolapSchema`; discovery runs once, in
  `#loadRolapStarAggregates` (gated by `UseAggregates`). `#finalCleanUp` /
  `#removeJdbcSchema` drop the cached JDBC snapshot when the schema dies.
- **Threading**: Runs on the schema-loading thread only.
- **Notes**: Any *error* recorded during recognition fails the whole schema
  load (`AggLoadingExceededErrorCount`) — a name-matched table with bad
  columns is deliberately not skipped silently. Scan scope can be narrowed
  with the `AggregateScanSchema`/`AggregateScanCatalog` connection properties.
  See [aggregate-tables](../topics/aggregate-tables.md).

### AggStar

- **Purpose**: The product of recognition: an aggregate-table mirror of a
  `RolapStar`, used at query time by `AggregationManager#findAgg` (cell loads)
  and `SqlMemberSource`/`SqlTupleReader#chooseAggStar` (member reads). Every
  column reuses the bit position of the corresponding `RolapStar` column, so
  matching is pure `BitKey` algebra.
- **Extends / implements**: none.
- **Key collaborators**: `JdbcSchema.Table.Column.Usage` (the recognized
  column meanings `#makeAggStar` builds from), `RolapStar` (bit positions,
  registration), `AggQuerySpec` (SQL generation against it),
  `AggregationManager` (`#superSetMatch` / `#select` acceptance tests).
- **Lifecycle / scope**: Built once at schema load by the static factory
  `AggStar#makeAggStar(star, dbTable, msgRecorder, approxRowCount)`; lives on
  the star for the schema's lifetime.
- **Threading**: Immutable after construction; shared by all queries.
- **Notes**: Maintains `bitKey` (all columns), `levelBitKey` (levels +
  foreign keys), `measureBitKey`, `foreignKeyBitKey`,
  `distinctMeasureBitKey`; `#getSize` returns row count — or row count × row
  width when `ChooseAggregateByVolume` is set. `hasIgnoredColumns()` (any
  unmatched column) forces rollup and disqualifies the table for
  distinct-count requests. `#isFullyCollapsed` is true when every level column
  lives directly on the aggregate fact table.

  **Inner classes** *(sub-entries; all non-static — instances belong to their
  `AggStar`)* — abstract **`AggStar.Table`**: common base for the two table
  kinds, holding columns, levels, and the child `DimTable` list, with inner
  **`Table.JoinCondition`** (the join to a parent table) and the column
  hierarchy: **`Table.Column`** (name, expression, datatype, and the
  *inherited `RolapStar` bit position*), **`Table.ForeignKey`** (a column
  joining to a dimension table) and **`Table.Level`** (a column carrying a
  hierarchy level, with ordinal/caption/property companions).
  **`AggStar.FactTable`**: the aggregate table itself — loads the fact-count
  column, measures, foreign keys, and levels from recognized usages; for
  non-collapsed levels it registers parent-level columns in
  `levelColumnsToJoin` so `AggQuerySpec` can emit the dimension joins;
  `#makeNumberOfRows` runs `SELECT COUNT(*)` unless `approxRowCount` was
  declared. **`FactTable.Measure`**: a measure column with its aggregator
  converted for the aggregate context — `#getRollupAggregator` and
  `#generateRollupString` produce e.g. `SUM(store_sales_sum)` or
  AvgFromSum's `SUM(sum)/SUM(fact_count)`; per distinct-count measure,
  `#getRollableLevelBitKey` names the levels that can be aggregated away
  without changing the distinct count. **`AggStar.DimTable`**: a dimension
  table that survived aggregation (non-collapsed levels join through it).

### Recognizer

- **Purpose**: Abstract template ("less about defining a type and more about
  code sharing", per its javadoc) that decides whether a candidate aggregate
  table has the required column categories: fact count, measures, foreign
  keys, levels.
- **Extends / implements**: none; subclassed by `DefaultRecognizer` and
  `ExplicitRecognizer`.
- **Key collaborators**: `JdbcSchema.Table` (the fact table and the
  candidate), `MessageRecorder` (all findings; errors abort schema load),
  `RolapAggregator` conversion via `#convertAggregator` (AVG-from-SUM etc.
  through the fact count column).
- **Lifecycle / scope**: One instance per candidate table per discovery run;
  discarded after `#check`.
- **Threading**: Schema-loading thread only.
- **Notes**: `#check` runs the fixed sequence: `checkIgnores` →
  `checkFactColumns` (exactly one numeric fact count, plus optional
  per-measure fact counts) → `checkMeasures` (abstract; then
  `generateImpliedMeasures`) → `checkForeignKeys` (via abstract
  `matchForeignKey`; zero matches = lost/collapsed dimension, multiple = error)
  → `checkLevels` (abstract `matchLevels`) → `checkUnusedColumns` (warn and
  mark ignored — which later forces rollup). Inner `Recognizer.Matcher`
  wraps a column-name matching rule. **(fork PATCH)** the only patch in this
  package: a commons-lang3 import.

**DefaultRecognizer** — the `Recognizer` driven by the naming-convention rules
(`DefaultRules`): measures, foreign keys, and levels are matched purely by
column-name patterns (`${hierarchy_name}_${level_name}`,
`${measure_column_name}_${aggregate_name}`, same-name foreign keys — see
[aggregate-tables](../topics/aggregate-tables.md) for the full template list).
Used only when `ReadAggregates` is on. `#matchLevels` sorts matches by level
depth and enforces contiguity (a matched level whose parent didn't match is an
error); a shallowest match below the hierarchy top makes the level
non-collapsed, which additionally requires unique members.

**ExplicitRecognizer** — the `Recognizer` driven by a schema-declared
`ExplicitRules.TableDef`: column meanings come from `<AggFactCount>`,
`<AggMeasure>`, `<AggLevel>`, `<AggForeignKey>`, `<AggIgnoreColumn>` mappings
rather than name patterns. `#makeMeasure` honors an explicit `rollupType` over
the derived rollup aggregator; collapsed vs non-collapsed is a per-level
declaration.

**DefaultRules** — JVM-wide singleton (`DefaultRules#getInstance`, synchronized)
holding the parsed naming-convention rules from the `AggregateRules` resource
(default classpath `/DefaultRules.xml`; `AggregateRuleTag` selects the active
`<AggRule>`, default `default`). `#matchesTableName` is the first-stage
candidate test (`agg_.+_<fact>` in this tree's rules file); property changes
force a reload on next access. The element model it parses into is the
generated `DefaultDef` (see Tier 3).

**ExplicitRules** — container for a cube's `<AggName>` / `<AggPattern>` /
`<AggExclude>` declarations; static `#excludeTable` and
`#getIncludeByTableDef` run over all groups (an exact `<AggName>` beats an
`<AggPattern>` regex). Inner types: `Group` (per-cube collection, built by
`Group#make`), abstract `TableDef` (the full column mapping, with inner
`Level`, `Measure`, and `RollupType` enum) subclassed by `NameTableDef` and
`PatternTableDef` (patterns carry their own nested excludes; both support
`ignorecase`), and the private `Exclude` interface with `ExcludeName` /
`ExcludePattern` implementations.

**JdbcSchema** — cached per-`DataSource` snapshot of database metadata:
static synchronized `#makeDB` keeps a `SoftReference` map (`dbMap`) so repeated
schema loads reuse the snapshot, `#clearDB` evicts it. Inner `Factory` /
`StdFactory` create instances; inner `Table` and `Table.Column` model the
catalog (`#loadTables` enumerates TABLEs and VIEWs; columns load lazily, only
for tables that pass a name match); `Table.Column.Usage`, typed by the
`UsageType` enum (`FACT_COUNT`, `MEASURE_FACT_COUNT`, `MEASURE`,
`FOREIGN_KEY`, `LEVEL`, `LEVEL_EXTRA`, `IGNORE`, ...), records what a column
was recognized *as* — the interchange format between recognizers and
`AggStar#makeAggStar`. `TableUsageType` marks a table as fact or agg.

**AggGen** — optional DDL suggester (`GenerateAggregateSql` property): for a
cell-load batch's columns, prints `CREATE TABLE` and `INSERT INTO ... SELECT`
statements for two candidate designs — a *lost-dimension* table
(`agg_l_XXX_<fact>`, keeping fact foreign keys) and a *collapsed* table
(`agg_c_XXX_<fact>`, dimension levels folded in). Invoked from
`FastBatchingCellReader.Batch#loadAggregation`; output goes to the log, not
executed. Refuses virtual cubes.

| Type (Tier 3) | One line |
|---|---|
| `DefaultDef` | Generated eigenbase-xom element model for the aggregate rules XML (lives in `src/generated/java/mondrian/rolap/aggmatcher/` beside `aggregates.dtd`); parsed into by `DefaultRules` — do not edit by hand. |
