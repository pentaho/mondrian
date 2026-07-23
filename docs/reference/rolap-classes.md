# Class reference: `mondrian.rolap` (core, `rolap.sql`, `rolap.cache`, `rolap.format`)

*Lookup catalog for every type in `mondrian.rolap` and its `sql`, `cache` and
`format` subpackages (the `rolap.agg` / `rolap.aggmatcher` subpackages are
catalogued in [rolap-agg-classes.md](rolap-agg-classes.md)). Grep this file for
a class name; read [../packages/rolap.md](../packages/rolap.md) for the
narrative map of the same classes. Three tiers: Tier 1 = full entry, Tier 2 =
one paragraph, Tier 3 = one line. Fork deviations from upstream Mondrian 9.3
are marked "(fork PATCH)"; the catalog of those is
[../topics/fork-changes.md](../topics/fork-changes.md).*

---

## Tier 1 — core classes

*— Connection and schema realization —*

### RolapConnection

- **Purpose:** The native `Connection` implementation — the object behind every
  olap4j connection. Holds the `DataSource`, the connection's `Role`, the
  shared `RolapSchema` (obtained from `RolapSchemaPool` in the constructor),
  and the connect-string properties.
- **Extends/Implements:** extends `ConnectionBase`.
- **Key collaborators:** `RolapSchemaPool` (schema acquisition),
  `RolapResultShepherd` (`RolapConnection#execute(Execution)` routes every run
  through it), `RolapResult` (built by `RolapConnection#executeInternal`),
  `RolapSchemaReader` (created by `#setRole`).
- **Lifecycle/scope:** one per external connection, until closed. Additionally,
  every `RolapSchema` owns one *internal* `RolapConnection` — the only code
  path where the constructor receives a non-null schema and skips the pool —
  used to parse schema formulas and serve cache control.
- **Threading:** a connection may be shared by threads; execution state lives in
  per-run `Execution`/`RolapResult` objects, not on the connection.
- **Notes/gotchas:** `UseSchemaPool=false` gives this connection a private,
  unshared schema (full load cost, no shared caches).
- **Inner classes:** **`RolapConnection.NonEmptyResult`** (static, extends
  `ResultBase`) — decorator applied by `#executeInternal` per NON EMPTY axis;
  filters out positions whose cells are all empty after evaluation. It is the
  in-memory safety net behind native NON EMPTY pushdown.

### RolapSchema

- **Purpose:** The realized schema: all `RolapCube`s, shared hierarchies,
  parameters, roles, the per-fact-table `RolapStar`s, the schema function
  table, and (transitively) every member cache. Built from catalog XML by
  `RolapSchema#load`.
- **Extends/Implements:** implements `mondrian.olap.Schema`.
- **Key collaborators:** `RolapSchemaPool` (owner), `MondrianDef` (typed XML
  model), `AggTableManager` (aggregate discovery at the end of load),
  `RolapNativeRegistry` (field `nativeRegistry`, native-evaluation entry
  point), the internal `RolapConnection`.
- **Lifecycle/scope:** pooled and shared by all connections with the same
  `SchemaKey`; evicted via `RolapSchemaPool#remove`/`#clear`, which calls
  `RolapSchema#finalCleanUp` (flushes segments, shuts down JDBC metadata
  caches). Loading is the most expensive metadata operation in the engine —
  see [../topics/schema-loading.md](../topics/schema-loading.md).
- **Threading:** effectively immutable metadata after load, plus concurrent
  caches; safe for concurrent query execution.
- **Inner classes:** **`RolapSchema.RolapStarRegistry`** — keeps one
  `RolapStar` per fact relation (`#getOrCreateStar`, synchronized, keyed by
  `RolapUtil#makeRolapStarKey`). **`RolapSchema.RolapSchemaFunctionTable`**
  (static, extends `FunTableImpl`) — layers schema-declared UDFs over
  `GlobalFunTable`/`BuiltinFunTable` for function resolution.

### RolapSchemaPool

- **Purpose:** Process-wide singleton (`RolapSchemaPool#instance`) mapping
  `SchemaKey → RolapSchema` so connections share loaded schemas.
- **Key collaborators:** `SchemaKey`/`ConnectionKey`/`SchemaContentKey` (map
  keys), `mondrian.util.ExpiringReference` (values; soft reference with an
  optional hard pin controlled by the `PinSchemaTimeout` connect property),
  `RolapConnection` (caller).
- **Lifecycle/scope:** static singleton for the JVM's lifetime.
- **Threading (fork PATCH):** both indexes (`mapKeyToSchema` and the
  `UseContentChecksum` index `mapMd5ToSchema`) are `ConcurrentHashMap`s, and
  schema creation is guarded by 100 striped `ReentrantReadWriteLock`s selected
  by key hash (`RolapSchemaPool#getLock`) — two threads racing for the *same*
  missing schema serialize, while different schemas load in parallel (upstream
  used one global lock). `#remove`/`#clear`/`#contains` take no lock.
- **Notes/gotchas:** the whole schema build runs on the connecting thread
  inside the stripe's write lock. With the default soft references a
  quiet schema can be GC'd and silently rebuilt on next use.

### RolapCube

- **Purpose:** `Cube` implementation, built from `<Cube>` or `<VirtualCube>`
  XML. Owns the cube's dimensions (as `RolapCubeDimension`s), the synthetic
  `[Measures]` dimension (measures are ordinary members of its single
  hierarchy), calculated members and named sets, and — for non-virtual cubes —
  the link to a `RolapStar`.
- **Extends/Implements:** extends `CubeBase`.
- **Key collaborators:** `RolapSchema.RolapStarRegistry` (star acquisition),
  `RolapStar.Table#makeMeasure` (via `RolapCube#register`, gives each stored
  measure its star column), `HierarchyUsage`/`RolapCube#registerDimension`
  (star wiring), `RolapCubeUsages` (virtual cubes).
- **Lifecycle/scope:** built during schema load; lives with the schema.
- **Threading:** immutable after load, except role-keyed memo maps.
- **Notes/gotchas:** a virtual cube has `fact == null` (`#isVirtual`) and *no
  star* — it borrows its base cubes' stars through
  `RolapVirtualCubeMeasure`s. If no measure uses the `count` aggregator, a
  hidden `Fact Count` measure is synthesized (`factCountMeasure`).
- **Inner classes:** **`RolapCube.RolapCubeSchemaReader`** (private, extends
  `RolapSchemaReader`) — the cube-scoped reader returned by
  `RolapCube#getSchemaReader(Role)`; adds cube calculated members/named sets
  (role-filtered, memoized per role) and implements `#getMemberByUniqueName`,
  which the schema-level reader leaves null.

### RolapHierarchy

- **Purpose:** `Hierarchy` implementation for the *shared* (schema-level)
  model: the hierarchy's relation, levels, All/null members, default member,
  and the hierarchy's single `MemberReader` (created in `RolapHierarchy#init`
  via `RolapSchema#createMemberReader`).
- **Extends/Implements:** extends `HierarchyBase`.
- **Key collaborators:** `SqlMemberSource`/`SmartMemberReader` (the reader it
  owns), `RolapSchemaReader` (calls `#createMemberReader(Role)` to build the
  role-wrapped stack), `SqlQuery` (`#addToFrom` joins the hierarchy's tables).
- **Lifecycle/scope:** schema lifetime; shared by every cube that uses the
  dimension — which is exactly why member caches are shared too.
- **Threading:** immutable metadata; the reader stack handles its own caching
  concurrency.
- **Notes/gotchas:** `#getDefaultMember` is lazy — first non-hidden root
  member on first call. **(fork PATCH)** `RolapHierarchy#getMemberReader` is
  `public` (Java 17+ reflective access from the mondrian-olap JRuby gem).
- **Inner classes:** **`RolapHierarchy.RolapNullMember`** (the `#null`
  member); **`RolapHierarchy.RolapCalculatedMeasure`** (calculated member that
  is also a `RolapMeasure`, carrying a cell formatter);
  **`RolapHierarchy.LimitedRollupMember`** (extends `RolapCubeMember`) and
  **`RolapHierarchy.LimitedRollupSubstitutingMemberReader`** (a
  `SubstitutingMemberReader`) — the pair implementing partial/hidden rollup
  policies: members are substituted with wrappers whose value aggregates only
  role-accessible children.

*— The metadata model — members —*

### RolapMember / RolapMemberBase

- **Purpose:** `RolapMember` is the interface for members of a
  `RolapHierarchy` (extends `Member` and `RolapCalculation` — every member can
  sit on the evaluator's calculation stack); `RolapMemberBase` is the standard
  implementation: key, parent, ordinal, caption, order key, property map.
- **Extends/Implements:** `RolapMemberBase` extends `MemberBase`, implements
  `RolapMember`.
- **Key collaborators:** `SqlMemberSource`/`SqlTupleReader.MemberBuilder`
  (construction), `MemberCacheHelper` (caching, keyed by `MemberKey`),
  `RolapCubeMember` (per-cube wrapper).
- **Lifecycle/scope:** cached members live as long as the schema (soft
  references in `mapKeyToMember`); uncached ones per query.
- **Threading (fork PATCH):** the property map is a `ConcurrentHashMap`
  (null values boxed, since `ConcurrentHashMap` rejects null) and the property
  getters dropped `synchronized`.
- **Notes/gotchas (fork PATCH):** when
  `mondrian.rolap.compareSiblingsByOrderKey` is set, the order key defaults to
  the member key (`assignOrderKeys`); string order keys are wrapped in
  **`RolapMemberBase.CaseInsensitiveString`** (fork-added inner class,
  `Collator`-based case/accent-insensitive comparison); `#keyToString` avoids
  scientific notation for large numeric keys (MONDRIAN-2703).

*— Query evaluation and results —*

### RolapEvaluator

- **Purpose:** The evaluation context: the current member of *every* hierarchy
  of the cube, a stack of pending `RolapCalculation`s (calculated
  members/tuples ordered by solve order), the active `CellReader`, and
  non-empty/native flags. `#evaluateCurrent` computes the value of the current
  cell context.
- **Extends/Implements:** implements `mondrian.olap.Evaluator`.
- **Key collaborators:** `RolapEvaluatorRoot` (shared per-execution state),
  `FastBatchingCellReader` (via `cellReader`), `Calc` trees (read and override
  the context), `RolapResult` (drives it).
- **Lifecycle/scope:** one root evaluator per execution; context changes are
  cheap to undo via `RolapEvaluator#savepoint` / `#restore`.
- **Threading:** confined to the executing thread.
- **Notes/gotchas:** `#getActiveNativeExpansions` provides cycle protection for
  native-set argument expansion; `nativeEnabled` is seeded from
  `EnableNativeNonEmpty || EnableNativeCrossJoin`. **(fork PATCH)**
  `RolapEvaluator#isDirty` exposes the cell reader's dirty state (used by
  profiling to attribute time per calculated member).

### RolapResult

- **Purpose:** The result of running a query — and the execution algorithm
  itself: the constructor evaluates the slicer, then each axis, then the cells
  in speculative passes (`RolapResult#executeBody` / `#executeStripe`),
  alternating evaluation with batched SQL loading until no cell is missing
  (see [../query-lifecycle.md](../query-lifecycle.md) stage 5).
- **Extends/Implements:** extends `ResultBase`.
- **Key collaborators:** `RolapEvaluator`, `FastBatchingCellReader`
  (`RolapResult#phase` polls `#isDirty` / triggers `#loadAggregations`),
  `RolapAxis` (materialized axes), `CellKey`/`Modulos` (cell addressing),
  `RolapNamedSetEvaluator` (created for WITH SET / schema named sets).
- **Lifecycle/scope:** one per execution; retained by the olap4j `CellSet`
  until it is closed.
- **Threading:** built on the shepherd worker thread; read afterwards by the
  caller.
- **Inner classes:** **`RolapResult.CellInfo`** — one computed cell: value,
  format string, `ValueFormatter`; **`RolapResult.CellInfoContainer`** — the
  cell-store contract, implemented by **`RolapResult.CellInfoMap`** (map-based,
  any dimensionality) and **`RolapResult.CellInfoPool`** (compact pooled
  storage for ≤ 4 axes).

### RolapCell

- **Purpose:** `Cell` implementation over a `RolapResult`: raw value
  (`#getValue`, with the internal null sentinel mapped to null), formatted
  value via the stored `ValueFormatter` and format string, cell properties,
  and drill-through (`RolapCell#drillThroughInternal` produces the SQL listing
  the fact rows behind the cell).
- **Extends/Implements:** implements `mondrian.olap.Cell`.
- **Key collaborators:** `RolapResult` (creates it in `#getCell` from a
  `CellInfo`), `mondrian.rolap.agg.AggregationManager` (drill-through SQL).
- **Lifecycle/scope:** created per access; cheap.
- **Threading:** read-only view; no shared mutable state.

*— The cell-reading bridge —*

### FastBatchingCellReader

- **Purpose:** The batching `CellReader` at the heart of the phase loop. It
  "doesn't really read cells": on a cache miss `FastBatchingCellReader#get`
  records a `CellRequest` (`#recordCellRequest`) and returns the sentinel
  `RolapUtil.valueNotReadyException`, so whole populations of missing cells
  are loaded together between evaluation passes.
- **Extends/Implements:** implements `CellReader`.
- **Key collaborators:** `RolapAggregationManager#makeRequest` (context →
  `CellRequest`), `mondrian.rolap.agg.SegmentCacheManager` (executes the
  loading), `RolapResult#phase` (calls `#isDirty` / `#loadAggregations`).
- **Lifecycle/scope:** one per execution.
- **Threading:** used on the executing thread; loading happens through the
  cache manager's actor.
- **Notes/gotchas:** every `MondrianProperties#CellBatchSize` recorded
  requests it throws `CellRequestQuantumExceededException` to force an early
  SQL round-trip — that exception and `valueNotReadyException` are control
  flow and must never be swallowed.
- **Inner/companion classes:** **`BatchLoader`** (package-private class in
  `FastBatchingCellReader.java`) — runs on the cache-manager actor; groups
  requests into per-`AggregationKey` **`BatchLoader.Batch`**es, probes the
  segment caches, plans rollups from existing segments, and emits SQL loads
  for the rest (see [../topics/cell-batching.md](../topics/cell-batching.md)).

### RolapAggregationManager

- **Purpose:** Abstract bridge between evaluation and the `rolap.agg`
  pipeline: `RolapAggregationManager#makeRequest` converts an evaluator's cell
  context into a `CellRequest` (measure + one predicate per constraining star
  column); `#makeDrillThroughRequest` builds the drill-through variant.
- **Extends/Implements:** abstract; the concrete singleton is
  `mondrian.rolap.agg.AggregationManager`.
- **Key collaborators:** `FastBatchingCellReader` (main caller),
  `RolapStar.Column` / `StarColumnPredicate` (request predicates),
  `RolapCubeLevel` (member → star column mapping).
- **Lifecycle/scope:** stateless static API plus the server-owned singleton.
- **Threading:** static methods, thread-safe.
- **Notes/gotchas (fork PATCH):** `#makeCellRequest` returns a null request
  when a coordinate is the `#null` member (upstream NPE'd later on the missing
  level reader) — a null request means "cell can never have data".

*— The star model —*

### RolapStar

- **Purpose:** The physical star schema: one instance per fact relation,
  modeling the join tree of fact and dimension tables and assigning every
  constrainable column a global bit position (`RolapStar#nextColumnCount`) —
  the coordinate system that `BitKey`s and the whole aggregation layer are
  expressed in.
- **Key collaborators:** `RolapSchema.RolapStarRegistry` (owner),
  `RolapCube#registerDimension` (grafts dimension tables via
  `RolapStar.Table#addJoin`), `RolapCubeLevel` (holds its star column),
  `RolapStatisticsCache`, `mondrian.rolap.agg` (aggregations per star).
- **Lifecycle/scope:** schema lifetime; shared by all cubes over the same fact
  table.
- **Threading:** structure is built during schema load; per-thread cell lookup
  goes through the thread-local `Bar`.
- **Notes/gotchas (fork PATCH):** `RolapStar.Table#addToFrom` adds the parent
  (fact) table to the FROM clause before the dimension table — join order
  matters to ClickHouse's join planner.
- **Inner classes:** **`RolapStar.Table`** — join-tree node (fact root,
  children per joined relation; `#addJoin`, `#findChild`, `#makeColumns`,
  `#makeMeasure`); **`RolapStar.Column`** — one constrainable column: bit
  position, datatype, expression, cardinality; **`RolapStar.Measure`**
  (extends `Column`) — fact column plus its `RolapAggregator`;
  **`RolapStar.Condition`** — a join predicate between parent and child
  table; **`RolapStar.Bar`** — the per-thread ("local bar") list of segments
  used for fast in-thread cell lookup; **`RolapStar.ColumnComparator`** —
  orders columns by name.

### BitKey

- **Purpose:** A compact set-of-bits abstraction over star column bit
  positions — "this request/segment/aggregation constrains exactly these
  columns". Supports and/or/xor, subset tests (`#isSuperSetOf`), and iteration
  over set bits. The currency of cell batching and aggregate matching.
- **Extends/Implements:** interface; `Serializable`, `Comparable<BitKey>`,
  `Iterable<Integer>`.
- **Key collaborators:** `RolapStar.Column#getBitPosition` (bit meaning),
  `mondrian.rolap.agg.AggregationKey`, aggregate-table matching in
  `rolap.aggmatcher`.
- **Lifecycle/scope:** value object, created via `BitKey.Factory#makeBitKey`
  which picks the implementation by required size.
- **Threading:** instances are mutable (`#set`); treat shared instances as
  read-only.
- **Inner classes:** **`BitKey.Factory`**, **`BitKey.AbstractBitKey`**, and
  the three implementations **`BitKey.Small`** (one `long`, ≤ 64 columns),
  **`BitKey.Mid128`** (two `long`s), **`BitKey.Big`** (a `long[]`).

*— Member reading and caching —*

### SqlMemberSource

- **Purpose:** The SQL bottom of every database-backed hierarchy: generates
  and executes member SQL — children (`#makeChildMemberSql`, with
  parent-child variants `#makeChildMemberSqlPC` / `#makeChildMemberSql_PCRoot`),
  level members (delegated to `SqlTupleReader`), level cardinality
  (`#makeLevelMemberCountSql`), whole-hierarchy load (`#makeKeysSql`), and
  key-based lookup (`#getMemberByKey` via `MemberKeyConstraint`).
- **Extends/Implements:** implements `MemberReader` and
  `SqlTupleReader.MemberBuilder` (so bulk tuple reads construct members the
  same way).
- **Key collaborators:** `SqlQuery`/`RolapUtil#executeQuery` (SQL),
  `MemberChildrenConstraint`/`TupleConstraint` (WHERE clauses),
  `MemberCache` (write-back), `AggregationManager#findAgg` (`#chooseAggStar`
  retargets member SQL at aggregate tables).
- **Lifecycle/scope:** one per hierarchy (plus cube-scoped subclasses); schema
  lifetime.
- **Threading:** stateless between calls apart from the injected cache.
- **Notes/gotchas (fork PATCH):** children fetched with order keys are sorted
  by `Sorter#sortSiblingMembers` / `#sortParentChildMembers`; carries naive
  linear-scan fallbacks of the fork's `#getLevelMemberByUniqueKey` /
  `#hasMemberChildren` lookups (the real cached versions live in
  `SmartMemberReader`).

### SmartMemberReader

- **Purpose:** The main member-caching layer: wraps a `MemberSource` with a
  `MemberCacheHelper`, answering children/level/navigation requests from cache
  and delegating misses to SQL, binning fetched children per parent on the way
  back (`#readMemberChildren`).
- **Extends/Implements:** implements `MemberReader` (and exposes its
  `MemberCache` via `#getMemberCache`).
- **Key collaborators:** `SqlMemberSource` (the source underneath),
  `MemberCacheHelper` (the four caches), `SqlConstraintFactory` (constraint
  choice), `RolapCubeHierarchy.CacheRolapCubeHierarchyMemberReader` (subclass).
- **Lifecycle/scope:** one per hierarchy, schema lifetime — caches outlive
  every query.
- **Threading:** cache concurrency handled by `MemberCacheHelper` and the
  fork's `ConcurrentHashMap` lookup caches.
- **Notes/gotchas (fork PATCH):** `#getMembersInLevel` hierarchizes level
  members *before* caching (bounded by
  `MondrianProperties#HierarchizeMaxLevelMembers`), so cached lists are sorted
  once; two fork-added `ConcurrentHashMap` caches back the lookup extensions —
  `levelMembersByUniqueKeyCache` (`#getLevelMemberByUniqueKey`) and
  `levelMemberUniqueNamesWithChildrenCache` (`#hasMemberChildren`) — both
  built lazily from level lists and *not* cleared by
  `MemberCacheHelper#flushCache`.

### MemberCacheHelper

- **Purpose:** Encapsulates member caching — the four maps behind every
  `SmartMemberReader`: `mapKeyToMember` (`MemberKey` → member, a
  `SoftSmartCache`; guarantees member-object uniqueness), `mapLevelToMembers`
  ((level, constraint key) → member list), `mapMemberToChildren` ((parent,
  constraint key) → children list), and `mapParentToNamedChildren` (parent →
  incrementally growing set of name-resolved children, a
  `SmartIncrementalCache`).
- **Extends/Implements:** implements `MemberCache`.
- **Key collaborators:** `SmartMemberReader`/`SqlTupleReader` (writers),
  `SqlConstraint#getCacheKey` (constraint-as-cache-key: entries only hit for
  equal constraints), `DataSourceChangeListener` (`#checkCacheStatus` flushes
  on external change), `CacheControlImpl` (`#removeMember` surgery).
- **Lifecycle/scope:** schema lifetime; never invalidated by time.
- **Threading:** internally synchronized caches (`SmartCache` contract).
- **Notes/gotchas:** `#putChildren` diverts `ChildByNameConstraint` results
  into the named-children map — they are deliberately partial and would poison
  the full children cache; `#getChildrenFromCache` symmetrically consults both
  (see [../topics/member-resolution.md](../topics/member-resolution.md) §4).

### RolapSchemaReader

- **Purpose:** The main `SchemaReader` implementation: pairs a `Role` with a
  `RolapSchema`, filtering every metadata answer (children, level members,
  default members, cardinality) through the role's grants. Schema-scoped:
  cube-level lookups (`#getCalculatedMember`, `#getMemberByUniqueName`) return
  null here and are implemented by `RolapCube.RolapCubeSchemaReader`.
- **Extends/Implements:** implements `SchemaReader`,
  `RolapNativeSet.SchemaReaderWithMemberReaderAvailable`,
  `NameResolver.Namespace`.
- **Key collaborators:** `RolapHierarchy#createMemberReader(Role)` (called by
  `#getMemberReader` to build each hierarchy's role-wrapped reader),
  `RolapNativeRegistry` (`#getNativeSetEvaluator`), `RolapUtil#locusSchemaReader`
  (`#withLocus` proxy for readers used outside a running query).
- **Lifecycle/scope:** one per (role, schema) use site — per connection, per
  cube, per internal use; the readers it caches are shared schema-wide.
- **Threading:** `hierarchyReaders` is a `ConcurrentHashMap` with
  double-checked reader construction.
- **Notes/gotchas (fork PATCH):** implements the fork's
  `#getLevelMemberByUniqueKey` and `#hasMemberChildren` `SchemaReader`
  extensions, threaded down the whole reader chain.

### SqlTupleReader

- **Purpose:** Reads whole member sets in one SQL statement — all members of a
  level, crossjoins of several levels, native set results. One `Target` per
  requested level; `#makeLevelMembersSql` / `#generateSelectForLevels` emit a
  single SELECT for all targets under the driving `TupleConstraint`; streamed
  rows become members, with per-parent children lists written back into the
  member caches mid-stream.
- **Extends/Implements:** implements `TupleReader`; inner private
  `SqlTupleReader.Target` extends `TargetBase` (the top-level `Target` class
  is the deprecated high-cardinality variant's helper).
- **Key collaborators:** `TupleConstraint` (drives the WHERE), `MemberBuilder`
  (from `MemberReader#getMemberBuilder` — constructs members),
  `RolapNativeSet.SetEvaluator` (main driver), `SqlMemberSource`
  (`#getMembersInLevel` delegates here), `#chooseAggStar` (aggregate-table
  retargeting when `TupleConstraint#supportsAggTables`).
- **Lifecycle/scope:** throwaway, one per read.
- **Threading:** single-threaded per instance.
- **Notes/gotchas (fork PATCH):** virtual-cube fixes — constraints are applied
  *before* level columns so level tables join last (except
  `TopCountConstraint`); per-base-cube UNION branches that produced no
  constraint are skipped instead of emitted unconstrained; if no SELECT is
  generated at all, `#sqlForEmptyTuple` is returned.

*— Native set evaluation —*

### RolapNativeSet

- **Purpose:** Shared machinery for set-shaped native evaluation ("analyses
  set expressions and executes them in SQL if possible"): `CrossJoinArg`
  handling, the interpreter-preference cost check (`#isPreferInterpreter`),
  evaluator context override (`#overrideContext` — resets arg hierarchies to
  their All member and pins a stored measure), and the per-instance native
  result cache (`SmartCache<Object, TupleList>`, a `SoftSmartCache` by
  default).
- **Extends/Implements:** extends `RolapNative`; subclasses are
  `RolapNativeCrossJoin`, `RolapNativeFilter`, `RolapNativeTopCount`.
- **Key collaborators:** `CrossJoinArgFactory` (argument recognition),
  `SqlTupleReader` (execution), `RolapSchemaReader` (entry via
  `#getNativeSetEvaluator`). Narrative:
  [../topics/native-evaluation.md](../topics/native-evaluation.md).
- **Lifecycle/scope:** one instance per subclass per schema (inside the
  registry); the result cache is therefore per schema per function family.
- **Threading:** cache is a synchronized `SmartCache`; evaluator state is
  saved/restored around context override.
- **Inner classes:** **`RolapNativeSet.SetConstraint`** (abstract, extends
  `SqlContextConstraint`) — the `TupleConstraint` carried into SQL: context
  predicates from the superclass plus each arg's own constraint; its
  `#getMemberChildrenConstraint` returns null so fact-constrained partial
  children lists are never cached. **`RolapNativeSet.SetEvaluator`**
  (implements `NativeEvaluator`) — drives `SqlTupleReader`, consults/populates
  the result cache (`#executeList`), re-applies role visibility
  (`#filterInaccessibleTuples`), and pads short TopCount results
  (`#setCompleteWithNullValues`).
  **`RolapNativeSet.SchemaReaderWithMemberReaderAvailable`** — the
  `SchemaReader` sub-interface exposing `#getMemberReader(Hierarchy)`, which
  native execution needs.

*— Cache control —*

### CacheControlImpl

- **Purpose:** Implementation of the public `mondrian.olap.CacheControl` API:
  cell-region algebra (`#createMemberRegion`, `#createCrossjoinRegion`,
  `#createUnionRegion`, `#createMeasuresRegion`) and region flushing
  (`#flush(CellRegion)` → segment-cache operations), member-set flushing
  (`#flush(MemberSet)` → member-cache surgery, preceded by
  `RolapNativeRegistry#flushAllNativeSetCache`), member edit commands
  (`#execute(MemberEditCommand)`), and schema eviction (`#flushSchema` /
  `#flushSchemaCache` → `RolapSchemaPool`).
- **Extends/Implements:** implements `CacheControl`.
- **Key collaborators:** `RolapCacheRegion` (compiled flush regions),
  `MemberCacheHelper#removeMember` (member eviction), `SegmentCacheManager`
  (cell flush execution), `RolapSchemaPool` (schema eviction).
- **Lifecycle/scope:** obtained per connection (`Connection#getCacheControl`);
  stateless between calls.
- **Threading:** operations execute under a `Locus`; segment operations are
  serialized through the cache-manager actor.
- **Notes/gotchas:** member flushes drop *every* native-set result for the
  schema — there is no finer-grained native-cache invalidation. Inner
  `MemberEditCommandPlus` implementations (`AddMemberCommand`,
  `DeleteMemberCommand`, `MoveMemberCommand`, `CompoundCommand`, …) update
  member caches in place. What each flush actually clears:
  [../topics/caching.md](../topics/caching.md).

*— SQL assembly —*

### SqlQuery (`rolap.sql`)

- **Purpose:** The dialect-aware SELECT builder every SQL producer in the
  engine fills: clause lists for SELECT/FROM/WHERE/GROUP BY/HAVING/ORDER BY,
  alias management, and rendering (`#toString`, or `#toSqlAndTypes` which
  pairs the SQL with expected `SqlStatement.Type`s per column).
- **Key collaborators:** `mondrian.spi.Dialect` (quoting, syntax capabilities —
  a `SqlQuery` is constructed over a dialect), `RolapStar.Table#addToFrom` /
  `RolapHierarchy#addToFrom` (populate FROM), constraint classes and
  `SqlConstraintUtils` (populate WHERE).
- **Lifecycle/scope:** throwaway; one per generated statement.
- **Threading:** single-threaded builder.
- **Notes/gotchas:** `#isSupported` is the veto flag native evaluation checks —
  generators clear it when a construct (e.g. an oversized `IN` list on a
  dialect without unlimited value lists) cannot be rendered. Inner classes:
  **`SqlQuery.ClauseList`** (a clause's string list) and **`SqlQuery.CodeSet`**
  (picks the best dialect-specific variant of a schema SQL expression, falling
  back to `generic`). See
  [../topics/sql-generation.md](../topics/sql-generation.md).

### SqlConstraintFactory

- **Purpose:** Singleton that chooses the right SQL constraint for member
  reads: `#getMemberChildrenConstraint` (default vs `SqlContextConstraint`),
  `#getLevelMembersConstraint` (default vs context/crossjoin-aware), and
  `#getChildByNameConstraint` (name-narrowed children SQL).
- **Key collaborators:** `SqlContextConstraint#isValidContext` (the gate),
  `MondrianProperties#EnableNativeNonEmpty` (master switch),
  `MondrianProperties#LevelPreCacheThreshold` (below it, small levels are read
  whole under `DefaultTupleConstraint` and cached instead of per-context SQL).
- **Lifecycle/scope:** process singleton (`SqlConstraintFactory#instance`).
- **Threading:** stateless apart from the property read.
- **Notes/gotchas:** this is where `EnableNativeNonEmpty` actually takes
  effect — it gates constrained *member reads*, not the `RolapNative*`
  classes (those are gated by the crossjoin/filter/topcount properties).

---

## Tier 2 — supporting classes

### Connection and schema

**`RolapConnectionProperties`** — enum of the allowed connect-string keywords:
`Provider`, `Jdbc`, `JdbcDrivers`, `JdbcUser`, `JdbcPassword`, `Catalog`,
`CatalogContent`, `DataSource`, `Role`, `UseSchemaPool`, `UseContentChecksum`,
`DynamicSchemaProcessor`, `JdbcConnectionUuid`, `PinSchemaTimeout`, and
friends. The javadoc on each constant is the de facto connect-string
documentation; `RolapConnectionProperties#JDBC_PREFIX` marks pass-through
`jdbc.*` properties.

**`HierarchyUsage`** — records how one hierarchy is used by one cube: the
foreign key joining the fact table, the usage name (a shared dimension used
twice gets two usages), the source dimension, and the optional level
restriction. Created by `RolapCube#createUsages`; consumed by
`RolapCube#registerDimension` when wiring the star.

### The metadata model

**`RolapDimension`** — `Dimension` implementation for the shared (schema-level)
model; owns its `RolapHierarchy`s. Extends `DimensionBase`. The synthetic
`[Measures]` dimension of each cube is also a `RolapDimension` (type
`MeasuresDimension`).

**`RolapLevel`** — `Level` implementation: key, name, ordinal, caption and
parent expressions, level flags (`#isSimple` excludes All and parent-child
levels — a native-evaluation precondition), properties, closure info. Extends
`LevelBase`. **(fork PATCH)** `RolapLevel#getTableName` is `public` for
Java 17+ reflective access from the mondrian-olap JRuby gem.

**`RolapCubeDimension` / `RolapCubeHierarchy` / `RolapCubeLevel` /
`RolapCubeMember`** — the cube-scoped wrapper family: one wrapper per cube
usage of a shared dimension/hierarchy/level/member, carrying per-cube state
(ordinal, usage name, table-alias remapping) while delegating data to the
shared object. `RolapCubeLevel` binds the level to its physical
`RolapStar.Column` (`starKeyColumn` — the same shared level maps to different
columns in different cubes); `RolapCubeMember` (extends
`DelegatingRolapMember`, implements `RolapMemberInCube`) rewrites unique
names. Query evaluation deals almost exclusively in this family.
`RolapCubeHierarchy` owns the cube-member reader — its inner
**`RolapCubeHierarchy.CacheRolapCubeHierarchyMemberReader`** (extends
`SmartMemberReader`) carries *two* cache helpers: `rolapCubeCacheHelper` for
the `RolapCubeMember` wrappers plus the inherited `cacheHelper` for raw
members read under fact-join constraints, which are cube-specific and must not
pollute the shared hierarchy's cache; its `#checkCacheStatus` flushes both.
The inner **`RolapCubeHierarchy.NoCacheRolapCubeHierarchyMemberReader`** is
the non-caching variant, and the inner static
**`RolapCubeHierarchy.RolapCubeSqlMemberSource`** (extends `SqlMemberSource`)
is the member builder that produces properly wrapped cube members for bulk and
native SQL reads. **(fork PATCH)** `RolapCubeHierarchy#getMemberReader` is
`public` (Java 17+ reflective access).

**`RolapMeasure` / `RolapStoredMeasure` / `RolapBaseCubeMeasure` /
`RolapVirtualCubeMeasure`** — the measures family. `RolapMeasure` is the
interface for all measures (exposes the cell `ValueFormatter`);
`RolapStoredMeasure` refines it for SQL-backed measures (fact column
expression, `RolapAggregator`, star measure, containing cube);
`RolapBaseCubeMeasure` (extends `RolapMemberBase`) is a stored measure of a
non-virtual cube; `RolapVirtualCubeMeasure` wraps a stored measure of a base
cube for use in a virtual cube — the route by which a virtual cube reaches its
base cubes' stars. (Calculated measures defined in a hierarchy are
`RolapHierarchy.RolapCalculatedMeasure`.)

**`RolapCalculatedMember`** — a member defined by an MDX `Formula` (query
WITH MEMBER or schema calculated member); extends `RolapMemberBase`. Its
expression is evaluated through the evaluator's calculation stack, never
fetched. **(fork PATCH)** `#getExpression` stamps the member's unique name on
its top-level `ResolvedFunCall` (`timingName`) so profiling can attribute
evaluation time per calculated member.

**`RolapAggregator`** — enumeration-style class (extends
`EnumeratedValues.BasicValue`, implements `mondrian.olap.Aggregator`) of the
aggregation operators: `Sum`, `Count`, `Min`, `Max`, `Avg`, `DistinctCount`.
Each knows its SQL rendering (`#getExpression`) and its rollup/aggregation
behavior; distinct-count is the special case that cannot roll up freely.

### Member readers and access control

**`MemberSource` / `MemberReader` / `MemberCache`** — the three contracts of
the member-reading stack. `MemberSource`: enumerate a hierarchy's members and
children, optionally writing into a `MemberCache`. `MemberReader extends
MemberSource`: adds navigation — parents, siblings, ranges, leads,
`#getMemberBuilder`. `MemberCache`: get/put members by key, children lists and
level lists keyed by constraint. **(fork PATCH)** `MemberReader` carries the
fork's `#getLevelMemberByUniqueKey` / `#hasMemberChildren` additions, threaded
through every implementation.

**`CacheMemberReader`** — `MemberReader` + `MemberCache` for finite,
pre-populated member arrays: it holds all members in memory and never issues
SQL. Used for the `[Measures]` hierarchy (over `MeasureMemberSource`) and for
custom `MemberSource`s that can enumerate everything up-front. (fork PATCH:
implements the two fork lookup methods by array scan.)

**`NoCacheMemberReader`** — cache-bypassing `MemberReader` + `MemberCache` for
high-cardinality or `DisableCaching` hierarchies: same navigation logic as
`SmartMemberReader` but backed by a throwaway `MemberNoCacheHelper`, so every
request is SQL. (fork PATCH: fork lookup methods delegate straight to SQL.)

**`DelegatingMemberReader`** — forwarding base class for `MemberReader`
decorators; redirects every call to an underlying reader. Base of
`RestrictedMemberReader` and `SubstitutingMemberReader`. (fork PATCH: forwards
the two fork lookup methods.)

**`RestrictedMemberReader`** — access-control decorator: filters members and
children through a role's `Role.HierarchyAccess` (grants, top/bottom level
clamps), used when a role's access to the hierarchy is `CUSTOM`. Extends
`DelegatingMemberReader`.

**`SmartRestrictedMemberReader`** — `RestrictedMemberReader` subclass that
additionally caches the computed access decisions per children list; also used
for ragged hierarchies under full access (it handles hidden members).

**`SubstitutingMemberReader`** — decorator that replaces members with
substitutes on the way up and desubstitutes arguments on the way down, so the
caches below only ever see plain members. Generic base of
`RolapHierarchy.LimitedRollupSubstitutingMemberReader` (partial/hidden rollup
policies).

### Evaluation support

**`RolapEvaluatorRoot`** — context shared by a whole tree of evaluators for one
execution: the query, cube, connection and schema reader, every hierarchy's
default member (seeding the initial context), compiled-expression and
expression-result caches (per-execution caching of `Exp` results), and named
set evaluators (`#evaluateNamedSet` / `#evaluateSet`).

**`RolapCalculation` / `RolapMemberCalculation` / `RolapTupleCalculation`** —
the calculation-stack protocol. `RolapCalculation` is the interface for
entries on the evaluator's stack of pending calculations (things that must be
computed before reaching atomic stored cells), ordered by solve order.
`RolapMemberCalculation` wraps a calculated member;
`RolapTupleCalculation` wraps a calculated tuple — a compound context change
over several hierarchies.

**`CellKey`** — coordinate key addressing a cell in the result's cell store:
one axis ordinal per axis. `CellKey.Generator#newCellKey` picks the
implementation by arity — inner classes `Zero`, `One`, `Two`, `Three`, `Many`.
Mutable and reused while `RolapResult#executeStripe` sweeps coordinates; copy
(`#copy`) before storing.

**`RolapAxis`** — `Axis` implementation: an immutable, materialized
`TupleList` of positions produced by axis evaluation.

**`RolapNamedSetEvaluator` / `RolapSetEvaluator`** — lazy, once-per-execution
set evaluation. `RolapNamedSetEvaluator` implements
`Evaluator.NamedSetEvaluator` for WITH SET / schema named sets (materialized
on first use, then iterated); `RolapSetEvaluator` provides the same protocol
for arbitrary set expressions that need stable evaluation within a query
(e.g. `Rank`). Both implement `TupleList.PositionCallback` to track the
current position. **(fork PATCH)** `RolapNamedSetEvaluator#ensureList` checks
cancellation inside its materialization loop, so a runaway named set can be
cancelled.

**`RolapResultShepherd`** — runs each execution on a worker thread from its
`ExecutorService` and polls for timeout/cancellation on a shepherd thread,
bubbling exceptions to the caller promptly (`#shepherdExecution`). Owned by
the server; `RolapConnection#execute` routes through it.

### Native evaluation

**`RolapNative`** — abstract factory for `mondrian.olap.NativeEvaluator`s: the
contract is `#createEvaluator(evaluator, fun, args)` returning null when the
call cannot run in SQL. Owns the enabled flag (`#setEnabled`) and the
test-listener hooks (`RolapNative.Listener`, `#notifyListener`).

**`RolapNativeRegistry`** — composite `RolapNative`, one per `RolapSchema`
(field `RolapSchema.nativeRegistry`): a map from upper-cased function name
(`NONEMPTYCROSSJOIN`, `CROSSJOIN`, `TOPCOUNT`, `FILTER`) to the registered
native implementation, consulted by
`RolapSchemaReader#getNativeSetEvaluator`. Also owns
`#flushAllNativeSetCache`, invoked by `CacheControlImpl#flush(MemberSet)`.

**`RolapNativeCrossJoin` / `RolapNativeFilter` / `RolapNativeTopCount`** — the
three concrete natives, all extending `RolapNativeSet`. CrossJoin computes a
NON EMPTY crossjoin as a fact-joined SQL statement (the only native permitted
against virtual cubes, via per-base-cube UNIONs) and tolerates calculated
members (`restrictMemberTypes()` false); Filter compiles the condition to a
`HAVING` clause; TopCount adds the ranking expression to SELECT/ORDER BY with
a row limit (its `BottomCount` branch is unreachable — no registry entry
routes that name). Filter and TopCount require strict, non-virtual contexts
and reject calculated members in their sets. Gates and fallbacks:
[../topics/native-evaluation.md](../topics/native-evaluation.md) §3.

**`RolapNativeSql`** — translates MDX scalar expressions (Filter conditions,
TopCount rankings) into SQL text where possible: literals, stored measures
(rendered as their aggregate), arithmetic, comparisons, `IIF`, `IsEmpty`,
boolean operators, `MATCHES` on capable dialects. Built from composable inner
`SqlCompiler`s (`CompositeSqlCompiler` tries each in turn); `#compile`
returning null vetoes native evaluation. Entry points:
`#generateFilterCondition`, `#generateTopCountOrderBy`.

**`CrossJoinArg` / `CrossJoinArgFactory` / `DescendantsCrossJoinArg` /
`MemberListCrossJoinArg`** (`rolap.sql`) — the "light constraint"
representation of native set arguments, one per hierarchy: each knows its
`RolapLevel` and how to contribute SQL (`CrossJoinArg#addConstraint`), plus
`#isPreferInterpreter`. `DescendantsCrossJoinArg` covers `level.Members`,
`member.Children` and `Descendants(member, level)`;
`MemberListCrossJoinArg` covers explicit enumerations (an `IN` predicate,
size-capped by `MaxConstraints`). `CrossJoinArgFactory#checkCrossJoinArg`
recognizes the supported input shapes (including membership-predicate
`Filter`s as extra predicate-only args) and
`#buildConstraintFromAllAxes` harvests args from all query axes for plain
crossjoins.

### SQL constraints

**`SqlContextConstraint`** — the non-empty workhorse: implements both
`MemberChildrenConstraint` and `TupleConstraint`, restricting member SQL to
the current evaluation context by joining the fact table and adding one
predicate per context member (via `SqlConstraintUtils#addContextConstraint`).
Its static `#isValidContext` is the central native-evaluation gate (virtual
cubes, calculated-member conflicts, base-cube derivation); its cache key
embeds the expanded context, slicer tuples and base cubes. Base class of
`RolapNativeSet.SetConstraint`. **(fork PATCH)** the `addedConstraint` field
records whether any constraint was actually added (consulted by
`SqlTupleReader`'s virtual-cube SELECT skipping), and
`#findVirtualCubeBaseCubes` adds the cube's default stored measure when a
query references only calculated measures, so a base cube can still be found.

**`DefaultMemberChildrenConstraint`** — the default children constraint: a
stateless singleton adding only the "WHERE parent = X" predicate, no fact
join. Deliberately its own cache key, so all unconstrained children reads
share one cache entry per parent.

**`ChildByNameConstraint`** — extends `DefaultMemberChildrenConstraint`;
narrows the children SQL to specific child name(s) via a
`level-name-column IN (…)` predicate, so resolving `[Store].[USA].[CA]`
fetches one child instead of all siblings. Cache key is
`(class, childNames)`; its partial results are diverted into
`MemberCacheHelper#mapParentToNamedChildren`.

**`DefaultTupleConstraint`** — the no-op `TupleConstraint` singleton:
unrestricted level-member reads (also chosen for small levels under
`LevelPreCacheThreshold`).

**`DescendantsConstraint`** — `TupleConstraint` restricting a level-members
query to descendants of a given parent list; backs native
`Descendants` evaluation.

**`MemberExcludeConstraint`** — `TupleConstraint` excluding an explicit member
list (`NOT IN` / `IS NOT NULL`); used on native paths that must exclude
members, e.g. the second query that pads a short TopCount result.

### SQL execution plumbing

**`RolapUtil`** — package utilities: `#executeQuery` (the single funnel from
`SqlQuery` text to a running `SqlStatement` under the current `Locus`), the
shared loggers (`MDX_LOGGER`, `SQL_LOGGER`, `MONITOR_LOGGER`,
`PROFILE_LOGGER`), the evaluation sentinels `RolapUtil.valueNotReadyException`
(the phase loop's "cell not loaded yet") and `RolapUtil.sqlNullValue` (the
canonical SQL NULL key), `#locusSchemaReader` (dynamic proxy pushing a `Locus`
around every reader call), and `#makeRolapStarKey`.

**`SqlStatement`** — wraps one JDBC statement's lifetime: typed column
accessors (enum `SqlStatement.Type` — `OBJECT`/`STRING`/`INT`/`LONG`/`DOUBLE`,
chosen per column, with `Accessor`s reading the result set), cancellation
hookup via `Locus`/`Execution`, and monitoring events. Inner
`SqlStatement.StatementLocus` identifies the statement in monitoring.
**(fork PATCH)** `#normalizeTemporalValue` converts driver-specific temporal
types (`java.time.LocalDateTime`/`LocalDate` from MySQL Connector/J 8 and
ClickHouse, reflective Oracle `TIMESTAMP`/`DATE`) to `java.sql` types so
downstream code can rely on `java.util.Date` instances.

**`SqlConstraintUtils`** — static helpers that render evaluation context into
`SqlQuery` WHERE conditions: `#addContextConstraint`, member/level constraints
(`#addMemberConstraint`, `#constrainLevel`), calculated-member expansion,
compound-slicer predicates, and `#generateSingleValueInExpr` (which clears
`SqlQuery#isSupported` when an `IN` list would exceed `MaxConstraints` on a
dialect without unlimited value lists). **(fork PATCH)**
`#addContextConstraint` returns whether it added anything (consumed by
`SqlContextConstraint.addedConstraint`), and the fact table is added to FROM
first for join-order-sensitive databases (ClickHouse).

### Caches (`rolap.cache` and friends)

**`SmartCache` / `SmartCacheImpl` / `SoftSmartCache` / `HardSmartCache`**
(`rolap.cache`) — the generic cache primitive under the member and native-set
caches. `SmartCache` defines the API (put/get/remove, thread-safe iteration
via `#execute(SmartCacheTask)`); `SmartCacheImpl` enforces synchronization
with a `ReentrantReadWriteLock` around a subclass-supplied map;
`SoftSmartCache` backs it with soft references (GC-evictable — the default
everywhere) and `HardSmartCache` with a plain map (pins entries; used for
tests).

**`SegmentCacheIndex` / `SegmentCacheIndexImpl`** (`rolap.cache`) — the data
structure that knows which segments contain which cells: `#locate` finds
segment headers matching a cell request's star/columns/values, `#add`
registers headers (including in-flight loads, so concurrent requests for the
same cells converge on one `SlotFuture`), `#loadSucceeded`/`#loadFailed`
complete them, and `#getConverter` recovers segment-body converters. One per
star, owned by the `SegmentCacheManager`; the heart of external segment-cache
lookup. **(fork PATCH)** bookkeeping maps are `ConcurrentHashMap`s and the
thread-safety assertions accept any of the cache manager's actor *threads*
(the fork runs an actor thread array instead of upstream's single thread).

---

## Tier 3 — the long tail

### Connection, schema and cache control

| Type | One-liner |
|---|---|
| `ConnectionKey` | MD5-based `StringKey` identifying a JDBC database connection definition — the connection half of a `SchemaKey` (overridable via the `JdbcConnectionUuid` property). |
| `SchemaContentKey` | MD5-based `StringKey` identifying the schema metadata content (catalog URL, or the catalog string when content is inline/generated) — the other half of a `SchemaKey`. |
| `SchemaKey` | `Pair<SchemaContentKey, ConnectionKey>`: the schema pool's map key; two connections share a schema exactly when both halves match. |
| `RolapConnectionPool` | Singleton Apache-DBCP connection pool used when the connect string gives a JDBC URL rather than an external `DataSource`. (fork PATCH) max active connections derives from the SQL-thread property + 10 instead of a hard-coded 50. |
| `RolapSchemaParameter` | A `<Parameter>` defined in schema XML; implements `Parameter` and `ParameterCompilable`, registers itself with the schema, settable per statement. |
| `RolapCacheRegion` | A region of multidimensional cache space — per-`RolapStar.Column` predicates plus measures — compiled from `CacheControl.CellRegion`s and matched against segments during flush. |
| `ScenarioImpl` | Implementation of org.olap4j `Scenario`: cell writeback — `#setCellValue` records overlays that evaluation applies on top of fact data. |
| `Test` | Leftover developer scratch class with a `main` method; not part of the engine. Ignore it. |

### Metadata and members

| Type | One-liner |
|---|---|
| `ArrayMemberSource` | Abstract `MemberSource` over a fixed in-memory member array — a flat, static hierarchy. |
| `MeasureMemberSource` | `ArrayMemberSource` for the `[Measures]` hierarchy; always wrapped in a `CacheMemberReader` (measures never need SQL). |
| `DelegatingRolapMember` | `RolapMemberBase` subclass delegating all calls to an underlying `RolapMember`; forwarding base for member wrappers such as `RolapCubeMember`. |
| `RolapAllCubeMember` | The 'All' member of a `RolapCubeHierarchy` — a `RolapCubeMember` over the shared hierarchy's All member. |
| `RolapMemberInCube` | `RolapMember` sub-interface for members that know their containing cube (`#getCube`). |
| `RolapProperty` | Definition of a member property: expression, type, formatter. (fork PATCH) internal properties are marked `dependsOnLevelValue`. |
| `RolapCubeUsages` | A virtual cube's `<CubeUsages>`: which base cubes it uses and `#shouldIgnoreUnrelatedDimensions(baseCubeName)`. |
| `MemberKey` | (parent member, key value) identity object used by `MemberCacheHelper#mapKeyToMember`. |
| `MemberNoCacheHelper` | Null-object `MemberCacheHelper` used by `NoCacheMemberReader` — accepts writes and remembers nothing. |
| `SmartIncrementalCache` | `SmartCache` wrapper whose entries are collections that can be *grown* (`#addToEntry`); backs the named-children cache, which accumulates per-name lookups instead of replacing. |
| `SmartMemberListCache` | `SmartCache` of member lists keyed by (object, `SqlConstraint` cache key) — the building block of the children and level-member caches. |

### Evaluation and results

| Type | One-liner |
|---|---|
| `CellReader` | Interface: `#get(RolapEvaluator)` returns the cell value for the current context; implementations are `FastBatchingCellReader` and the agg layer's cache readers. |
| `Modulos` | Ordinal ↔ coordinate-array arithmetic used by `RolapResult`/`CellInfoPool`; `Modulos.Generator` picks a `Zero`/`One`/`Two`/`Three`/`Many` implementation by axis count. |
| `RolapDependencyTestingEvaluator` | Testing evaluator (enabled by `MondrianProperties#TestExpDependencies`) that verifies expressions really depend only on the hierarchies they declare. |
| `RolapProfilingEvaluator` | Evaluator decorator collecting profiling statistics per `Calc`. (fork PATCH) records the `timingName` stamped on calculated-member function calls, attributing time per calculated member. |

### Cell loading and the star

| Type | One-liner |
|---|---|
| `CompoundPredicateInfo` | Converts a compound slicer's tuple list (for one measure) into a `StarPredicate` plus its cache-key string; created for compound-slicer cell requests. |
| `GroupingSetsCollector` | Accumulates `GroupingSet`s when GROUPING SETS-based batch loading is enabled, so several rollups share one SQL statement. |
| `RolapStatisticsCache` | Per-star cache of table/column/query cardinality estimates, delegating to the dialect's `StatisticsProvider`s; used for planning decisions (e.g. `#getRelationCardinality`). |
| `StarPredicate` | Interface: a constraint over one or more `RolapStar.Column`s (AND/OR composition, `BitKey` of touched columns); implementations live in `rolap.agg`. |
| `StarColumnPredicate` | `StarPredicate` refinement constraining exactly one column; the element type of cell-request predicate lists. |

### Tuple reading helpers

| Type | One-liner |
|---|---|
| `TupleReader` | Interface describing `SqlTupleReader`'s protocol: add one target per level (`#addLevelMembers`), then `#readTuples` / `#readMembers`; includes the `MemberBuilder` callback contract. |
| `TargetBase` | Base helper for the SQL tuple readers: one per requested level, accumulates that level's members while rows stream (`#internalAddRow` implemented by subclasses). |
| `Target` | Deprecated helper of `HighCardSqlTupleReader` tracking target levels for the streaming path. |
| `HighCardSqlTupleReader` | Deprecated streaming `SqlTupleReader` variant for high-cardinality levels — reads tuples lazily instead of materializing the list. |
| `ResultLoader` | Deprecated row-iteration helper feeding the streaming (high-cardinality) path's partial lists. |
| `TupleConstraintStruct` | Mutable carrier of (members, disjoint tuple lists) while `SqlConstraintUtils` assembles compound constraints. |
| `StringList` | Tiny comma-separated string builder used by SQL assembly. |

### `rolap.sql` contracts

| Type | One-liner |
|---|---|
| `SqlConstraint` | Root constraint contract: `#getCacheKey` — constraint identity *is* member-cache identity, so equal keys are the only way cached reads are reused. |
| `MemberChildrenConstraint` | `SqlConstraint` for children queries: `#addMemberConstraint` (restrict to parent) and `#addLevelConstraint`; implemented by the `*MemberChildrenConstraint` classes and `SqlContextConstraint`. |
| `TupleConstraint` | `SqlConstraint` for level-member/tuple queries: `#addConstraint`, per-parent `#getMemberChildrenConstraint` for cache write-back, `#getEvaluator`, `#supportsAggTables`. |
| `MemberKeyConstraint` | `TupleConstraint` restricting a level read to particular column values — backs `SqlMemberSource#getMemberByKey`. |
| `SqlQueryChecker` | One-method test hook (`#onGenerate(SqlQuery)`) for inspecting generated SQL. |

### `rolap.cache`

| Type | One-liner |
|---|---|
| `CachePool` | Legacy shell of the old cost-based cache manager; today only counts pinned objects (`#instance`), kept for compatibility. |
| `MemorySegmentCache` | Default in-JVM implementation of the `mondrian.spi.SegmentCache` SPI: a soft-reference map of segment headers to bodies. |

### `rolap.format`

| Type | One-liner |
|---|---|
| `FormatterFactory` | Single point for creating element formatters from schema declarations (class name or script): `#createCellFormatter`, `#createRolapMemberFormatter`, `#createPropertyFormatter`, with defaults when no custom formatter is declared. |
| `FormatterCreateContext` | Immutable parameter object (builder pattern) describing the formatter to create: element name, class/script from XML. |
| `DefaultFormatter` | Default value formatter shared by member and property formatting: numbers rendered without a trailing `.0`. |
| `DefaultRolapMemberFormatter` | Default `MemberFormatter` SPI implementation delegating to `DefaultFormatter`. |
| `PropertyFormatterAdapter` | Adapts `DefaultFormatter` to the `PropertyFormatter` SPI. |
