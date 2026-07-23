# Package guide: `mondrian.rolap`

*The engine core: everything between the logical model of `mondrian.olap` and
the SQL that hits the database. This document is the class map for the
package's ~104 top-level classes (≈180 files counting subpackages); the
narratives live in the topic documents —
[schema-loading](../topics/schema-loading.md),
[member-resolution](../topics/member-resolution.md),
[cell-batching](../topics/cell-batching.md),
[native-evaluation](../topics/native-evaluation.md),
[caching](../topics/caching.md), [sql-generation](../topics/sql-generation.md)
— and the execution spine is [query-lifecycle.md](../query-lifecycle.md).*

`mondrian.rolap` implements the interfaces of `mondrian.olap` against a
relational star schema. Almost every abstract type you meet elsewhere in the
engine — `Connection`, `Schema`, `Cube`, `Member`, `Evaluator`, `Result`,
`Cell` — has its one concrete implementation here, prefixed `Rolap`. The
subpackages divide cleanly: `rolap.agg` (cell batching and segments) and
`rolap.aggmatcher` (aggregate-table recognition) have their own guides
([rolap-agg.md](rolap-agg.md), [rolap-aggmatcher.md](rolap-aggmatcher.md));
`rolap.sql`, `rolap.cache`, and `rolap.format` are small and covered briefly
[at the end](#subpackages) of this document.

The clusters below partition the top-level classes: every class in the package
appears in exactly one table. Fork-specific behavior is flagged
"(fork PATCH)"; the full patch catalog is
[topics/fork-changes.md](../topics/fork-changes.md).

## 1. Connection and schema realization

A `RolapConnection` is the native connection object behind every olap4j
connection. On construction it asks the process-wide `RolapSchemaPool` for a
`RolapSchema`, which is loaded once per distinct (schema content, connection
key) pair and shared across connections — schema loading is the most expensive
metadata operation in the engine, and everything else (member caches, stars,
segment indexes) hangs off the shared schema instance. The whole flow is
narrated in [schema-loading.md](../topics/schema-loading.md).

| Class | Role |
|---|---|
| `RolapConnection` | The native `Connection`: holds the `DataSource`, `Role`, and shared `RolapSchema`; `#execute` routes query runs through the `RolapResultShepherd`, then wraps NON EMPTY axes in `NonEmptyResult` |
| `RolapConnectionProperties` | Enum of allowed connect-string keywords (`Jdbc`, `Catalog`, `CatalogContent`, `Role`, `UseSchemaPool`, …) |
| `RolapConnectionPool` | Singleton JDBC connection pool (Apache DBCP) used when the connect string gives a JDBC URL rather than an external `DataSource` |
| `RolapSchema` | The realized schema: cubes, shared hierarchies, roles, the `RolapStarRegistry` (one `RolapStar` per fact table), the function table, and all per-hierarchy member readers/caches |
| `RolapSchemaPool` | Process singleton keeping `SchemaKey → RolapSchema`; concurrent maps + striped locks replace upstream's single lock (fork PATCH) |
| `ConnectionKey` | MD5-based identity of a JDBC connection definition — half of a `SchemaKey` |
| `SchemaContentKey` | MD5-based identity of the schema XML content — the other half |
| `SchemaKey` | `Pair<SchemaContentKey, ConnectionKey>`: the pool's map key |
| `RolapSchemaParameter` | A `<Parameter>` defined in schema XML; compiles to a `ParameterCalc` and is settable per statement |

## 2. The metadata model

Two parallel class families implement the dimensional model, and telling them
apart is essential for reading this package. The **shared** family —
`RolapDimension`, `RolapHierarchy`, `RolapLevel`, `RolapMemberBase` — models a
dimension as declared in the schema, possibly shared by several cubes. The
**cube-scoped** family — `RolapCubeDimension`, `RolapCubeHierarchy`,
`RolapCubeLevel`, `RolapCubeMember` — wraps the shared objects for one
particular cube usage, because the same shared dimension can join to different
fact-table columns in different cubes (or twice in one cube under different
names). Cube levels know their `RolapStar.Column`; cube members carry
cube-specific unique names while delegating data to the underlying shared
member. Query evaluation deals almost exclusively in the cube-scoped family.
Construction order and star wiring are traced in
[schema-loading.md](../topics/schema-loading.md).

| Class | Role |
|---|---|
| `RolapCube` | `Cube` implementation; built from `<Cube>` or `<VirtualCube>` XML; owns dimensions, measures, the synthetic `[Measures]` hierarchy, and (non-virtual only) a `RolapStar` |
| `RolapDimension` / `RolapHierarchy` / `RolapLevel` | The shared dimensional model; `RolapHierarchy#createMemberReader` builds the role-wrapped reader stack, `RolapLevel` holds key/name/ordinal/parent expressions and level flags |
| `RolapCubeDimension` / `RolapCubeHierarchy` / `RolapCubeLevel` | Cube-scoped wrappers; `RolapCubeLevel` binds the level to its star columns; `RolapCubeHierarchy` owns the cube-member reader (`#getMemberReader` made public — fork PATCH) |
| `RolapMember` | Interface for members of a `RolapHierarchy` (extends `Member` and `RolapCalculation`) |
| `RolapMemberBase` | Standard member implementation: key, parent, ordinal, properties (concurrency PATCHes in property map handling — fork PATCH) |
| `DelegatingRolapMember` | Forwarding base class for member wrappers |
| `RolapMemberInCube` | Interface for members that know their cube |
| `RolapCubeMember` | Cube-scoped member: wraps a shared `RolapMember` (extends `DelegatingRolapMember`) and rewrites unique names |
| `RolapAllCubeMember` | The 'All' member of a `RolapCubeHierarchy` |
| `RolapMeasure` | Interface for all measures (stored and calculated); exposes the `ValueFormatter` |
| `RolapStoredMeasure` | Interface for SQL-backed measures: fact column expression, `RolapAggregator`, star measure |
| `RolapBaseCubeMeasure` | Stored measure of a non-virtual cube — a fact column/expression plus aggregator |
| `RolapVirtualCubeMeasure` | Measure of a virtual cube delegating to a stored measure in a base cube |
| `RolapCalculatedMember` | Member defined by an MDX `Formula` (WITH MEMBER or schema calculated member) |
| `RolapProperty` | Definition of a member property (expression, type, formatter) |
| `HierarchyUsage` | How one hierarchy joins one cube's fact table (foreign key, usage name, level restriction) |
| `RolapCubeUsages` | Virtual cube's `<CubeUsages>`: which base cubes it uses and whether unrelated dimensions are ignored |

## 3. Schema reading and access control

`RolapSchemaReader` is the main implementation of the `SchemaReader` façade:
every metadata question the evaluator or name resolution asks ("children of
this member", "default member of this hierarchy") passes through it, filtered
by a `Role`. Role enforcement on member data is done by wrapping the
hierarchy's member reader in access-control decorators when
`RolapSchemaReader#getMemberReader` first touches a hierarchy. The stack and
its behavior are narrated in
[member-resolution.md](../topics/member-resolution.md).

| Class | Role |
|---|---|
| `RolapSchemaReader` | `SchemaReader` over (role, schema); caches one role-wrapped `MemberReader` per hierarchy; carries the fork's `#getLevelMemberByUniqueKey` / `#hasMemberChildren` extensions (fork PATCH) |
| `RestrictedMemberReader` | Decorator filtering members by a role's `HierarchyAccess` (grants, top/bottom level clamps) |
| `SmartRestrictedMemberReader` | `RestrictedMemberReader` subclass that caches access decisions per children list |
| `SubstitutingMemberReader` | Decorator replacing members with substitutes (used for limited-rollup hierarchies) |

`LimitedRollupSubstitutingMemberReader` — the substituting reader used when a
role's rollup policy hides some children — is a private inner class of
`RolapHierarchy`, not a separate file.

## 4. Member reading and caching

Each hierarchy gets a layered stack of `MemberReader` decorators: SQL at the
bottom (`SqlMemberSource`), caching in the middle (`SmartMemberReader` and its
`MemberCacheHelper`), cube wrapping and role wrapping on top. The caches live
on the shared `RolapSchema` and outlive every query. This is the subject of
[member-resolution.md](../topics/member-resolution.md); the interplay with
flushing is in [caching.md](../topics/caching.md).

| Class | Role |
|---|---|
| `MemberSource` | Bottom interface: enumerate a hierarchy's members/children into lists |
| `MemberReader` | `MemberSource` + navigation (siblings, ranges, leads); carries the fork's two lookup extensions (fork PATCH) |
| `MemberCache` | Contract for member caches: get/put member by key, children lists, level lists |
| `SqlMemberSource` | The SQL bottom of every database-backed hierarchy: generates and runs member/children/count SQL; also the `SqlTupleReader.MemberBuilder`; unique-key lookup, `hasMemberChildren`, order-key sorting (fork PATCH) |
| `SmartMemberReader` | The main caching layer: wraps a `MemberSource` with a `MemberCacheHelper`; hierarchizes cached lists and adds unique-key caches on concurrent maps (fork PATCH) |
| `CacheMemberReader` | Reader for finite, pre-populated member arrays (measures, `ArrayMemberSource` hierarchies) — never runs SQL |
| `NoCacheMemberReader` | Cache-bypassing reader for high-cardinality or caching-disabled hierarchies |
| `DelegatingMemberReader` | Forwarding base class for all reader decorators |
| `MemberCacheHelper` | The four member caches: member-by-key, children-by-parent, members-by-level, named-children (all constraint-keyed) |
| `MemberNoCacheHelper` | Null-object `MemberCacheHelper` used by `NoCacheMemberReader` |
| `MemberKey` | (parent, key-value) identity used by the member-by-key cache |
| `SmartMemberListCache` | `SmartCache` of member lists keyed by (object, `SqlConstraint`) — the building block of children/level caches |
| `SmartIncrementalCache` | List cache that can be *grown* incrementally (named-children lookups add entries instead of replacing) |
| `ArrayMemberSource` | Static, flat in-memory hierarchy source |
| `MeasureMemberSource` | `ArrayMemberSource` for the `[Measures]` hierarchy |

The cube-scoped reader layer — `CacheRolapCubeHierarchyMemberReader`,
`NoCacheRolapCubeHierarchyMemberReader`, and `RolapCubeSqlMemberSource` (the
member builder that produces `RolapCubeMember`s) — consists of inner classes
of `RolapCubeHierarchy`.

## 5. Query evaluation and results

`RolapResult` *is* the execution algorithm: its constructor evaluates the
slicer, the axes, and then the cells in speculative passes against a
`RolapEvaluator` (see [query-lifecycle.md](../query-lifecycle.md) stage 5).
The evaluator holds the current member of every hierarchy plus a stack of
pending calculations (`RolapCalculation`s — calculated members and tuples
sorted by solve order), with cheap `#savepoint`/`#restore` state handling.
Per-execution expression caching lives in `RolapEvaluatorRoot#expResultCache`.

| Class | Role |
|---|---|
| `RolapEvaluator` | The evaluation context: current members, calculation stack, cell reader, savepoint/restore; `#evaluateCurrent` computes one cell |
| `RolapEvaluatorRoot` | Shared root of an evaluator tree: schema reader, default members, compiled-expression and expression-result caches |
| `RolapCalculation` | Interface for entries on the evaluator's calculation stack |
| `RolapMemberCalculation` | `RolapCalculation` for a calculated member |
| `RolapTupleCalculation` | `RolapCalculation` for a calculated tuple (compound context change) |
| `RolapProfilingEvaluator` | Evaluator decorator collecting profiling statistics |
| `RolapDependencyTestingEvaluator` | Testing evaluator that verifies declared expression dependencies |
| `RolapResult` | The result: runs the phased evaluate→batch→SQL loop; inner `CellInfo` / `CellInfoMap` / `CellInfoPool` store computed cell values |
| `RolapAxis` | Materialized axis: an immutable `TupleList` of positions |
| `RolapCell` | `Cell` implementation: value, formatted value, drill-through SQL (`#drillThroughInternal`) |
| `CellKey` | Coordinate key (one ordinal per axis) addressing cells in the cell store |
| `Modulos` | Ordinal ↔ coordinate-array arithmetic used by `RolapResult`/`CellInfoPool` |
| `RolapNamedSetEvaluator` | Lazy, once-per-query evaluation of a WITH SET / schema named set (created by `RolapResult`) |
| `RolapSetEvaluator` | Same protocol for arbitrary set expressions needing stable evaluation (e.g. `Rank`) |
| `RolapResultShepherd` | Runs each execution on a worker thread and polls for timeout/cancellation, bubbling exceptions to the caller promptly |
| `ScenarioImpl` | olap4j `Scenario` implementation: cell writeback overlays applied at evaluation time |

`NonEmptyResult`, the decorator that filters all-empty positions off a NON
EMPTY axis after evaluation, is a static inner class of `RolapConnection`.

## 6. The cell-reading bridge

Where evaluation meets data: a `CellReader` answers "value of the current cell
context". The batching implementation never answers a miss directly — it
records a `CellRequest` and lies, returning a sentinel, so that whole
populations of cells can be loaded with few SQL statements between evaluation
passes. The pipeline it feeds is `mondrian.rolap.agg`
([rolap-agg.md](rolap-agg.md)); the narrative is
[cell-batching.md](../topics/cell-batching.md).

| Class | Role |
|---|---|
| `CellReader` | Interface: `#get(RolapEvaluator)` for the current context's cell value |
| `FastBatchingCellReader` | The batching `CellReader`: records `CellRequest`s on miss, returns `RolapUtil.valueNotReadyException`; `#loadAggregations` hands accumulated requests to the cache manager |
| `BatchLoader` | Package-private class in `FastBatchingCellReader.java`: groups requests into per-`AggregationKey` batches on the cache-manager actor; probes caches, plans rollups, emits SQL loads |
| `RolapAggregationManager` | Abstract bridge to the agg pipeline: `#makeRequest` converts an evaluator context into a `CellRequest` (null-member handling — fork PATCH); concrete singleton is `mondrian.rolap.agg.AggregationManager` |
| `CompoundPredicateInfo` | Converts a compound slicer's tuple list into a star predicate + its cache-key string (created by `RolapEvaluator` and `RolapAggregationManager`) |
| `GroupingSetsCollector` | Accumulates `GroupingSet`s when GROUPING SETS-based loading is enabled |

## 7. The star model

`RolapStar` is the physical half of the schema: one per fact table, modeling
the join tree of fact and dimension tables and assigning every constrainable
column a global *bit position*. `BitKey`s — compact bit sets over those
positions — are the currency of the whole aggregation layer: they say "this
segment/aggregation/request constrains exactly these columns" cheaply. See
[cell-batching.md](../topics/cell-batching.md) and
[sql-generation.md](../topics/sql-generation.md).

| Class | Role |
|---|---|
| `RolapStar` | The star: fact table join tree, column registry (bit positions), per-thread `Bar` cell cache; inner classes `Table` (join-tree node), `Column` (one constrainable column), `Measure` (fact column + aggregator), `Condition` (join condition), `Bar` (thread-local segment list) |
| `BitKey` | Set-of-bits abstraction with `Small`/`Mid128`/`Big` implementations chosen by column count |
| `StarPredicate` | Interface: a constraint over one or more star columns (implementations live in `rolap.agg`) |
| `StarColumnPredicate` | `StarPredicate` refinement constraining exactly one column |
| `RolapAggregator` | Enumeration of aggregation operators (sum, count, min, max, avg, distinct-count) with their SQL rendering and rollup rules |
| `RolapStatisticsCache` | Per-star cache of table/column cardinality estimates (via the dialect's statistics providers), used for query planning decisions |

## 8. Native set evaluation and tuple reading

The `RolapNative*` family recognizes set expressions — NON EMPTY crossjoins,
`Filter`, `TopCount` — that can be computed as one constrained SQL statement
instead of in memory, and `SqlTupleReader` executes them (it also serves
plain level-member and children reads issued by `SqlMemberSource`). The
constraint classes here translate evaluation context into SQL WHERE
conditions; their interfaces live in `rolap.sql`. The decision logic,
fallback conditions, and result caching are narrated in
[native-evaluation.md](../topics/native-evaluation.md).

| Class | Role |
|---|---|
| `RolapNative` | Base factory for native evaluators; owns the enable flags and listener hooks |
| `RolapNativeRegistry` | Composite consulted by the crossjoin/count/filter/topcount functions: tries each registered `RolapNative` in turn |
| `RolapNativeSet` | Shared machinery for set-shaped natives: `CrossJoinArg` analysis, the `SetEvaluator`/`SetConstraint` pair, and the native result cache |
| `RolapNativeCrossJoin` | NON EMPTY CrossJoin in SQL |
| `RolapNativeFilter` | `Filter(set, condition)` in SQL (condition becomes HAVING) |
| `RolapNativeTopCount` | `TopCount(set, n, expr)` in SQL (ORDER BY + row limit) |
| `RolapNativeSql` | Translates MDX scalar expressions (the filter condition, topcount ordering) into SQL text, when possible |
| `TupleReader` | Interface describing `SqlTupleReader`'s public protocol (targets, `#readTuples`/`#readMembers`) |
| `SqlTupleReader` | Reads whole member sets/crossjoins in one SQL statement; writes parent/child groups back into member caches; virtual-cube SELECT handling (fork PATCH) |
| `HighCardSqlTupleReader` | Deprecated streaming variant for high-cardinality levels |
| `TargetBase` / `Target` | Per-level accumulators used by the tuple readers while reading rows |
| `ResultLoader` | Row-iteration helper for the streaming (high-cardinality) path |
| `TupleConstraintStruct` | Mutable carrier of (members, disjoint tuple lists) while assembling constraints |
| `SqlConstraintFactory` | Chooses the right constraint for a task: default vs context-aware, honoring the enable properties |
| `SqlContextConstraint` | The workhorse constraint: restricts member SQL to the current evaluation context via fact-table join (base of `RolapNativeSet.SetConstraint`) |
| `DefaultMemberChildrenConstraint` | Plain "WHERE parent = X" children constraint |
| `ChildByNameConstraint` | Children constraint narrowed to specific child name(s) — enables single-member lookup SQL |
| `DefaultTupleConstraint` | No-op tuple constraint (unrestricted level members) |
| `DescendantsConstraint` | Restricts a level-members query to descendants of given parents (native `Descendants`) |
| `MemberExcludeConstraint` | Excludes an explicit member list (used by native filter paths) |

## 9. Cache control and invalidation

The programmatic flush API. `CacheControlImpl` implements
`mondrian.olap.CacheControl`: cell-region flushes (translated to segment-cache
operations), member-set flushes and member edit commands (applied to the
member caches), and whole-schema eviction. The map of what lives in which
cache — and what each flush actually clears — is
[caching.md](../topics/caching.md).

| Class | Role |
|---|---|
| `CacheControlImpl` | `CacheControl` implementation: `CellRegion` algebra and flushing, `MemberSet` flushing, member edit commands, schema cache eviction |
| `RolapCacheRegion` | A region of multidimensional cache space: per-column predicates + measures, produced from `CellRegion`s and matched against segments |

## 10. SQL execution plumbing

Everything that actually runs a statement goes through this small cluster:
producers assemble a `mondrian.rolap.sql.SqlQuery`, then
`RolapUtil#executeQuery` executes it as a `SqlStatement` under the current
`Locus`. See [sql-generation.md](../topics/sql-generation.md).

| Class | Role |
|---|---|
| `RolapUtil` | Package utilities: `#executeQuery`, the SQL logger, and the evaluation sentinels (`valueNotReadyException`) that the phase loop relies on |
| `SqlStatement` | Wraps one JDBC statement's lifetime: typed column accessors, cancellation hookup, monitoring events; JDBC temporal-value normalization (fork PATCH) |
| `SqlConstraintUtils` | Static helpers that render evaluation context into `SqlQuery` WHERE conditions (member constraints, level constraints, calculated-member expansion) |
| `StringList` | Tiny comma-separated string builder used by SQL assembly |

`Test` is a leftover developer scratch class with a `main` method — ignore it.

<a name="subpackages"></a>
## Subpackages in brief

**`rolap.sql`** — SQL text assembly and the constraint contracts.
`SqlQuery` is the dialect-aware SELECT builder every SQL producer fills;
`SqlConstraint` / `MemberChildrenConstraint` / `TupleConstraint` are the
interfaces implemented by the constraint classes of cluster 8 (plus
`MemberKeyConstraint` for key-based lookup); the `CrossJoinArg` family
(`CrossJoinArgFactory`, `DescendantsCrossJoinArg`, `MemberListCrossJoinArg`)
is the "light" representation of set arguments that native evaluation
analyzes. Details: [sql-generation.md](../topics/sql-generation.md) and
[native-evaluation.md](../topics/native-evaluation.md).

**`rolap.cache`** — generic cache primitives and the segment index.
`SmartCache`/`SmartCacheImpl` with `SoftSmartCache` (soft references; the
default backing of all member caches) and `HardSmartCache` (testing);
`SegmentCacheIndex`/`SegmentCacheIndexImpl` (which segments cover which
cells, including in-flight loads — the heart of segment cache lookup, with
concurrency PATCHes); `MemorySegmentCache` (the default in-JVM
`SegmentCache` SPI implementation); `CachePool` (legacy shell). Details:
[caching.md](../topics/caching.md).

**`rolap.format`** — `FormatterFactory` creates member/cell/property
formatters from schema declarations (class name or script), falling back to
`DefaultRolapMemberFormatter`/`DefaultFormatter` and adapting property
formatters via `PropertyFormatterAdapter`. Small and self-contained.

**`rolap.agg`** — the cell/aggregation pipeline (`CellRequest`, `Segment`,
`SegmentCacheManager`, predicates): see [rolap-agg.md](rolap-agg.md).

**`rolap.aggmatcher`** — aggregate-table discovery and matching
(`AggTableManager`, `AggStar`, recognizers): see
[rolap-aggmatcher.md](rolap-aggmatcher.md) and
[aggregate-tables.md](../topics/aggregate-tables.md).

## Where to look when…

| You are asking… | Start at |
|---|---|
| Why does connecting take so long / why did my schema reload? | `RolapSchemaPool#get`, `SchemaKey` — [schema-loading.md](../topics/schema-loading.md) |
| Where does this member/children SQL come from? | `SqlMemberSource#getMemberChildren`, `SqlConstraintFactory` — [member-resolution.md](../topics/member-resolution.md) |
| Why is a member stale / how do I flush members? | `MemberCacheHelper`, `CacheControlImpl#flush(MemberSet)` — [caching.md](../topics/caching.md) |
| Why does a user see (or not see) this member? | `RestrictedMemberReader`, `RolapSchemaReader#getMemberReader` |
| Where is a cell value actually computed? | `RolapResult#executeStripe` → `RolapEvaluator#evaluateCurrent` — [query-lifecycle.md](../query-lifecycle.md) stage 5 |
| Why did evaluation run another pass / what is `valueNotReadyException`? | `FastBatchingCellReader#get`, `RolapResult#phase` — [cell-batching.md](../topics/cell-batching.md) |
| What SQL loads cell data, and why this GROUP BY? | `BatchLoader`, then `rolap.agg` — [rolap-agg.md](rolap-agg.md), [sql-generation.md](../topics/sql-generation.md) |
| Why didn't my NON EMPTY crossjoin / TopCount / Filter run as SQL? | `RolapNativeRegistry`, `RolapNativeSet#isValidContext` — [native-evaluation.md](../topics/native-evaluation.md) |
| What do these bit positions / `BitKey`s mean? | `RolapStar.Column#getBitPosition`, `BitKey` |
| How does a calculated member change what gets evaluated? | `RolapEvaluator`'s calculation stack, `RolapCalculation`, solve order |
| Where do format strings and formatted values come from? | `RolapResult.CellInfo`, `RolapMeasure#getFormatter`, `rolap.format` |
| Why was this query cancelled / timed out? | `RolapResultShepherd`, `mondrian.server.Execution` |
