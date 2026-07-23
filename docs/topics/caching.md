# Caching: What Is Cached Where, and How to Flush It

*Mondrian keeps three distinct families of cache — schema objects, member
lists, and cell/segment data — with different scopes, lifetimes, and flush
APIs. This document is the map across all three; the mechanics of each live in
[cell-batching.md](cell-batching.md), [member-resolution.md](member-resolution.md),
and [schema-loading.md](schema-loading.md).*

The three families are easily confused because they are all "the cache" in
casual conversation, yet flushing one does not flush the others (with one
exception: dropping a schema takes everything below it along). The table below
is the orientation; each family then gets its own section.

| Cache family | What is stored | Scope / owner | Eviction | Flush API |
|---|---|---|---|---|
| **Schema cache** | Whole `RolapSchema` objects (cubes, hierarchies, stars, member caches) | JVM-wide singleton `RolapSchemaPool` | `SoftReference` / `ExpiringReference` (see `PinSchemaTimeout`) | `CacheControl#flushSchemaCache` (all), `CacheControl#flushSchema` (one) |
| **Member caches** | Member objects and member lists per hierarchy (`MemberCacheHelper`) | The owning `RolapSchema` — shared by all connections on that schema | Soft references (`SoftSmartCache`); `DataSourceChangeListener` | `CacheControl#flush(MemberSet)`, member edit commands, dies with the schema |
| **Cell / segment caches** | Aggregated cell data (`Segment` / `SegmentBody`) at three levels: thread-local `RolapStar.Bar`, in-JVM `MemorySegmentCache`, external `SegmentCache` SPI | Thread → JVM → external store; indexed per schema by `SegmentCacheIndex` | Thread-local cleared per query; soft references in JVM; external cache's own policy | `CacheControl#flush(CellRegion)` |

Smaller caches worth knowing, covered at the end: the `mondrian.rolap.cache`
`SmartCache` primitives that the member caches are built from, the native-set
result cache (`RolapNativeSet`), and the per-execution evaluator caches inside
`RolapEvaluatorRoot`.

## 1. The schema cache (`RolapSchemaPool`)

Loading a `RolapSchema` is the most expensive thing the engine does short of
running SQL: it parses the catalog XML, builds every cube, hierarchy, and
`RolapStar`, and scans for aggregate tables. `RolapSchemaPool` (a process
singleton, `RolapSchemaPool#instance`) therefore shares loaded schemas across
connections.

**Keying.** `RolapSchemaPool#get` computes a `SchemaKey` = pair of
`SchemaContentKey` (an MD5 over the effective catalog content — after
`CatalogContent` inlining or `DynamicSchemaProcessor` expansion) and
`ConnectionKey` (identity of the underlying database connection: the
`JdbcConnectionUuid` property if given, otherwise a digest over data source
identity, catalog URL, and JDBC user). Two connections share a schema — and
therefore its member caches and segment index — only when both halves match.
With `UseContentChecksum=true`, lookup goes through a second map keyed by the
content MD5 alone (`RolapSchemaPool#getByChecksum`); with `UseSchemaPool=false`
the pool is bypassed entirely and every connection loads a private schema.

**Retention.** Pool entries are `ExpiringReference<RolapSchema>` — a
`SoftReference` subclass that can additionally pin a hard reference. The
`PinSchemaTimeout` connect-string property controls it: the default `"-1s"`
means plain soft-reference behavior (the GC may drop an idle schema under
memory pressure); `"0"` pins the schema permanently; a positive value like
`"1h"` holds a hard reference that is renewed on every access and released
after the timeout. (fork PATCH) Both pool maps are `ConcurrentHashMap`s and
the upstream single read-write lock is replaced by an array of 100 striped
locks (`RolapSchemaPool#getLock`), so loading one schema does not block
lookups of others.

**Flushing.** `CacheControl#flushSchemaCache` empties the whole pool
(`RolapSchemaPool#clear`); `CacheControl#flushSchema` removes one schema
(`RolapSchemaPool#remove`). Both call `RolapSchema#finalCleanUp` on each
evicted schema, which does two things:

- `RolapSchema#flushSegments` — through the schema's internal connection's
  `CacheControl`, flushes the measures region of every cube. This runs the
  normal cell-region flush path (section 4), so it removes the schema's
  segment headers from the shared `SegmentCacheIndex` *and* deletes the
  bodies from the in-JVM and external segment caches. There is no separate
  "forget segment data" mechanism — schema eviction reuses region flushing.
- `RolapSchema#flushJdbcSchema` — discards the `AggTableManager`'s cached
  JDBC metadata about aggregate tables.

The member caches are not explicitly cleared: they hang off the
`RolapSchema` object and become garbage together with it. The (empty)
per-schema entry in the `SegmentCacheIndexRegistry` remains keyed by
`SchemaKey`; a reload of identical content over the same connection produces
the same key and reuses it.

Consequence to internalize: **flushing a schema flushes everything** — members
and segments die with it — while flushing cells or members never touches the
schema object.

## 2. Member caches

Every hierarchy whose members come from SQL has a `SmartMemberReader` wrapping
its `SqlMemberSource`, and the reader's `MemberCacheHelper` holds four maps
(all soft-reference-based, built on the `SmartCache` primitives):

- `mapKeyToMember` — `MemberKey` (parent + key value) → `RolapMember`;
  guarantees member object uniqueness.
- `mapLevelToMembers` — `(RolapLevel, constraint cache key)` → member list,
  for "all members of a level" reads.
- `mapMemberToChildren` — `(RolapMember, constraint cache key)` → children
  list.
- `mapParentToNamedChildren` — parent → incrementally growing collection of
  children fetched by name (`ChildByNameConstraint`).

The constraint is part of the key: children fetched under a non-empty context
constraint are cached separately from the unconstrained list. (fork PATCH)
`SmartMemberReader` adds two more `ConcurrentHashMap` caches for the
fork-specific unique-key lookups (`SchemaReader#getLevelMemberByUniqueKey`,
`SchemaReader#hasMemberChildren`). Cube hierarchies layer an additional
helper for `RolapCubeMember` wrappers
(`RolapCubeHierarchy.CacheRolapCubeHierarchyMemberReader`). All of this lives
as long as the `RolapSchema` and is shared by every connection and query on
it. Details in [member-resolution.md](member-resolution.md).

**Invalidation paths:**

- `MemberCacheHelper#flushCache` clears all four maps and resets each level's
  cached `approxRowCount`. It is invoked automatically by
  `MemberCacheHelper#checkCacheStatus` when a configured
  `DataSourceChangeListener` (the `DataSourceChangeListener` connect-string
  property) reports `isHierarchyChanged` — the SPI hook for "my dimension
  tables change and I want Mondrian to notice".
- `CacheControl#flush(MemberSet)` surgically evicts members: for each member
  in the set (`CacheControl#createMemberSet`), `CacheControlImpl#flushMember`
  calls `MemberCacheHelper#removeMember`, which drops the member from all
  four maps (including level lists for its level and deeper levels). The
  affected cell regions are then flushed too, and the native-set result cache
  is cleared first (`RolapNativeRegistry#flushAllNativeSetCache`).
- Member edit commands — `CacheControl#createAddCommand`,
  `#createDeleteCommand`, `#createMoveCommand`, `#createSetPropertyCommand`,
  composed with `#createCompoundCommand` and run through
  `CacheControl#execute` — mutate the member caches in place (for slowly
  changing dimensions) and flush the cell regions crossed with every cube's
  measures. They require `MondrianProperties#EnableRolapCubeMemberCache` to
  be `false` and refuse parent-child hierarchies.

## 3. Segment caches — the three levels

Cell data is cached as **segments**: one measure over a cross-product of
constrained column values (see [cell-batching.md](cell-batching.md) for how
they are built). A segment's identity is its `SegmentHeader`, whose fields are
the schema name **and schema content checksum**, cube name, measure name, the
star's fact table alias, the constrained columns with their value sets, the
constrained-columns `BitKey`, the compound-predicate strings, and any excluded
regions accumulated by flushes. Headers are `Serializable` and identical
across connections — and across JVMs sharing an external cache — for the same
schema content.

The three levels:

1. **Thread-local: `RolapStar.Bar`.** Each `RolapStar` keeps a
   `ThreadLocal<Bar>` holding a list of `SoftReference<SegmentWithData>` plus
   a map of in-progress `Aggregation`s. `RolapStar#getCellFromCache` scans it
   lock-free; `RolapStar#register` adds to it. This is a *working cache for
   the current statement*, not a long-lived store:
   `RolapConnection#executeInternal` calls
   `RolapCube#clearCachedAggregations(true)` → `RolapStar#clearCachedAggregations(true)`
   both before executing and in the `finally` block after — and forced
   clearing empties only the calling thread's `Bar`. So the thread-local
   level never carries data from one query to the next; it exists so that the
   hot evaluation loop can read cells without touching any shared structure.

2. **Shared in-JVM: `MemorySegmentCache` + `SegmentCacheIndex`.** Unless
   `DisableLocalSegmentCache` (or `DisableCaching`) is set,
   `SegmentCacheManager#<init>` registers a `MemorySegmentCache` — a
   `ConcurrentHashMap<SegmentHeader, SoftReference<SegmentBody>>`. Soft
   references mean the GC evicts segment bodies under memory pressure;
   `MemorySegmentCache#get` prunes cleared entries. Alongside the data store,
   the `SegmentCacheManager.SegmentCacheIndexRegistry` keeps one
   `SegmentCacheIndexImpl` per `SchemaKey` ((fork PATCH) a
   `ConcurrentHashMap` populated via `computeIfAbsent`): the index knows which
   headers exist, which are currently loading (futures), and answers
   `SegmentCacheIndex#locate` / `#findRollupCandidates` / `#intersectRegion`.
   The index can therefore reference a header whose soft-referenced body has
   been collected; the load path tolerates this by dropping the header from
   the index and falling back to SQL on the next pass.

3. **External: the `SegmentCache` SPI.** `SegmentCacheWorker#initCache`
   instantiates implementations from the `mondrian.rolap.SegmentCache`
   property, Java service discovery (first implementor found), and the
   `SegmentCache.SegmentCacheInjector`. Each cache is wrapped in a
   `SegmentCacheWorker`; all workers (in-JVM and external) are unioned behind
   `SegmentCacheManager.CompositeSegmentCache` — `get` returns the first hit,
   `put`/`remove` go to every worker. External caches may fire events:
   `SegmentCacheManager.AsyncCacheListener` translates `ENTRY_CREATED` /
   `ENTRY_DELETED` from another JVM into actor commands
   (`SegmentCacheManager#externalSegmentCreated` / `#externalSegmentDeleted`)
   that keep the local index in sync. A cache advertising
   `supportsRichIndex()` can store constrained (partially flushed) headers;
   one that does not gets whole-segment removals instead (section 4).

**Read order on a cell miss.** `FastBatchingCellReader#get` tries, in order:

1. the thread-local `Bar` (`AggregationManager#getCellFromCache` deliberately
   checks *only* the local cache — no locks);
2. if this reader has not missed yet (`missCount == 0`), a synchronous
   `SegmentCacheManager#peek`: an actor round-trip that consults the index
   and the composite cache, including bodies still being loaded by other
   statements (futures). A hit is converted to a `SegmentWithData` and
   *promoted into the thread-local `Bar`* via `RolapStar#register`, then
   re-read. The one-shot guard makes warm-cache queries complete in a single
   evaluation pass without paying an actor round-trip per cell;
3. otherwise the request is recorded and the sentinel
   `RolapUtil.valueNotReadyException` is returned; the batch phase takes over.

During the batch phase, `BatchLoader#loadFromCaches` retries the shared level
per request — headers already scheduled, then `SegmentCacheIndex#locate`,
then (if `EnableInMemoryRollup` and the aggregator supports fast aggregation)
`SegmentCacheIndex#findRollupCandidates` to build the segment by rolling up
resident segments instead of SQL — before `BatchLoader#loadFromSql` gives up
and generates SQL. Freshly loaded segments travel the other way: registered
into the thread-local `Bar`, announced to the index
(`SegmentCacheManager#loadSucceeded`), and written to the composite cache —
in-JVM and external — by `SegmentLoader#cacheSegment` off the actor thread.

## 4. Flushing cell regions

The `CacheControl` API (obtained per connection via
`RolapConnection#getCacheControl`, implemented by an anonymous
`CacheControlImpl` subclass in `AggregationManager#getCacheControl`) flushes
cells by *region*:

- Build a region from members and crossjoins: `createMemberRegion`,
  `createMeasuresRegion`, `createCrossjoinRegion`, `createUnionRegion`. A
  flushable region must include the measures dimension
  (`CacheControlImpl#flushInternal` throws otherwise) — flush the crossjoin
  of `createMeasuresRegion(cube)` with your member region to mean "all
  measures".
- `CacheControl#flush(CellRegion)` normalizes the region into a union of
  crossjoins and submits one `SegmentCacheManager.FlushCommand` per crossjoin
  through the actor. The command asks each affected star's index for
  intersecting headers (`SegmentCacheIndex#intersectRegion`), then per header:
  - if the region has no axis constraints (a whole-measure flush), or the
    header cannot represent the exclusion (`SegmentHeader#canConstrain` is
    false — e.g. the flushed values overlap an already-excluded region), the
    header is **removed** from the index and the segment deleted from all
    caches;
  - otherwise the header is **constrained**: `SegmentHeader#constrain`
    produces a new header with the flushed values recorded as an excluded
    region, the index is updated in place, and each cache worker either
    re-keys the stored body under the new header (rich-index caches) or
    simply evicts it (plain caches).

  So flushing a region that partially overlaps a segment does not discard the
  whole segment in a rich-index cache — the untouched cells stay servable,
  and only the excluded region will be re-fetched from SQL. The returned
  `SegmentCacheManager.FlushResult` carries the external-cache tasks, which
  the `CacheControl` implementation executes on the manager's
  `cacheExecutor`.

Do not confuse this with `RolapConnection#executeInternal` calling
`RolapCube#clearCachedAggregations(true)` around every query: as section 3
showed, that clears **only the calling thread's `RolapStar.Bar`** — it is
statement hygiene, not a cache flush, and shared segments survive it.

## 5. Concurrency and consistency

- **All shared segment-index state is owned by the `SegmentCacheManager`
  actor.** Index mutations arrive as commands/events on a queue and are
  processed by dedicated daemon threads, so `SegmentCacheIndexImpl` needs no
  fine-grained locking. (fork PATCH) Upstream's single actor thread is
  replaced by a configurable pool (system property
  `mondrian.rolap.agg.SegmentCacheManager.actorThreads`, default 1, capped at
  100) with the constraint that at most one message per schema
  (`SchemaKey`) is in flight at a time — messages for a busy schema are
  requeued — preserving the per-schema serialization the actor model
  guarantees while letting unrelated schemas proceed in parallel.
- **Readers never block.** The evaluation hot path touches only the
  thread-local `Bar`; shared structures reached outside the actor (pool maps,
  index registry, member caches) are concurrent maps and soft-reference
  caches, several of them (fork PATCH) conversions from upstream synchronized
  maps.
- **Caches are not transactional.** Mondrian assumes the warehouse is stable
  between flushes: once a segment or member list is cached, inserts and
  updates in the underlying database are invisible until you flush the
  affected regions/members, register a `DataSourceChangeListener`, drop the
  schema, or run with caching disabled. Nothing validates cached data against
  the database. The schema content checksum inside `SegmentHeader` protects
  against *schema* drift (a changed catalog yields different headers), not
  against *data* drift.

## 6. Smaller caches worth knowing

- **`SmartCache` family** (`mondrian.rolap.cache`) — the building blocks:
  `SoftSmartCache` (commons-collections `ReferenceMap`, soft keys and
  values), `HardSmartCache` (plain map), used via `SmartMemberListCache` and
  `SmartIncrementalCache` by the member caches, and directly elsewhere.
- **Native set cache** — each `RolapNativeSet` (native NonEmpty / TopCount /
  Filter evaluation) memoizes result `TupleList`s in a `SoftSmartCache` keyed
  by the SQL constraint. Schema-lifetime; cleared by
  `RolapNativeRegistry#flushAllNativeSetCache`, which
  `CacheControl#flush(MemberSet)` invokes before editing members.
- **Per-execution evaluator caches** — `RolapEvaluatorRoot` holds
  `expResultCache`/`tmpExpResultCache` (memoized expression results, see
  `RolapEvaluatorRoot#putCacheResult`; the temporary map is cleared between
  evaluation phases via `RolapEvaluator#clearExpResultCache`), `compiledExps`
  (`RolapEvaluatorRoot#getCompiled`, expressions compiled outside the normal
  query-compile path), and — on `RolapResult.RolapResultEvaluatorRoot` — one
  `RolapNamedSetEvaluator` per WITH SET, so a named set is evaluated once per
  execution. All of these live and die with a single `RolapResult`; they are
  never shared and never need flushing.

## 7. Property reference

Engine-wide properties (`MondrianProperties`, settable in
`mondrian.properties` or as system properties):

| Property | Path | Default | Effect |
|---|---|---|---|
| `DisableCaching` | `mondrian.rolap.star.disableCaching` | `false` | Kills segment caching entirely: no in-JVM cache worker is created, the composite cache's `put` is a no-op, the synchronous `peek` is skipped, `BatchLoader#loadFromCaches` short-circuits, and thread-local aggregations are cleared after each query. |
| `DisableLocalSegmentCache` | `mondrian.rolap.star.disableLocalSegmentCache` | `false` | Omits only the in-JVM `MemorySegmentCache`; external `SegmentCache` SPI caches still work. |
| `SegmentCache` | `mondrian.rolap.SegmentCache` | — | Fully qualified class name of an external `mondrian.spi.SegmentCache` implementation. |
| `EnableInMemoryRollup` | `mondrian.rolap.EnableInMemoryRollup` | `true` | Allows building a needed segment by rolling up resident finer-grained segments instead of running SQL. |
| `SegmentCacheManagerNumberSqlThreads` | `mondrian.rolap.maxSqlThreads` | `100` | Size of the `sqlExecutor` pool that runs segment-populating SQL; exceeding it fails the query rather than queueing. |
| `SegmentCacheManagerNumberCacheThreads` | `mondrian.rolap.maxCacheThreads` | `100` | Size of the `cacheExecutor` pool for external-cache I/O. |
| `EnableRolapCubeMemberCache` | `mondrian.rolap.EnableRolapCubeMemberCache` | `true` | Must be set to `false` before member edit commands (`CacheControl#execute`) are allowed. |

Per-connection properties (connect string, `RolapConnectionProperties`):

| Property | Default | Effect |
|---|---|---|
| `UseSchemaPool` | `true` | If `false`, bypass the schema pool: this connection gets a private, unshared `RolapSchema`. |
| `UseContentChecksum` | `false` | Pool lookup by catalog-content MD5, so a changed catalog is reloaded and an identical one shared even across differing URLs. |
| `PinSchemaTimeout` | `-1s` | Schema retention: negative = plain soft reference, `0` = pinned forever, positive duration (`d/h/m/s/ms`) = hard-pinned, renewed on access. |
| `DataSourceChangeListener` | — | Class name of a `mondrian.spi.DataSourceChangeListener` polled by member and aggregation caches to self-invalidate on data change. |

(fork PATCH) Additionally, the system property
`mondrian.rolap.agg.SegmentCacheManager.actorThreads` (default 1) sets the
number of cache-manager actor threads described in section 5.
