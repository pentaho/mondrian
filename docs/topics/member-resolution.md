# Member Resolution: Readers, Constraints, and Caches

*How the engine finds members — children of a member, all members of a level, a
member named `[Store].[USA].[CA]` — and how it avoids asking the database twice.
This expands the "where members come from" sidebar of
[../query-lifecycle.md](../query-lifecycle.md) (stage 5); the object-ownership
context is in [../architecture.md](../architecture.md), and construction of the
hierarchies these readers serve is in [schema-loading.md](schema-loading.md).*

Everything here follows one pattern: a per-hierarchy **stack of `MemberReader`
decorators** — SQL at the bottom, caching in the middle, cube- and role-specific
wrapping on top — accessed through the **`SchemaReader` façade**. The caches in
the middle belong to the shared `RolapSchema` and outlive every query.

## 1. The `SchemaReader` façade

`mondrian.olap.SchemaReader` is how the rest of the engine (name resolution,
the evaluator, MDX functions) asks schema questions: "children of this member",
"members of this level", "default member of this hierarchy". The main
implementation is `RolapSchemaReader`, which pairs a `Role` with a
`RolapSchema` — every answer it gives is filtered through that role's access
grants (`RolapSchemaReader#getCubeDimensions`, `#getHierarchyLevels`,
`#getLevelCardinality` all consult the role before touching data).

It comes in three layers:

- **`RolapSchemaReader`** — schema-scoped. Cannot resolve cube-level artifacts:
  `#getCalculatedMember` and `#getMemberByUniqueName` return null at this level.
- **`RolapCube.RolapCubeSchemaReader`** (inner class of `RolapCube`, extends
  `RolapSchemaReader`) — adds the cube's context: calculated members and named
  sets defined in the cube's WITH-formulas (role-filtered, with a per-role
  memo in `RolapCube.roleToAccessibleCalculatedMembers`), and
  `#getMemberByUniqueName` implemented as a compound lookup rooted at the cube.
  Obtained via `RolapCube#getSchemaReader(Role)`.
- **`Query.QuerySchemaReader`** — a thin per-query wrapper that also sees the
  query's own WITH members/sets.

The evaluator's reader is wired in `RolapEvaluatorRoot#<init>`:
`schemaReader = query.getSchemaReader(true)`, where `Query#getSchemaReader`
builds a `QuerySchemaReader` over `cube.getSchemaReader(connectionRole)`. The
same reader also seeds the evaluator with every hierarchy's default member via
`SchemaReader#getHierarchyDefaultMember`.

Two mechanics worth knowing:

- **`#getMemberReader` caching.** `RolapSchemaReader#getMemberReader` builds
  each hierarchy's reader lazily (double-checked locking over the
  `hierarchyReaders` `ConcurrentHashMap`) by calling
  `RolapHierarchy#createMemberReader(role)` — the point where the role wrapper
  (section 2) is applied. The wrapper is cheap and per-reader; the expensive
  cached reader underneath is shared schema-wide.
- **`#withLocus`.** SQL execution requires a `mondrian.server.Locus` on the
  thread. Readers handed out outside a running query — `RolapSchema#getSchemaReader`
  returns `new RolapSchemaReader(defaultRole, this).withLocus()` — are wrapped
  by `RolapUtil#locusSchemaReader`, a dynamic proxy that pushes/pops a Locus
  around every call, so member SQL triggered by metadata browsing still has an
  execution context.

**(fork PATCH)** This fork adds two methods to the `SchemaReader` interface,
threaded through the entire reader chain (`DelegatingSchemaReader`,
`DelegatingMemberReader`, `SmartMemberReader`, `CacheMemberReader`,
`SqlMemberSource`, `RolapSchemaReader`, and the hierarchy classes):

- `SchemaReader#getLevelMemberByUniqueKey(Level, Object)` — find a level member
  by its key value without composing and parsing a unique name;
- `SchemaReader#hasMemberChildren(Member)` — cheap "does this member have at
  least one child" test.

Both are backed by dedicated caches in `SmartMemberReader` (section 4).

## 2. The `MemberReader` decorator stack

Three interfaces define the contract: `MemberSource` (enumerate members,
optionally write into a cache), `MemberReader extends MemberSource` (adds
navigation: children, parent, level members, ranges, lead/lag), and
`MemberCache` (the write-back contract). The concrete stack, bottom-up:

```
SchemaReader  (RolapSchemaReader / RolapCubeSchemaReader — role-scoped façade)
   │   #getMemberReader(hierarchy)         cached per (reader, hierarchy)
   ▼
role wrapper   RestrictedMemberReader / SmartRestrictedMemberReader /
   │           LimitedRollupSubstitutingMemberReader
   │           (only when the role's access is not ALL)
   ▼
cube layer     RolapCubeHierarchy.CacheRolapCubeHierarchyMemberReader
   │           (or NoCache… variant) — wraps members as RolapCubeMember;
   │           owns rolapCubeCacheHelper + inherited cacheHelper
   ▼
cache layer    SmartMemberReader (MemberCacheHelper)
   │           | NoCacheMemberReader (MemberNoCacheHelper)
   │           | CacheMemberReader (finite, fully-loaded sources)
   ▼
SQL            SqlMemberSource — generates and executes member SQL
```

**Bottom: `SqlMemberSource`.** One per hierarchy, created in
`RolapSchema#createMemberReader` (or per cube-hierarchy by the cube layer). It
implements both `MemberReader` and `SqlTupleReader.MemberBuilder` — the latter
so the bulk tuple reader (section 5) can construct `RolapMember`s the same way.

**Cache layer — which variant, chosen where.** `RolapHierarchy#init` calls
`RolapSchema#createMemberReader(sharedName, hierarchy, memberReaderClass)`,
which decides:

- a custom `memberReaderClass` from the schema XML is instantiated directly
  (if it only implements `MemberSource`, it is wrapped in `CacheMemberReader`);
- a high-cardinality dimension, or `MondrianProperties#DisableCaching`, gets a
  **`NoCacheMemberReader`** — same navigation logic but a throwaway
  `MemberNoCacheHelper`, so every request is SQL (caching members while the
  cell cache is disabled would give inconsistent results);
- otherwise the normal case: **`SmartMemberReader`** over the
  `SqlMemberSource`, with a `MemberCacheHelper` the source writes back into.

**`CacheMemberReader`** is the third variant: for sources that can enumerate
all members up-front it holds the complete member array in memory and never
issues SQL. The measures hierarchy always uses it — the regular-cube
constructor installs `new CacheMemberReader(new MeasureMemberSource(...))`.

Shared dimensions share this layer naturally: all cubes using a shared
dimension hold the same `RolapHierarchy` object, whose single `memberReader`
(and thus member cache) was created once in `RolapHierarchy#init`.

**Cube layer — why it exists.** Queries deal in `RolapCubeMember`s: per-cube
wrappers of the underlying `RolapMember`s, because the same shared dimension
can appear in a cube under a different name (`[Time]` used as `[Ship Time]`)
and members must carry cube-specific unique names and levels.
`RolapCubeHierarchy#<init>` builds a
`CacheRolapCubeHierarchyMemberReader` — or the `NoCache…` variant when the
dimension is high-cardinality or `MondrianProperties#EnableRolapCubeMemberCache`
is off. The cache variant extends `SmartMemberReader` and therefore carries
**two** cache helpers:

- `rolapCubeCacheHelper` — caches the `RolapCubeMember` wrappers;
- the inherited `cacheHelper` — caches raw `RolapMember`s read with a
  fact-table join (i.e. under a `SqlContextConstraint`), which are cube-specific
  and must not pollute the shared hierarchy's cache.

Plain (non-context) reads delegate to the shared hierarchy's own reader
(`rolapHierarchy.getMemberReader()`), so the shared cache still does the heavy
lifting; the cube layer wraps results via
`CacheRolapCubeHierarchyMemberReader#lookupCubeMember`. Its
`#getMemberBuilder` returns a `RolapCubeSqlMemberSource` so native set SQL
(section 5) also produces properly wrapped cube members. **(fork PATCH)**
`RolapCubeHierarchy#getMemberReader` is public in this fork.

**Top: role wrappers.** `RolapHierarchy#createMemberReader(Role)` inspects the
role's access to the hierarchy:

- `ALL` — the raw reader, no wrapper (ragged hierarchies still get a
  `SmartRestrictedMemberReader`, which also handles hidden members);
- `CUSTOM` with rollup policy `FULL` — `SmartRestrictedMemberReader`, a
  `RestrictedMemberReader` (itself a `DelegatingMemberReader`) that filters
  children through `Role.HierarchyAccess` and additionally caches the computed
  access per children list;
- `CUSTOM` with `PARTIAL` or `HIDDEN` — `LimitedRollupSubstitutingMemberReader`
  (a `SubstitutingMemberReader`), which replaces members needing limited rollup
  with `RolapHierarchy.LimitedRollupMember`s whose value is a synthesized
  `$RollupAccessibleChildren()` expression aggregating only accessible children
  (a constant null for `HIDDEN`). `SubstitutingMemberReader` is the generic
  base: it desubstitutes arguments on the way down and substitutes results on
  the way up, so the caches below only ever see plain members.

## 3. SQL member fetch: `SqlMemberSource` and constraints

`SqlMemberSource#getMemberChildren` funnels into
`SqlMemberSource#getMemberChildren2`, which picks the SQL shape:

- parent level is parent-child → `SqlMemberSource#makeChildMemberSqlPC`
  (WHERE parent-column = parent key);
- child level is parent-child and parent is the All member →
  `SqlMemberSource#makeChildMemberSql_PCRoot` (root rows of the parent-child
  table, honoring the configured null-parent value);
- ordinary level → `SqlMemberSource#makeChildMemberSql`.

`makeChildMemberSql` selects the child level's key, ordinal, caption, and
property expressions (joining the hierarchy's dimension tables via
`RolapHierarchy#addToFrom`), and delegates the WHERE clause to the
**constraint**:

- `MemberChildrenConstraint#addMemberConstraint` — restricts to the parent
  member (`SqlConstraintUtils#addMemberConstraint`);
- `MemberChildrenConstraint#addLevelConstraint` — in non-empty context, joins
  the level to the fact table so only children with data return.

Level members go through `SqlMemberSource#getMembersInLevel`, which delegates
to a `SqlTupleReader` (section 5) even for a single level; level cardinality
uses `SqlMemberSource#getLevelMemberCount` →
`SqlMemberSource#makeLevelMemberCountSql` (`count(distinct …)`, or a wrapped
`count(*)` subquery when the dialect can't count distinct composite keys).
Bulk member load (`SqlMemberSource#getMembers`) reads the whole hierarchy in
one statement via `SqlMemberSource#makeKeysSql`. SQL text assembly and dialect
differences are covered in [sql-generation.md](sql-generation.md).

**The constraint family** (`mondrian.rolap`, contracts in
`mondrian.rolap.sql`):

- `DefaultMemberChildrenConstraint` — a stateless singleton; only the parent
  predicate, no fact join. The default, and deliberately its own cache key.
- `ChildByNameConstraint extends DefaultMemberChildrenConstraint` — used when a
  member name is being resolved (`[Store].[USA].[CA]`):
  `RolapSchemaReader#lookupMemberChildByName` asks
  `SqlConstraintFactory#getChildByNameConstraint`, and the constraint adds
  `level-name-column IN (…)` via `SqlConstraintUtils#constrainLevel`, so the
  engine fetches one child instead of all siblings. Its cache key is
  `(class, childNames)`.
- `MemberExcludeConstraint` — the inverse (NOT IN / IS NOT NULL), created by
  `RolapNativeSet` when native evaluation needs to exclude members.
- `SqlContextConstraint` — the non-empty workhorse: constrains the SQL to the
  current evaluation context by joining the fact table and adding a predicate
  per context member. Its cache key embeds the (expanded) context members,
  slicer tuples, and virtual-cube base cubes — so identical contexts share
  cache entries and different contexts don't.

**Who picks the constraint: `SqlConstraintFactory`** (singleton).

- `#getMemberChildrenConstraint(evaluator)` returns the default unless native
  non-empty is enabled (`MondrianProperties#EnableNativeNonEmpty`) *and*
  `SqlContextConstraint#isValidContext` accepts the context — no virtual cube
  (unless base cubes can be derived), no measure/member conflicts, no
  unsupported calculated members — in which case a `SqlContextConstraint` is
  built.
- `#getLevelMembersConstraint(evaluator, levels)` similarly returns
  `DefaultTupleConstraint` for null/invalid contexts, but also whenever the
  level's cardinality is below `MondrianProperties#LevelPreCacheThreshold` —
  below that it is cheaper to read the whole level once and cache it than to
  run per-context SQL forever. Otherwise, a NON EMPTY context whose axes yield
  crossjoin args becomes a `RolapNativeCrossJoin.NonEmptyCrossJoinConstraint`;
  else a `SqlContextConstraint`.
- `#getChildByNameConstraint` falls back to the default constraint for ragged
  hierarchies (name lookup spans levels, WHERE won't work) and again for small
  child levels under `LevelPreCacheThreshold`.

**Aggregate-table retargeting.** When the constraint is a
`SqlContextConstraint` and `MondrianProperties#UseAggregates` is on,
`SqlMemberSource#chooseAggStar` builds the level/measure `BitKey`s from a
`CellRequest` for the current context and asks `AggregationManager#findAgg`
for a matching `AggStar` — the child-member SQL then joins the (smaller)
aggregate table instead of the fact table, including collapsed-level handling
when the aggregate table has the level inlined. See
[aggregate-tables.md](aggregate-tables.md).

## 4. Member caches: `MemberCacheHelper`

`MemberCacheHelper` (one per `SmartMemberReader`, plus the cube layer's
second instance) implements `MemberCache` with four maps:

| Map | Key | Value | Notes |
|---|---|---|---|
| `mapKeyToMember` | `MemberKey` (parent, key value) | member | `SoftSmartCache` — GC-evictable; guarantees member-object uniqueness |
| `mapLevelToMembers` | (`RolapLevel`, `TupleConstraint` cache key) | member list | level members per constraint |
| `mapMemberToChildren` | (`RolapMember`, `MemberChildrenConstraint` cache key) | children list | children per constraint |
| `mapParentToNamedChildren` | `RolapMember` | growing sorted set of children | `SmartIncrementalCache`, fed by name lookups |

**Constraint-as-cache-key** is the central semantic: a children list read under
a `SqlContextConstraint` (only non-empty children for one context) is cached
under *that constraint's* cache key and can never answer an unconstrained
request — and vice versa. Only equal constraints (`getCacheKey`) hit. The
default constraints are singletons, so all unconstrained reads share one entry
per parent/level.

`ChildByNameConstraint` results would poison `mapMemberToChildren` (they are
deliberately partial), so `MemberCacheHelper#putChildren` diverts them into
`mapParentToNamedChildren`, which grows incrementally as more names are
resolved. Symmetrically, `MemberCacheHelper#getChildrenFromCache` special-cases
`ChildByNameConstraint` via `MemberCacheHelper#findNamedChildrenInCache`: it
can be satisfied either from a complete default-constraint children list or
from the accumulated named-children set, and returns null unless *every*
requested name is found.

**Cache write-back** happens at two levels. `SmartMemberReader#readMemberChildren`
bins fetched children by parent and calls `MemberCacheHelper#putChildren` per
parent (also caching empty lists — "has no children" is an answer).
`SqlTupleReader` (section 5) does the same from bulk reads: while streaming a
result set it assembles per-parent sibling lists and, when
`TupleConstraint#getMemberChildrenConstraint(parent)` returns a usable
constraint, writes them back mid-stream and at `SqlTupleReader.Target#internalClose`
— skipping parents above the target level whose child lists may be incomplete.

**(fork PATCH)** additions in `SmartMemberReader`:

- `SmartMemberReader#getMembersInLevel` hierarchizes members
  (`Sorter#hierarchizeMemberList`) *before* caching, so cached level lists are
  in hierarchical order once instead of being re-sorted per query; skipped for
  lists larger than `MondrianProperties#HierarchizeMaxLevelMembers`.
- Two new `ConcurrentHashMap` caches back the fork's lookup extensions:
  `levelMembersByUniqueKeyCache` (level → key → member) for
  `SmartMemberReader#getLevelMemberByUniqueKey`, and
  `levelMemberUniqueNamesWithChildrenCache` (level → unique names of members
  with at least one child, computed from the child level's members) for
  `SmartMemberReader#hasMemberChildren`. Both are built lazily from
  `getMembersInLevel` on first use and are not cleared by
  `MemberCacheHelper#flushCache` — they live as long as the reader (i.e. the
  schema).

## 5. Bulk set reading

Reading members one parent at a time is the fallback, not the norm. Whole sets
— all members of a level, a crossjoin of levels, descendants of many parents —
are read by **`SqlTupleReader`**: each level becomes a `Target` (built via the
`MemberBuilder` from `MemberReader#getMemberBuilder`), `SqlTupleReader#makeLevelMembersSql`
generates one statement for all targets under the driving `TupleConstraint`,
and the streamed rows are turned into members with children lists written back
into the member caches as described above. `HighCardSqlTupleReader`
(deprecated) is the lazy-streaming variant used for high-cardinality
dimensions. **(fork PATCH)** `SqlTupleReader#makeLevelMembersSql` carries
virtual-cube fixes: constraints are applied before level tables are added and
SELECT-less sub-statements are skipped, so per-base-cube SQL stays valid. The
main driver of this machinery is native set evaluation
(`RolapNativeSet.SetConstraint extends SqlContextConstraint`) — see
[native-evaluation.md](native-evaluation.md).

## 6. Invalidation lifecycle

Member caches are never invalidated by time; something must flush them:

- **`MemberCacheHelper#flushCache`** clears all four maps and resets each
  level's `approxRowCount` (which `RolapSchemaReader#getLevelCardinality`
  treats as a cache). The cube layer's
  `CacheRolapCubeHierarchyMemberReader#checkCacheStatus` flushes both its own
  helpers and the underlying shared hierarchy's.
- **`DataSourceChangeListener`** (a `mondrian.spi` hook): every cached read in
  `SmartMemberReader` first runs `MemberCacheHelper#checkCacheStatus`, which
  asks `DataSourceChangeListener#isHierarchyChanged(hierarchy)` and flushes on
  change — polling-style invalidation for externally-modified dimension tables.
- **`CacheControl`**: `CacheControlImpl#flush(MemberSet)` →
  `CacheControlImpl#flushMember` evicts individual members;
  `MemberCacheHelper#removeMember` does the surgical work — dropping the
  member's key entry, level-list entries for its level and below, and its
  appearances as parent or sibling (sibling lists under non-trivial constraints
  are evicted whole). `CacheControl.MemberEditCommand`s (add/delete/move/
  set-property, run by `CacheControlImpl#execute`) update caches in place. See
  [caching.md](caching.md) for how this relates to cell/segment flushing.
- **Schema eviction** is the blunt instrument: removing the schema from
  `RolapSchemaPool` drops every reader and cache with it.

## 7. Practical notes

- **Member caches are schema-wide and effectively permanent.** They hang off
  `RolapHierarchy`/`RolapCubeHierarchy` objects owned by the pooled
  `RolapSchema`, shared by all connections. In a long-lived process, dimension
  data read once is served from memory until an explicit flush, a
  `DataSourceChangeListener` trigger, or schema eviction — a stale-member bug
  is almost always a missing flush, not a broken cache.
- **Constraint identity determines cache hits.** Requests miss each other's
  entries unless their constraints' `getCacheKey` values are equal: NON EMPTY
  navigation (`SqlContextConstraint`, keyed by context) rarely reuses entries
  across queries, while unconstrained navigation (singleton default
  constraints) always does. When debugging "why did this run SQL again", look
  at the constraint's cache key first.
- **Role wrappers are outside the cache.** Access control filters and
  substitutes *after* the shared cache, so restricted roles reuse the same
  cached members as unrestricted ones; there is no per-role member cache.
- **High-cardinality dimensions and `DisableCaching` trade memory for SQL**:
  with `NoCacheMemberReader` every navigation step is a database round-trip.
- `mapKeyToMember` uses soft references — under memory pressure members can be
  collected and re-read; the uniqueness guarantee is preserved by
  re-registration on read.
