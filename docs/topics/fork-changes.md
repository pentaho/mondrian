# Fork changes vs upstream Mondrian 9.3.0.0

*Catalog of this repository's deltas versus upstream Pentaho Mondrian 9.3.0.0.
The fork exists to serve the mondrian-olap JRuby gem; its changes cluster into
behavioral fixes/extensions, concurrency hardening, Java 17 / JRuby platform
accommodations, and dependency swaps.*

**Maintenance rule:** when you add a new `// PATCH:` to the source, add it to
this catalog in the same change. If this file ever drifts, the source of truth
is the marker scan:

```
grep -rn "PATCH:" mondrian/src/main/java --include="*.java"
```

(~194 hits at the time of writing; a handful more live under `mondrian/src/it/java`.)
A few fork deltas carry no marker because they are whole new files
(`FormatAwareFunDef`) or build-level changes (the poms) — those are covered in
the later sections.

---

## 1. Behavioral changes

### 1.1 Member-lookup API additions

Two methods were added to the schema/member reader stack, primarily for
callers (such as the mondrian-olap gem) that need fast key-based member access
and existence-of-children checks without materializing child lists:

- `SchemaReader#getLevelMemberByUniqueKey(Level, Object)` — find a member of a
  level by its key value.
- `SchemaReader#hasMemberChildren(Member)` — does a member have at least one
  child?

Both are declared on the interfaces and threaded through every implementation
in the decorator stack:

| Layer | Classes touched |
|---|---|
| Schema reader | `SchemaReader`, `DelegatingSchemaReader`, `RolapSchemaReader` |
| Member reader | `MemberReader`, `DelegatingMemberReader`, `CacheMemberReader`, `NoCacheMemberReader` |
| Caching implementation | `SmartMemberReader` |
| SQL fallback | `SqlMemberSource` |

The real implementations live in `SmartMemberReader`: it builds (lazily, on
first call) a per-level `ConcurrentHashMap` of key → member
(`getLevelMemberByUniqueKey`) and a per-level set of parent unique names that
have children (`hasMemberChildren`), both derived from the already-cached
level member lists. `SqlMemberSource` carries naive linear-scan versions that
"typically will not be used".

### 1.2 Member ordering and order keys

A cluster of patches makes sibling ordering by order key work consistently,
including for parent-child hierarchies and NULL/string keys:

- `RolapMemberBase`: when `mondrian.rolap.compareSiblingsByOrderKey` is on,
  the order key defaults to the member key if none was set
  (`assignOrderKeys` flag, applied in `RolapMemberBase#getOrderKey`). String
  order keys are wrapped in the fork-added
  `RolapMemberBase.CaseInsensitiveString` (a `Collator`-based wrapper giving
  case- and accent-insensitive comparison); a null order key is stored as
  `RolapUtil.sqlNullValue` so it is not silently defaulted to the key.
- `Sorter`: new `Sorter#sortSiblingMembers` and
  `Sorter#sortParentChildMembers` sort children fetched by
  `SqlMemberSource#getMemberChildren` when order keys are assigned;
  `Sorter#compareSiblingMembersByOrderKey` explicitly collates NULL order keys
  before all other values *on both sides* and falls through to
  ordinal/member comparison on ties — returning 0 for distinct members broke
  the total order required by `Comparator` and produced
  "Comparison method violates its general contract!".
- `SmartMemberReader#getMembersInLevel` hierarchizes level members once,
  before caching (bounded by `hierarchizeMaxLevelMembers`), and
  `FunUtil#levelMembers` skips re-hierarchizing when the hierarchy's reader is
  a `SmartMemberReader` — the cached list is already ordered.
- `RolapMemberBase#keyToString` (MONDRIAN-2703): large numeric keys no longer
  render member names in scientific notation.

### 1.3 SQL generation — virtual cube tuple queries

Upstream's `SqlTupleReader` could emit invalid or wasteful UNION branches when
reading tuples for a virtual cube. The fork changes
`SqlTupleReader#generateSelectForLevels` and its caller so that:

- `SqlConstraintUtils#addContextConstraint` (and the tuple variant) now
  returns a `boolean`, and `SqlContextConstraint` records it in its
  `addedConstraint` field.
- For a sub-cube of a virtual cube where the current context member belongs
  to an ignored unrelated dimension (no constraint could be added), the
  branch's SELECT is skipped entirely (`SqlTupleReader` returns null for that
  branch and the UNION `prependString` is only appended when a SELECT is
  actually produced).
- If *no* SELECT gets generated at all, `SqlTupleReader` falls back to
  `sqlForEmptyTuple`.
- Constraints are applied *before* the level columns are added
  (`SqlTupleReader#generateSelectForLevels`, "apply constraints first so that
  level tables are added last"), except for
  `RolapNativeTopCount.TopCountConstraint`, whose `SUM(...)` select-list
  expression must come after the level columns.
- `SqlContextConstraint` adds the cube's default measure to the base-cube
  list when the selected calculated measures contain no stored measure, so a
  base cube can still be chosen.

### 1.4 SQL generation — join order (ClickHouse-motivated)

`RolapStar.Table#addToFrom` adds the parent (fact) table to the FROM clause
*before* the dimension table, and `AbstractQuerySpec#nonDistinctGenerateSql` /
`SqlConstraintUtils#addContextConstraint` add the fact table to FROM first.
Both carry attribution to
`github.com/SergeiSemenkov/mondrian` commit `365ea5eb`; the motivation stated
in the comments is join performance on ClickHouse (whose join planner is
sensitive to table order).

### 1.5 JDBC temporal value normalization

`SqlStatement#normalizeTemporalValue` (new static method, applied to
accessor results) converts driver-specific temporal types to `java.sql`
types so downstream code can rely on temporal values being `java.util.Date`
instances:

- `java.time.LocalDateTime` → `Timestamp` (MONDRIAN-2714; MySQL Connector/J
  8.0.23+ returns `LocalDateTime` for datetime columns),
- `java.time.LocalDate` → `java.sql.Date` (ClickHouse JDBC driver),
- `oracle.sql.TIMESTAMP` / `oracle.sql.DATE` → converted reflectively (they
  are not `java.util.Date` subclasses unless `oracle.jdbc.J2EE13Compliant`
  is set; reflective because the Oracle driver is not a compile-time
  dependency).

### 1.6 Dialect fixes

| Class | Change |
|---|---|
| `JdbcDialectImpl` | MONDRIAN-2702: default type map sends `Types.BIGINT` to `SqlStatement.Type.LONG` instead of `DOUBLE` (no precision loss for 64-bit keys/measures) |
| `OracleDialect`, `PostgreSqlDialect` | `supportsMultiValueInExpr` returns true (both support multi-column/row-expression `IN` predicates), enabling multi-column IN lists instead of OR chains |
| `MicrosoftSqlServerDialect` | MONDRIAN-990: `quoteStringLiteral` prepends `N` to string literals so non-ASCII characters survive |
| `MariaDBDialect` | `deduceSupportsSelectNotInGroupBy` returns false (MariaDB ColumnStore rejects selecting columns absent from GROUP BY) |

`ClickHouseDialect` itself carries no `PATCH:` markers; only the
ClickHouse-motivated changes listed elsewhere in this document are fork
deltas.

### 1.7 MDX function library (`mondrian.olap.fun`)

- **Min/Max with dates** — `MinMaxFunDef` is substantially rewritten to
  support DateTime expressions in addition to numeric ones. Because
  Mondrian statically types every calculated member as Numeric regardless of
  its runtime return type, date support relies on runtime `instanceof`
  dispatch in `FunUtil` extreme-value helpers, and `MinMaxFunDef#compileCall`
  compiles the value expression as a generic scalar (not a `DoubleCalc`,
  whose `evaluateDouble` would throw "Expected NUMERIC" on a `Date`). The
  statically-DateTime 2-arg form compiles to an `AbstractDateTimeCalc` so
  `compileDateTime` callers (e.g. VBA `DateDiff` via `JavaFunDef`) can cast
  safely.
- **`FormatAwareFunDef`** (new interface, `mondrian.olap`) — lets a function
  control which argument's format string is inferred for a calculated member
  that has no explicit FORMAT_STRING. `Formula` consults it instead of the
  default depth-first walk (which could pick a numeric measure inside a
  Filter condition); `MinMaxFunDef` implements it (use the value expression
  of the 2-arg form), and `UdfResolver`'s `UdfFunDef` adapter forwards it
  when the wrapped UDF implements the interface.
- **`skipJavaFunDefs`** — `BuiltinFunTable#defineFunctions` reads the
  `mondrian.olap.fun.skipJavaFunDefs` system property (comma-separated
  function names) and skips registering those Vba/Excel `JavaFunDef`s, so a
  schema-level `UserDefinedFunction` with the same name can be used without
  an ambiguous-match error. Read via `System.getProperty` directly (not
  `MondrianProperties`) so tests toggling it per-case see the live value.
- **`Vba`** — the VBA date functions (`DateAdd`, `DateDiff`, `DatePart`,
  `Day`, `Month`, `Year`, `Hour`, `Minute`, `Second`, `Weekday`,
  `DateValue`, `TimeValue`, …) accept `Object` instead of `Date` and coerce
  via the package-private `Vba#castToDate`, again because calculated members
  are always statically typed Numeric. Also: MONDRIAN-2730 fix in `Vba#int_`
  (upstream `v < 0 && v > dv` wrongly returned 0 for −1 < x < 0; now
  `v <= 0`), and deprecated `new Double(s)` replaced with `Double.valueOf`.
- **`JavaFunDef`** — argument evaluation treats the MDX null sentinel
  (`nullValue`) like Java null (upstream only checked Java null), and
  coerces `BigDecimal` arguments to `double` when the target method's
  parameter is `double`.
- **`CaseMatchFunDef` / `CaseTestFunDef`** — CASE expressions accept generic
  Value-typed branches: return type falls back to the first branch's type,
  Member/Tuple branch values are compiled as scalars, and BigDecimal/Double
  operands are compared by double value.
- **`CoalesceEmptyFunDef`** — resolves DateTime and generic Value argument
  types in addition to Numeric and String.
- **`GenerateFunDef`** — treats a second argument that is a measure with a
  string expression as the string form of `Generate` (returning
  `StringType`), not the set form.
- **`UdfResolver`** — MONDRIAN-2661: if a UDF returns a `TupleList`, use it
  directly instead of re-wrapping; arity is taken from the actual argument
  list rather than fixed at construction.
- **`AggregateFunDef`** — when `mondrian.rolap.EnableInMemoryRollup=false`
  and `Aggregate` is called over single-dimension stored members (no
  expression argument), evaluation reuses the distinct-count code path,
  generating one SQL query with IN conditions instead of rolling up in
  memory (`AggregateFunDef#useRollupAggregate` gates this). Motivation:
  large compound slicers over multiple dimensions could otherwise cause
  `OutOfMemoryError`.
- **`MatchesUdf`** — returns false instead of throwing when an argument is
  null, and stringifies a non-String first argument.
- **`NonEmptyCrossJoinFunDef`** — wrapped in `QueryTiming` start/end marks
  (see profiling below).

### 1.8 Evaluation guards and fixes

- `RolapAggregationManager#makeCellRequest` returns a null cell request when
  a coordinate is the `#null` member (such a member's level has no level
  reader; upstream NPE'd later).
- `Formula`'s cycle detection tracks `Member` objects instead of
  `MemberExpr` objects (validation may create new `MemberExpr` wrappers
  around the same member, and `MemberExpr` uses identity equality), and
  catches `MondrianException` during dependent-formula validation (e.g.
  multiplication of measures mis-parsed as a set crossjoin) instead of
  failing the whole validation.
- `UnionRoleImpl#getAccessDetails` (MONDRIAN-2641): the
  `CachingHierarchyAccess` wrapper for unions of >5 roles is disabled — the
  cache returned stale access decisions for union roles.
- `RolapProperty` marks internal properties as `dependsOnLevelValue`.
- `QueryTiming` guards the ms/invocation average against division by zero.
- Cancellation: `CancellationChecker#checkCancelOrTimeout` checks the
  interval condition *outside* the `synchronized (execution)` block (the
  interval is now cached on `Execution`, exposed via
  `Execution#getCheckCancelOrTimeoutInterval`), avoiding lock traffic on
  every iteration; `RolapNamedSetEvaluator#ensureList` checks cancellation
  inside the named-set materialization loop to break out of effectively
  infinite set evaluations.

### 1.9 Profiling/timing extensions

`QueryTiming#markFullCount(name, duration, count)` (new) records a duration
with an explicit invocation count. `ResolvedFunCall` gained a `timingName`
property; `RolapCalculatedMember#getExpression` stamps the calculated member's
unique name on its top-level function call, and `RolapProfilingEvaluator`
uses it to attribute evaluation time per calculated member (using
`Evaluator#isDirty` / `Evaluator#getMissCount`, with `isDirty` added to the
`Evaluator` interface and implemented in `RolapEvaluator`).
`NonEmptyCrossJoinFunDef` marks its own timing.

## 2. Concurrency hardening

Changes that replace upstream's coarse synchronization so many connections /
schemas can be served concurrently:

- **`RolapSchemaPool`** — `mapKeyToSchema` / `mapMd5ToSchema` become
  `ConcurrentHashMap`s, and schema creation is guarded by an array of striped
  locks selected by key hash (`RolapSchemaPool#getLock`): two different
  schemas can now load concurrently, while two requests for the same schema
  still serialize. `remove`/`clear` need no global lock anymore.
- **`SegmentCacheManager`** — upstream's single actor thread becomes a thread
  array (`SegmentCacheManager#threads`, sized by the
  `mondrian.rolap.agg.SegmentCacheManager.actorThreads` system property),
  with the constraint that **at most one command per schema is in flight at a
  time**: `Actor#run` tracks in-progress schema keys in a
  `ConcurrentHashMap` (`processingSchemaKeys`) and requeues messages whose
  schema is busy. Supporting changes: a `getSchema()` method on the actor
  `Message`/`Command` types and on `Locus`
  (`Locus#getSchema` reads it from the execution's statement), shutdown
  coordination across threads, and `ConcurrentHashMap` instead of
  `Collections.synchronizedMap` for the manager's bookkeeping maps.
  `SegmentCacheWorker` and `SegmentCacheIndexImpl` are adapted to the thread
  array (index thread-safety checks accept any actor thread;
  `SegmentCacheIndexImpl` uses `ConcurrentHashMap`).
- **`SmartMemberReader`** — the two fork-added lookup caches (§1.1) are
  `ConcurrentHashMap`s.
- **`RolapMemberBase`** — the property-map factory produces
  `ConcurrentHashMap`s and the property getters drop `synchronized`; null
  property values are boxed (a `ConcurrentHashMap` cannot store null).
- **`RolapConnectionPool`** — max active pooled connections is derived from
  `MondrianProperties#SegmentCacheManagerNumberSqlThreads` + 10 instead of a
  hard-coded 50, keeping the pool aligned with the SQL executor size.

## 3. Platform and consumer compatibility

- **Java 17 accessibility** — methods that the mondrian-olap gem calls
  reflectively are made `public` so they work without `--add-opens`:
  `MondrianOlap4jConnection#getMondrianConnection`,
  `RolapLevel#getTableName`, `RolapHierarchy#getMemberReader` (and the
  override in `RolapCubeHierarchy`).
- **JRuby driver registration** — `MondrianOlap4jDriver`'s static initializer
  skips `DriverManager` registration when
  `-Dmondrian.olap4j.registerDriver=false` is set. Registration is not
  needed by the gem and pinned a global reference to the JRuby runtime,
  preventing its garbage collection.
- **Deprecation cleanups** — `new Double(...)` (removal-deprecated since
  Java 9) replaced with `Double.valueOf` in `Vba`.

## 4. Dependency and build changes

Visible in the root `pom.xml` and `mondrian/pom.xml` plus import-level
patches:

| Dependency | Upstream 9.3 | This fork | Where visible in code |
|---|---|---|---|
| Guava | Guava cache/annotations | **Caffeine 2.9.3** (Guava removed entirely) | `TupleExpMemoComparator` (cache swap; Caffeine does not wrap exceptions in `UncheckedExecutionException`, so `CellRequestQuantumExceededException` propagates without the unwrap dance), `Sorter` (dropped `@VisibleForTesting`) |
| commons-lang | `commons-lang` 2.x | **`commons-lang3` 3.17.0** | `RolapConnection`, `Recognizer`, `RolapConnectionTest`, `MondrianFoodMartLoader` (import swaps) |
| Maven repositories | Pentaho Nexus | **local `lib-repo/` file repository only**; everything else from Maven Central | root `pom.xml` `<repositories>`; `lib-repo/` holds eigenbase-xom/-properties/-resgen, javacup, olap4j-xmla, olap4j-tck |
| Parent POM | Pentaho parent | standalone `mondrian-olap:mondrian-parent-pom` | root `pom.xml` |
| Log4j 2 | 2.x | pinned 2.17.1 | version property in root `pom.xml` |

Build targets Java 8 bytecode (`maven.compiler.release` 8); the module
artifact is `mondrian-olap-java`.

## 5. Miscellaneous / test infrastructure

- `MondrianFoodMartLoader` (under `src/it/java`) gained ClickHouse support:
  skip unique indexes (unsupported), `TRUNCATE TABLE` instead of
  `DELETE FROM`, and `Nullable(...)` column wrappers (ClickHouse columns are
  non-nullable by default).
- The primary test suite for fork changes is the JRuby/Minitest suite in the
  repo-root `test/` directory (run via `rake test`), which exercises the
  built JAR through the mondrian-olap gem; the legacy Java tests are kept
  but not extended (see `AGENTS.md`).
