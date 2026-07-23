# SQL Generation and the Dialect Layer

*How the engine writes SQL and how database differences are absorbed. This
document covers every place SQL text is produced — segment loads, member
fetches, native set evaluation, drill-through — the `SqlQuery` assembly buffer
they all share, the predicate classes that become WHERE clauses, and the
`mondrian.spi.Dialect` SPI. Context: [../query-lifecycle.md](../query-lifecycle.md)
stage 5; neighbors: [cell-batching.md](cell-batching.md),
[member-resolution.md](member-resolution.md),
[aggregate-tables.md](aggregate-tables.md),
[native-evaluation.md](native-evaluation.md).*

Every SQL statement the engine emits is built the same way: some producer
fills a `mondrian.rolap.sql.SqlQuery` (which is dialect-aware from the moment
it is constructed), the query renders to text plus a list of expected column
types, and `RolapUtil#executeQuery` runs it through a `SqlStatement`. What
varies is the producer.

## The four SQL producers

### (a) Segment loads — the aggregation pipeline

The heaviest producer: the batched cell-loading SQL described in
[cell-batching.md](cell-batching.md). When a batch of `CellRequest`s cannot be
satisfied from caches, `SegmentLoader#createExecuteSql` calls
`AggregationManager#generateSql(GroupingSetsList, compoundPredicateList)`,
which picks a *query spec*:

- If `MondrianProperties#UseAggregates` is on **and** there are no compound
  predicates, `AggregationManager#findAgg` looks for a matching aggregate
  table; a hit produces an `AggQuerySpec` over the `AggStar`
  (see [aggregate-tables.md](aggregate-tables.md)).
- Otherwise a `SegmentArrayQuerySpec` generates SQL against the fact table.

Both produce one `SELECT ... GROUP BY` per batch: one select item per
constrained star column plus one aggregated select item per measure. This is
the deepest machinery and gets its own sections below.

### (b) Member fetches — `SqlMemberSource`

Whenever the member readers ([member-resolution.md](member-resolution.md))
need members that are not cached, `SqlMemberSource` builds SQL:
`SqlMemberSource#makeChildMemberSql` (children of a member; parent-child
hierarchies use `#makeChildMemberSqlPC` / `#makeChildMemberSql_PCRoot`),
`#makeKeysSql` (all members of a hierarchy), and
`#makeLevelMemberCountSql` (cardinality estimates). The fork's
`SqlMemberSource#getLevelMemberByUniqueKey` and `#hasMemberChildren`
(fork PATCH) generate SQL the same way.

These queries differ from segment SQL in how joins are emitted: dimension
tables are added via `RolapHierarchy#addToFrom` keyed on the level's key/
ordinal/property expressions, not via the star's fact-table join tree. The
fact table only enters the query when a `MemberChildrenConstraint` /
`TupleConstraint` demands it (NON EMPTY context — the constraint's
`addMemberConstraint` / `addLevelConstraint` methods append the joins and
WHERE conditions through `SqlConstraintUtils`). `SqlMemberSource#chooseAggStar`
can retarget such constrained queries at an aggregate table. Member queries
add dialect-aware `ORDER BY` items (`SqlQuery#addOrderBy`) so children come
back in ordinal order.

### (c) Native set evaluation — `SqlTupleReader`

Native evaluation of NonEmpty/TopCount/Filter/CrossJoin fetches whole tuple
sets with one statement: `SqlTupleReader#readTuples` / `#readMembers` build
SQL in `SqlTupleReader#makeLevelMembersSql`, applying the active
`TupleConstraint` and joining the levels of every target hierarchy. This fork
patches `#makeLevelMembersSql` for virtual-cube handling (constraints applied
before level tables; SELECT-less statements skipped) (fork PATCH). The whole
subsystem is covered in [native-evaluation.md](native-evaluation.md).

### (d) Drill-through — `DrillThroughQuerySpec`

`RolapCell#getDrillThroughSQL` builds a `DrillThroughCellRequest`
(`RolapAggregationManager#makeDrillThroughRequest`) and hands it to
`AggregationManager#getDrillThroughSql`, which uses `DrillThroughQuerySpec` —
an `AbstractQuerySpec` subclass with `isAggregate()` false (row detail, no
GROUP BY), `isOrdered()` true (deterministic ORDER BY for humans), and a
`countOnly` mode used by `RolapCell#getDrillThroughCount` (`count(*)`).
Requested RETURN fields that do not apply to the cell's cube (virtual-cube
cases) are emitted as `NULL` placeholder columns
(`DrillThroughQuerySpec#appendInapplicableFields`).
`RolapCell#drillThroughInternal` then executes the SQL via
`RolapUtil#executeQuery` with `maxRowCount`/`firstRowOrdinal`, downgrading to
a forward-only cursor when the dialect lacks scrollable result sets
(`Dialect#supportsResultSetConcurrency`).

## `SqlQuery`: the assembly buffer

`SqlQuery` is a mutable, non-thread-safe builder holding one `ClauseList` per
SQL clause: `select`, `from` (a `FromClauseList` that also tracks JOIN … ON
clauses), `where`, `groupBy`, `having`, `orderBy`, plus `groupingSets` and
`groupingFunctions`. Producers append fragments; `SqlQuery#toString` /
`#toBuffer` renders the final statement (multi-line when
`MondrianProperties#GenerateFormattedSql` is set).

It is **dialect-aware from construction**: the constructor takes a `Dialect`
and every append consults it. `RolapStar#getSqlQuery` builds one from the
star's dialect (captured from `RolapSchema#getDialect` at star construction);
`SqlQuery#newQuery(dataSource, err)` asks `DialectManager` for the data
source's dialect. `SqlQuery#getDialect` exposes it so predicate and constraint
code can quote values consistently.

The interesting mechanics:

- **SELECT and aliases.** `SqlQuery#addSelect(expression, type)` appends
  `expr as "cN"`, generating the alias via `SqlQuery#nextColumnAlias`
  (`"c" + select.size()`) — except on DB2/AS400 and Derby, where aliases are
  omitted because those databases misbehave with them. Each select item also
  records its expected `SqlStatement.Type` so the reader can pick typed JDBC
  accessors later. `addSelectGroupBy` adds to SELECT and GROUP BY together;
  `addGroupBy(expression, alias)` substitutes the quoted alias when
  `Dialect#requiresGroupByAlias`.
- **FROM and joins.** `SqlQuery#addFrom(RelationOrJoin, alias, failIfExists)`
  handles all schema relation kinds: plain tables (`addFromTable`, applying
  the table's filter to WHERE and optional dialect hints), `<View>`s (the
  view's `SqlQuery.CodeSet#chooseQuery` picks the SQL variant matching the
  dialect's product name), inline tables (converted to a `select ... union`
  relation via `RolapUtil#convertInlineTableToRelation`, using
  `Dialect#generateInline`), joins, and subqueries (`addFromQuery`, honoring
  `Dialect#allowsAs`). The `fromAliases` list deduplicates relations. Join
  conditions become ANSI `JOIN ... ON` when `Dialect#allowsJoinOn`, otherwise
  WHERE equi-joins.
- **Star joins.** Segment SQL adds dimension tables via
  `RolapStar.Table#addToFrom`, which walks up the snowflake to the fact table
  adding each parent and its `RolapStar.Condition` join condition as a WHERE
  clause. This fork reorders the walk so each parent table is added to FROM
  *before* its child (fork PATCH — improves join performance on ClickHouse,
  which is sensitive to FROM order).
- **Grouping sets.** `SqlQuery#addGroupingSet` collects column lists rendered
  as `group by grouping sets ((...), (...))`, and `#addGroupingFunction` adds
  `grouping(expr) as "gN"` select items so the reader can tell which rollup
  row it is looking at.
- **Output.** `SqlQuery#toSqlAndTypes` returns the SQL string paired with the
  per-column type list — the shape every producer hands to
  `RolapUtil#executeQuery`.

## Segment SQL in detail: the `QuerySpec` family

`QuerySpec` is the interface (star, measures with `"m0".."mN"` aliases,
constrained columns with `"c0".."cN"` aliases, one `StarColumnPredicate` per
column, `generateSqlQuery()`). `AbstractQuerySpec` implements the shared
assembly; `SegmentArrayQuerySpec` (fact table) and `DrillThroughQuerySpec`
extend it. `AggQuerySpec` is a parallel implementation over `AggStar` that
does *not* extend `AbstractQuerySpec` (the classes would merge if AggStar ever
merged into RolapStar); it mirrors the same shape, with a `rollup` flag that
wraps measure columns in their rollup aggregator
(`AggStar.FactTable.Measure#generateRollupString`) when the aggregate table is
coarser-grained than the request.

`AbstractQuerySpec#generateSqlQuery` picks one of two strategies based on
distinct-count support:

- **`nonDistinctGenerateSql`** — the normal path. Adds the fact table to FROM
  first (fork PATCH, from an upstream-tracking ClickHouse performance fix —
  upstream added it implicitly through the first column's join walk); then per
  constrained column: `RolapStar.Table#addToFrom` (joins), the column
  expression to SELECT/GROUP BY, and its predicate to WHERE via
  `RolapStar.Column#createInExpr`; then compound predicates
  (`AbstractQuerySpec#extraPredicates`); then measures.
- **`distinctGenerateSql`** — used when the dialect cannot express the
  request's distinct counts: `!Dialect#allowsCountDistinct` with any
  distinct-count measure, or `!Dialect#allowsMultipleCountDistinct` with more
  than one. It rewrites `count(distinct x)` as a plain aggregate over a
  `select distinct` subquery aliased `dummyname` (Greenplum variant: GROUP BY
  instead of DISTINCT). Further distinct-count splitting decisions — whether a
  batch's measures must be loaded by separate statements
  (`Dialect#allowsMultipleDistinctSqlMeasures`,
  `#allowsCountDistinctWithOtherAggs`) — happen earlier, in the batch logic
  ([cell-batching.md](cell-batching.md)).

Measure select items come from `RolapAggregator#getExpression`, which wraps
the measure's column expression in the aggregate function (`sum(...)`,
`count(distinct ...)`, ...); `RolapStar.Measure` carries its aggregator. When
grouping sets are in play (`Dialect#supportsGroupingSets` plus
`MondrianProperties#EnableGroupingSets`; decided in the batch consolidation
logic), `SegmentArrayQuerySpec#addGroupingSets` / `#addGroupingFunction` emit
one grouping set per segment group so several rollups load with one statement.

## Predicates become WHERE clauses

Cell constraints travel as `mondrian.rolap.StarPredicate` objects
(multi-column) and their single-column subtype `StarColumnPredicate`. Both
declare `toSql(SqlQuery, StringBuilder)` — every predicate knows how to render
itself against a dialect. The family (in `mondrian.rolap.agg`):

- `ValueColumnPredicate` — `expr = literal`, or `expr is null` for
  `RolapUtil.sqlNullValue`. Literals are quoted by
  `Dialect#quote(buf, value, datatype)`, which dispatches on
  `Dialect.Datatype` to the string/numeric/date/timestamp quoting methods.
  `MemberColumnPredicate` is a `ValueColumnPredicate` that remembers the
  member it came from (used by cache-region matching).
- `ListColumnPredicate` — an IN list of `ValueColumnPredicate`s. Its `toSql`
  backtracks to produce the tightest form: `x in (1, 2, 3)`,
  `x is null`, `(x = 1 or x is null)`, or `(x in (1, 2) or x is null)`.
- `LiteralStarPredicate` — renders `true`/`false`; `LiteralStarPredicate.TRUE`
  is the "unconstrained" marker, which is why the query specs skip WHERE
  fragments that render exactly `"true"`.
- Logical combinators — `ListPredicate` is the base (parenthesized children
  joined by an operator), with `AndPredicate` and `OrPredicate`. These carry
  the *compound predicates* (compound slicers, aggregate lists) attached to a
  `CellRequest` beside its per-column constraints. `OrPredicate#toSql`
  collapses same-column-set disjuncts into single- or multi-column IN lists —
  `(country, state) in ((..), (..))` — gated by
  `Dialect#supportsMultiValueInExpr` (checked in `AndPredicate#toInListSql`
  and in `SqlConstraintUtils`). `MemberTuplePredicate` renders member-tuple
  ranges directly.
- `RangeColumnPredicate` and `MinusStarPredicate` do **not** implement `toSql`
  (they inherit `AbstractColumnPredicate#toSql`, which throws): they exist for
  cache-region arithmetic — `RolapAggregationManager#makeCacheRegion` builds
  ranges for `CacheControl` flushing — not for query generation.

The query specs funnel single-column predicates through
`RolapStar.Column#createInExpr(expr, predicate, datatype, sqlQuery)`, which
exists mainly to patch up predicates created without a column (it wraps the
expression string in a synthetic column before calling `toSql`).

Predicates are *optimized* before any SQL is written — oversized IN lists are
dropped in favor of reading more rows (`Aggregation#optimizePredicates`,
`MondrianProperties#MaxConstraints`); see
[cell-batching.md](cell-batching.md).

## The Dialect SPI

`mondrian.spi.Dialect` encapsulates everything database-specific: quoting,
literal syntax, and a long list of capability flags that generation code
consults. The most consequential methods, by concern:

| Concern | Methods |
|---|---|
| Identifier quoting | `quoteIdentifier` (several overloads), `getQuoteIdentifierString` |
| Literals | `quoteStringLiteral`, `quoteNumericLiteral`, `quoteBooleanLiteral`, `quoteDateLiteral`, `quoteTimeLiteral`, `quoteTimestampLiteral`; `quote(buf, value, Datatype)` dispatches by column datatype |
| FROM/JOIN shape | `allowsAs`, `allowsJoinOn`, `allowsFromQuery`, `requiresAliasForFromQuery` |
| Alias rules | `requiresGroupByAlias`, `requiresOrderByAlias`, `allowsOrderByAlias`, `requiresHavingAlias`, `allowsSelectNotInGroupBy` |
| Ordering | `generateOrderItem` (ASC/DESC with NULLS FIRST/LAST emulation), `requiresUnionOrderByOrdinal`, `requiresUnionOrderByExprToBeInSelectClause` |
| Aggregation features | `allowsCountDistinct`, `allowsMultipleCountDistinct`, `allowsCountDistinctWithOtherAggs`, `allowsMultipleDistinctSqlMeasures`, `allowsCompoundCountDistinct`, `supportsGroupingSets`, `supportsGroupByExpressions` |
| IN-list features | `supportsUnlimitedValueList` (IN-list length cap), `supportsMultiValueInExpr` (tuple IN) |
| Result reading | `getType(ResultSetMetaData, i)` → `SqlStatement.Type` per column, `supportsResultSetConcurrency`, `getMaxColumnNameLength` |
| Misc | `getDatabaseProduct`, `generateInline` (inline-table SQL), `generateRegularExpression` / `allowsRegularExpressionInWhereClause`, `getStatisticsProviders`, `appendHintsAfterFromClause` |

There is no LIMIT/OFFSET abstraction in this Dialect version: row limits are
applied through JDBC (`Statement#setMaxRows` and row skipping in
`SqlStatement#execute`), not in generated SQL.

### How a dialect is chosen

`DialectManager#createDialect(dataSource, connection)` runs a chain of
`DialectFactory`s: every class listed in
`META-INF/services/mondrian.spi.Dialect` is discovered at startup
(`ServiceDiscovery`), each contributing its public static `FACTORY` (a
`JdbcDialectFactory` that accepts a connection when
`JdbcDialectImpl#getProduct` maps the JDBC metadata's product name/version to
the factory's `DatabaseProduct`) or, lacking one, a constructor-based factory.
The first factory that accepts the connection wins; generic `JdbcDialectImpl`
is the fallback, and results are cached per data source
(`DialectManager.CachingDialectFactory`). An explicit dialect class name can
also be passed to the three-argument `DialectManager#createDialect`.
`RolapSchema#getDialect` creates (from the cache) the dialect used by all of
the schema's stars.

### The implementations (`mondrian.spi.impl`)

`JdbcDialectImpl` is the base: it deduces the quote string, product, and
capabilities from JDBC metadata and holds the default JDBC-type →
`SqlStatement.Type` map — where this fork maps `BIGINT` to `LONG` instead of
`DOUBLE` (fork PATCH, MONDRIAN-2702). Subclasses, one line each:

- `AccessDialect` — Microsoft Access (JET); `ClickHouseDialect` — ClickHouse,
  added entirely in this fork (fork PATCH): backslash-aware string literals,
  tuple IN support, and a `CLICKHOUSE` `DatabaseProduct`.
- `Db2Dialect` — IBM DB2; `Db2OldAs400Dialect` — legacy DB2/AS400 (extends it).
- `DerbyDialect` — Apache Derby; `FirebirdDialect` — Firebird;
  `GoogleBigQueryDialect` — Google BigQuery; `HsqldbDialect` — HSQLDB.
- `HiveDialect` — Apache Hive; `ImpalaDialect` — Impala (extends Hive).
- `InformixDialect` — Informix; `IngresDialect` — Ingres;
  `VectorwiseDialect` — Actian Vectorwise (extends Ingres);
  `InterbaseDialect` — Interbase.
- `LucidDbDialect` — LucidDB; `SqlStreamDialect` — SQLstream (extends it).
- `MicrosoftSqlServerDialect` — SQL Server; this fork prefixes string literals
  with `N` for Unicode correctness (fork PATCH, MONDRIAN-990).
- `MySqlDialect` — MySQL (also detects Infobright);
  `InfobrightDialect` — Infobright (extends MySQL);
  `MariaDBDialect` — MariaDB (extends MySQL); this fork disables
  select-not-in-group-by for ColumnStore compatibility (fork PATCH).
- `MonetDbDialect`, `NeoviewDialect`, `NuoDbDialect`, `SybaseDialect`,
  `TeradataDialect`, `VerticaDialect`, `SnowflakeDialect` — the respective
  databases; `PdiDataServiceDialect` — Pentaho Data Services.
- `PostgreSqlDialect` — PostgreSQL; this fork enables tuple IN via row
  expressions (fork PATCH). Extended by `GreenplumDialect`, `NetezzaDialect`,
  and `RedshiftDialect`.
- `OracleDialect` — Oracle; this fork enables tuple IN (fork PATCH).

## Executing: `SqlStatement`

All generated SQL runs through `RolapUtil#executeQuery`, which wraps it in a
`mondrian.rolap.SqlStatement` and calls `SqlStatement#execute`. That method:

- checks `Execution#checkCancelOrTimeout` (via the thread's `Locus`) before
  taking a connection and again before executing, and registers the JDBC
  statement with the `Execution` so cancellation can reach into the database
  (segment-load statements register with the cache manager through a callback
  instead, since they may outlive the requesting query);
- throttles engine-wide concurrency with a semaphore sized by
  `MondrianProperties#QueryLimit`;
- emits `SqlStatementStartEvent` / `SqlStatementExecuteEvent` monitor events
  and logs to the SQL logger (below);
- builds one typed `SqlStatement.Accessor` per column
  (`getInt`/`getDouble`/`getObject`...) from the type list: the suggested
  types from `SqlQuery#toSqlAndTypes` win, else `Dialect#getType` infers from
  `ResultSetMetaData`, else `OBJECT`.

The `OBJECT` accessor funnels values through
`SqlStatement#normalizeTemporalValue` (fork PATCH): driver-specific temporal
types — `java.time.LocalDateTime`/`LocalDate` from newer MySQL and ClickHouse
drivers, `oracle.sql.TIMESTAMP`/`DATE` from Oracle — are converted to
`java.sql` types so all downstream code can rely on `java.util.Date`
instances.

## Practical notes

**When generated SQL is wrong for a database, suspect the dialect first.**
Most database-specific misbehavior traces to a capability flag or quoting
method on the dialect (wrong `DatabaseProduct` detection in
`JdbcDialectImpl#getProduct` is a classic — the generic fallback dialect then
answers every capability question with defaults). Only if the SQL is wrong
*for every database* look at the producer: the query spec for segment SQL,
`SqlMemberSource`/`SqlConstraintUtils` for member SQL, `SqlTupleReader` for
native sets.

**To see the SQL**, enable DEBUG on the log4j category `mondrian.sql`
(`RolapUtil.SQL_LOGGER`): every statement logs with a sequential id, the
`Locus` component (e.g. `Segment.load`, `SqlMemberSource.getMemberChildren`),
the SQL text, and on completion timing or the failure. Set
`MondrianProperties#GenerateFormattedSql` to get multi-line SQL. The
aggregation and member producers additionally log through their own class
categories, and `RolapUtil#setHook` lets tests intercept every statement
(`RolapUtil.ExecuteQueryHook`).
