# The Life of an MDX Query

This document traces one MDX statement end to end through the engine, from the
mondrian-olap JRuby gem down to SQL and back. It is the spine of this
documentation set: every stage links to the package/topic documents that cover
it in depth. All claims are anchored to the code as `ClassName#methodName`.

```
 MDX string
    │  1. entry            Connection#execute (gem) → olap4j prepareOlapStatement
    ▼
 parse                     JavaccParserValidatorImpl → MdxParserImpl (JavaCC)
    │  2. parsing          → mondrian.olap.Query AST (mondrian.mdx nodes)
    ▼
 resolve + validate        Query#resolve: IdBatchResolver, QueryValidator,
    │  3. binding          FunTable lookup → ResolvedFunCall tree
    ▼
 compile                   BetterExpCompiler: Exp trees → Calc trees
    │  4. compilation      (Query.axisCalcs, Query.slicerCalc)
    ▼
 execute                   RolapConnection#execute → RolapResult#<init>
    │  5. evaluation       slicer → axes → cells, in phases:
    │                        evaluate → record CellRequests → run batched SQL
    │                        → re-evaluate … until no cell is missing
    ▼
 result                    RolapResult / RolapCell → olap4j CellSet
    │  6. consumption      → Mondrian::OLAP::Result (Ruby)
    ▼
 axes, cell values, formatted values
```

Stages 1–4 all happen while *preparing* the statement; only stages 5–6 run at
execute time. A prepared `Query` is fully resolved and compiled and can be
executed repeatedly.

---

## Stage 1 — Entry: from the JRuby gem into olap4j

The mondrian-olap gem drives Mondrian exclusively through the **olap4j** API
(the JDBC-like Java OLAP API); it never calls the native engine directly for
query execution. In the gem (`lib/mondrian/olap/connection.rb`):

- `Mondrian::OLAP::Connection#connect` instantiates
  `mondrian.olap4j.MondrianOlap4jDriver` by reflection (JRuby's classloader
  setup prevents normal `java.sql.DriverManager` registration — see the
  `PATCH` in `MondrianOlap4jDriver`), calls `driver.connect(connection_string,
  properties)` and unwraps the JDBC connection to an `org.olap4j.OlapConnection`.
  The connection string is `jdbc:mondrian:Jdbc=...;JdbcDrivers=...;Catalog=...`
  (or inline `CatalogContent=`), with credentials in `JdbcUser`/`JdbcPassword`
  properties.
- `Mondrian::OLAP::Connection#execute` calls
  `OlapConnection#prepareOlapStatement(mdx)`, sets any parameters, then
  `PreparedOlapStatement#executeQuery()`, and wraps the returned
  `org.olap4j.CellSet` in a `Mondrian::OLAP::Result`.

On the Java side, `MondrianOlap4jConnection#prepareOlapStatement` creates a
`MondrianOlap4jPreparedStatement` and registers it with the engine's
`MondrianServer` (`MondrianServer#addStatement`). The adapter classes in
`mondrian.olap4j` are thin: each wraps the corresponding native object
(`MondrianOlap4jConnection` → `RolapConnection`, `MondrianOlap4jCellSet` →
`RolapResult`, …).

Key classes:

- `MondrianOlap4jDriver` — olap4j driver entry point; parses the
  `jdbc:mondrian:` URL.
- `MondrianOlap4jConnection` — olap4j `OlapConnection` over a `RolapConnection`;
  `#getMondrianConnection` exposes the native connection (made public in this
  fork for Java 17+ callers).
- `MondrianOlap4jPreparedStatement` / `MondrianOlap4jStatement` — statement
  adapters; parsing happens in their constructor (stage 2).
- `mondrian.server.MondrianServer` — the engine instance; tracks statements,
  owns the `AggregationManager` (and through it all shared caches).

## Stage 2 — Parsing: MDX text → `Query` AST

Parsing happens in the **prepared statement constructor**, not at execute time.
`MondrianOlap4jPreparedStatement#<init>` calls
`MondrianOlap4jStatement#parseQuery`, which wraps the work in
`Locus.execute(connection, "Parsing query", ...)` — `mondrian.server.Locus` is
the thread-local execution context that the SQL and profiling layers require —
and calls the native `Connection#parseStatement`.

The chain:

- `ConnectionBase#parseStatement` → `ConnectionBase#createParser`, which returns
  a `JavaccParserValidatorImpl` (the only live implementation; the pre-JavaCC
  parser path is dead code).
- `JavaccParserValidatorImpl#parseInternal` instantiates the JavaCC-generated
  `MdxParserImpl` (grammar: `mondrian/parser/MdxParser.jj`) and invokes its
  `statementEof()` production.
- Grammar actions call back into `MdxParserValidator.QueryPartFactory` —
  concretely `mondrian.olap.Parser.FactoryImpl#makeQuery` — which constructs
  the `mondrian.olap.Query`.

The product is a `mondrian.olap.Query`: an array of `QueryAxis` objects (plus a
slicer axis), WITH-clause `Formula`s, and expression trees made of
`mondrian.mdx` nodes — `Id` for not-yet-bound identifiers,
`UnresolvedFunCall` for operators and functions, literals, and so on.
`mondrian.olap.Exp` is the common expression interface.

## Stage 3 — Resolution and validation: names → schema objects, calls → functions

The `Query` constructor immediately calls `Query#resolve` — so by the time the
statement is prepared, every identifier is bound and every function call is
typed. Order of operations inside `Query#resolve`:

1. `Query#createFormulaElements` — register WITH MEMBER / WITH SET names.
2. `IdBatchResolver#resolve` — batch-resolves `Id` nodes against the schema
   (members, levels, hierarchies, dimensions) through the connection's
   `SchemaReader`. Batching matters: resolving member names can require SQL
   (via the member readers — see stage 5 sidebar), and doing it per-identifier
   would be pathological.
3. `Query#createValidator` → `QueryValidator` (extends `ValidatorImpl`).
4. `Query#resolve(Validator)` — walks the tree; `ValidatorImpl#getDef` performs
   function lookup: `FunTable#getResolvers(name, syntax)` returns candidate
   `Resolver`s, each of which may produce a `FunDef` given the argument types
   (implicit type conversions are costed and arbitrated here). Every
   `UnresolvedFunCall` becomes a `ResolvedFunCall` holding its `FunDef`.

The function table is layered: `RolapSchema.RolapSchemaFunctionTable` contains
the schema's user-defined functions plus `GlobalFunTable`, which wraps
`BuiltinFunTable` — the registry of all built-in MDX functions, implemented in
`mondrian.olap.fun`.

## Stage 4 — Compilation: `Exp` trees → `Calc` trees

Still inside `Query#resolve`, as its final step: expressions are compiled into
`mondrian.calc.Calc` objects — the executable form. MDX is never interpreted
from the AST at runtime.

- `Query#createCompiler` → `ExpCompiler.Factory#getExpCompiler`, which returns
  the default `mondrian.calc.impl.BetterExpCompiler` (a subclass of
  `AbstractExpCompiler`, which owns the generic `compile` / `compileScalar` /
  `compileAs` machinery). If profiling is enabled the compiler is wrapped by
  `RolapUtil#createProfilingCompiler`.
- `Query#compile` compiles each WITH formula, each axis
  (`Query.axisCalcs[i]`), and the slicer (`Query.slicerCalc`).

A `Calc` is a small evaluator object typed by result: `MemberCalc`,
`TupleCalc`, list/iterable calcs for sets, `DoubleCalc`, etc. Each
`FunDef#compileCall` produces the `Calc` subtree for its function, choosing
result styles (LIST vs ITERABLE vs MUTABLE_LIST) negotiated by the compiler.
At execute time, calcs are evaluated repeatedly against a mutable evaluator
context (next stage).

## Stage 5 — Execution: `RolapResult` and the phased cell-loading loop

Execution starts at `PreparedOlapStatement#executeQuery()`. The olap4j
`MondrianOlap4jCellSet` **is** the execution object — it extends
`mondrian.server.Execution`, which carries cancellation/timeout state.
`MondrianOlap4jCellSet#execute` calls `RolapConnection#execute(Execution)`,
which routes through the server's `RolapResultShepherd` (watchdog for timeout/
cancellation) into `RolapConnection#executeInternal`, which builds the
`RolapResult`. The `RolapResult` constructor is the whole evaluation
algorithm.

### 5.1 Setup

`RolapResult#<init>` creates:

- a `RolapEvaluator` (or a `RolapProfilingEvaluator` when profiling) rooted in a
  `RolapEvaluatorRoot`, which seeds the context with every hierarchy's default
  member via `SchemaReader#getHierarchyDefaultMember`;
- a `FastBatchingCellReader` — the batching cell reader that drives the entire
  aggregation pipeline;
- the cell store: `CellInfoPool` (≤ 4 axes, a compact pooled representation) or
  `CellInfoMap`, addressed by `CellKey` (one ordinal per axis).

The **evaluator** is the heart of expression evaluation: it holds the current
member of *every* hierarchy of the cube (the "cell context"), and `Calc`s read
and temporarily override that context. Context changes are cheap to undo via
`RolapEvaluator#savepoint` / `#restore`.

### 5.2 Slicer, then axes

The slicer (WHERE clause) is evaluated first — `RolapResult#<init>` loads its
members, sets them as the evaluator's context, and materializes it as a
`RolapAxis`. Compound slicers (multi-member sets) install an aggregate
calculation over the set. Then each query axis is evaluated:
axis member lists are determined (`RolapResult#loadMembers`, honoring
`AxisMembers` limits), then each axis `Calc` is executed
(`RolapResult#evalExecute`) and materialized into a `RolapAxis` (a list of
tuple positions). Axis evaluation may itself require cell values (e.g. an axis
with a `Filter` on measure values), so axis execution participates in the same
phase protocol described next.

> **Sidebar: where members come from.** Whenever evaluation (or identifier
> resolution in stage 3) needs members — children of a member, all members of a
> level — the request goes through `SchemaReader` to a per-hierarchy stack of
> `MemberReader`s: `SqlMemberSource` generates and runs SQL,
> `SmartMemberReader` caches results in `MemberCacheHelper` (keyed by parent
> and by `(level, constraint)`), and `RestrictedMemberReader` applies
> role-based access control on top. Member caches belong to the shared
> `RolapSchema` and persist across queries. Native set evaluation
> (`RolapNativeSet` + `SqlTupleReader`) can fetch whole crossjoins/filtered
> sets as one SQL statement instead of iterating in memory. Details:
> `topics/member-resolution.md` (planned).

### 5.3 The phase loop: evaluate → batch → SQL → re-evaluate

Mondrian never fetches one cell's value with one SQL query. Instead,
`RolapResult#executeBody` runs **speculative evaluation passes**:

1. `RolapResult#executeStripe` recurses over the axes, positioning the shared
   `CellKey`, and evaluates the cell at each coordinate via
   `Evaluator#evaluateCurrent`.
2. Every base-measure read lands in `FastBatchingCellReader#get`. On a cache
   miss it does **not** run SQL — it records a `CellRequest`
   (`RolapAggregationManager#makeRequest`: measure + one predicate per
   constraining star column) and returns the sentinel
   `RolapUtil.valueNotReadyException`. The cell is left unset for this pass.
3. After the pass, `RolapResult#phase` asks `FastBatchingCellReader#isDirty`;
   if requests were recorded, `FastBatchingCellReader#loadAggregations` fires:
   requests are grouped into batches by `AggregationKey`
   (star + constrained-column `BitKey` + compound predicates), batches are
   satisfied from the segment caches where possible, rolled up from existing
   segments where possible, and otherwise turned into SQL
   (`AggregationManager#generateSql` → `SqlQuery`, possibly targeting an
   aggregate table via the aggmatcher) executed on the cache manager's SQL
   executor. Results become `Segment`s — one measure over a
   cross-product of constrained column values — registered into the caches.
4. The loop repeats: evaluate again (now hitting warm caches), record whatever
   new requests appear (calculated members can cascade into new base cells),
   load again — until a pass completes with no misses. Runaway queries are cut
   off after `MondrianProperties#MaxEvalDepth` iterations; oversized request
   floods are chunked by
   `CellRequestQuantumExceededException` (thrown every
   `MondrianProperties#CellBatchSize` requests to force an early SQL
   round-trip).

On the final pass each computed value is stored in the cell store together
with its format string and a `ValueFormatter`
(`RolapResult.CellInfo`). Cancellation and timeout are checked at phase
boundaries via the `Execution`.

This protocol is why evaluation-path code must never swallow exceptions
blindly: `valueNotReadyException` and `CellRequestQuantumExceededException`
are control flow, and must propagate to the phase loop.

Deep dives: `topics/cell-batching.md`, `topics/sql-generation.md`,
`topics/caching.md`, `topics/aggregate-tables.md` (planned).

### 5.4 NON EMPTY

`RolapConnection#executeInternal` wraps the finished result in a
`NonEmptyResult` for each axis marked NON EMPTY, filtering out positions whose
cells are all empty. (Where possible, non-emptiness is instead pushed down
into member/set SQL via constraints or native evaluation — that happens
earlier, during axis evaluation.)

## Stage 6 — Result consumption: cells back to Ruby

`RolapResult#getCell(int[] pos)` looks up the `CellInfo` and returns a
`RolapCell`; `RolapCell#getValue` returns the raw value (with
`Util#nullValue` mapped to null) and `RolapCell#getFormattedValue` applies the
stored `ValueFormatter` and format string (the format-string engine is
`mondrian.util.Format`).

olap4j wraps these: `MondrianOlap4jCellSet#getAxes` exposes axis positions and
members; `MondrianOlap4jCellSet#getCell` wraps `RolapCell` in
`MondrianOlap4jCell`.

The gem (`lib/mondrian/olap/result.rb`) walks that API lazily:
`Result#axes` / `#axis_positions` read `CellSetAxis#getPositions` →
`Position#getMembers`; `Result#values` (via `#recursive_values`) reads
`CellSet#getCell(coordinates)` → `Cell#getValue` / `#getFormattedValue`,
converting Java values to Ruby (`BigDecimal`, CLOB → String, …). Drill-through
(`Result#drill_through`) reaches the underlying `RolapCell` and mirrors
`RolapCell#drillThroughInternal` to produce a SQL statement listing the fact
rows behind a cell.

Profiling, when enabled on the statement, is delivered through
`mondrian.spi.ProfileHandler#explain` with the query plan (the `Calc` tree)
and SQL/timing statistics.

---

## Recap: one query's object cast

| Phase | Object | Created by | Lives for |
|---|---|---|---|
| prepare | `MondrianOlap4jPreparedStatement` | `MondrianOlap4jConnection#prepareOlapStatement` | until closed |
| prepare | `Query` (resolved + compiled) | parser via `Parser.FactoryImpl#makeQuery` | with the statement |
| execute | `MondrianOlap4jCellSet` = `Execution` | `PreparedOlapStatement#executeQuery` | one run |
| execute | `RolapResult` | `RolapConnection#executeInternal` | with the cell set |
| execute | `RolapEvaluator` (+ savepoint stack) | `RolapResult#<init>` | one run |
| execute | `FastBatchingCellReader`, `CellRequest`s | `RolapResult#<init>` / phase loop | one run |
| execute | `Segment` / `SegmentWithData` | aggregation pipeline | **cached beyond the query** (schema-wide) |
| read | `RolapCell` → `MondrianOlap4jCell` | `RolapResult#getCell` | per access |
