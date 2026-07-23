# Mondrian Architecture

Mondrian is a **ROLAP** (relational OLAP) engine: it accepts MDX queries against
a logical multidimensional model (cubes, dimensions, hierarchies, measures),
translates the parts of the query that need data into SQL against a relational
star/snowflake schema, caches what it reads, and assembles a multidimensional
result set. There is no persistent MOLAP storage — the relational database is
the only source of data; everything else is in-memory caching.

This document gives the layer map, the package map, and the runtime object
model. The end-to-end control flow is in [query-lifecycle.md](query-lifecycle.md).

## Layer map

```
        mondrian-olap JRuby gem              XMLA / web clients
                 │                                  │
                 ▼                                  ▼
        mondrian.olap4j                       mondrian.xmla
        (olap4j JDBC-style driver;            (XML for Analysis
         the primary API surface)              endpoint)
                 │                                  │
                 └────────────────┬─────────────────┘
                                  ▼
        mondrian.server — MondrianServer, Statement, Execution, Locus
                                  │
                                  ▼
        mondrian.olap — the logical model and query AST:
        Connection, Schema, Cube, Hierarchy, Member, Query, Validator,
        FunTable        ◄── mondrian.parser / mondrian.mdx (MDX → AST)
                                  │
                                  ▼
        mondrian.calc — compiled Calc expression trees
                        ◄── mondrian.olap.fun (MDX function library)
                                  │
                                  ▼
        mondrian.rolap — the engine: RolapConnection, RolapSchema,
        RolapEvaluator, RolapResult, member readers, RolapStar
                                  │
                                  ▼
        mondrian.rolap.agg — batched cell loading: CellRequest,
        Segment, SegmentCacheManager     (+ rolap.aggmatcher: agg tables)
                                  │
                                  ▼
        mondrian.rolap.sql (SqlQuery assembly) + mondrian.spi.Dialect
                                  │
                                  ▼
        JDBC → MySQL / PostgreSQL / Oracle / SQL Server / ClickHouse / …
```

Two rules of thumb explain most of the code:

- **`mondrian.olap` is the "what", `mondrian.rolap` is the "how".** `olap`
  defines abstract model interfaces (`Cube`, `Hierarchy`, `Member`, `Evaluator`,
  `Result`); `rolap` implements them against relational tables (`RolapCube`,
  `RolapHierarchy`, `RolapMember`, `RolapEvaluator`, `RolapResult`). When you
  hold an interface type, the concrete object is almost always the `Rolap*`
  class.
- **Data is fetched in batches, never cell by cell.** Both member retrieval
  (via member readers) and cell value retrieval (via the segment pipeline) are
  designed to collect many needs first and satisfy them with few SQL
  statements. This shapes the two-pass evaluation protocol described in
  [query-lifecycle.md](query-lifecycle.md).

## Package map

Sizes are approximate and refer to `mondrian/src/main/java/mondrian/`.

| Package | Size | Role |
|---|---|---|
| `mondrian.olap` | 233 files | Logical model interfaces and base classes; `Query` AST; `Validator`; connection properties; `MondrianProperties` (engine configuration); `Util` |
| `mondrian.olap.fun` | ~111 files | Implementations of the built-in MDX functions (`Crossjoin`, `Filter`, `Aggregate`, …); `BuiltinFunTable` registers them; `Resolver`s match call syntax to `FunDef`s |
| `mondrian.mdx` | 11 files | Typed AST node classes for parsed MDX expressions (`UnresolvedFunCall`, `ResolvedFunCall`, `MemberExpr`, `Id`, …) |
| `mondrian.parser` | 3 files + grammar | JavaCC-generated MDX parser (`MdxParserImpl`, from `MdxParser.jj`) and its factory plumbing |
| `mondrian.calc` | 60 files | Compiled-expression layer: `Calc` interfaces by result type (`MemberCalc`, `TupleIterable` calcs, …), `ExpCompiler` / `BetterExpCompiler`, abstract calc base classes |
| `mondrian.rolap` | 180 files | The engine core: schema realization (`RolapSchema`, `RolapCube`, `RolapStar`), evaluation (`RolapEvaluator`, `RolapResult`, `RolapCell`), member reading/caching, native SQL evaluation (`RolapNative*`) |
| `mondrian.rolap.agg` | ~60 files | Cell/aggregation pipeline: `CellRequest` batching, `Segment` model, `SegmentCacheManager`, SQL query specs, star predicates |
| `mondrian.rolap.aggmatcher` | ~20 files | Aggregate table discovery and matching (`AggTableManager`, `AggStar`, recognizers) |
| `mondrian.rolap.cache` | small | Generic cache primitives (`SmartCache`, soft/hard variants, `SegmentCacheIndex`) |
| `mondrian.rolap.sql` | small | `SqlQuery` (SQL text assembly), `SqlConstraint` family, `MemberChildrenConstraint` / `TupleConstraint` |
| `mondrian.rolap.format` | small | Cell/member value formatting SPI implementation |
| `mondrian.spi` | 65 files | Service provider interfaces + impls: `Dialect` (per-database SQL flavor), `SegmentCache`, `DynamicSchemaProcessor`, `DataSourceResolver`, `UserDefinedFunction`, … |
| `mondrian.server` | 42 files | `MondrianServer` (engine instance), `Statement`/`Execution` (query lifecycle + cancellation), `Locus` (thread execution context), monitoring |
| `mondrian.olap4j` | 30 files | Adapter implementing the olap4j API on top of the native engine (`MondrianOlap4jConnection`, `...Statement`, `...CellSet`) — the entry point used by the mondrian-olap gem |
| `mondrian.xmla` | 22 files | XML for Analysis (SOAP) endpoint on top of olap4j |
| `mondrian.udf` | 9 files | Bundled user-defined functions |
| `mondrian.util` | 53 files | General utilities: `Pair`, `Format` (the MDX format-string engine), `BlockingHashMap`, service discovery, … |
| `mondrian.i18n`, `mondrian.resource` | small | Localization; generated message resources |
| `mondrian.recorder`, `mondrian.web`, `mondrian.tui` | small | Message recording for aggmatcher; legacy servlet/TUI helpers |

## Runtime object model

Who creates whom, and what lives how long:

```
JVM process
 └─ MondrianServer (MondrianServerImpl; a "default" instance per registry,
    plus per-connection instances if requested)
     ├─ AggregationManager            MondrianServerImpl#<init>
     │   └─ SegmentCacheManager       AggregationManager#<init>
     │       ├─ Actor thread(s), cacheExecutor, sqlExecutor
     │       ├─ SegmentCacheIndexRegistry   (per-RolapSchema segment index)
     │       └─ CompositeSegmentCache       (in-JVM MemorySegmentCache
     │                                       + external SegmentCache SPI)
     └─ RolapSchemaPool (process singleton, RolapSchemaPool#instance)
         └─ RolapSchema (shared, keyed by catalog content + connection key)
             ├─ RolapCube*  ─ RolapCubeDimension/Hierarchy/Level, measures
             ├─ RolapStar*  (one per fact table; RolapStarRegistry)
             │   └─ RolapStar.Table tree, RolapStar.Column (bit positions)
             ├─ per-hierarchy MemberReader stacks + member caches
             └─ AggTableManager (aggregate table matching)

RolapConnection (one per open connection; holds Role + SchemaReader)
 └─ Statement (mondrian.server.Statement; one per prepared statement)
     ├─ Query (parsed, resolved, compiled AST; reusable)
     └─ Execution (one per run; cancellation/timeout state)
         └─ RolapResult
             ├─ RolapEvaluator (+ RolapEvaluatorRoot)
             ├─ FastBatchingCellReader (drives the agg pipeline)
             ├─ RolapAxis[] (materialized axis tuples)
             └─ CellInfo store (CellKey-indexed cell values)
```

Lifetime notes:

- **`RolapSchema` is expensive and shared.** Loading parses the catalog XML and
  builds every cube, hierarchy, and star. `RolapSchemaPool` shares instances
  across connections whose schema key matches (see `RolapSchemaPool#get`);
  member caches therefore also live schema-wide and outlive any one query.
- **`RolapStar` is the bridge between the logical and physical model.** One per
  fact table, owned by the schema. Every constrainable column gets a unique
  *bit position*, and `BitKey`s (sets of bit positions) are the currency used
  throughout the aggregation code to identify column sets cheaply.
- **Segment data is cached at three levels**: a thread-local soft-reference
  cache on the star (`RolapStar#getCellFromCache`), the shared in-JVM
  `MemorySegmentCache`, and optionally an external `mondrian.spi.SegmentCache`.
  All shared-cache traffic is serialized through the `SegmentCacheManager`
  actor. Details in the cell-batching and caching topic documents.
- **`Query` objects are fully resolved and compiled at parse time**
  (`Query#resolve` runs in the constructor), so a prepared statement can be
  executed repeatedly without re-analysis.

## Core design patterns

Recurring mechanisms you must know to read this codebase:

- **Two-pass batched evaluation.** Cell evaluation runs speculative passes:
  the first pass records every needed cell as a `CellRequest` and aborts each
  cell with a sentinel; then all requests are loaded in bulk via SQL; then the
  pass repeats against the now-warm cache. See
  [query-lifecycle.md](query-lifecycle.md) stage 5.
- **Sentinel exception protocol.** "Value not loaded yet" is signaled by
  returning/throwing shared singleton objects
  (`RolapUtil.valueNotReadyException`, `CellRequestQuantumExceededException.INSTANCE`)
  rather than by checked control flow. Code catching generic exceptions on the
  evaluation path must rethrow these.
- **Layered readers/decorators.** Both member access and schema access are
  stacks of decorators: `SqlMemberSource` (SQL) wrapped by `SmartMemberReader`
  (cache) wrapped by `RestrictedMemberReader` (role-based access control), all
  behind `SchemaReader`. Adding behavior = adding a wrapper.
- **Compile-then-evaluate expressions.** MDX expressions are not interpreted
  from the AST; they are compiled once into `Calc` trees
  (`mondrian.calc`, via `BetterExpCompiler`) and evaluated many times against a
  mutable `RolapEvaluator` context, which supports cheap state save/restore
  (`RolapEvaluator#savepoint` / `#restore`).
- **Native SQL pushdown.** Certain set operations (NonEmpty crossjoins,
  TopCount, Filter) can be evaluated as SQL instead of in memory
  (`RolapNativeRegistry` and the `RolapNative*` classes) — a major performance
  path with its own topic document (planned).
- **SPI extensibility.** Database differences live behind `mondrian.spi.Dialect`;
  caching, schema preprocessing, data sources, functions, and formatting all
  have SPI hooks under `mondrian.spi`.
- **Actor for shared cache state.** `SegmentCacheManager` serializes all
  shared segment-index mutation through a command queue processed by dedicated
  threads (in this fork: several threads, but at most one in-flight command per
  schema), so the index needs no fine-grained locking.

## Threading model (short version)

- A query executes on its caller's thread; SQL for segment loading runs on
  `SegmentCacheManager`'s `sqlExecutor` thread pool, external-cache I/O on its
  `cacheExecutor`, and index bookkeeping on the actor thread(s).
- Schema-wide structures (schema pool, member caches, segment index registry)
  are concurrent; this fork carries several `PATCH`es replacing upstream
  synchronized maps with `ConcurrentHashMap` and striped locks
  (e.g. `RolapSchemaPool`, `SmartMemberReader`).
- `Execution` carries cancellation/timeout state and is checked at cell-loading
  phase boundaries (`Execution#checkCancelOrTimeout`).

## This fork vs upstream

This repository is a maintained fork of Pentaho Mondrian 9.3.0.0 for the
mondrian-olap JRuby gem. Local changes are marked with `// PATCH:` comments.
Recurring themes: concurrency hardening (concurrent maps, multi-threaded
segment-cache actor), member-lookup extensions
(`SchemaReader#getLevelMemberByUniqueKey`, `SchemaReader#hasMemberChildren`),
SQL generation fixes (virtual cubes, ClickHouse), Guava → Caffeine, and
commons-lang → commons-lang3. The full catalog is in
[topics/fork-changes.md](topics/fork-changes.md);
`grep -rn "PATCH:" mondrian/src/main/java` is the raw source of truth.
