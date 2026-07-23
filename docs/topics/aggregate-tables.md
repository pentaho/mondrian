# Aggregate Tables

*How Mondrian discovers pre-summarized copies of a fact table at schema-load
time, models them as `AggStar`s, and substitutes them for the fact table when
loading segments and members. Builds on the segment pipeline described in
[cell-batching.md](cell-batching.md) and the SQL assembly described in
[sql-generation.md](sql-generation.md); discovery runs at the end of the schema
load covered in [schema-loading.md](schema-loading.md).*

## The concept

An aggregate table is a physical table containing the fact table's measures
pre-aggregated to a coarser grain — fewer dimension columns, or dimension
columns collapsed to a higher level (e.g. month instead of day). If a segment
load only needs columns the aggregate table carries, Mondrian can answer it
from that table with far fewer rows scanned. Nothing at the MDX level changes;
the substitution happens entirely inside SQL generation.

Every aggregate table must carry a **fact count column** (conventionally
`fact_count`) recording how many fact rows each aggregate row summarizes. It is
what makes average measures reconstructible (`AVG = SUM(sum_col) /
SUM(fact_count)`) and is verified during recognition
(`Recognizer#checkFactColumns`: exactly one, numeric — anything else is an
error).

## The properties that gate everything

All are in `MondrianProperties` (see the generated
`mondrian/olap/MondrianProperties.java`):

| Property | Key | Effect as implemented |
|---|---|---|
| `UseAggregates` | `mondrian.rolap.aggregates.Use` | Master switch. Gates the schema-load scan (`AggTableManager#initialize`) *and* is re-read at query time (`AggregationManager#generateSql`, `SqlMemberSource#chooseAggStar`), so it can be toggled dynamically. |
| `ReadAggregates` | `mondrian.rolap.aggregates.Read` | Additionally required for **naming-convention** recognition (`DefaultRules`). Checked per candidate table inside `AggTableManager#loadRolapStarAggregates`. |
| `ChooseAggregateByVolume` | `mondrian.rolap.aggregates.ChooseByVolume` | If true, `AggStar#getSize` returns row count × row width instead of row count, changing the "smallest first" ordering. |
| `AggregateRules` | `mondrian.rolap.aggregates.rules` | Location of the naming-rules XML; default `/DefaultRules.xml`, a classpath resource (physically `mondrian/src/main/resources/DefaultRules.xml`). Can be a URL. |
| `AggregateRuleTag` | `mondrian.rolap.aggregates.rule.tag` | Selects which `<AggRule tag="...">` inside that file is active; default `default`. |
| `GenerateAggregateSql` | `mondrian.rolap.aggregates.generateSql` | Turns on the `AggGen` DDL suggestion generator (see below). |

Note the division of labor as actually coded in this tree: with only
`UseAggregates=true`, schema-declared `<AggName>`/`<AggPattern>` tables are
recognized; naming-convention discovery of arbitrary `agg_*` tables requires
`ReadAggregates=true` as well. (The property javadocs describe the upstream
intent — "Read scans, Use uses" — slightly differently; the code path in
`AggTableManager#initialize` and `#loadRolapStarAggregates` is authoritative.)

## Discovery at schema load

`RolapSchema` owns one `AggTableManager`. At the end of `RolapSchema#load`,
after all cubes and `RolapStar`s exist (so every star column already has its
bit position), it calls `AggTableManager#initialize(connectInfo)`, which — if
`UseAggregates` is on — runs `AggTableManager#loadRolapStarAggregates`:

1. **Snapshot the database.** `JdbcSchema.makeDB(dataSource)` returns a
   per-DataSource cached snapshot; `JdbcSchema#loadTables` enumerates every
   `TABLE` and `VIEW` via `DatabaseMetaData#getTables`. The scan can be scoped
   with the `AggregateScanSchema` / `AggregateScanCatalog` connection
   properties (`RolapConnectionProperties`); otherwise the connection's
   default schema/catalog is used. Columns are loaded lazily
   (`JdbcSchema.Table#load`) only for tables that pass a name match.
2. **Bind the fact table.** For each `RolapStar`,
   `AggTableManager#bindToStar` annotates the JDBC fact-table columns with
   *usages*: `MEASURE` for columns backing `RolapStar.Measure`s, `FOREIGN_KEY`
   for columns joining to dimension tables (or degenerate-dimension columns).
   These usages are what the recognizers match against.
3. **Test every table.** For each table in the snapshot: schema-declared
   `<AggExclude>`s are honored first (`ExplicitRules.excludeTable`); then an
   explicit match is tried (`ExplicitRules.getIncludeByTableDef` — an exact
   `<AggName>` beats an `<AggPattern>` regex); failing that, and only when
   `ReadAggregates` is on, the default naming rules are tried
   (`DefaultRules#matchesTableName`).
4. **Verify the columns.** A name match alone is not enough — the matching
   rule set's `columnsOK` runs a `Recognizer` over the candidate (below). Only
   if it returns true is `AggStar.makeAggStar` called and the result added via
   `RolapStar#addAggStar`, which keeps the list sorted by `AggStar#getSize`
   ascending. An aggregate that turns out to have zero rows is dropped with an
   `AggTableZeroSize` warning.

All findings go through a `MessageRecorder` (`mondrian.recorder.ListRecorder`);
at the end the recorder's info/warning/error messages are flushed to the
`AggTableManager` logger, and **if any errors were recorded the whole schema
load fails** with `AggLoadingExceededErrorCount`. A table whose name matches
but whose columns don't check out is therefore not silently skipped — it is an
error (this is deliberate; see the comment at the end of the candidate loop in
`AggTableManager#loadRolapStarAggregates`).

## Recognition: one template, two implementations

`Recognizer` (package `mondrian.rolap.aggmatcher`) is a shared
template — "less about defining a type and more about code sharing", per its
own javadoc. `Recognizer#check` runs the fixed sequence:

1. `checkIgnores` — mark columns matching the ignore matcher.
2. `checkFactColumns` — exactly one numeric fact count column (plus optional
   per-measure fact count columns, `<measure column>_<fact count column>`).
3. `checkMeasures` (abstract) — at least one measure must match
   (`checkNosMeasures`), then `generateImpliedMeasures` (if the fact table has
   both SUM and AVG of a column and the aggregate has only one of them, the
   other is derived through the fact count column).
4. `checkForeignKeys` (via abstract `matchForeignKey`) — for each fact-table
   foreign key, zero matches means the dimension was *lost or collapsed*;
   more than one match is an error.
5. `checkLevels` / `matchLevels` (abstract) — for hierarchies whose foreign
   key was not found, try to match level columns directly on the aggregate
   table (*collapsed* dimensions).
6. `checkUnusedColumns` — any column with no usage is warned about and marked
   ignored. This is not free: the resulting `AggStar` reports
   `hasIgnoredColumns()`, which forces rollup at query time and disqualifies
   the table for distinct-count measures.

The measure aggregators are converted for the aggregate context by
`Recognizer#convertAggregator`: an AVG fact measure over a SUM aggregate
column becomes `RolapAggregator.AvgFromSum` (dividing by the fact count),
and similarly `AvgFromAvg` / `SumFromAvg`.

### DefaultRecognizer — naming conventions

`DefaultRecognizer` matches by name patterns from the rules XML
(`DefaultRules`, a JVM-wide singleton parsed from the `AggregateRules`
resource; the element model is the generated `DefaultDef` in
`src/generated/java/mondrian/rolap/aggmatcher/`). In this tree's
`DefaultRules.xml`, the active `default` rule says:

- **Table match**: `<TableMatch pretemplate="agg_.+_"/>` — a candidate matches
  fact table `F` if its name matches `agg_.+_F`. FoodMart-style names such as
  `agg_c_14_sales_fact_1997` or `agg_l_05_sales_fact_1997` all fit this; the
  `l`/`c`/`lc`/`pl` infixes are a human convention for *lost* / *collapsed*
  dimensions (echoed by `AggGen`, whose generated names use the prefixes
  `agg_l_XXX_` and `agg_c_XXX_`), not something the matcher interprets.
- **Fact count**: `<FactCountMatch/>` — the default column name `fact_count`.
- **Levels** (`<LevelMap>`): a column matches a level if it equals (per regex)
  `${hierarchy_name}_${level_name}` (lowercased),
  `${hierarchy_name}_${level_column_name}` (lowercased),
  `${usage_prefix}${level_column_name}`, or `${level_column_name}`.
- **Measures** (`<MeasureMap>`): `${measure_name}` (lowercased),
  `${measure_column_name}`, or `${measure_column_name}_${aggregate_name}`
  (e.g. `store_sales_sum`).
- **Foreign keys** (`<ForeignKeyMatch>`): same column name as in the fact
  table.

`DefaultRecognizer#matchLevels` collects one aggregate column per level of a
hierarchy, sorts matches by level depth, and enforces contiguity: if a level
matched but its parent level did not, that is an error. If the shallowest
matched level is not the hierarchy's top level, the level is treated as
**non-collapsed** (see below), which additionally requires the level to have
unique members.

### ExplicitRecognizer — schema-declared tables

Explicit rules come from the schema XML: `<AggName>`, `<AggPattern>` and
`<AggExclude>` elements nested in the cube's fact `<Table>` element, collected
per cube into an `ExplicitRules.Group` (`ExplicitRules.Group#make`, surfaced
through `AggTableManager#getAggGroups`). A `TableDef` (subclassed
`NameTableDef` / `PatternTableDef`, both with an `ignorecase` option and
patterns with their own nested excludes) carries the full column mapping:

- `<AggFactCount>` — the fact count column (name match is case-insensitive);
  optional `<AggMeasureFactCount>` per-measure fact counts.
- `<AggIgnoreColumn>` — columns to skip deliberately.
- `<AggForeignKey factColumn=... aggColumn=.../>` — fact-FK → agg-FK renames;
  a fact foreign key with no mapping is simply a lost dimension.
- `<AggMeasure name="[Measures].[...]" column=.../>` — measures by unique
  name, with an optional explicit `rollupType` taking precedence over the
  derived rollup aggregator (`ExplicitRecognizer#makeMeasure`).
- `<AggLevel name="[Dim].[Level]" column=... collapsed=.../>` — levels by
  unique name, with optional `ordinalColumn`, `captionColumn` and
  `<AggLevelProperty>` columns (marked `LEVEL_EXTRA`), and an explicit
  `collapsed` flag.
- `approxRowCount` attribute — supplies the size estimate so Mondrian need not
  run `SELECT COUNT(*)` (`AggStar.FactTable#makeNumberOfRows` otherwise counts
  the table, consulting the star's statistics cache first).

## AggStar: the star's aggregate mirror

A recognized table becomes an `AggStar` (`AggStar#makeAggStar`), which
deliberately parallels `RolapStar`: a `FactTable` (the aggregate table itself)
with inner `Column`, `Level` and `Measure` classes, and `DimTable` children
for foreign keys that survived aggregation. Crucially, every `AggStar` column
reuses the **bit position of the corresponding `RolapStar` column**, so the
whole matching problem reduces to `BitKey` algebra. An `AggStar` maintains:

- `bitKey` — all its columns; `levelBitKey` — levels + foreign keys;
  `measureBitKey`; `foreignKeyBitKey`; `distinctMeasureBitKey`.
- Per distinct-count measure, a `rollableLevelBitKey`
  (`AggStar.FactTable.Measure#getRollableLevelBitKey`): the levels reachable
  through that measure's own dimension, which can be aggregated away without
  changing the distinct count.

A **collapsed** level lives directly on the aggregate fact table (e.g. a
`month` column with no `time_by_day` join). A **non-collapsed** level means
the aggregate kept the dimension's key column at some level and must still
join to the dimension table(s) to reach parent levels:
`AggStar.FactTable#loadLevel` registers each parent level's column in
`levelColumnsToJoin` with a synthetic join path, and `AggQuerySpec` later
emits those joins. `AggStar#isFullyCollapsed` is true only when every level
column is collapsed.

## Query-time selection: AggregationManager#findAgg

When a segment must be loaded from SQL, `AggregationManager#generateSql`
first checks `UseAggregates` and that the request carries **no compound
predicates** — batches constrained by compound slicers/aggregate lists never
use aggregate tables (this is the trace point where
`AggregationKey`-level compound predicates opt a batch out). It then calls
`AggregationManager#findAgg(star, levelBitKey, measureBitKey, rollup)`:

- Candidates are scanned in `RolapStar#getAggStars` order — smallest first
  (per `AggStar#getSize`, row count or volume) — and the first acceptable one
  wins.
- The basic test is `AggStar#superSetMatch`: the aggregate's full `BitKey`
  must be a superset of the request's levels ∪ measures. The requested level
  bit key is first expanded with all parent-level bits
  (`AggregationManager#expandLevelBitKey`), since parent columns don't change
  grain.
- **No distinct-count measures involved:** accept immediately. The out-param
  `rollup[0]` is set true when the aggregate is coarser or wider than the
  request — its level bit key differs from the requested one, it is not fully
  collapsed, or it has ignored columns — telling `AggQuerySpec` to re-aggregate
  with `GROUP BY`.
- **Distinct-count measures involved:** rolling up a `COUNT(DISTINCT ...)` is
  generally wrong (distinct counts are not additive), so extra checks apply:
  a table with ignored columns is rejected (unknown granularity); each
  requested distinct measure contributes its `rollableLevelBitKey`, and
  `AggStar#select` accepts only if the aggregate's levels equal the request's
  except for those rollable ("core") levels; leftover foreign keys not
  themselves the distinct-counted column reject the table; and an empty
  requested level key rejects it too (nothing to constrain the distinct set).
  When such a rollup is executed, a distinct measure that is one of the
  aggregate's foreign keys is re-aggregated with the non-distinct `COUNT`
  (`AggStar.FactTable.Measure#getRollupAggregator`).

`AggQuerySpec` then builds the SQL: same predicate machinery as the fact-table
path (`RolapStar.Column.createInExpr`), but `FROM` the aggregate table, joining
`DimTable`s / `levelColumnsToJoin` paths only as needed, and — when `rollup` is
set — wrapping each measure in its rollup aggregator
(`Measure#generateRollupString`, e.g. `SUM(store_sales_sum)` or AvgFromSum's
`SUM(sum)/SUM(fact_count)`) with a `GROUP BY` over the requested columns. It is
a parallel implementation of the `QuerySpec` idea rather than a subclass of
`AbstractQuerySpec` — see [sql-generation.md](sql-generation.md).

Aggregate tables also serve **member queries**, not just cell loads.
`SqlMemberSource#chooseAggStar` (used by `#makeChildMemberSql`) and
`SqlTupleReader#chooseAggStar` (native/tuple reads) build level/measure bit
keys from the evaluator context and call the same `findAgg`. Preconditions:
`UseAggregates` on, a constraint that supports aggregates
(`SqlContextConstraint` / `TupleConstraint#supportsAggTables`), a real (not
calculated) current measure, a non-virtual cube (tuple path), and a
satisfiable `CellRequest` from the context. If the target level is collapsed
in the chosen aggregate and needs no extra columns, the member SQL selects
straight from the aggregate table without touching the dimension table;
non-collapsed levels join back to it (`SqlMemberSource#isLevelCollapsed`).

## AggGen: suggesting aggregate tables

With `GenerateAggregateSql=true`, `FastBatchingCellReader.Batch#loadAggregation`
invokes `AggGen` (package `aggmatcher`) for each batch: it prints — to stdout,
not the logger — `CREATE TABLE` and `INSERT INTO ... SELECT` statements for
two candidate designs covering that batch's columns: a *lost-dimension* table
(`agg_l_XXX_<fact>`) keeping fact-table foreign keys, and a *collapsed* table
(`agg_c_XXX_<fact>`) with dimension levels folded in. It refuses virtual
cubes. (The `mondrian.rolap.aggtab` package, despite its name, contains only a
`package.html` placeholder describing tools that are not in this tree.)

## Practical notes

To verify an aggregate table is being used, enable DEBUG logging on
`mondrian.rolap.agg.AggregationManager`: every segment load logs either
`MATCH` (with the level/measure/aggregate bit keys and the chosen table) or
`NO MATCH` (with the bit keys and every registered `AggStar`). DEBUG on
`mondrian.rolap.aggmatcher.AggTableManager` dumps each star with its
recognized aggregates after loading, and the recognition recorder's messages
(info/warn/error) land on the same logger.

Common reasons a table is not used, all grounded in the checks above:

- `ReadAggregates` is off, so `agg_*` tables are never even name-matched
  (explicit `<AggName>` tables still work).
- The name doesn't match `agg_.+_<exact fact table name>` per this tree's
  `DefaultRules.xml`.
- Fact count problems: no `fact_count` column, more than one, or a non-numeric
  one — recorded as errors, which abort aggregate loading entirely
  (`AggLoadingExceededErrorCount`).
- No measure column matched (`NoMeasureColumns` error), or one fact measure
  matched several aggregate columns (`AggMultipleMatchingMeasure`).
- Level columns match but skip a parent level, or a non-collapsed bottom level
  lacks unique members (both `DefaultRecognizer#matchLevels` errors).
- Extra unmatched columns: only warnings, but the resulting ignored columns
  force rollup and exclude the table from all distinct-count requests.
- At query time: the batch carries compound predicates (compound slicers,
  aggregated members), the request involves a distinct-count measure the
  aggregate can't safely serve, or the context measure is calculated (member
  fetch paths).

The `aggmatcher` package is essentially unpatched in this fork — the only
`PATCH` marker is the commons-lang3 import in `Recognizer` — so upstream
Mondrian 9.3 documentation about aggregate tables applies here without
caveats.
