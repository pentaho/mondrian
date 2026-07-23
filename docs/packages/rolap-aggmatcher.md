# Package: `mondrian.rolap.aggmatcher`

*Aggregate table discovery: scanning the database for pre-summarized copies of
a fact table and modeling each recognized one as an `AggStar`. This is a map of
the package only — the full narrative (properties, recognition rules, query-time
selection, troubleshooting) is in
[../topics/aggregate-tables.md](../topics/aggregate-tables.md).*

## Role

The package answers one question at schema-load time: *which physical tables
are aggregates of which `RolapStar` fact table, and what does each column
mean?* Everything here runs once, at the end of `RolapSchema#load` (via
`AggTableManager#initialize`); the products — `AggStar` instances registered on
each `RolapStar` — are consumed at query time by `mondrian.rolap.agg`
(`AggregationManager#findAgg` picks one per segment load) and by the member
readers (`SqlMemberSource#chooseAggStar`, `SqlTupleReader#chooseAggStar`).

## Classes

| Type | Role |
|---|---|
| `AggTableManager` | Orchestrator, one per `RolapSchema`. `#initialize(connectInfo)` → `#loadRolapStarAggregates`: snapshot the JDBC schema, annotate each star's fact-table columns with usages (`#bindToStar`), run excludes/recognizers over every candidate table, register survivors via `RolapStar#addAggStar`. Errors recorded during recognition fail the whole schema load. |
| `JdbcSchema` | Cached per-DataSource snapshot of database metadata (`JdbcSchema.makeDB`). Inner `Table` / `Table.Column` model tables and columns; `Column.Usage` (typed by the `UsageType` enum: `FACT_COUNT`, `MEASURE`, `FOREIGN_KEY`, `LEVEL`, `IGNORE`, …) records what a column was recognized *as*. Column loading is lazy — only tables passing a name match are inspected. |
| `Recognizer` | Abstract template shared by both recognizers. `#check` runs the fixed sequence: ignores → fact count column → measures → foreign keys → levels → unused columns. Also converts aggregators for the aggregate context (`#convertAggregator`, e.g. AVG-from-SUM via the fact count). |
| `DefaultRecognizer` | `Recognizer` driven by the naming-convention rules (`DefaultRules`); matches measures, foreign keys and levels purely by column-name patterns. Used only when `ReadAggregates` is on. |
| `ExplicitRecognizer` | `Recognizer` driven by a schema-declared `ExplicitRules.TableDef`; column meanings come from `<AggFactCount>`, `<AggMeasure>`, `<AggLevel>`, `<AggForeignKey>` mappings rather than name patterns. |
| `DefaultRules` | JVM-wide singleton holding the parsed naming-convention rules (table match, fact-count match, level/measure mappers). `#matchesTableName` is the first-stage name test. |
| `ExplicitRules` | Container for a cube's `<AggName>` / `<AggPattern>` / `<AggExclude>` declarations. Inner types: `Group` (per-cube collection), abstract `TableDef` with `NameTableDef` / `PatternTableDef`, and `Exclude` implementations (`ExcludeName`, `ExcludePattern`). |
| `AggStar` | The product: an aggregate-table mirror of `RolapStar`, built by `AggStar.makeAggStar`. Inner classes parallel the star model — abstract `Table` (with `JoinCondition`, `Column`, `ForeignKey`, `Level`), `FactTable` (with `Measure`), `DimTable`. Columns reuse the corresponding `RolapStar` column bit positions, so query-time matching is pure `BitKey` algebra. |
| `DefaultDef` | Generated element model for the rules XML — produced by eigenbase-xom from the `aggregates` XOM model (`aggregates.dtd` sits beside it in `src/generated/java/mondrian/rolap/aggmatcher/`). Do not edit by hand. |
| `AggGen` | Optional DDL suggester (`GenerateAggregateSql` property): for each cell-load batch, prints `CREATE TABLE` / `INSERT INTO` SQL for candidate *lost* (`agg_l_XXX_`) and *collapsed* (`agg_c_XXX_`) aggregate tables. Invoked from `FastBatchingCellReader.Batch#loadAggregation`. |

## Where the naming rules live

The naming-convention rules are an XML resource: by default `/DefaultRules.xml`
on the classpath (physically `mondrian/src/main/resources/DefaultRules.xml`),
loaded once into the `DefaultRules` singleton. The `AggregateRules` property
(`mondrian.rolap.aggregates.rules`) overrides the location — a classpath
resource path or a URL — and `AggregateRuleTag` selects which `<AggRule>`
inside the file is active (default `default`). Changing the property forces a
reload on next access. What the default rules actually match (the
`agg_.+_<fact>` table pattern, `fact_count`, the level/measure name templates)
is spelled out in
[../topics/aggregate-tables.md](../topics/aggregate-tables.md).

## Entry points

| Called from | Into this package | When |
|---|---|---|
| `RolapSchema#load` (end of schema load, after all stars exist) | `AggTableManager#initialize` → `#loadRolapStarAggregates` | Schema load — the only time discovery runs |
| `AggregationManager#findAgg` (from `#generateSql`) | `RolapStar#getAggStars` → `AggStar#superSetMatch` / `#select` | Every segment load, choosing an aggregate for the cell SQL |
| `SqlMemberSource#chooseAggStar` | same `findAgg` machinery | Member-children SQL against an aggregate table |
| `SqlTupleReader#chooseAggStar` | same `findAgg` machinery | Native/tuple reads against an aggregate table |
| `FastBatchingCellReader.Batch#loadAggregation` | `AggGen` | Only when `GenerateAggregateSql` is on |

## Neighbors

- **`mondrian.rolap`** — `RolapStar` owns the registered `AggStar` list (sorted
  smallest-first by `AggStar#getSize`); star columns supply the bit positions
  the whole matching scheme is built on.
- **`mondrian.rolap.agg`** — consumes `AggStar` at query time; `AggQuerySpec`
  (in `agg`, not here) generates the actual SQL against the aggregate table.
- **`mondrian.recorder`** — `MessageRecorder` collects recognition
  info/warnings/errors; errors abort schema load.

This package is essentially unpatched relative to upstream Mondrian 9.3 — the
only `PATCH` marker is a commons-lang3 import in `Recognizer`.
