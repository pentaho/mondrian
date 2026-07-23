# Package guide: `mondrian.olap4j`

*The olap4j driver adapter: implements the `org.olap4j` API (a JDBC-style Java
OLAP API) on top of the native engine — the primary API surface for the
mondrian-olap JRuby gem. Entry/exit control flow:
[../query-lifecycle.md](../query-lifecycle.md) stages 1, 2 and 6; the native
interfaces being wrapped: [olap.md](olap.md); the statement/execution machinery
underneath: server.md (planned).*

## Role and the wrapping pattern

Everything in this package follows one pattern: **each olap4j object is a thin
adapter holding (a) the native engine object it represents and (b) a
back-pointer to the `MondrianOlap4jConnection`** (directly, or through the
statement/cell-set/schema it hangs off). The adapter delegates the work to the
native object and uses the connection back-pointer for context — converting
further native objects to olap4j wrappers (the `MondrianOlap4jConnection#toOlap4j`
overload family), reaching the `Factory`, and creating exceptions via the
shared `Helper`. Adapters are created on demand and are cheap; nothing in this
package caches or computes — all state lives in the native objects. The
correspondence:

| olap4j adapter | wraps (native) |
|---|---|
| `MondrianOlap4jConnection` | `mondrian.rolap.RolapConnection` |
| `MondrianOlap4jStatement` | *is* a `mondrian.server.Statement` (extends `StatementImpl`) |
| `MondrianOlap4jCellSet` | `mondrian.olap.Result` (a `RolapResult`); *is* a `mondrian.server.Execution` |
| `MondrianOlap4jCellSetAxis` / `...Position` | `mondrian.rolap.RolapAxis` / one index into its `TupleList` |
| `MondrianOlap4jCell` | `mondrian.rolap.RolapCell` |
| `MondrianOlap4jCube`, `...Dimension`, `...Hierarchy`, `...Level`, `...Member`, `...Measure`, `...NamedSet`, `...Property` | the same-named `mondrian.olap` metadata objects |

Unwrapping goes the other way: every adapter implements `OlapWrapper`
(`unwrap(Class)`), so callers can reach the native object —
`connection.unwrap(RolapConnection.class)`, or any metadata adapter down to
its `OlapElement` via `MondrianOlap4jMetadataElement#unwrapImpl`.

## Connection establishment

`MondrianOlap4jDriver` is a standard JDBC `Driver` for URLs starting with
`jdbc:mondrian:` (`MondrianOlap4jConnection#acceptsURL`; the prefix
`jdbc:mondrian:engine:` is also accepted). The rest of the URL is a Mondrian
connect string (`Jdbc=...;JdbcDrivers=...;Catalog=...` — see
`mondrian.rolap.RolapConnectionProperties`), parsed by
`Util#parseConnectString`; entries from the JDBC `Properties` argument are
merged on top of it in `MondrianOlap4jConnection#<init>`, so credentials can be
passed as `JdbcUser`/`JdbcPassword` properties instead of inside the URL.

**(fork PATCH)** The driver's static initializer skips
`DriverManager.registerDriver` unless the system property
`mondrian.olap4j.registerDriver` allows it: registration is unnecessary under
JRuby (the gem instantiates the driver reflectively and calls
`MondrianOlap4jDriver#connect` directly) and the registered instance held a
global reference to the JRuby runtime that prevented its garbage collection.

Construction chain:

1. `MondrianOlap4jDriver#connect` → `Factory#newConnection`.
2. `Factory` exists because the driver historically supported several JDBC
   versions (3.0/4.0/4.1) whose interfaces differ per JDK; today only
   `FactoryJdbc41Impl` remains (hardwired in `MondrianOlap4jDriver#<init>`),
   creating `...Jdbc41` subclasses of the abstract adapters. The JDBC-interface
   boilerplate (hundreds of `UnsupportedOperationException` stubs) lives in
   `FactoryJdbc4Plus`'s abstract inner classes (`AbstractConnection`,
   `AbstractCellSet`, …), which the `Jdbc41` classes extend.
3. `MondrianOlap4jConnection#<init>` calls the **native**
   `mondrian.olap.DriverManager#getConnection` with the merged property list —
   this is where the `RolapConnection` (and, through the schema pool, possibly
   a shared `RolapSchema`) comes to life — then builds the olap4j metadata
   roots: one `MondrianOlap4jDatabase`, its catalogs via the `CatalogFinder`
   interface (implemented by `MondrianServerImpl`), and the
   `mondrian.olap.Schema` → `MondrianOlap4jSchema` map (`schemaMap`).

A connection registers each statement it creates with the engine
(`MondrianServer#addStatement` in `#createStatement` /
`#prepareOlapStatement`), which is how the server's statement registry and
cancellation-by-id work.

## Statement and execution

`MondrianOlap4jStatement` implements both `org.olap4j.OlapStatement` and the
native `mondrian.server.Statement` (via `StatementImpl`) — one object plays
both roles. `MondrianOlap4jStatement#parseQuery` runs the native parse inside
`Locus.execute` (stage 2 of the [lifecycle](../query-lifecycle.md)) and also
builds the `MondrianOlap4jCellSetMetaData`. For
`MondrianOlap4jPreparedStatement` this happens **in the constructor**, so a
prepared statement holds a fully resolved and compiled `Query`;
`#executeQuery` merely re-executes it. Parameters are set through
`Query#getParameters` (the prepared statement doubles as its own
`OlapParameterMetaData`).

A statement has at most one open cell set (`openCellSet`);
`MondrianOlap4jStatement#executeOlapQueryInternal` closes the previous one,
publishes the new one, then executes it *outside* the monitor so another
thread can call `#cancel` mid-flight. The JDBC `#executeQuery(String)`
(relational `ResultSet` variant) is only valid for `DRILLTHROUGH` and
`EXPLAIN` statements (`MondrianOlap4jStatement#executeQuery2`); MDX SELECT
goes through `executeOlapQuery` / `PreparedOlapStatement#executeQuery`.

### `MondrianOlap4jCellSet` extends `Execution`

The central design point of the package: the cell set — olap4j's result-set
object — **is the engine's execution object**. `MondrianOlap4jCellSet` extends
`mondrian.server.Execution`, constructed with the statement and its timeout
(`super(olap4jStatement, olap4jStatement.getQueryTimeoutMillis())`), and
`MondrianOlap4jCellSet#execute` passes **itself** to
`RolapConnection#execute(Execution)`. It therefore carries all `Execution`
state for the run: cancellation flag, timeout deadline, execution id, phase
counters and SQL-statement tracking — checked by the engine at cell-loading
phase boundaries (`Execution#checkCancelOrTimeout`). This is what makes
`Statement#cancel` trivial: it just calls `openCellSet.cancel()` (an
`Execution` method), no lookup needed; `MondrianOlap4jStatement#start`
correspondingly starts `openCellSet` as the current execution.

After execution the cell set holds the `RolapResult` and materializes one
`MondrianOlap4jCellSetAxis` per result axis plus a filter (slicer) axis.
The result-reading surface:

- `MondrianOlap4jCellSetMetaData` — per-query metadata, built at **parse**
  time from the `Query` (axes, cube, cell properties); shared between prepared
  statement and cell set. `MondrianOlap4jCellSetAxisMetaData` describes one
  axis (hierarchies, properties) from its `QueryAxis`.
- `MondrianOlap4jCellSetAxis#getPositions` — a lazy `AbstractList` view over
  the `RolapAxis`'s `TupleList`; a `Position` is just (tupleList, index), and
  `Position#getMembers` converts members to olap4j wrappers on access.
- `MondrianOlap4jCellSet#getCell` — coordinates/ordinal → `RolapResult#getCell`
  → wrap in `MondrianOlap4jCell` (out-of-range coordinates become
  `IndexOutOfBoundsException` in `#getCellInternal`).
- `MondrianOlap4jCell` — value/formatted value/properties delegate to
  `RolapCell`. `Cell#drillThrough` is implemented
  (`MondrianOlap4jCell#drillThroughInternal` → `RolapCell#drillThroughInternal`,
  returning the SQL statement's wrapped `ResultSet`; returns null if
  `RolapCell#canDrillThrough` is false). `Cell#setValue` is implemented too —
  it is writeback into the connection's active `Scenario`
  (`RolapCell#setValue`), not a database write.

Scenario support has **no adapter class here**: `createScenario` /
`setScenario` / `getScenario` on the connection delegate straight to
`RolapConnection`, whose `mondrian.rolap.ScenarioImpl` directly implements
`org.olap4j.Scenario`. The many pure-JDBC `ResultSet` methods on the cell set
(`next`, `getString`, …) throw `UnsupportedOperationException` — consume it
through the olap4j `CellSet` methods.

## Metadata surface

Two parallel metadata surfaces exist: the **object model** — the
`Catalog`/`Schema`/`Cube`/… adapters below, browsed from
`OlapConnection#getOlapSchema` / `Cube#getDimensions` / etc. — and the
**rowsets**: `MondrianOlap4jDatabaseMetaData` (`OlapDatabaseMetaData`)
implements `getCubes`, `getOlapDimensions`, `getMeasures`, … by delegating to
the XMLA metadata machinery (`XmlaUtil#getMetadataRowset`, e.g.
`MDSCHEMA_CUBES`) and returning the rows as a fixed `ResultSet`
(`Factory#newFixedResultSet` — despite its name, `EmptyResultSet` is the
general fixed-row result set; "empty" is just its default content).

**Access control**: metadata sees exactly what the connection's current
`Role` allows. Adapters fetch members and children through
`RolapConnection#getSchemaReader` (the role-aware reader) or
`Cube#getSchemaReader(role)` with the connection's `RolapConnection#getRole`,
and `MondrianOlap4jCatalog#getSchemas` filters out schemas where
`Role#getAccess` is `Access.NONE`. The role is switched per connection via
`MondrianOlap4jConnection#setRoleName` / `#setRoleNames` (multiple names form
a `RoleImpl#union`). Because member retrieval may run SQL, these metadata
calls wrap themselves in a `Locus` (`SchemaReader#withLocus`,
`Locus.execute`) — the same thread-context requirement as query execution.

`MondrianOlap4jExtra` (singleton, exposed via
`connection.unwrap(XmlaHandler.XmlaExtra.class)`) provides the internals the
`mondrian.xmla` layer needs beyond the olap4j API: drill-through execution,
schema load date, level cardinality via `SchemaReader`, the function list from
the schema's `FunTable`, keywords, cube flags, etc. It exists for the XMLA
endpoint, not for ordinary API clients.

Localization: `MondrianOlap4jConnection#setLocale` delegates to
`RolapConnection#setLocale`; caption/description getters on the cube,
hierarchy and level adapters call `OlapElement#getLocalized(CAPTION|DESCRIPTION,
locale)` with the connection locale (`MondrianOlap4jSchema#getLocale`).

## Exception mapping

`MondrianOlap4jConnection.Helper` (a package-visible nested class, one
instance per connection at `MondrianOlap4jConnection#helper`) centralizes
`OlapException` creation: `#createException` overloads attach a cell context
and/or cause, `#toOlapException` adapts `SQLException`, and `#deduceSqlState`
maps native exception types to SQLSTATE-style strings
(`ResourceLimitExceededException`, `QueryTimeoutException`,
`MondrianEvaluationException`, `QueryCanceledException`). Statement execution
catches `MondrianException` at the boundary and rethrows through the helper,
so olap4j callers see `OlapException` with the native exception as cause.

## Class table

| Class | Role |
|---|---|
| `MondrianOlap4jDriver` | JDBC driver entry point; `jdbc:mondrian:` URLs; **(fork PATCH)** skips DriverManager self-registration for JRuby |
| `Factory` / `FactoryJdbc41Impl` / `FactoryJdbc4Plus` | JDBC-version indirection; only the 4.1 implementation is live; `FactoryJdbc4Plus` holds the abstract JDBC-stub bases |
| `MondrianOlap4jConnection` | `OlapConnection` over `RolapConnection`; owns `Helper`, `schemaMap`, `toOlap4j` converters, role/locale switching; `#getMondrianConnection` **(fork PATCH: public)** |
| `MondrianOlap4jStatement` | `OlapStatement` that *is* a `mondrian.server.Statement`; parse via `Locus`, one open cell set, cancel support |
| `MondrianOlap4jPreparedStatement` | Parses (and thus resolves + compiles) MDX in the constructor; doubles as `OlapParameterMetaData` |
| `MondrianOlap4jCellSet` | `CellSet` over `RolapResult`; **extends `Execution`** (cancellation/timeout state for the run) |
| `MondrianOlap4jCellSetMetaData` / `...CellSetAxisMetaData` | Query-shape metadata built from the parsed `Query` / one `QueryAxis` |
| `MondrianOlap4jCellSetAxis` | Lazy `Position` list over a `RolapAxis` tuple list (inner class `MondrianOlap4jPosition`) |
| `MondrianOlap4jCell` | `Cell` over `RolapCell`: value, format, properties, drill-through, scenario writeback |
| `MondrianOlap4jDatabaseMetaData` | `OlapDatabaseMetaData`; metadata rowsets via `XmlaUtil#getMetadataRowset` |
| `MondrianOlap4jDatabase` / `...Catalog` / `...Schema` | Container adapters (Mondrian has one database; catalogs from `CatalogFinder`); catalog filters schemas by `Role` |
| `MondrianOlap4jCube` / `...Dimension` / `...Hierarchy` / `...Level` / `...Member` / `...Measure` / `...NamedSet` | Metadata element adapters; member lookups go through the role-aware `SchemaReader` under `Locus` |
| `MondrianOlap4jMetadataElement` | Base class: `OlapWrapper` unwrap down to the native `OlapElement` |
| `MondrianOlap4jProperty` / `IMondrianOlap4jProperty` | Property adapter; `MEMBER_EXTENSIONS`/`CELL_EXTENSIONS` expose Mondrian-only properties beyond the olap4j standard |
| `MondrianOlap4jExtra` | `XmlaHandler.XmlaExtra` implementation — driver internals for the XMLA layer |
| `CatalogFinder` | Interface (implemented by `MondrianServerImpl`) to enumerate catalogs/schemas per connection |
| `EmptyResultSet` | Fixed-row (header list + row list) JDBC `ResultSet`; backs metadata rowsets and EXPLAIN output |
| `Unsafe` | Public escape hatch to package-private methods; `Unsafe#setStatementProfiling` enables per-statement profiling |
| `MondrianOlap4jDriverVersion` | Generated (in `src/generated`): driver name/version constants |

## Practical notes: olap4j vs the native API

The mondrian-olap JRuby gem talks olap4j for everything the API covers —
connect, prepare, execute, axes, cells, metadata browsing. For engine services
that olap4j does not expose, the bridge is
`MondrianOlap4jConnection#getMondrianConnection` — **(fork PATCH)** made
`public` so callers work on Java 17+, where package-private access is no
longer forcible. From the `RolapConnection` the gem reaches:

- `RolapConnection#getCacheControl` — cache flushing
  ([../topics/caching.md](../topics/caching.md));
- `RolapConnection#getSchemaReader` and the native schema objects — lookups
  beyond olap4j metadata (e.g. this fork's
  `SchemaReader#getLevelMemberByUniqueKey`);
- `RolapCell` (via the cell adapter's package-visible `cell` field or
  `drillThroughInternal`) — drill-through with field lists and row counts;
- `mondrian.server.Statement#enableProfiling` (or `Unsafe#setStatementProfiling`)
  — query plan and timing delivery through `mondrian.spi.ProfileHandler`.

Rule of thumb: olap4j for query execution and result reading; the native
connection for cache control, deeper schema introspection, and profiling.
