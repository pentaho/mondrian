# Class reference: SPI, server, olap4j, XMLA, util, and peripheral packages

*Lookup catalog for every type in `mondrian.spi` (+ `spi.impl`), `mondrian.server`
(+ `server.monitor`), `mondrian.olap4j`, `mondrian.xmla` (+ `xmla.impl`),
`mondrian.util`, `mondrian.udf`, `mondrian.recorder`, `mondrian.i18n`,
`mondrian.web`, and `mondrian.tui` — grep it for a class name rather than reading
it whole. Three tiers: **Tier 1** classes get a full entry (`###` heading with
purpose/collaborators/lifecycle/threading), **Tier 2** a bold-lead paragraph
(related types grouped), **Tier 3** one table line. Narrative context lives in
the package guides: [../packages/spi.md](../packages/spi.md),
[../packages/server.md](../packages/server.md),
[../packages/olap4j.md](../packages/olap4j.md),
[../packages/xmla.md](../packages/xmla.md),
[../packages/util.md](../packages/util.md).*

---

## `mondrian.spi` and `mondrian.spi.impl`

### `Dialect`

- **Purpose**: Description of one SQL flavor — everything database-specific
  that SQL generation asks: identifier and string-literal quoting, FROM/JOIN/
  ORDER BY/GROUP BY shape rules, capability flags (`allowsCountDistinct`,
  `supportsGroupingSets`, `supportsMultiValueInExpr`, `requiresAliasForFromQuery`,
  …), IN-list limits, and JDBC-to-internal type mapping (`Dialect#getType`).
- **Extends / implements**: Standalone interface; nested enums
  `Dialect.DatabaseProduct` (the known-database registry) and `Dialect.Datatype`
  (schema column types with per-dialect literal quoting).
- **Key collaborators**: `DialectManager` (resolution and caching),
  `JdbcDialectImpl` (base implementation and generic fallback),
  `mondrian.rolap.sql.SqlQuery` and the `rolap.agg` predicate/segment SQL
  generators (the main callers), `RolapSchema#getDialect`.
- **Lifecycle / scope**: One instance per `DataSource`, created on first use via
  `DialectManager#createDialect` and cached by
  `DialectManager.CachingDialectFactory`; all stars, queries, and threads of a
  schema share it.
- **Threading**: Shared concurrently by every query on the schema — an
  implementation must be immutable after construction (as `JdbcDialectImpl` is,
  deducing all state from JDBC metadata in its constructor).
- **Notes**: Implementations are discovered from
  `META-INF/services/mondrian.spi.Dialect`; each contributes a
  `public static FACTORY` (`JdbcDialectFactory`), which requires a public
  `(java.sql.Connection)` constructor. Method-by-method treatment:
  [../topics/sql-generation.md](../topics/sql-generation.md).

### `SegmentCache`

- **Purpose**: SPI for external (out-of-JVM-heap) storage of aggregation
  segments: `get`/`put`/`remove` a `SegmentBody` keyed by `SegmentHeader`,
  `getSegmentHeaders` to enumerate contents, and listener registration so a
  distributed cache can announce entries created elsewhere.
- **Extends / implements**: Standalone interface; nested
  `SegmentCache.SegmentCacheListener` (+ `SegmentCacheEvent` with
  `EventType.ENTRY_CREATED`/`ENTRY_DELETED`) and
  `SegmentCache.SegmentCacheInjector` (static holder for programmatically added
  cache instances).
- **Key collaborators**: `mondrian.rolap.agg.SegmentCacheWorker` (wraps each
  cache and guards its calls), `SegmentCacheManager` (orchestration),
  `SegmentHeader`/`SegmentBody`/`SegmentColumn` (the data contract).
- **Lifecycle / scope**: One instance per Mondrian server (interface javadoc).
  Instances are collected by `SegmentCacheWorker#initCache` from three sources:
  the `mondrian.rolap.SegmentCache` property, `META-INF/services` lookup, and
  `SegmentCacheInjector#addCache`.
- **Threading**: Implementations must be thread-safe (stated in the interface
  javadoc); the engine calls potentially slow cache operations from
  `cacheExecutor` threads, never from the cache-manager actor thread.
- **Notes**: A cache may "forget" segments at any time — `SegmentCache#get`
  returning null is normal. The in-JVM
  `mondrian.rolap.cache.MemorySegmentCache` implements the same interface but
  is wired unconditionally. See [../topics/caching.md](../topics/caching.md).

**`DynamicSchemaProcessor` / `FilterDynamicSchemaProcessor`** — rewrites the
catalog XML string before parsing (templating, i18n, per-tenant edits). Named
by the `DynamicSchemaProcessor` connect-string property; a fresh instance is
created per schema (re)load in `RolapSchemaPool#processDynamicSchema`, and the
class name is part of `SchemaContentKey`, so different processors mean
different pooled schemas. `FilterDynamicSchemaProcessor` (in `spi.impl`) is the
bundled base: it reads the catalog URL via Apache VFS and lets subclasses
filter the stream (`FilterDynamicSchemaProcessor#filter`). See also
`mondrian.i18n.LocalizingDynamicSchemaProcessor` below.

**`UserDefinedFunction`** — adds an MDX function: name, signature
(parameter/return types from `mondrian.olap.type`), and an
`execute(Evaluator, Argument[])` body. Registered per schema via
`<UserDefinedFunction name="…" className="…"/>` (or a nested `<Script>`),
handled by `RolapSchema#defineFunction`, or globally via
`META-INF/services/mondrian.spi.UserDefinedFunction`, loaded by
`GlobalFunTable`. The class must be public (static if nested);
`Util#createUdf` prefers a `public Udf(String name)` constructor and falls back
to no-arg. Bundled implementations: the `mondrian.udf` package below.

**Formatter SPIs — `CellFormatter`, `MemberFormatter`, `PropertyFormatter`** —
display-rendering hooks for cell values (overrides the format string), member
captions, and member property values. Declared here but constructed by
`mondrian.rolap.format.FormatterFactory` during schema realization from
`<Measure>`/`<Level>`/`<Property>` attributes or child elements (the element
supersedes the attribute). Each formatter is created once per schema element at
load time — needs a public no-arg constructor — and is then invoked from
evaluation threads, so keep implementations stateless.

**`ProfileHandler`** — receives the executed `Calc` plan text and `QueryTiming`
after a query completes. Programmatic only: attach via
`mondrian.server.Statement#enableProfiling`; `Query#<init>` auto-attaches a
handler that writes to the `mondrian.profile` logger when that logger is at
DEBUG.

**`CatalogLocator` (+ `CatalogLocatorImpl`, `IdentityCatalogLocator`,
`ServletContextCatalogLocator`)** — translates the `Catalog` connect property
into a real URL. No property hook: an instance is passed explicitly to
`MondrianServer#createWithRepository` or `mondrian.olap.DriverManager#getConnection`
and applied via `CatalogLocator#locate`. `IdentityCatalogLocator` and
`CatalogLocatorImpl` both return the path unchanged;
`ServletContextCatalogLocator` resolves `/WEB-INF/…`-style paths against a
`ServletContext`.

**`DataSourceResolver` / `JndiDataSourceResolver`** — maps the `DataSource`
connect-string value to a `javax.sql.DataSource`. Named by the
`mondrian.spi.dataSourceResolverClass` property; one shared instance is lazily
created in `RolapConnection#getDataSourceResolver`. The default
`JndiDataSourceResolver` performs a JNDI lookup.

**`DataSourceChangeListener` (+ `DataSourceChangeListenerImpl`,
`DataSourceChangeListenerImpl2`, `DataSourceChangeListenerImpl3`,
`DataSourceChangeListenerImpl4`)** — tells member and aggregation caches when the underlying
database changed (`isHierarchyChanged`, `isAggregationChanged`), forcing
reload. Named by the `DataSourceChangeListener` connect-string property,
created per schema in `RolapSchema#createDataSourceChangeListener`, consulted
by `MemberCacheHelper` and `RolapStar`. The four numbered `Impl` classes are
trivial always-unchanged/always-changed implementations for testing.

**`DialectFactory` / `DialectManager` / `JdbcDialectFactory`** — the dialect
resolution machinery. `DialectManager#createDialect(dataSource, connection)`
builds a chain of factories: `ServiceDiscovery` reads the services file, each
dialect class contributes its `public static FACTORY` (or a reflection-based
factory around its `(Connection)` constructor), each factory accepts when
`JdbcDialectImpl#getProduct` maps the connection metadata to its
`DatabaseProduct`, first acceptance wins, generic `JdbcDialectImpl` is the
fallback, and `DialectManager.CachingDialectFactory` caches the result per
`DataSource`. An explicit dialect class name can be passed to the
three-argument `DialectManager#createDialect`; engine call sites pass null
(auto-detect). There is no `DialectResolver` interface in this codebase.

**`JdbcDialectImpl`** — the base class of every bundled dialect and itself the
generic fallback dialect. Deduces quoting characters, product, and capability
defaults from `DatabaseMetaData` in the constructor and never mutates state
afterwards. Also owns statistics-provider assembly
(`JdbcDialectImpl#computeStatisticsProviders`). **(fork PATCH)** MONDRIAN-2702:
the default JDBC type mapping sends `Types.BIGINT` to `SqlStatement.Type.LONG`
instead of `DOUBLE`, so 64-bit keys and measures don't lose precision.

**`ClickHouseDialect`** — **(fork PATCH: added entirely in this fork)** dialect
for ClickHouse (`DatabaseProduct.CLICKHOUSE`): MySQL-style backslash-aware
string-literal quoting (`ClickHouseDialect#quoteStringLiteral`) and
multi-value `IN` support, registered in the services file like any other
dialect.

**PATCHed dialects — `OracleDialect`, `PostgreSqlDialect`,
`MicrosoftSqlServerDialect`, `MariaDBDialect`** — bundled dialects carrying
fork patches beyond the `JdbcDialectImpl` base:

- `OracleDialect` — (fork PATCH) supports multiple columns in `IN` conditions
  (tuple IN predicates).
- `PostgreSqlDialect` — (fork PATCH) supports row expressions in `IN`
  conditions; base of the Greenplum/Netezza/Redshift dialects.
- `MicrosoftSqlServerDialect` — (fork PATCH) MONDRIAN-990: prepends `N` to
  quoted string literals so UTF-8 special characters survive.
- `MariaDBDialect` — extends `MySqlDialect`; (fork PATCH) reports that
  ColumnStore does not allow selecting columns absent from `GROUP BY`.

**`SegmentHeader` / `SegmentBody` / `SegmentColumn`** — data contracts, not
extension points: the vocabulary a `SegmentCache` must speak. `SegmentHeader`
is the identity of a cached segment (measure, constrained columns, compound
predicates, and the schema checksum, so an entry can never be served to a
different schema); `SegmentBody` is its serializable cell data; `SegmentColumn`
is one constrained column of a header (name plus an `ArraySortedSet` of
values). All `Serializable`. See [../topics/caching.md](../topics/caching.md).

**`StatisticsProvider` / `SqlStatisticsProvider` / `JdbcStatisticsProvider`** —
row-count and cardinality estimates used when choosing aggregate tables.
Configured by the `mondrian.statistics.providers` property (and per-product
`mondrian.statistics.providers.<PRODUCT>`, e.g. `.MYSQL`), read in
`JdbcDialectImpl#computeStatisticsProviders`, exposed via
`Dialect#getStatisticsProviders`, consumed by `RolapStatisticsCache`.
`SqlStatisticsProvider` (default) issues `select count(*)`-style queries;
`JdbcStatisticsProvider` reads JDBC index metadata instead.

**`Scripts`** — factory for script-backed SPI implementations: wraps script
text in a function declaration and compiles it against the SPI interface via
`Util#compileScript` (JSR-223). JavaScript is the only language the
`Scripts.ScriptLanguage` enum accepts. Has factories for script-based
formatters, UDFs, `DynamicSchemaProcessor`, `DataSourceResolver`, and
`DataSourceChangeListener`, but only the formatter and UDF variants are
reachable from schema XML (`<Script>` child elements).

### Remaining `mondrian.spi` / `spi.impl` classes

| Class | Extends | One line |
|---|---|---|
| `DialectUtil` | — | One helper, `DialectUtil#cleanUnicodeAwareCaseFlag`, used by dialects translating Java regexes to database regex syntax |
| `AccessDialect` | `JdbcDialectImpl` | Dialect for Microsoft Access (the JET engine) |
| `Db2Dialect` | `JdbcDialectImpl` | Dialect for IBM DB2 |
| `Db2OldAs400Dialect` | `Db2Dialect` | Dialect for old IBM DB2/AS400 versions |
| `DerbyDialect` | `JdbcDialectImpl` | Dialect for Apache Derby |
| `FirebirdDialect` | `JdbcDialectImpl` | Dialect for Firebird |
| `GoogleBigQueryDialect` | `JdbcDialectImpl` | Dialect for Google BigQuery |
| `GreenplumDialect` | `PostgreSqlDialect` | Dialect for Greenplum |
| `HiveDialect` | `JdbcDialectImpl` | Dialect for Apache Hive |
| `HsqldbDialect` | `JdbcDialectImpl` | Dialect for HSQLDB |
| `ImpalaDialect` | `HiveDialect` | Dialect for Cloudera Impala |
| `InfobrightDialect` | `MySqlDialect` | Dialect for Infobright |
| `InformixDialect` | `JdbcDialectImpl` | Dialect for Informix |
| `IngresDialect` | `JdbcDialectImpl` | Dialect for Ingres |
| `InterbaseDialect` | `JdbcDialectImpl` | Dialect for Interbase |
| `LucidDbDialect` | `JdbcDialectImpl` | Dialect for LucidDB |
| `MonetDbDialect` | `JdbcDialectImpl` | Dialect for MonetDB |
| `MySqlDialect` | `JdbcDialectImpl` | Dialect for MySQL; base of `MariaDBDialect` and `InfobrightDialect` |
| `NeoviewDialect` | `JdbcDialectImpl` | Dialect for HP Neoview |
| `NetezzaDialect` | `PostgreSqlDialect` | Dialect for Netezza |
| `NuoDbDialect` | `JdbcDialectImpl` | Dialect for NuoDB |
| `PdiDataServiceDialect` | `JdbcDialectImpl` | Dialect for Pentaho PDI data services |
| `RedshiftDialect` | `PostgreSqlDialect` | Dialect for Amazon Redshift |
| `SnowflakeDialect` | `JdbcDialectImpl` | Dialect for Snowflake |
| `SqlStreamDialect` | `LucidDbDialect` | Dialect for the SQLstream streaming SQL system |
| `SybaseDialect` | `JdbcDialectImpl` | Dialect for Sybase |
| `TeradataDialect` | `JdbcDialectImpl` | Dialect for Teradata |
| `VectorwiseDialect` | `IngresDialect` | Dialect for Ingres Vectorwise (`DatabaseProduct.VECTORWISE`) |
| `VerticaDialect` | `JdbcDialectImpl` | Dialect for Vertica |

---

## `mondrian.server` and `mondrian.server.monitor`

### `MondrianServer`

- **Purpose**: One engine instance — the container of everything shared across
  connections: aggregation/segment caches, the monitor, the result shepherd,
  and the connection/statement registries. Home package is `mondrian.olap`
  (abstract class); it gets its full entry here because its implementation and
  registry live in `mondrian.server`.
- **Extends / implements**: Abstract class; nested `MondrianServer.MondrianVersion`.
- **Key collaborators**: `MondrianServerImpl` (the only implementation),
  `MondrianServerRegistry` (creation and lookup), `RolapConnection#getServer`.
- **Lifecycle / scope**: The *static server* (`MondrianServer#forId(null)`)
  lives as long as the JVM and is what the mondrian-olap JRuby gem always uses;
  separate instances for XMLA hosting come from
  `MondrianServer#createWithRepository`. `MondrianServer#shutdown` disposes an
  instance (the static server refuses).
- **Threading**: All accessors safe for concurrent use; state lives in
  concurrent/weak maps inside the implementation.
- **Notes**: `MondrianServer#forConnection` is just
  `RolapConnection#getServer`. `MondrianServer#forId` promises null for an
  unknown id but `MondrianServerRegistry#serverForId` actually throws — see
  [../packages/server.md](../packages/server.md).

### `MondrianServerImpl`

- **Purpose**: The implementation of `MondrianServer`: creates and owns the
  `AggregationManager` (and through it all segment caches and cache executors),
  the `RolapResultShepherd`, a `MonitorImpl`, the `Repository` +
  `CatalogLocator`, and weak-valued maps of open connections and statements.
- **Extends / implements**: Extends `MondrianServer`; implements
  `mondrian.olap4j.CatalogFinder` and `XmlaHandler.ConnectionFactory`.
- **Key collaborators**: `MondrianServerRegistry` (constructs it),
  `RolapConnection`/`RolapSchema` (register through
  `MondrianServer#addConnection`/`#addStatement`, which also emit the monitor
  start/end events), `FileRepository` (XMLA connection creation).
- **Lifecycle / scope**: Created by the registry; registered as a JMX MBean
  named `mondrian.server:type=Server-<id>` (`MondrianServerImpl#registerMBean`);
  after `#shutdown` every accessor throws "Server already shutdown".
- **Threading**: Connection/statement registries are weak-valued concurrent
  maps; safe from any thread.
- **Notes**: `MondrianServerImpl#shutdown(boolean)` logs a warning and refuses
  for the static server, so in embedded use the server and its caches are
  JVM-lifetime.

### `Statement` / `StatementImpl`

- **Purpose**: The engine's internal statement context: holds the compiled
  `Query`, the `ProfileHandler`, and the query timeout
  (`Statement#setQueryTimeoutMillis`), and runs at most one `Execution` at a
  time (`StatementImpl#start`/`#end`/`#cancel`).
- **Extends / implements**: `Statement` is the interface; `StatementImpl` the
  abstract base. `MondrianOlap4jStatement` extends `StatementImpl` on the API
  path; `RolapConnection#createInternalStatement` creates internal ones (every
  `RolapSchema` keeps an internal connection whose statement hosts
  schema-maintenance work).
- **Key collaborators**: `Execution` (one run), `RolapConnection`,
  `ProfileHandler` (via `Statement#enableProfiling`), the server's statement
  registry.
- **Lifecycle / scope**: One per API statement or internal context; default
  timeout comes from the `QueryTimeout` property, converted to milliseconds, in
  `StatementImpl`.
- **Threading**: `start`/`end` are called from the executing thread;
  `StatementImpl#cancel` may be called from any thread — before a run starts it
  sets a `cancelBeforeStart` flag so the next run is canceled immediately.
- **Notes**: See [../packages/server.md](../packages/server.md) §2 for the full
  statement/execution lifecycle.

### `Execution`

- **Purpose**: One run of a statement — state machine
  (`Execution.State`: `FRESH`, `RUNNING`, `ERROR`, `CANCELED`, `TIMEOUT`,
  `DONE`), cancellation and timeout checking, the registry of live JDBC
  statements to cancel, and per-run statistics (phase counts, cache hit/miss
  counts, `QueryTiming`).
- **Extends / implements**: Standalone class; `MondrianOlap4jCellSet` *extends*
  it on the olap4j path, so the cell set is the execution.
- **Key collaborators**: `StatementImpl` (installs it), `Locus` (carries it),
  `SqlStatement` (registers JDBC statements via `Execution#registerStatement`),
  `RolapResultShepherd` (polls `Execution#isCancelOrTimeout`), the monitor
  (`ExecutionStartEvent`/`ExecutionPhaseEvent`/`ExecutionEndEvent`).
- **Lifecycle / scope**: Created per run with the statement and timeout;
  chains to a parent — the constructor captures `Locus.peek().execution`, and
  `#cancel`/`#checkCancelOrTimeout`/`#isCancelOrTimeout` recurse into it, so
  nested internal statements inherit cancellation.
- **Threading**: Cancellation is cooperative: `Execution#cancel` (any thread)
  flips state and cancels registered SQL via `Util#cancelStatement` (never
  close — closing happens on the owning thread); the executing thread must poll
  `Execution#checkCancelOrTimeout`, which throws the canonical
  `QueryCanceled`/`QueryTimeout`/`MemoryLimitExceededException` error.
- **Notes**: **(fork PATCH)** `Execution#getCheckCancelOrTimeoutInterval`
  exposes the `CheckCancelOrTimeoutInterval` property value once per execution
  for `mondrian.util.CancellationChecker`, keeping the hot polling path
  uncontended.

### `Locus`

- **Purpose**: Point of execution from which a service is invoked: an immutable
  triple {`execution`, `component`, `message`} on a per-thread stack — how deep
  layers (member readers, segment loaders, SQL execution) find the current
  `Execution` without parameter threading.
- **Extends / implements**: Standalone class; the stack is a
  `ThreadLocal<ArrayStack<Locus>>`.
- **Key collaborators**: `Execution`, `SqlStatement` (requires a locus at
  construction; stamps monitor events with its statement/execution ids —
  `SqlStatement.StatementLocus` adds purpose and cell-request count),
  `SqlMemberSource`/`SqlTupleReader`/`SegmentCacheManager` (call `Locus#peek`).
- **Lifecycle / scope**: Pushed and popped around each engine entry via the
  `Locus.execute(connection|execution, component, action)` idioms; the
  connection overload wraps the action in a fresh `Execution` on the
  connection's internal statement (no timeout).
- **Threading**: Strictly thread-local; calling engine internals from a thread
  with no pushed locus makes `Locus#peek` throw
  `java.util.EmptyStackException` — the classic symptom.
- **Notes**: **(fork PATCH)** `Locus#getSchema` exposes the execution's schema
  for the per-schema segment-cache actor.

**`MondrianServerRegistry`** — JVM-wide singleton registry and factory
(`MondrianServerRegistry.INSTANCE`): holds the static server (created with an
`ImplicitRepository`), creates repository-backed servers
(`MondrianServerRegistry#createWithRepository`), looks servers up by lock-box
moniker (`MondrianServerRegistry#serverForId` — throws for an unknown non-null
id), and owns the shared `mondrian.util.LockBox`
(`MondrianServerRegistry.lockBox`) through which `Role` objects and server ids
pass connect-string boundaries.

**`Repository` / `ImplicitRepository` / `FileRepository`** — how a server knows
which databases/catalogs/schemas it hosts. `ImplicitRepository` means "no
repository": each connection names its own catalog in the connect string and
the repository cannot create connections — the embedded/static-server case,
including the mondrian-olap gem. `FileRepository` parses a `datasources.xml`
supplied by a `RepositoryContentFinder`, caches the parsed
`FileRepository.ServerInfo`, re-reads on the `XmlaSchemaRefreshInterval`, and
materializes connections by driving the olap4j driver with the catalog's
connect string plus the server's lock-box moniker as the `Instance` property
(`FileRepository#getConnection`).

**`RepositoryContentFinder` / `StringRepositoryContentFinder` /
`UrlRepositoryContentFinder` / `DynamicContentFinder`** — callback that fetches
the repository XML: from a constant string, from a URL, or (extending the URL
finder) with periodic reloads so datasources-config changes are picked up
without redeploy. Assembled with `FileRepository` in
`MondrianXmlaServlet#createConnectionFactory`.

**Monitor core — `Monitor`, `MonitorImpl`, `MonitorMXBean`, `Message`,
`Visitor`** — the event-based monitoring subsystem. `Monitor` (returned by
`MondrianServer#getMonitor`) is an actor: `Monitor#sendEvent` enqueues
fire-and-forget events; `MonitorImpl`'s single static daemon thread ("Mondrian
Monitor", shared by all servers in the JVM, bounded queue of 1000) applies them
to mutable counter workspaces so no counter needs locking. Reads go through the
same queue: `Monitor#getServer`/`#getConnections`/`#getStatements`/
`#getSqlStatements` post `Command` messages and block for immutable `Info`
snapshots. `Message` is the common contract of events and commands, dispatched
through the `Visitor` interface; `MonitorMXBean` exposes the monitor over JMX
(registered by `MondrianServerImpl`). History maps are LRU-bounded by the
`ExecutionHistorySize` property; every event is also logged to
`RolapUtil.MONITOR_LOGGER`. The subpackage carries no fork PATCHes.

**Event family — `Event` and subclasses** — immutable records sent to the
monitor, one per lifecycle edge, in five groups under abstract bases:
`ConnectionEvent` (`ConnectionStartEvent`, `ConnectionEndEvent` — emitted by
`MondrianServerImpl#addConnection`/`#removeConnection`); `StatementEvent`
(`StatementStartEvent`, `StatementEndEvent` — `#addStatement`/
`#removeStatement`); `ExecutionEvent` (`ExecutionStartEvent`,
`ExecutionPhaseEvent`, `ExecutionEndEvent` — `Execution#start`/`#tracePhase`/
`#end`); `SqlStatementEvent` (`SqlStatementStartEvent`,
`SqlStatementExecuteEvent`, `SqlStatementEndEvent` — from `SqlStatement`, with
execute nanos and row counts); and `CellCacheEvent`
(`CellCacheSegmentCreateEvent` — tagged SQL/rollup/external —
and `CellCacheSegmentDeleteEvent`, from `SegmentCacheManager`).

**Info family — `Info` and subclasses** — immutable counter snapshots returned
by the monitor's polling commands, produced by `fix()`-ing the internal
workspaces: `ServerInfo`, `ConnectionInfo`, `StatementInfo`, `ExecutionInfo`,
`SqlStatementInfo`. Their `toString` renders via `mondrian.util.BeanMap`.

---

## `mondrian.olap4j`

### `MondrianOlap4jConnection`

- **Purpose**: `org.olap4j.OlapConnection` over a native
  `mondrian.rolap.RolapConnection` — the root of the adapter layer and the
  primary API entry point for the mondrian-olap JRuby gem. Owns the
  `toOlap4j` converter overload family, the `schemaMap`
  (`mondrian.olap.Schema` → `MondrianOlap4jSchema`), role and locale switching
  (`#setRoleName`/`#setRoleNames`, `#setLocale`), and the nested `Helper` that
  maps native exceptions to `OlapException` (SQLSTATE deduction in
  `Helper#deduceSqlState`).
- **Extends / implements**: Implements `OlapConnection` (JDBC-version stubs
  inherited via the `Factory` hierarchy).
- **Key collaborators**: `MondrianOlap4jDriver` + `Factory` (construction),
  native `mondrian.olap.DriverManager#getConnection` (called in the
  constructor — where the `RolapConnection`, and possibly a pooled
  `RolapSchema`, comes to life), `CatalogFinder` (implemented by
  `MondrianServerImpl`) for the catalog list, `MondrianServer#addStatement`
  (statement registration).
- **Lifecycle / scope**: One per `Driver#connect` call; connect-string entries
  from the JDBC `Properties` argument are merged over the URL's connect string
  in the constructor (so `JdbcUser`/`JdbcPassword` can be passed separately).
- **Threading**: As thread-safe as the underlying `RolapConnection`; adapters
  hang off it and are created on demand (no caching, no computed state).
- **Notes**: **(fork PATCH)** `MondrianOlap4jConnection#getMondrianConnection`
  is `public` so callers (the gem) can reach the native connection on Java 17+,
  where package-private access is no longer forcible.

### `MondrianOlap4jCellSet`

- **Purpose**: `org.olap4j.CellSet` over a `mondrian.olap.Result`
  (`RolapResult`) — *and* the engine's execution object for the run: it
  **extends `mondrian.server.Execution`**, so cancellation flag, timeout
  deadline, execution id, phase counters, and SQL-statement tracking all live
  on the cell set itself.
- **Extends / implements**: Extends `Execution`, implements `CellSet`
  (constructor: `super(olap4jStatement, olap4jStatement.getQueryTimeoutMillis())`).
- **Key collaborators**: `MondrianOlap4jStatement` (publishes it as
  `openCellSet` and starts it as the current execution),
  `RolapConnection#execute(Execution)` (`MondrianOlap4jCellSet#execute` passes
  *itself*), `MondrianOlap4jCellSetAxis`/`MondrianOlap4jCell` (result reading),
  `MondrianOlap4jCellSetMetaData` (shape metadata built at parse time).
- **Lifecycle / scope**: One per statement execution; a statement has at most
  one open cell set — executing again closes the previous one.
- **Threading**: Execution happens on the calling thread (via the shepherd);
  `Statement#cancel` from another thread just calls `openCellSet.cancel()` — an
  `Execution` method — and the engine notices at its cooperative check points.
- **Notes**: The pure-JDBC `ResultSet` methods (`next`, `getString`, …) throw
  `UnsupportedOperationException`; consume results through the olap4j `CellSet`
  surface. Out-of-range cell coordinates become `IndexOutOfBoundsException` in
  `MondrianOlap4jCellSet#getCellInternal`.

### `MondrianOlap4jDriver`

- **Purpose**: The JDBC `Driver` entry point for URLs starting with
  `jdbc:mondrian:` (also `jdbc:mondrian:engine:`); the rest of the URL is a
  Mondrian connect string parsed by `Util#parseConnectString`.
- **Extends / implements**: Implements `java.sql.Driver`.
- **Key collaborators**: `Factory`/`FactoryJdbc41Impl` (hardwired in
  `MondrianOlap4jDriver#<init>`; `#connect` → `Factory#newConnection`),
  `MondrianOlap4jConnection#acceptsURL`.
- **Lifecycle / scope**: Stateless apart from its factory; one instance is
  normally self-registered with `java.sql.DriverManager` at class load.
- **Threading**: Stateless; safe.
- **Notes**: **(fork PATCH)** the static initializer skips
  `DriverManager.registerDriver` when the system property
  `mondrian.olap4j.registerDriver` is set to `"false"` — registration is
  unnecessary under JRuby (the mondrian-olap gem instantiates the driver
  reflectively and calls `MondrianOlap4jDriver#connect` directly), and the
  registered instance held a global reference to the JRuby runtime that
  prevented its garbage collection.

**`MondrianOlap4jStatement` / `MondrianOlap4jPreparedStatement`** — the
statement adapters. `MondrianOlap4jStatement` implements both
`org.olap4j.OlapStatement` and the native `mondrian.server.Statement` (extends
`StatementImpl`) — one object plays both roles. `#parseQuery` runs the native
parse inside `Locus.execute` and builds the cell-set metadata;
`#executeOlapQueryInternal` closes the previous cell set, publishes the new
one, then executes outside the monitor so another thread can cancel mid-flight.
The relational `#executeQuery(String)` is only valid for `DRILLTHROUGH` and
`EXPLAIN` (`MondrianOlap4jStatement#executeQuery2`).
`MondrianOlap4jPreparedStatement` parses — and therefore resolves and
compiles — the MDX **in its constructor**, so `#executeQuery` merely
re-executes; it doubles as its own `OlapParameterMetaData`, with parameters set
through `Query#getParameters`.

**Result-reading adapters — `MondrianOlap4jCell`, `MondrianOlap4jCellSetAxis`,
`MondrianOlap4jCellSetMetaData`, `MondrianOlap4jCellSetAxisMetaData`** —
`MondrianOlap4jCellSetAxis#getPositions` is a lazy `AbstractList` view over the
`RolapAxis`'s `TupleList` (a `Position` — inner class
`MondrianOlap4jPosition` — is just tupleList + index, converting members to
wrappers on access). `MondrianOlap4jCell` delegates value/formatted
value/properties to `RolapCell`; `Cell#drillThrough` works
(`MondrianOlap4jCell#drillThroughInternal` → `RolapCell#drillThroughInternal`)
and `Cell#setValue` is scenario writeback (`RolapCell#setValue`), not a
database write. `MondrianOlap4jCellSetMetaData` is built at parse time from the
`Query` (axes, cube, cell properties) and shared between prepared statement and
cell set; `MondrianOlap4jCellSetAxisMetaData` describes one axis from its
`QueryAxis`.

**`Factory` / `FactoryJdbc41Impl` / `FactoryJdbc4Plus`** — JDBC-version
indirection: the driver historically supported JDBC 3.0/4.0/4.1, whose
interfaces differ per JDK. Today only `FactoryJdbc41Impl` is live (hardwired in
the driver constructor), creating `...Jdbc41` subclasses of the abstract
adapters; `FactoryJdbc4Plus` holds the abstract inner classes
(`AbstractConnection`, `AbstractCellSet`, …) carrying the hundreds of
`UnsupportedOperationException` JDBC stubs.

**Metadata element adapters — `MondrianOlap4jMetadataElement`,
`MondrianOlap4jDatabase`, `MondrianOlap4jCatalog`, `MondrianOlap4jSchema`,
`MondrianOlap4jCube`, `MondrianOlap4jDimension`, `MondrianOlap4jHierarchy`,
`MondrianOlap4jLevel`, `MondrianOlap4jMember`, `MondrianOlap4jMeasure`,
`MondrianOlap4jNamedSet`** — thin wrappers over the same-named
`mondrian.olap` objects, each holding the native object plus a connection
back-pointer, created on demand by the `toOlap4j` converters.
`MondrianOlap4jMetadataElement` is the base: `OlapWrapper` unwrap down to the
native `OlapElement` (`#unwrapImpl`). Access control is inherited from the
native layer: lookups go through the role-aware
`RolapConnection#getSchemaReader` under a `Locus` (member retrieval may run
SQL), and `MondrianOlap4jCatalog#getSchemas` filters schemas whose
`Role#getAccess` is `Access.NONE`. Mondrian has exactly one
`MondrianOlap4jDatabase`; catalogs come from `CatalogFinder`.
`MondrianOlap4jMeasure` wraps a `mondrian.rolap.RolapStoredMeasure`; caption
and description getters localize via `OlapElement#getLocalized` with the
connection locale.

**`MondrianOlap4jProperty` / `IMondrianOlap4jProperty`** — the member/cell
property adapter and its interface refinement (adds the owning level).
`MondrianOlap4jProperty.MEMBER_EXTENSIONS`/`CELL_EXTENSIONS` expose
Mondrian-only properties beyond the olap4j standard set.

**`MondrianOlap4jDatabaseMetaData`** — `OlapDatabaseMetaData`: implements
`getCubes`, `getOlapDimensions`, `getMeasures`, … by delegating to the XMLA
metadata machinery (`XmlaUtil#getMetadataRowset`, e.g. `MDSCHEMA_CUBES`) and
returning the rows as a fixed `ResultSet` via `Factory#newFixedResultSet`.

**`MondrianOlap4jExtra`** — the `XmlaHandler.XmlaExtra` implementation
(singleton, reached via `connection.unwrap(XmlaHandler.XmlaExtra.class)`):
driver internals the XMLA layer needs beyond the olap4j API — drill-through
execution, schema load date, level cardinality, the schema's function list,
keywords, cube flags. Exists for the XMLA endpoint, not ordinary API clients.

### Remaining `mondrian.olap4j` classes

| Class | One line |
|---|---|
| `CatalogFinder` | Strategy interface to enumerate catalogs/schemas per connection; implemented by `MondrianServerImpl` |
| `EmptyResultSet` | Fixed-row (header list + row list) JDBC `ResultSet`; despite the name it backs all metadata rowsets and EXPLAIN output — "empty" is just its default content |
| `Unsafe` | Public escape hatch to package-private methods; `Unsafe#setStatementProfiling` enables per-statement profiling |

---

## `mondrian.xmla` and `mondrian.xmla.impl`

### `XmlaHandler`

- **Purpose**: The XML for Analysis protocol brain: `XmlaHandler#process`
  dispatches on `XmlaRequest#getMethod` — **DISCOVER** → `XmlaHandler#discover`
  (rowset lookup in `RowsetDefinition`, populate, stream) and **EXECUTE** →
  `XmlaHandler#execute` (obtain an `OlapConnection`, run the statement, wrap
  the `CellSet` in an `MDDataSet_Multidimensional`/`MDDataSet_Tabular` — or
  `TabularRowSet` for drillthrough — and serialize via `unparse(SaxWriter)`).
- **Extends / implements**: Standalone class; nested interfaces
  `XmlaHandler.ConnectionFactory` (how it gets `OlapConnection`s — implemented
  by `MondrianServer`, keyed by databaseName/catalogName/roleName) and
  `XmlaHandler.XmlaExtra` (Mondrian-specific abilities beyond olap4j;
  implemented by `MondrianOlap4jExtra`).
- **Key collaborators**: `XmlaRequest`/`XmlaResponse`, `RowsetDefinition` +
  `Rowset`, `SaxWriter`, the olap4j API (it consumes connections, not engine
  internals).
- **Lifecycle / scope**: Constructed with a `ConnectionFactory` and URL prefix
  (`XmlaHandler#<init>`); servlet-independent — also driven directly by
  `mondrian.tui.XmlaSupport` and `XmlaUtil#getMetadataRowset`.
- **Threading**: Stateless apart from the factory; one handler serves
  concurrent servlet requests.
- **Notes**: The mondrian-olap JRuby gem does not use this path — nothing here
  is on its execution route. No fork PATCHes anywhere under `mondrian/xmla`.

**Servlet chain — `XmlaServlet` → `DefaultXmlaServlet` →
`MondrianXmlaServlet`; variants `DynamicDatasourceXmlaServlet`,
`Olap4jXmlaServlet`** — `XmlaServlet` (abstract, implements `XmlaConstants`) is
the phase skeleton driven by `XmlaServlet#doPost`; `impl/DefaultXmlaServlet`
adds the DOM-based SOAP plumbing (`#unmarshallSoapMessage`, `#handleSoapBody`,
`#marshallSoapMessage`); `impl/MondrianXmlaServlet` is the concrete entry point
that boots a `MondrianServer` from a datasources XML
(`MondrianServer#createWithRepository` with a `UrlRepositoryContentFinder`) and
uses it as the `ConnectionFactory`. `impl/DynamicDatasourceXmlaServlet` swaps
in a `mondrian.server.DynamicContentFinder` so config changes are picked up
without redeploy; `impl/Olap4jXmlaServlet` serves XML/A over *any* olap4j
driver (driver class + connect string from servlet config, DBCP-pooled), not
just Mondrian.

**`RowsetDefinition` / `Rowset`** — `RowsetDefinition` is the enum of every
Discover rowset (`MDSCHEMA_CUBES`, `DBSCHEMA_TABLES`, `DISCOVER_PROPERTIES`,
…), each constant defining its columns and sort order and acting as the factory
for its populator (`RowsetDefinition#getRowset`). `Rowset` is the abstract base
of those populators (subclasses live as inner classes of `RowsetDefinition`):
`Rowset#unparse` streams rows produced by `Rowset#populateImpl` from olap4j
metadata, applying the request's restrictions.

**`XmlaRequest` / `XmlaResponse` / `DefaultXmlaRequest` /
`DefaultXmlaResponse`** — the request/response abstractions between servlet and
handler: method, properties, restrictions, and statement text on the way in; a
`SaxWriter` on the way out. The `impl` defaults parse the SOAP body via DOM
(`DefaultXmlaRequest`) and wrap an output stream (`DefaultXmlaResponse`).

**`SaxWriter` / `DefaultSaxWriter` / `JsonSaxWriter`** — output abstraction:
SAX-like events in, document out, so the same serialization code produces XML
(`DefaultSaxWriter`) or JSON (`JsonSaxWriter`).

### Remaining `mondrian.xmla` / `xmla.impl` classes

| Class | One line |
|---|---|
| `Enumeration` | Container of the value enumerations Discover reports (access, methods, provider type, …) |
| `PropertyDefinition` | Enum of every XML/A property (`DataSourceInfo`, `Format`, `Content`, …) with type and access |
| `XmlaConstants` | Namespaces, SOAP fault codes, and the `Method`/`Format`/`Content` enums shared across the package |
| `XmlaException` | `MondrianException` carrying SOAP fault code/string alongside the cause; thrown throughout the handler |
| `XmlaUtil` | Helpers: element/text extraction, name encoding, `XmlaUtil#getMetadataRowset` for calling Discover programmatically |
| `XmlaRequestCallback` | Hook to pull authentication/context out of the HTTP request and SOAP header before processing |
| `AuthenticatingXmlaRequestCallback` | Abstract `XmlaRequestCallback` base specialized in authenticating incoming requests |

---

## `mondrian.util`

### `Format`

- **Purpose**: The MDX/VB format-string engine (~3300 lines): implements Visual
  Basic `Format()` semantics — `"#,##0.00"`, `"Currency"`, `"yyyy-mm-dd"`,
  positive/negative/zero/null sections separated by `;` — for numbers, strings,
  and dates. Every formatted cell value goes through it.
- **Extends / implements**: Standalone class; a `Format` instance is a
  *compiled* format string (parsed once into a `BasicFormat` tree). Nested
  `Format.FormatLocale` carries locale symbols (decimal separator, currency
  symbol, month names).
- **Key collaborators**: `RolapEvaluator#format` /
  `RolapEvaluator#getFormatString` (cell rendering), the MDX `Format()`
  function (`olap.fun.FormatFunDef`), the XMLA layer; `DigitList` and
  `MondrianFloatingDecimal` are its private number-transcoding helpers.
- **Lifecycle / scope**: `Format#get(formatString, locale)` caches instances in
  a static LRU map capped at `Format#CacheLimit` (1000) entries keyed by
  (format string, locale).
- **Threading**: Cached instances are shared across all query threads; a
  compiled format is effectively immutable after parse.
- **Notes**: If a cell renders oddly, look here first. No fork PATCHes.

### `ClassResolver`

- **Purpose**: Instantiates a class by name — the mechanism behind every
  "class name in a property" extension point: `ClassResolver#instantiateSafe`
  loads the class and calls a matching public constructor.
- **Extends / implements**: Interface; `ClassResolver.AbstractClassResolver`
  implements the instantiation logic, and
  `ClassResolver.ThreadContextClassResolver` (the `ClassResolver.INSTANCE`
  singleton) resolves through the thread context classloader.
- **Key collaborators**: `RolapSchemaPool` (schema processors),
  `RolapConnection` (data-source resolvers), `DialectManager` (explicit dialect
  classes), `SegmentCacheWorker` (external segment caches), `Util`.
- **Lifecycle / scope**: Stateless singleton used at configuration-resolution
  points, mostly schema/connection setup.
- **Threading**: Stateless; safe from any thread.
- **Notes**: Failures surface as `CreationException`. Property-named SPI
  classes therefore need a public no-argument constructor.

**`ByteString`** — immutable byte-array wrapper with proper
`equals`/`hashCode`/hex `toString`, used to carry MD5 digests:
`RolapSchema#getChecksum` exposes the catalog-content digest,
`RolapSchemaPool` keys its by-content map on it, and `spi.SegmentHeader`
includes the schema checksum so external segment-cache entries can never be
served to a different schema.

**`ExpiringReference`** — a `SoftReference` subclass that additionally holds a
hard reference until a timeout elapses, renewing on each `get`. Sole consumer:
`RolapSchemaPool`, wrapping every pooled `RolapSchema`. The timeout comes from
the `PinSchemaTimeout` connect-string property (default `"-1s"`); values ≤ 0
pin forever, a positive value turns the pool into a keep-alive-while-used
cache.

**`Pair` / `Triple`** — `Pair` is the immutable, `Comparable`, hashable
2-tuple used pervasively (~47 files) as compound key and compound return value
— (member, value) pairs in sorters, cache keys, `Util#parseInterval` results;
`mondrian.rolap.SchemaKey` extends `Pair<SchemaContentKey, ConnectionKey>`.
`Triple` is the three-element sibling, currently unused outside the package.
(Note: neither appears in some generated inventories; both live in
`mondrian/util/`.)

**`BlockingHashMap` / `SlotFuture` / `CompletedFuture`** — concurrency
primitives for the `SegmentCacheManager` actor pattern. `BlockingHashMap#get`
blocks until another thread `put`s the value for that key (response
correlation in `SegmentCacheManager` and `MonitorImpl`); `SlotFuture` is a
write-once `Future` completed by whichever thread produces the value
(`SegmentCacheIndexImpl` registers one per in-flight segment load so concurrent
queries share one load); `CompletedFuture` is the trivial already-done variant.

**`CancellationChecker`** — cheap cancel/timeout polling for tight loops:
`CancellationChecker#checkCancelOrTimeout(iteration, execution)` calls
`Execution#checkCancelOrTimeout` every `CheckCancelOrTimeoutInterval`
iterations; sprinkled through crossjoin/filter evaluation, SQL result reading,
and `RolapResult`. **(fork PATCH)** Upstream synchronized on the `Execution`
before testing the interval; this fork reads the interval once via
`Execution#getCheckCancelOrTimeoutInterval` and tests the modulus *before*
entering the synchronized block, removing per-iteration lock traffic.

**`MemoryMonitor` family — `MemoryMonitor`, `AbstractMemoryMonitor`,
`NotificationMemoryMonitor`, `FauxMemoryMonitor`, `MemoryMonitorFactory`** —
low-memory detection, present but off by default. `NotificationMemoryMonitor`
is the real implementation (JVM `MemoryPoolMXBean` usage-threshold
notifications); `FauxMemoryMonitor` the no-op; `AbstractMemoryMonitor` the
shared listener/threshold logic; `MemoryMonitorFactory` (an
`ObjectFactory.Singleton`) returns the no-op unless
`MondrianProperties#MemoryMonitor` is set. When enabled,
`RolapConnection#execute` registers a listener that flags the current
`Execution` out-of-memory so the query aborts instead of taking the JVM down.

**`ServiceDiscovery`** — `META-INF/services` lookup (pre-dating
`java.util.ServiceLoader`): `ServiceDiscovery#forClass` returns the registered
implementing classes. Used by `DialectManager` for `Dialect` and by
`GlobalFunTable` for `UserDefinedFunction` discovery.

### Remaining `mondrian.util` classes

| Class | One line |
|---|---|
| `ArraySortedSet` | `SortedSet` over a sorted array; the compact representation of segment axis keys (`SegmentAxis`, `SegmentBuilder`, `spi.SegmentColumn`) |
| `ArrayStack` | `ArrayList`-backed unsynchronized stack (faster `java.util.Stack` replacement); backs `Locus`, `ValidatorImpl`, the XMLA SAX writers |
| `Base64` | Standalone Base64 codec (used by `LockBox`) |
| `BeanMap` | View of an object's bean properties as a map; `toString` of the monitor `Info` objects |
| `Bug` | Named boolean constants gating code paths for known unfixed bugs |
| `CacheMap` | Small LRU map (used only inside `FilteredIterableList`) |
| `CartesianProductList` | Virtual `List<List<T>>` cross product without materialization (`CrossJoinFunDef`, `SegmentCacheIndexImpl` rollup enumeration) |
| `CombiningGenerator` | Power set of a collection (`SegmentCacheManager` rollup candidate search) |
| `Composite` | Static factories viewing several lists/iterables as one (`RolapNativeRegistry`, `XmlaHandler`) |
| `CompositeList` | List composed of several backing lists |
| `ConcatenableList` | List backed by a collection of sub-lists; member-children accumulation in `SmartMemberReader`, `FunUtil` |
| `Counters` | Static debug counters for SQL statement accounting (`SqlStatement`) |
| `CreationException` | `MondrianException` thrown on `ObjectFactory`/`ClassResolver` instantiation failure |
| `DelegatingInvocationHandler` | Reflective partial-proxy base: handles a call by finding an identically-signed method on itself (`SqlStatement`'s JDBC wrappers) |
| `DigitList` | Numeric value ↔ digit-string transcoding helper private to `Format` |
| `FilteredIterableList` | Lazy filtered list view (`SqlConstraintUtils`, role filtering in `RolapConnection`) |
| `IdentifierParser` | Parses `[Store].[USA].[CA]`-style member/tuple/set strings; extends `org.olap4j.impl.IdentifierParser`; used via `mondrian.olap.Util`, `Query`, `FunUtil` |
| `IteratorIterable` | Caches an iterator's output so it can be re-iterated as an `Iterable` |
| `LockBox` | Passes live objects across string-only boundaries: `LockBox#register` returns an unguessable moniker resolvable while the `Entry` is held; `MondrianServerRegistry.lockBox` carries `Role` objects and server ids through connect strings |
| `MDCUtil` | Copies the log4j MDC context across actor/executor thread hops (`Execution`, `SegmentCacheManager`, `SegmentLoader`) |
| `MondrianFloatingDecimal` | Representation of a number as a list of digits; `Format` helper |
| `ObjectFactory` | Property-overridable singleton factory pattern (`MemoryMonitorFactory`, `ExpCompiler.Factory`) |
| `ObjectPool` | Low-memory open-addressing `HashSet` replacement (`RolapResult`, `RolapConnectionPool`) |
| `PartiallyOrderedSet` | Partially-ordered set with cover-graph navigation (`SegmentCacheIndexImpl`) |
| `PrimeFinder` | Prime capacity tables for `ObjectPool`'s hashing |
| `PropertyUtil` | Build-time generator producing `MondrianProperties.java` from `MondrianProperties.xml` |
| `Schedule` | Time-event series generator; legacy, effectively unused |
| `SpatialValueTree` | Unused prototype of a dimensional value index (absent from some inventories; the file exists) |
| `SpatialValueTree2` | Variation of `SpatialValueTree`; also unused |
| `StringKey` | Type-safe immutable string wrapper; base of `SchemaContentKey` and `ConnectionKey` |
| `TraversalList` | Transposed view of an array of lists (`NativizeSetFunDef`, `HighCardSqlTupleReader`) |
| `UnionIterator` | Iterator over the union of several `Iterable`s (`RolapHierarchy`) |
| `UnsupportedList` | `List` base whose methods all throw `UnsupportedOperationException` (except `isEmpty`); base for lazy views (`RolapCubeHierarchy`, `TraversalList`) |
| `UtilCompatible` | Historical JDK-version shim interface; `mondrian.olap.Util` routes version-sensitive calls (statement cancel, memory info, `quotePattern`) through a static instance |
| `UtilCompatibleJdk15` | JDK 1.5 implementation of `UtilCompatible` |
| `UtilCompatibleJdk16` | JDK 1.6 implementation (the one `mondrian.olap.Util` instantiates; vestigial with Java 8 as the floor, but still the call path) |
| `XmlParserFactoryProducer` | XXE-hardened XML parser/transformer factories (XMLA, `tui.XmlUtil`) |

---

## `mondrian.udf`

**Date UDFs — `CurrentDateMemberUdf`, `CurrentDateMemberExactUdf`,
`CurrentDateStringUdf`** — the `CurrentDateMember([Time], "format"[, BEFORE|
AFTER|EXACT])` and `CurrentDateString("format")` MDX functions: format the
current date with the VB format-string language (`mondrian.util.Format`), then
resolve it to a member of the given hierarchy (with nearest-member matching in
the base UDF; the `Exact` variant omits the match-type argument) or return it
as a string. Registered — like every class in this package — via
`META-INF/services/mondrian.spi.UserDefinedFunction`, loaded by
`GlobalFunTable`, so they are available to all schemas.

**Predicate and value UDFs — `InUdf`, `MatchesUdf`, `InverseNormalUdf`,
`LastNonEmptyUdf`, `NullValueUdf`, `ValUdf`** — `IN` (member in set),
`MATCHES` (string against Java regex; **(fork PATCH)** null-safe: checks that
arguments are present and coerces the first argument to a string),
`InverseNormal` (inverse normal distribution), `LastNonEmpty(set, measure)`
(last member of a set whose measure is non-empty — classic inventory-style
semantics), `NullValue()` (always Java null), and `Val` (VB `Val()` numeric
coercion).

---

## `mondrian.recorder`

**`MessageRecorder` / `AbstractRecorder` / `ListRecorder` / `LoggerRecorder` /
`PrintStreamRecorder` / `RecorderException`** — a small structured
warning/error collector: a context stack (`MessageRecorder#pushContextName`)
prefixes each message with where processing was, and an error budget throws
`RecorderException` (a `MondrianException`) after too many errors.
`AbstractRecorder` implements the bookkeeping; the three concrete recorders
deliver messages to a `List`, a log4j `Logger`, or `PrintStream`s. Only
client: the aggregate-table subsystem — `AggTableManager` and the
`rolap.aggmatcher` recognizers report match failures through it so one bad
aggregate table produces a readable log instead of an abort. See
[../topics/aggregate-tables.md](../topics/aggregate-tables.md).

---

## `mondrian.i18n`

**`LocalizingDynamicSchemaProcessor`** — the package's single class: a
`FilterDynamicSchemaProcessor` that substitutes `%{key}` tokens in the catalog
XML from a locale-specific resource bundle (named by the
`mondrian.rolap.localePropFile` property — `MondrianProperties#LocalePropFile`
— with the locale taken from the `Locale` connect-string property, falling
back to the JVM default) before
parsing, so one schema definition serves translated captions. Enabled via the
`DynamicSchemaProcessor` connect-string property like any schema processor.

---

## `mondrian.web`

Legacy servlet/JSP companion for the demo webapp; predates olap4j (drives
`mondrian.olap.Connection` directly). Nothing else in the engine depends on it,
and it carries no fork PATCHes.

| Class | One line |
|---|---|
| `MdxQueryServlet` (`web.servlet`) | `HttpServlet` that receives an MDX query, executes it, and renders the result as an HTML table |
| `ApplResources` (`web.taglib`) | Servlet-application-scope holder for compiled XSLT stylesheets |
| `DomBuilder` (`web.taglib`) | Transforms a mondrian `Result` into a DOM document |
| `Listener` (`web.taglib`) | `ServletContextListener` creating/destroying the `ApplResources` with the servlet lifecycle |
| `QueryTag` (`web.taglib`) | JSP tag: creates a `ResultCache` and initializes it with the MDX query |
| `ResultCache` (`web.taglib`) | Holds a query/result pair in the user's HTTP session (`HttpSessionBindingListener`) |
| `TransformTag` (`web.taglib`) | JSP tag: renders a `ResultCache` result through an XSLT stylesheet |

---

## `mondrian.tui`

Text/test UI utilities: a command-line MDX shell and just enough fake servlet
plumbing to drive the XMLA servlet in-process (used by test harnesses).

| Class | One line |
|---|---|
| `CmdRunner` | Command-line utility that reads and executes MDX commands (the `bin/` shell scripts' engine) |
| `MockHttpServletRequest` | Partial `HttpServletRequest` — just enough to talk to the XMLA code in the same JVM |
| `MockHttpServletResponse` | Partial `HttpServletResponse` counterpart capturing the response bytes |
| `MockServletConfig` | Partial `ServletConfig` for initializing the XMLA servlet in-JVM |
| `MockServletContext` | Partial `ServletContext` counterpart |
| `NamespaceContextImpl` | `javax.xml.namespace.NamespaceContext` over an immutable prefix→URI map, for XPath over XMLA responses |
| `XmlUtil` | XML parsing/validation/transform helpers used to validate XMLA responses |
| `XmlaSupport` | Support for making XMLA requests and inspecting the responses (drives `XmlaHandler` and the servlet directly) |
