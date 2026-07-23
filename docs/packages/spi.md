# `mondrian.spi` — Service Provider Interfaces

*Every officially supported extension point of the engine lives in
`mondrian.spi`, with bundled implementations in `mondrian.spi.impl`. Context:
[../architecture.md](../architecture.md). The deep treatment of the largest
SPI, `Dialect`, is in [../topics/sql-generation.md](../topics/sql-generation.md);
the `SegmentCache` SPI's place in the cache hierarchy is in
[../topics/caching.md](../topics/caching.md).*

## Role

Anything a deployment can plug in without patching the engine goes through an
interface in this package: the SQL flavor spoken to the database, an external
segment cache, catalog preprocessing, data-source lookup, user-defined MDX
functions, and display formatting. Each SPI is named by a string somewhere —
a `MondrianProperties` property, a connect-string property, a schema XML
attribute/element, or a `META-INF/services` file — and resolved at runtime.
Two utilities in `mondrian.util` do the resolving:

- **`ClassResolver.INSTANCE#instantiateSafe(className)`** — loads the class
  through the thread context classloader and calls its public no-argument
  constructor. This is the instantiation path for every "class name in a
  property" SPI.
- **`ServiceDiscovery#forClass(iface)`** — reads
  `META-INF/services/<interface-name>` files (the JAR service-provider
  convention) and returns the implementing classes. Used for `Dialect` and
  `UserDefinedFunction` discovery.

## The extension points

| SPI interface | Customizes | How registered | Default / bundled impls |
|---|---|---|---|
| `Dialect` (+ `DialectFactory`) | Per-database SQL syntax and capability flags | `META-INF/services/mondrian.spi.Dialect` via `ServiceDiscovery`; chained and cached by `DialectManager`; an explicit class name can be passed to the three-argument `DialectManager#createDialect` (engine call sites pass null = auto-detect) | ~35 dialects in `spi.impl` (list below); generic `JdbcDialectImpl` fallback |
| `SegmentCache` | External (out-of-JVM-heap) storage for aggregation segments | Property `mondrian.rolap.SegmentCache`, **and** `META-INF/services` lookup, **and** instances handed to `SegmentCache.SegmentCacheInjector#addCache` — all three collected by `SegmentCacheWorker#initCache` | None bundled here; the in-JVM `mondrian.rolap.cache.MemorySegmentCache` implements the same interface but is wired unconditionally (see [../topics/caching.md](../topics/caching.md)) |
| `DynamicSchemaProcessor` | Rewrites the catalog XML string before parsing (templating, i18n, per-tenant edits) | Connect-string property `DynamicSchemaProcessor`; instantiated per schema load in `RolapSchemaPool#processDynamicSchema` | `FilterDynamicSchemaProcessor` (reads the catalog URL via Apache VFS and lets subclasses filter the stream) |
| `DataSourceResolver` | Maps the `DataSource` connect-string value to a `javax.sql.DataSource` | Property `mondrian.spi.dataSourceResolverClass`; one shared instance, lazily created in `RolapConnection#getDataSourceResolver` | `JndiDataSourceResolver` (default: JNDI lookup) |
| `DataSourceChangeListener` | Tells member/aggregation caches when the underlying database changed, forcing reload | Connect-string property `DataSourceChangeListener`; created per schema in `RolapSchema#createDataSourceChangeListener`, consulted by `MemberCacheHelper` and `RolapStar` | None by default; `DataSourceChangeListenerImpl`…`Impl4` are trivial always-unchanged/always-changed test implementations |
| `UserDefinedFunction` | Adds MDX functions | Schema element `<UserDefinedFunction name="…" className="…"/>` (or nested `<Script>`), handled by `RolapSchema#defineFunction`; globally via `META-INF/services/mondrian.spi.UserDefinedFunction`, loaded by `GlobalFunTable` | The `mondrian.udf` package (`CurrentDateMemberUdf`, `LastNonEmptyUdf`, `ValUdf`, …) is registered through the services file |
| `MemberFormatter` | Member caption rendering | `<Level formatter="…">` attribute or `<MemberFormatter>` child element | `DefaultRolapMemberFormatter` (in `mondrian.rolap.format`) |
| `CellFormatter` | Cell value rendering (overrides the format string) | `<Measure formatter="…">` attribute or `<CellFormatter>` child; `CELL_FORMATTER` / `CELL_FORMATTER_SCRIPT` properties on calculated members | None (format-string formatting is the default) |
| `PropertyFormatter` | Member property value rendering | `<Property formatter="…">` attribute or `<PropertyFormatter>` child | `PropertyFormatterAdapter` over the default formatter |
| `ProfileHandler` | Receives the executed `Calc` plan and `QueryTiming` after a query | Programmatic only: `mondrian.server.Statement#enableProfiling`; `Query#<init>` auto-attaches one that writes to the `mondrian.profile` logger when that logger is at DEBUG | Anonymous logger-backed handler in `Query` |
| `CatalogLocator` | Translates the `Catalog` connect property to a real URL (e.g. servlet-relative paths) | Passed explicitly to `MondrianServer#createWithRepository` or `DriverManager#getConnection`; applied in `DriverManager` via `CatalogLocator#locate` | `IdentityCatalogLocator` (default, no-op), `CatalogLocatorImpl` (no-op), `ServletContextCatalogLocator` |
| `StatisticsProvider` | Row-count / cardinality estimates used when choosing aggregate tables | Properties `mondrian.statistics.providers` and `mondrian.statistics.providers.<PRODUCT>` (e.g. `.MYSQL`), read in `JdbcDialectImpl#computeStatisticsProviders`; exposed via `Dialect#getStatisticsProviders`, consumed by `RolapStatisticsCache` | `SqlStatisticsProvider` (default: `select count(*)` queries), `JdbcStatisticsProvider` (JDBC metadata) |

Three more types in the package are **data contracts, not extension points**:
`SegmentHeader` (the identity of a cached segment), `SegmentBody` (its values,
serializable), and `SegmentColumn` (one constrained column of a header). They
are the vocabulary a `SegmentCache` implementation must speak — see
[../topics/caching.md](../topics/caching.md).

Note there is no property-based hook for `CatalogLocator` or `ProfileHandler`;
both are pure API parameters. There is also no `DialectResolver` interface in
this codebase — dialect resolution is entirely `DialectManager` +
`DialectFactory`.

## The Dialect SPI

`Dialect` encapsulates everything database-specific that SQL generation code
needs to ask: identifier and literal quoting, FROM/JOIN/ORDER BY shape rules,
aggregation capabilities (`allowsCountDistinct`, `supportsGroupingSets`, …),
IN-list limits, and result-set type mapping (`Dialect#getType`). The
method-by-method table and the exact call sites in `SqlQuery` and the
predicates live in [../topics/sql-generation.md](../topics/sql-generation.md)
— read that for anything below the class level.

### Resolution chain

`DialectManager#createDialect(dataSource, connection)`:

1. `ServiceDiscovery` reads `META-INF/services/mondrian.spi.Dialect`
   (this fork's file lists all bundled dialects, including the fork-added
   `ClickHouseDialect`).
2. Each dialect class contributes its `public static FACTORY` field — a
   `JdbcDialectFactory` created with the class and its `DatabaseProduct` — or,
   lacking one, a reflection-based factory around a `(Connection)` constructor.
3. The factories form a chain; each is offered the live JDBC connection and
   accepts when `JdbcDialectImpl#getProduct` maps the connection metadata's
   product name/version to the factory's product. First acceptance wins;
   generic `JdbcDialectImpl` is the fallback.
4. `DialectManager.CachingDialectFactory` caches the resulting dialect per
   `DataSource`, so all stars, queries, and threads of a schema share one
   instance (`RolapSchema#getDialect`).

### Bundled dialects (`mondrian.spi.impl`)

Names only — behavior details in
[../topics/sql-generation.md](../topics/sql-generation.md):

- **Major databases**: `MySqlDialect`, `MariaDBDialect` (fork PATCH:
  ColumnStore select-not-in-group-by restriction),
  `PostgreSqlDialect` (fork PATCH: tuple IN),
  `OracleDialect` (fork PATCH: tuple IN), `MicrosoftSqlServerDialect`
  (fork PATCH: `N'…'` Unicode literals), `Db2Dialect`, `Db2OldAs400Dialect`,
  `DerbyDialect`, `HsqldbDialect`, `AccessDialect`, `SybaseDialect`,
  `InformixDialect`, `IngresDialect`, `InterbaseDialect`, `FirebirdDialect`,
  `NuoDbDialect`.
- **Analytics / warehouse engines**: `ClickHouseDialect` (added entirely in
  this fork — fork PATCH), `GoogleBigQueryDialect`, `SnowflakeDialect`,
  `RedshiftDialect`, `VerticaDialect`, `GreenplumDialect`, `NetezzaDialect`,
  `TeradataDialect`, `HiveDialect`, `ImpalaDialect`, `MonetDbDialect`,
  `VectorwiseDialect`, `InfobrightDialect`.
- **Legacy / niche**: `LucidDbDialect`, `SqlStreamDialect`, `NeoviewDialect`,
  `PdiDataServiceDialect`.

The base class `JdbcDialectImpl` itself carries a fork PATCH (MONDRIAN-2702:
`BIGINT` maps to `LONG`, not `DOUBLE`). `DialectUtil` (in `mondrian.spi`, not
`impl`) is a one-method helper, `DialectUtil#cleanUnicodeAwareCaseFlag`, used
by dialects when translating Java regexes to database regex syntax.

## Formatter SPIs and `mondrian.rolap.format`

The three formatter SPIs are declared in `mondrian.spi` but constructed by the
`mondrian.rolap.format` package during schema realization. Each schema element
supports both an attribute and a child element; **the element supersedes the
attribute** (`FormatterCreateContext.Builder#formatterDef` vs `#formatterAttr`):

- `<Measure formatter="com.example.F"/>` or
  `<Measure><CellFormatter className="com.example.F"/></Measure>` →
  `RolapCube#createMeasure` → `FormatterFactory#createCellFormatter`.
- `<Level formatter=…>` / `<MemberFormatter>` → `RolapLevel#<init>` →
  `FormatterFactory#createRolapMemberFormatter`.
- `<Property formatter=…>` / `<PropertyFormatter>` →
  `RolapLevel#createProperties` → `FormatterFactory#createPropertyFormatter`.

Instead of `className`, each formatter element (and `<UserDefinedFunction>`)
may contain a `<Script language="JavaScript">…</Script>` child. `Scripts` (in
`spi.impl`) wraps the script text in a function declaration and compiles it
against the SPI interface via `Util#compileScript` (JSR-223); JavaScript is the
only language the `Scripts.ScriptLanguage` enum accepts. `Scripts` also has
factory methods for script-based `DynamicSchemaProcessor`,
`DataSourceResolver`, and `DataSourceChangeListener`, but only the formatter
and UDF variants are reachable from schema XML.

## Non-dialect contents of `mondrian.spi.impl`

`CatalogLocatorImpl`, `IdentityCatalogLocator`, `ServletContextCatalogLocator`
(catalog locators); `DataSourceChangeListenerImpl` through `Impl4` (trivial
change listeners); `FilterDynamicSchemaProcessor`; `JndiDataSourceResolver`;
`JdbcDialectFactory` and `JdbcDialectImpl` (the dialect machinery);
`JdbcStatisticsProvider` and `SqlStatisticsProvider`; `Scripts`.

## Implementing an SPI — practical notes

What the instantiation code actually requires:

- **Constructors.** Every property-named SPI (`SegmentCache`,
  `DynamicSchemaProcessor`, `DataSourceResolver`, `DataSourceChangeListener`,
  `StatisticsProvider`) and every formatter needs a **public no-argument
  constructor** (`ClassResolver.AbstractClassResolver#instantiateSafe`,
  `FormatterFactory#createFormatter`). A `Dialect` registered through
  `JdbcDialectFactory` needs a **public `(java.sql.Connection)` constructor**
  — the factory constructor throws otherwise. A `UserDefinedFunction` class
  must be public (and static if nested); `Util#createUdf` prefers a
  `public Udf(String name)` constructor and falls back to no-arg.
- **Dialect sharing.** One dialect instance per `DataSource` is cached and
  used concurrently by every query on the schema. `JdbcDialectImpl` deduces
  all its state from JDBC metadata in the constructor and never mutates it
  afterwards; a custom dialect must be equally immutable or internally
  synchronized.
- **`DynamicSchemaProcessor` is per-load.** `RolapSchemaPool` instantiates a
  fresh processor each time a schema is (re)loaded, so no state survives
  between loads. Its class name is part of `SchemaContentKey`, i.e. two
  connections with different processors get different pooled schemas.
- **`SegmentCache` must be thread-safe** — the interface javadoc says so
  explicitly, and the engine enforces the flip side: `SegmentCacheWorker`
  asserts that potentially slow cache calls never run on the
  `SegmentCacheManager` actor threads (they run on `cacheExecutor` instead).
  One instance serves the whole JVM.
- **Formatters are per-element singletons.** A formatter is created once per
  schema element at schema-load time and then invoked from evaluation threads;
  keep them stateless.
