# `mondrian.util` — General-Purpose Utilities

*Supporting cast for every layer described in [architecture.md](../architecture.md):
format-string rendering, checksums, SPI class loading, cancellation checks, and
the collection/concurrency primitives the engine is built from. Nothing here has
OLAP semantics of its own — but a handful of these classes sit on hot paths and
are worth knowing before you read `rolap` or `olap.fun` code.*

## Role, and the `Util` that isn't here

`mondrian.util` (53 files) holds general-purpose helpers: things that could in
principle live in a third-party library but predate one or needed Mondrian-specific
behavior. Confusingly, the single most-used utility class in the codebase is
**not** in this package: `mondrian.olap.Util` (see [olap.md](olap.md)) is the
grab-bag of static helpers — string/identifier quoting, error factories,
connect-string parsing, `Util#newIdentityHashSet` and friends. The two overlap
at the seams: `mondrian.olap.Util` delegates JDK-version-specific operations to
`mondrian.util.UtilCompatible` (see below), wraps `mondrian.util.IdentifierParser`
for member-name parsing, and builds `ArraySortedSet` instances for segment
metadata. Rule of thumb: static helper *methods* live in `mondrian.olap.Util`;
reusable *classes* live here.

## Featured classes

### `Format` — the MDX/VB format-string engine

The largest and most consequential class in the package (~3300 lines). It
implements Visual Basic `Format()` semantics — the format-string language that
MDX inherits (`"#,##0.00"`, `"Currency"`, `"yyyy-mm-dd"`, positive/negative/zero/null
sections separated by `;`) — for numbers, strings, and dates. Every formatted
cell value goes through it: `RolapEvaluator#getFormatString` reads the measure's
`FORMAT_STRING` property, and `RolapEvaluator#format` renders the value via
`Format#get(formatString, locale)`. The MDX `Format()` function
(`olap.fun.FormatFunDef`) and the XMLA layer use it too. A `Format` instance is
a *compiled* format string (parsed once into a `BasicFormat` tree); `Format#get`
caches instances in a static LRU map capped at `Format#CacheLimit` (1000)
entries keyed by (format string, locale). Locale-specific symbols (decimal
separator, currency symbol, month names) come from `Format.FormatLocale`;
`DigitList` and `MondrianFloatingDecimal` are its private number-transcoding
helpers. If a cell renders oddly, this is where to look.

### `Pair` (and `Triple`)

An immutable, `Comparable`, hashable 2-tuple — the workhorse compound key and
compound return value, used in ~47 files across every package (e.g.
`Pair<Long, TimeUnit>` from `Util#parseInterval`, (member, value) pairs in
sorters, (schema, cube) keys in caches). `Triple` is the same idea with three
elements, currently unused outside the package.

### `ByteString` — schema and segment checksums

An immutable byte-array wrapper with proper `equals`/`hashCode`/hex `toString`,
used to carry MD5 digests: `RolapSchema#getChecksum` exposes the digest of the
catalog XML content, `RolapSchemaPool` keys its by-content map on it,
`SchemaContentKey` and `ConnectionKey` build on it (via `StringKey`), and
`spi.SegmentHeader` includes the schema checksum so segment-cache entries can
never be served to a different schema. See
[../topics/schema-loading.md](../topics/schema-loading.md).

### `ExpiringReference` — schema pool TTL

A `SoftReference` subclass that additionally holds a hard reference until a
timeout elapses, renewing the timer on each `get`. Its one consumer is
`RolapSchemaPool`, which wraps every pooled `RolapSchema` in one. The timeout
comes from the `PinSchemaTimeout` connect-string property (default `"-1s"`);
any value ≤ 0 means the hard reference is kept forever, so by default schemas
are never soft-collected — a positive value turns the pool into a
keep-alive-while-used cache.

### `ClassResolver` and `ServiceDiscovery` — SPI instantiation

The two mechanisms by which pluggable implementations
([spi.md](spi.md)-style extension points) become objects. `ClassResolver` loads
and instantiates a class by name with constructor arguments;
`ClassResolver#INSTANCE` (a `ThreadContextClassResolver`) is used wherever a
class name arrives via a property or connect string — schema processors and
data-source resolvers in `RolapConnection`/`RolapSchemaPool`, dialect classes
in `DialectManager`, external segment caches in `SegmentCacheWorker`.
`ServiceDiscovery` implements `META-INF/services` lookup (pre-dating
`java.util.ServiceLoader`); `DialectManager` uses it to find registered
`Dialect` implementations and `GlobalFunTable` to find user-defined functions
on the classpath.

### `CancellationChecker` — cheap cancel/timeout polling in tight loops

`CancellationChecker#checkCancelOrTimeout(iteration, execution)` is sprinkled
through every loop that can run millions of iterations — crossjoin and filter
evaluation (`CrossJoinFunDef`, `FilterFunDef`, `FunUtil`, `Sorter`), SQL result
reading (`SqlTupleReader`, `SqlMemberSource`, `SegmentLoader`), and
`RolapResult` itself. Every `CheckCancelOrTimeoutInterval` iterations it calls
`Execution#checkCancelOrTimeout`, which throws if the statement was cancelled
or timed out. **(fork PATCH)** Upstream synchronized on the `Execution` before
testing the interval; this fork reads the interval via
`Execution#getCheckCancelOrTimeoutInterval` and tests the modulus *before*
entering the synchronized block, removing per-iteration lock traffic.

### `BlockingHashMap` and `SlotFuture` — actor plumbing

Two small concurrency primitives serving the `SegmentCacheManager` actor
pattern (see [rolap-agg.md](rolap-agg.md)). `BlockingHashMap#get` blocks until
another thread `put`s the value for that key — it correlates responses with
waiting requesters in `SegmentCacheManager` and `server.MonitorImpl`.
`SlotFuture` is a write-once `Future` completed by whichever thread eventually
produces the value; `SegmentCacheIndexImpl` registers a `SlotFuture` per
in-flight segment load so concurrent queries needing the same segment wait on
one load instead of duplicating it. `CompletedFuture` is the trivial
already-done variant.

### `MemoryMonitor` family — present but off by default

`MemoryMonitor` (interface), `AbstractMemoryMonitor`,
`NotificationMemoryMonitor` (real implementation, driven by JVM
`MemoryPoolMXBean` usage-threshold notifications), and `FauxMemoryMonitor`
(no-op). `MemoryMonitorFactory` returns the no-op unless the
`mondrian.util.memoryMonitor.enable` property (`MondrianProperties#MemoryMonitor`)
is set. When enabled, `RolapConnection#execute` registers a listener that flags
the current `Execution` as out-of-memory so the query aborts instead of taking
the JVM down. Off by default, so treat OOM-cancellation as opt-in behavior.

### Collection helpers on the evaluation path

A few of the many list classes here are load-bearing: `CartesianProductList`
presents the cross product of component lists as a virtual `List<List<T>>`
without materializing it (`CrossJoinFunDef`'s immediate crossjoin,
`SegmentCacheIndexImpl` rollup candidate enumeration); `ArrayStack` is an
`ArrayList`-backed stack (faster, unsynchronized `java.util.Stack` replacement)
used by `Locus`, `ValidatorImpl`, `Query`, and the XMLA SAX writers;
`ConcatenableList` backs member-children accumulation in `SmartMemberReader`
and `FunUtil`; `ArraySortedSet` is the compact sorted-array `SortedSet` used
for segment axis keys (`SegmentAxis`, `SegmentBuilder`, `spi.SegmentColumn`).

## The rest, in one line each

| Class | Role |
|---|---|
| **Collections** | |
| `Composite` / `CompositeList` | View several lists/iterables as one (`RolapNativeRegistry`, `XmlaHandler`) |
| `UnionIterator` | Iterate a union of iterables (`RolapHierarchy`) |
| `TraversalList` | Transposed view of an array of lists (`NativizeSetFunDef`, `HighCardSqlTupleReader`) |
| `FilteredIterableList` | Lazy filtered list view (`SqlConstraintUtils`, `RolapConnection` role filtering) |
| `UnsupportedList` | Base list that throws on everything; base for lazy views (`RolapCubeHierarchy`) |
| `IteratorIterable` | Caches an iterator so it can be re-iterated as an `Iterable` |
| `CombiningGenerator` | Power set of a collection (`SegmentCacheManager` rollup search) |
| `ObjectPool` | Low-memory open-addressing `HashSet` replacement (`RolapResult`, `RolapConnectionPool`); `PrimeFinder` supplies its capacities |
| `PartiallyOrderedSet` | Poset with cover-graph navigation (`SegmentCacheIndexImpl`) |
| `CacheMap` | Small LRU map (used only inside `FilteredIterableList`) |
| **Concurrency** | |
| `Schedule` | Time-event generator; effectively unused legacy |
| `Counters` | Static debug counters for SQL statement accounting (`SqlStatement`) |
| `MDCUtil` | Copies the log4j MDC across the actor/executor thread hops (`Execution`, `SegmentCacheManager`, `SegmentLoader`) |
| **Reflection / compat** | |
| `UtilCompatible` + `UtilCompatibleJdk15`/`Jdk16` | Historical JDK-version shim: `mondrian.olap.Util` holds a static `UtilCompatible compatible = new UtilCompatibleJdk16()` and routes version-sensitive calls (statement cancel, memory info, `quotePattern`) through it; with Java 8 as the floor the indirection is vestigial but still the call path |
| `ObjectFactory` | Property-overridable singleton factory pattern (`MemoryMonitorFactory`, `ExpCompiler.Factory`) |
| `DelegatingInvocationHandler` | Reflective partial-proxy base (`SqlStatement`'s JDBC wrappers) |
| `BeanMap` | Bean-properties-as-map view (monitoring `Info` objects' `toString`) |
| **Misc** | |
| `IdentifierParser` | Parses `[Store].[USA].[CA]`-style member/tuple/set strings (via `mondrian.olap.Util`, `Query`, `FunUtil`) |
| `LockBox` | Passes live objects (connections) across API boundaries by string moniker (`MondrianServerRegistry`, XMLA) |
| `StringKey` | Type-safe string wrapper; base of `SchemaContentKey`, `ConnectionKey` |
| `Bug` | Named boolean constants gating code paths for known unfixed bugs |
| `Base64` | Standalone Base64 codec (used by `LockBox`) |
| `XmlParserFactoryProducer` | XXE-hardened XML parser factories (XMLA, `tui.XmlUtil`) |
| `SpatialValueTree`, `SpatialValueTree2` | Unused prototypes of a dimensional value index |
| `PropertyUtil` | Build-time generator for `MondrianProperties.java` |
| `CreationException` | Thrown by `ObjectFactory`/`ClassResolver` failures |

## Neighbor packages

**`mondrian.recorder`** — the `MessageRecorder` interface plus `ListRecorder`,
`LoggerRecorder`, and `PrintStreamRecorder` implementations: a small structured
warning/error collector with a context stack (`MessageRecorder#pushContextName`)
and an error budget (`RecorderException` after too many errors). Its only
client is the aggregate-table subsystem — `AggTableManager` and the
`rolap.aggmatcher` recognizers report match failures through it so one bad
aggregate table produces a readable log instead of an abort. See
[../topics/aggregate-tables.md](../topics/aggregate-tables.md).

**`mondrian.i18n`** — a single class, `LocalizingDynamicSchemaProcessor`: a
`FilterDynamicSchemaProcessor` (see [spi.md](spi.md)) that substitutes
`%{key}` tokens in the catalog XML from a locale-specific properties file
before parsing, so one schema definition can serve translated captions. It is
referenced by name in `RolapConnectionProperties` documentation and enabled via
the `DynamicSchemaProcessor` connect-string property.
