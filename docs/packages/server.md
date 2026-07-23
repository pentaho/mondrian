# Package guide: `mondrian.server` (and `mondrian.server.monitor`)

*The runtime harness around query execution: `MondrianServer` instances and
what they own, the `Statement`/`Execution` lifecycle with cancellation and
timeouts, the `Locus` thread-local execution context, event-based monitoring,
and catalog repositories for XMLA. Where this sits in the stack:
[architecture.md](../architecture.md); how one query flows through these
objects: [query-lifecycle.md](../query-lifecycle.md); the API adapters that
drive it: [olap4j.md](olap4j.md) and [xmla.md](xmla.md).*

Only 15 classes plus the `monitor/` subpackage, but they appear on every code
path: every SQL statement the engine runs is tied to an `Execution` through a
`Locus`, and every connection, statement, and cache event flows through the
server's `Monitor`. Note that the abstract `MondrianServer` class itself lives
in `mondrian.olap`; this package holds its implementation and registry.

## 1. Server instances: `MondrianServer`, `MondrianServerImpl`, `MondrianServerRegistry`

A `MondrianServer` is one engine instance. `MondrianServerImpl#<init>` creates
and owns:

- the **`AggregationManager`** — and through its `SegmentCacheManager` all
  shared segment caches, the cache executors, and the cache-index actor;
- the **`RolapResultShepherd`** — the query watchdog (§3);
- a **`MonitorImpl`** — the event sink (§5), also registered as a JMX MBean
  named `mondrian.server:type=Server-<id>` (`MondrianServerImpl#registerMBean`);
- a **`Repository`** + `CatalogLocator` — catalog lookup for XMLA (§6);
- weak-valued maps of open connections and statements
  (`MondrianServer#addConnection`/`#addStatement` and the `remove*`
  counterparts, which also emit the monitor start/end events);
- a reference to the JVM-wide **`LockBox`** (below).

**Static vs. embedded servers.** `MondrianServerRegistry.INSTANCE` holds one
*static server* — created with an `ImplicitRepository`, meaning "no repository:
each connection names its own catalog in the connect string". This is the
server the mondrian-olap JRuby gem always uses: the olap4j driver path funnels
into `mondrian.olap.DriverManager#getConnection`, which reads the `Instance`
connect-string property and calls `MondrianServer#forId` — `null` (the normal
case) yields the static server, and the resulting `RolapConnection` stores the
server (`RolapConnection#getServer`; `MondrianServer#forConnection` is just
that getter). Separate server instances exist for XMLA hosting:
`MondrianServer#createWithRepository` builds one around a `FileRepository`.
Note `MondrianServerRegistry#serverForId` *throws* for an unknown non-null id,
despite `MondrianServer#forId`'s javadoc promising null.

**Lifecycle.** `MondrianServer#shutdown` (or `MondrianServer#dispose(id)`)
shuts down the aggregation manager, monitor, repository, and shepherd, and
makes every accessor throw "Server already shutdown". The static server
refuses to shut down (`MondrianServerImpl#shutdown(boolean)` logs a warning),
so in the gem's embedded use the server and its caches live as long as the JVM.

**`LockBox`** (`mondrian.util.LockBox`) passes live objects across a
string-only boundary such as a connect string: `LockBox#register(Object)`
returns an `Entry` whose unguessable moniker can later be resolved with
`LockBox#get(moniker)`; the value stays resolvable only while someone holds
the `Entry`. All servers share one lock box (`MondrianServerRegistry.lockBox`).
Two uses: `RolapConnection#<init>` resolves each name in the `Role`
connect-string property against the lock box first — so a caller can register
a programmatic `Role` object and pass its moniker — before falling back to
`RolapSchema#lookupRole`; and `FileRepository#getConnection` registers the
*server itself* and passes the moniker as the `Instance` property so the
resulting connection binds to that server rather than the static one.

## 2. `Statement` and `Execution`

`mondrian.server.Statement` is the engine's internal statement context —
usually implemented by `MondrianOlap4jStatement` on top of the abstract
`StatementImpl`, but also created internally
(`RolapConnection#createInternalStatement`; every `RolapSchema` keeps an
internal connection whose statement hosts schema-maintenance work). A
statement holds the compiled `Query`, the `ProfileHandler`, and the query
timeout (`Statement#setQueryTimeoutMillis`; defaults to the `QueryTimeout`
property, converted to milliseconds, in `StatementImpl`).

An `Execution` is one run of a statement — "loosely corresponds to a CellSet",
and literally so on the olap4j path: `MondrianOlap4jCellSet` *extends*
`Execution`, passing the statement's timeout to the constructor. At most one
execution per statement at a time: `StatementImpl#start` installs it and calls
`Execution#start` (state `FRESH` → `RUNNING`, timeout deadline armed,
`ExecutionStartEvent` fired); `StatementImpl#end` clears it and calls
`Execution#end` (state → `DONE`, registered SQL statements dropped, segment
registrations released via `Execution#unregisterSegmentRequests`,
`ExecutionEndEvent` fired). `StatementImpl#cancel` before a run starts sets a
`cancelBeforeStart` flag so the next run is canceled immediately.

**Cancellation and timeout are cooperative.** `Execution#cancel` flips the
state to `CANCELED` and calls `Execution#cancelSqlStatements`, which invokes
`Util#cancelStatement` on every registered JDBC statement (registered by
`SqlStatement` via `Execution#registerStatement`) — cancel but never close,
because closing must happen on the thread that runs the statement. Nothing
else happens until the executing thread polls
`Execution#checkCancelOrTimeout`, which throws the canonical `QueryCanceled`,
`QueryTimeout`, or (after `Execution#setOutOfMemory`)
`MemoryLimitExceededException` error. Real call sites:

- `RolapResult#executeStripe` — once per axis stripe during cell evaluation;
- `RolapEvaluator#_push` — every evaluator context push;
- `SqlStatement#execute` — immediately before and after handing SQL to JDBC,
  and again in `FastBatchingCellReader`'s batch loader before a queued load
  request starts;
- tight loops (`FilterFunDef`, `CrossJoinFunDef`, `Sorter`,
  `SqlMemberSource` fetch loops) go through
  `mondrian.util.CancellationChecker#checkCancelOrTimeout`, which only checks
  every N iterations. **(fork PATCH)** The interval is read once per execution
  (`Execution#getCheckCancelOrTimeoutInterval`, `CheckCancelOrTimeoutInterval`
  property) and the modulo test happens outside the synchronized block, so the
  hot path is uncontended.

Executions chain: the constructor captures `Locus.peek().execution` as its
parent, so a nested execution (an internal statement spawned mid-query)
inherits cancellation — `#cancel`, `#checkCancelOrTimeout`, and
`#isCancelOrTimeout` all recurse into the parent. `Execution` also accumulates
the per-run statistics (phase count, cell/expression cache hit/miss counts,
`QueryTiming`) that `Execution#tracePhase` — called from `RolapResult#phase` —
and the end event deliver to the monitor.

## 3. `RolapResultShepherd` — the server-side watchdog

Lives in `mondrian.rolap` but is owned by the server
(`MondrianServer#getResultShepherd`) and completes the cancellation story.
`RolapConnection#execute` does not run the query on the caller's thread:
`RolapResultShepherd#shepherdExecution` wraps the evaluation in a `FutureTask`
executed on a fixed-size pool (`RolapConnectionShepherdNbThreads` threads,
unbounded queue; pool exhaustion throws `QueryLimitReached`) while the caller
blocks on `FutureTask#get`. A single `Timer` thread polls every registered
(task, execution) pair on the `RolapConnectionShepherdThreadPollingInterval`;
when `Execution#isCancelOrTimeout` turns true it cancels the future *without*
interrupting (`cancel(false)`), which unblocks the caller immediately. The
caller's catch block in `#shepherdExecution` then cancels pending SQL, calls
`Execution#checkCancelOrTimeout` so the canonical exception is thrown, and
otherwise unwraps `ExecutionException`s to rethrow the real cause. The worker
thread keeps running until it reaches its next cooperative check — hence the
many call sites above.

## 4. `Locus` — where am I executing?

A `Locus` is an immutable triple {`execution`, `component`, `message`}
("SqlTupleReader.readTuples", "Loading cells", …) kept on a per-thread stack
(`Locus#push`/`#pop`/`#peek`, backed by a `ThreadLocal<ArrayStack<Locus>>`).
It is how deep layers find the current `Execution` (and through it statement →
connection → server → caches) without threading parameters everywhere. The two
idioms:

```java
Locus.execute(rolapConnection, "component", action);  // wraps action in a
    // fresh Execution on the connection's internal statement (no timeout)
Locus.execute(execution, "component", action);        // push/try/finally pop
```

Examples: `MondrianOlap4jStatement#parseQuery` wraps parsing in
`Locus.execute(connection, "Parsing query", …)`; `RolapResult` pushes a
`"Loading cells"` locus around the cell-loading phases;
`RolapConnection#<init>` pushes an `"Initializing connection"` locus around
schema loading.

SQL execution requires a locus: `SqlStatement` takes one at construction, uses
`locus.execution` to register the JDBC statement for cancellation and to check
cancel/timeout, and stamps monitor events with the locus's statement and
execution ids (`SqlStatement.StatementLocus` adds the purpose and cell-request
count). Code that fetches members or tuples reaches for the ambient context
via `Locus.peek()` (`SqlMemberSource`, `SqlTupleReader`,
`SegmentCacheManager`). When no locus has been pushed, `Locus#peek` throws
`java.util.EmptyStackException` (from `ArrayStack#peek`) — the classic symptom
of calling engine internals from a thread outside any `Locus.execute` wrapper.
**(fork PATCH)** `Locus#getSchema` exposes the execution's schema for the
per-schema segment-cache actor.

## 5. `mondrian.server.monitor` — events, counters, snapshots

`Monitor` (the interface returned by `MondrianServer#getMonitor`) is an
actor: `Monitor#sendEvent` enqueues fire-and-forget `Event` objects;
`MonitorImpl`'s single static daemon thread ("Mondrian Monitor", shared by all
servers in the JVM, bounded queue of 1000) applies them to mutable counter
workspaces, so no counter needs locking. Emission sites:

- `MondrianServerImpl#addConnection`/`#removeConnection` and
  `#addStatement`/`#removeStatement` — `Connection*Event`, `Statement*Event`;
- `Execution#start`/`#end`/`#cancel` and `#tracePhase` —
  `ExecutionStartEvent`, `ExecutionEndEvent`, `ExecutionPhaseEvent`;
- `SqlStatement` — `SqlStatementStartEvent`/`ExecuteEvent`/`EndEvent` with
  execute nanos and row counts;
- `SegmentCacheManager` — `CellCacheSegmentCreateEvent` (tagged SQL / rollup /
  external) and `CellCacheSegmentDeleteEvent`.

Reading the counters goes through the same queue: `Monitor#getServer`,
`#getConnections`, `#getStatements`, `#getSqlStatements` post `Command`
messages and block for the reply — immutable `Info` snapshots (`ServerInfo`,
`ConnectionInfo`, `StatementInfo`, `ExecutionInfo`, `SqlStatementInfo`)
produced by `fix()`-ing the workspaces. History maps are LRU-bounded by the
`ExecutionHistorySize` property (a retired-executions map absorbs cache events
that arrive after their execution ended), every event is also logged to
`RolapUtil.MONITOR_LOGGER`, and `MonitorMXBean` exposes it all over JMX.
The subpackage carries no fork PATCHes.

## 6. Repositories (XMLA multi-catalog)

A `Repository` tells a server what databases/catalogs/schemas it hosts.
Embedded use — including the mondrian-olap gem — needs none:
`ImplicitRepository` answers every question from the connection's own schema
and cannot create connections. XMLA hosting uses `FileRepository`: it parses a
`datasources.xml` supplied by a `RepositoryContentFinder`
(`StringRepositoryContentFinder` for literal XML, `UrlRepositoryContentFinder`
for a URL, `DynamicContentFinder` adding timed reloads), caches the parsed
`FileRepository.ServerInfo`, re-reads it on the `XmlaSchemaRefreshInterval`,
and materializes connections by driving the olap4j driver with the catalog's
connect string plus the server's lock-box moniker as `Instance`.
`MondrianXmlaServlet#createConnectionFactory` is the assembly point. See
[xmla.md](xmla.md).

## 7. Class table

| Class | Role |
|---|---|
| `MondrianServerImpl` | The engine instance: owns `AggregationManager`, shepherd, monitor, repository, connection/statement registries; also `CatalogFinder` and XMLA `ConnectionFactory` |
| `MondrianServerRegistry` | JVM-wide factory/registry: static server, `serverForId` lock-box lookup, `createWithRepository` |
| `Statement` / `StatementImpl` | Internal statement context: query, timeout, profiling, one-execution-at-a-time `start`/`end`/`cancel` |
| `Execution` | One statement run: state machine, cancel/timeout checks, SQL-statement registry, phase statistics **(fork PATCH: cached cancellation-check interval)** |
| `Locus` | Thread-local execution-context stack; `Locus.execute` idiom **(fork PATCH: `#getSchema`)** |
| `MonitorImpl` | Actor thread applying events to counters; serves `Info` snapshots; JMX MXBean |
| `Repository` | SPI: what databases/catalogs/schemas a server hosts |
| `ImplicitRepository` | No repository — catalog comes from each connect string (embedded / static server) |
| `FileRepository` | `datasources.xml`-backed repository with timed refresh; creates per-catalog olap4j connections |
| `RepositoryContentFinder` | SPI: fetch the repository XML |
| `StringRepositoryContentFinder` / `UrlRepositoryContentFinder` / `DynamicContentFinder` | Constant string / URL / URL with periodic reload |
| **monitor/** | |
| `Monitor` / `MonitorMXBean` / `Message` / `Visitor` | Event-sink interface, its JMX facet, the message contract, and the dispatch visitor |
| `Event` + `ConnectionStartEvent`/`ConnectionEndEvent`, `StatementStartEvent`/`StatementEndEvent`, `ExecutionStartEvent`/`ExecutionPhaseEvent`/`ExecutionEndEvent`, `SqlStatementStartEvent`/`SqlStatementExecuteEvent`/`SqlStatementEndEvent`, `CellCacheSegmentCreateEvent`/`CellCacheSegmentDeleteEvent` | Immutable event records, one per lifecycle edge (abstract bases: `ConnectionEvent`, `StatementEvent`, `ExecutionEvent`, `SqlStatementEvent`, `CellCacheEvent`) |
| `Info` + `ServerInfo`, `ConnectionInfo`, `StatementInfo`, `ExecutionInfo`, `SqlStatementInfo` | Immutable counter snapshots returned by the polling commands |
