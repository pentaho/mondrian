# Schema Loading: from Catalog XML to a Queryable `RolapSchema`

*This document traces how a Mondrian catalog (schema XML) becomes a live,
queryable `RolapSchema`: the schema pool and its keys, catalog content
acquisition, XML parsing, cube and star construction, and the hand-off to
`SchemaReader`. It assumes the orientation given in
[../architecture.md](../architecture.md) and the query path in
[../query-lifecycle.md](../query-lifecycle.md); member reading is covered in
[member-resolution.md](member-resolution.md).*

Loading a schema is the single most expensive metadata operation in the
engine: it parses the catalog XML, instantiates every cube, dimension,
hierarchy, level and measure, builds one `RolapStar` join tree per fact table,
and kicks off aggregate-table discovery. Everything the engine later caches
hangs off the resulting `RolapSchema` instance, which is why schemas are
pooled and shared across connections.

## 1. Entry point: `RolapConnection` asks the pool

Every external connection begins in `RolapConnection#<init>`. When called with
`schema == null` (the normal case — only the pool itself passes a non-null
schema, see below), the constructor:

1. creates a bootstrap `mondrian.server.Statement` and pushes a `Locus`
   ("Initializing connection"), because schema loading may run SQL (aggregate
   discovery, default-member lookups) and the SQL layer requires a thread-local
   `Locus`;
2. calls `RolapSchemaPool.instance().get(...)` — one overload takes
   `(catalogUrl, connectionKey, jdbcUser, dataSourceStr, connectInfo)` where
   `connectionKey` is the JDBC connect string concatenated with the JDBC
   properties, the other takes an externally supplied `javax.sql.DataSource`;
3. resolves the connection's `Role` (section 7) and calls
   `RolapConnection#setRole`, which builds the connection's
   `RolapSchemaReader`.

The pool is consulted unless the `UseSchemaPool` connect-string property is
explicitly `false`, in which case `RolapSchemaPool#get` short-circuits to
`RolapSchemaPool#createRolapSchema` and returns a fresh, unregistered schema —
every such connection pays the full load cost and shares no caches.

One subtlety: each `RolapSchema` owns an **internal connection**. The
`RolapSchema` constructor creates a `RolapConnection` passing itself as the
`schema` argument — the one code path where `RolapConnection#<init>` skips
the pool (otherwise schema loading would recurse). This internal connection
parses calculated-member and named-set formulas during load, and later serves
cache-control operations.

## 2. The schema pool

`RolapSchemaPool` is a process-wide singleton (`RolapSchemaPool#instance`)
mapping keys to shared `RolapSchema` instances.

### Keys

`RolapSchemaPool#get` builds a `SchemaKey`, which is a pair of:

- **`ConnectionKey`** — identifies the *database* the schema reads from. If
  the `JdbcConnectionUuid` connect property is set, that string is used
  verbatim (letting a caller declare "these connections hit the same
  database"). Otherwise `ConnectionKey#create` MD5-hashes either the
  DataSource identity (JVM instance UUID + `System.identityHashCode` of the
  DataSource) or the tuple (connection key = JDBC URL + JDBC properties,
  catalog URL, JDBC user, DataSource string).
- **`SchemaContentKey`** — identifies the *metadata content*.
  `SchemaContentKey#create` hashes the actual catalog string when
  `CatalogContent` or `DynamicSchemaProcessor` is in play (inline or generated
  content has no stable URL), and just the catalog URL otherwise. Note the
  URL-keyed case means editing the file behind an unchanged URL does **not**
  produce a new key — see `UseContentChecksum` below.

Two connections share a pooled schema exactly when both halves match: same
metadata against a different database must not share member/segment caches,
and vice versa.

### Lookup maps and locking (fork PATCH)

The pool holds two maps:

- `mapKeyToSchema`: `SchemaKey → ExpiringReference<RolapSchema>` — the primary
  index, used by `RolapSchemaPool#getByKey`.
- `mapMd5ToSchema`: `ByteString → ExpiringReference<RolapSchema>` — an MD5
  content-hash index used by `RolapSchemaPool#getByChecksum` when the
  `UseContentChecksum` property is `true`. In that mode the catalog string is
  hashed on every `get`, so connections whose catalog *content* is identical
  share a schema even if their keys differ, and changed content yields a new
  schema. (An upstream `REVIEW` comment notes this map ignores the connection
  half of the key and could over-share across databases.)
  `RolapSchemaPool#putSchema` registers a checksum-loaded schema in both maps.

Upstream guards both maps with plain `HashMap`s behind a single
`ReentrantReadWriteLock`. This fork replaces that (fork PATCH): both maps are
`ConcurrentHashMap`s, and the single lock is replaced by an array of 100
striped `ReentrantReadWriteLock`s selected by key hash
(`RolapSchemaPool#getLock`). `RolapSchemaPool#lookUp` runs under the stripe's
read lock; `getByKey`/`getByChecksum` do a double-checked re-lookup under the
stripe's write lock before calling `createRolapSchema`. The practical effect:
two connections racing for the *same* missing schema serialize (one builds,
the other waits and reuses), while schemas on different stripes load fully in
parallel — upstream serialized all schema creation globally.
`RolapSchemaPool#remove`, `#clear`, `#getRolapSchemas` and `#contains` take no
lock at all (fork PATCH), relying on the concurrent maps.

Note that the entire schema build — XML parse, cubes, stars, aggregate
discovery — runs inside the stripe's write lock, on the connecting thread.

### Reference lifetime: `ExpiringReference` and `PinSchemaTimeout`

Pool values are `mondrian.util.ExpiringReference`s — `SoftReference`
subclasses with an optional hard-reference pin controlled by the
`PinSchemaTimeout` connect property:

- negative (the default, `"-1s"`): plain soft reference — the GC may reclaim
  an unused schema under memory pressure, and the next `get` rebuilds it;
- `0`: permanent hard reference — the schema is pinned until explicitly
  removed;
- positive (e.g. `"1h"`): the schema is held hard for that duration, and the
  timer is re-armed on every pool access, so actively used schemas stay pinned.

### Eviction

`RolapSchemaPool#remove` (by connect parameters or by schema) and
`RolapSchemaPool#clear` drop map entries and then call
`RolapSchema#finalCleanUp` on each evicted schema, which flushes its cell
segments and shuts down its `AggTableManager` JDBC metadata caches. The public
route to these is `CacheControlImpl#flushSchema` /
`CacheControlImpl#flushSchemaCache` — see [caching.md](caching.md).

## 3. Where the catalog content comes from

Before any key is computed, `RolapSchemaPool#getSchemaContent` resolves the
catalog text, in precedence order:

1. **`CatalogContent` property** — the schema XML passed inline in the connect
   string. Used as-is, unless a `DynamicSchemaProcessor` is also configured,
   in which case `DynamicSchemaProcessor#processCatalog` post-processes it.
2. **`DynamicSchemaProcessor` SPI** — if only a catalog URL is given, the
   named class is instantiated via `ClassResolver.INSTANCE` and
   `DynamicSchemaProcessor#processSchema(catalogUrl, connectInfo)` produces
   the content (typical uses: templating, localization, per-tenant rewriting).
3. **Catalog URL** — otherwise `Util#readVirtualFileAsString` reads the URL
   (Apache Commons VFS, so `file:`, `http:` and other VFS schemes work).

Having neither `Catalog` nor `CatalogContent` is an error.

## 4. XML → object model: `RolapSchema#load`

`RolapSchemaPool#createRolapSchema` invokes the `RolapSchema` constructor,
which sets up the schema's identity (a generated UUID `id`, the `SchemaKey`,
the MD5 checksum), a root `defaultRole` (`Util#createRootRole` — all access),
the internal connection, and an `AggTableManager`, then calls
`RolapSchema#load(catalogUrl, catalogStr, connectInfo)`:

1. **Parse.** `XOMUtil#createDefaultParser` (the eigenbase-xom library) parses
   the catalog string (or streams the URL) into a `DOMWrapper`. The MD5
   checksum is computed here if the pool didn't already have one.
2. **Version gate.** `RolapSchema#checkSchemaVersion` rejects Mondrian-4
   catalogs: a `metamodelVersion` attribute with a major version above the
   engine's supported "3", or — absent that attribute —
   `RolapSchema#hasMondrian4Elements` detecting `<PhysicalSchema>` or
   `<Cube>/<MeasureGroups>` elements, produces a load error.
3. **Typed model.** `new MondrianDef.Schema(def)` maps the DOM onto
   `MondrianDef` — classes *generated by eigenbase-xom from `Mondrian.xml`*,
   one per schema element (`MondrianDef.Cube`, `.Dimension`, `.Measure`, …).
   All subsequent construction walks these typed objects, never the raw DOM.
4. **Build.** The private `RolapSchema#load(MondrianDef.Schema)` walks the
   model in a fixed order:
   - schema name and annotations;
   - **user-defined functions** — each `<UserDefinedFunction>` (class or
     script) is validated by `RolapSchema#defineFunction`, then all are
     wrapped in a `RolapSchema.RolapSchemaFunctionTable`, the function table
     that layers schema UDFs over the built-ins (its place in function
     resolution is described in
     [../query-lifecycle.md](../query-lifecycle.md), stage 3). UDFs come
     first because calculated members parsed later may call them;
   - validation that public (shared) dimensions declare no `foreignKey`;
   - **parameters** — each `<Parameter>` becomes a `RolapSchemaParameter`
     (registering itself with the schema in its constructor);
   - **cubes** — each enabled `<Cube>` becomes a
     `RolapCube(schema, xmlSchema, xmlCube, …)` (section 5); construction is
     self-registering via `RolapSchema#addCube`;
   - **virtual cubes** — each enabled `<VirtualCube>` uses the virtual-cube
     `RolapCube` constructor; these must come after base cubes, which they
     reference;
   - **named sets** — `RolapSchema#createNamedSet` parses each formula through
     the internal connection;
   - **roles** — section 7 — and the schema `defaultRole` attribute.
5. **Aggregates + timestamp.** Finally `AggTableManager#initialize(connectInfo)`
   runs aggregate-table discovery/matching against JDBC metadata (see
   [aggregate-tables.md](aggregate-tables.md)), and
   `RolapSchema#setSchemaLoadDate` stamps the load time.

## 5. Cube construction

All cubes funnel through the private base constructor `RolapCube#<init>`,
which the regular-cube and virtual-cube constructors both delegate to.

### The base constructor (both kinds)

- For a non-virtual cube, the star comes first:
  `RolapSchema.RolapStarRegistry#getOrCreateStar(fact)` (section 6). A
  `cache="false"` cube disables aggregation caching on its (possibly shared)
  star.
- A synthetic **`[Measures]` dimension** is created as `dimensions[0]` — a
  `RolapDimension` of type `MeasuresDimension` with a single
  `measuresHierarchy`. Measures are ordinary members of this hierarchy; there
  is no separate "measure" concept at evaluation time.
- Each `<CubeDimension>` goes through `RolapCube#getOrCreateDimension`, then
  `RolapCube#createUsages` (non-virtual only) and
  `RolapCube#registerDimension` (star wiring, section 6).
- The `closureColumnBitKey` is sized from `RolapStar#getColumnCount` once all
  columns exist, and the cube registers itself via `RolapSchema#addCube`.

### Regular cubes: measures

The regular constructor (`RolapCube#<init>` taking `MondrianDef.Cube`) then
builds the measure members:

- `RolapHierarchy#newMeasuresLevel` creates the single measures level; each
  `<Measure>` becomes a `RolapBaseCubeMeasure` via `RolapCube#createMeasure`
  (aggregator, format, datatype, ordinal).
- If no measure uses the `count` aggregator, a hidden **`Fact Count`** measure
  is synthesized and stored as `factCountMeasure` — the engine needs an atomic
  row count for distinct-count rollups and drill-through.
- The measures hierarchy's member reader is a
  `CacheMemberReader(new MeasureMemberSource(...))` — measures are a finite,
  static member list, so they use the no-SQL cached reader (see
  [member-resolution.md](member-resolution.md)).
- The `defaultMeasure` attribute, if it matched a stored measure, is applied
  with `RolapHierarchy#setDefaultMember`; then `RolapCube#init` runs
  per-dimension initialization and `RolapCube#register`, which calls
  `RolapStar.Table#makeMeasure` on the fact table for every stored measure —
  giving each measure its star column and bit position. Calculated members
  and cube named sets are parsed last (they need a functioning cube), and the
  member reader is rebuilt to include them.

### Virtual cubes

The virtual-cube constructor passes `fact = null` (so `RolapCube#isVirtual`
is true and there is **no star** — a virtual cube borrows the stars of its
base cubes). Each `<VirtualCubeMeasure>` is looked up in its base cube:
stored measures are wrapped as `RolapVirtualCubeMeasure` (pointing at the
underlying `RolapStoredMeasure` and hence the base cube's star), while
calculated measures are re-resolved from their original XML against the base
cube so their dimension ordinals fit the virtual cube. `RolapCubeUsages`
records the `<CubeUsage>` declarations (e.g. `ignoreUnrelatedDimensions`).

### The `RolapCube*` wrapper pattern — and why it exists

`RolapCube#getOrCreateDimension` resolves each cube dimension: a
`<DimensionUsage>` reuses the schema-level shared dimension
(`RolapSchema#getSharedHierarchy(usage.source)`), otherwise a private
`RolapDimension` is built — and **either way the result is wrapped in a
`RolapCubeDimension`**, whose hierarchies and levels are correspondingly
wrapped as `RolapCubeHierarchy` and `RolapCubeLevel`.

The reason: a shared dimension is a *single* schema-level object used by many
cubes, but plenty of state is inherently per-cube — the dimension's ordinal
inside the cube, the name given by the usage, table alias remapping when the
same dimension joins one fact table several times (`RolapCubeHierarchy` keeps
an alias map per usage), and above all the physical mapping:
`RolapCubeLevel#starKeyColumn` holds the `RolapStar.Column` (and therefore the
bit position) that this level joins through *in this cube's star*. The same
shared `[Time]` level maps to different star columns in different cubes.
Members read for a cube hierarchy are likewise wrapped as `RolapCubeMember`s
over the shared `RolapMember`s — the reader stack that does this is described
in [member-resolution.md](member-resolution.md).

## 6. Star construction

`RolapStar` is the physical side of the model: one instance per fact-table
relation, owned by the schema through `RolapSchema.RolapStarRegistry`.
`RolapStarRegistry#getOrCreateStar` (synchronized) keys stars by
`RolapUtil#makeRolapStarKey`, creates the star on first use, and asks the
`SegmentCacheManager` to preload any externally cached segments for it
(`SegmentCacheManager#loadCacheForStar`).

`RolapStar#<init>` creates the root `RolapStar.Table` for the fact relation.
`RolapStar.Table#addJoin` walks a `MondrianDef.Join` recursively, creating
(or reusing, via `RolapStar.Table#findChild`) one child `Table` per joined
relation, each carrying its `RolapStar.Condition` join predicate — the
snowflake becomes a tree rooted at the fact table.

`RolapCube#registerDimension` performs the wiring for every hierarchy usage of
every cube dimension: it first normalizes snowflake join order
(`RolapCube#reorder`) and clips levels below a `DimensionUsage` `level`
attribute (`RolapCube#snip`), calls `RolapStar.Table#addJoin` to graft the
dimension tables onto the star, then walks the hierarchy's levels from top to
bottom calling `RolapCube#makeColumns` → `RolapStar.Table#makeColumns`, which
creates a `RolapStar.Column` per level key expression and links it into the
level's `RolapCubeLevel#starKeyColumn`.

Every `RolapStar.Column` receives a unique, monotonically assigned **bit
position** within its star (`RolapStar#nextColumnCount` in the `Column`
constructor). Bit positions are the currency of the whole aggregation layer:
sets of constrained columns are represented as `BitKey`s, compared and matched
cheaply during cell batching and aggregate-table matching (see
[../architecture.md](../architecture.md) and
[cell-batching.md](cell-batching.md)).

At the very end of schema load, `AggTableManager#initialize` scans the
database for candidate aggregate tables and matches them to each star's
structure — details in [aggregate-tables.md](aggregate-tables.md).

## 7. Default members, roles, and the `SchemaReader` hand-off

**Default members.** A hierarchy's `defaultMember` XML attribute is resolved
during hierarchy initialization; the measures hierarchy gets its default set
explicitly during cube construction. For everything else,
`RolapHierarchy#getDefaultMember` is *lazy*: on first call it takes the first
non-hidden root member — from `MemberReader#getRootMembers` plus schema
calculated members — which for an `hasAll` hierarchy is the All member, and
may legitimately be a non-visible member (a cube with no explicit measures
defaults to the hidden `Fact Count`). These defaults seed every query's
initial evaluator context (`RolapEvaluatorRoot` — see
[../query-lifecycle.md](../query-lifecycle.md), stage 5).

**Roles.** Each `<Role>` becomes a `RoleImpl` via `RolapSchema#createRole`,
applying `SchemaGrant`/`CubeGrant`/`HierarchyGrant`/`MemberGrant` elements
(`RolapSchema#handleSchemaGrant` and friends); `<Union>` roles combine others
via `RolapSchema#createUnionRole`. The schema's `defaultRole` starts as the
all-access root role and is replaced if the `<Schema defaultRole=...>`
attribute names a role. At connect time, the `Role` connect property is
resolved per name — first against the server `LockBox` (programmatically
registered roles), then `RolapSchema#lookupRole` — with multiple names merged
by `RoleImpl#union`; absent the property, the connection gets
`RolapSchema#getDefaultRole`.

**Hand-off.** The loaded schema is consumed through `SchemaReader`:
`RolapConnection#setRole` creates a `RolapSchemaReader(role, schema)` for the
connection, and `RolapSchema#getSchemaReader` supplies a default-role reader
for internal use. The reader lazily builds one role-wrapped `MemberReader` per
hierarchy on first access — from here on, metadata access is the subject of
[member-resolution.md](member-resolution.md).

## 8. Practical notes

- **What forces a reload.** A new `SchemaKey` (different catalog URL, JDBC
  connect string/user, DataSource, or `JdbcConnectionUuid`); changed catalog
  content when `UseContentChecksum=true`; explicit eviction
  (`CacheControl#flushSchema` / `#flushSchemaCache` →
  `RolapSchemaPool#remove`/`#clear`); GC of a soft-referenced schema under
  memory pressure (the default `PinSchemaTimeout=-1s`); or `UseSchemaPool=false`,
  which reloads on *every* connection. Conversely, editing a catalog file
  behind an unchanged URL without `UseContentChecksum` does **not** reload —
  the content key hashes the URL, not the file.
- **Cost.** A reload is the full pipeline above: XML parse, every cube and
  star, plus `AggTableManager#initialize` hitting JDBC metadata. It also
  abandons the old schema's member caches; segments are flushed by
  `RolapSchema#finalCleanUp` on explicit eviction. Prefer letting the pool
  share schemas; use `PinSchemaTimeout` if soft-reference eviction causes
  reload churn.
- **Thread safety.** The pool's striped locks (fork PATCH) serialize
  concurrent loads of the *same* schema (one thread builds, the rest wait on
  the stripe) while allowing distinct schemas to load in parallel. The build
  runs on the connecting thread, inside the stripe's write lock, with a
  bootstrap `Locus` pushed for any SQL it needs. Post-load, `RolapSchema` is
  effectively immutable metadata plus concurrent caches; the star registry's
  methods are synchronized.
