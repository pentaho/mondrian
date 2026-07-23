# Class reference: `mondrian.olap`, `mondrian.olap.type`, `mondrian.olap.fun`, `mondrian.mdx`, `mondrian.parser`

*Lookup catalog — grep for a class name. Every top-level type in these packages
appears exactly once, tiered by importance: **Tier 1** classes get a full entry
(purpose, collaborators, lifecycle), **Tier 2** a paragraph, **Tier 3** one
table line. Narrative context lives in [../packages/olap.md](../packages/olap.md),
[../packages/olap-fun.md](../packages/olap-fun.md), and
[../packages/mdx-parser.md](../packages/mdx-parser.md); the big picture is
[../architecture.md](../architecture.md).*

---

## Package `mondrian.olap`

The abstract OLAP model, the `Query` AST, the validator, and engine-wide
services. Concrete implementations live in `mondrian.rolap`.

Generated types are not separate entries here: `MondrianProperties` (from
`MondrianProperties.xml`; covered under `MondrianPropertiesBase` below),
`MondrianDef` (the schema-XML object model generated from `Mondrian.xml`
by the eigenbase-xom `XOMGenTask` — the `<Schema>`/`<Cube>`/`<Dimension>`
element classes that `RolapSchema` loading walks), and the CUP-generated
`Parser`/`ParserSym` (dead parse path, but `Parser.FactoryImpl` — the default
`QueryPartFactory` — lives on it; see
[../packages/mdx-parser.md](../packages/mdx-parser.md)).

### Query

- **Purpose**: a parsed MDX SELECT statement — axes, slicer, WITH formulas —
  plus, after compilation, the executable `Query#axisCalcs` / `Query#slicerCalc`.
- **Extends/Implements**: extends `QueryPart`.
- **Key collaborators**: owned by a `mondrian.server.Statement`; holds
  `QueryAxis[]`, `Formula[]`, `Parameter`s; `Query#resolve` runs
  `IdBatchResolver`, a `Validator` (`Query#createValidator`), and an
  `ExpCompiler` over the tree.
- **Lifecycle/scope**: the constructor calls `Query#resolve()`, so a `Query`
  never exists unvalidated; a prepared statement re-executes the same `Query`
  without re-analysis. One `Query` per statement text.
- **Notes/gotchas**: `Query#toString` re-resolves and unparses the **resolved**
  form (unique names, canonical function rendering) — not the original text.
  `Query#ignoreInvalidMembers` folds the `strictValidation` parse flag together
  with the `IgnoreInvalidMembers*` properties.

### QueryAxis

- **Purpose**: one axis of a `Query`: the set expression, the NON EMPTY flag,
  and its `AxisOrdinal` (slicer axis included — the slicer is a `QueryAxis`
  built by `Parser.FactoryImpl#makeQuery`).
- **Extends/Implements**: extends `QueryPart`.
- **Key collaborators**: `Query` (owner); `QueryAxis#compile` produces the axis
  `Calc` stored in `Query#axisCalcs`; carries `SubtotalVisibility` and
  dimension-property lists for drill state.
- **Lifecycle/scope**: same as its `Query`.
- **Notes/gotchas**: `QueryAxis#resolve` validates the set expression as a set;
  NON EMPTY is enforced later by `RolapResult` axis evaluation, not here.

### Formula

- **Purpose**: one WITH MEMBER / WITH SET clause; creates and owns the
  resulting calculated member or query-scoped named set.
- **Extends/Implements**: extends `QueryPart`.
- **Key collaborators**: `Query` (owner); `Validator` (resolution);
  `Formula#getMdxMember` / `#getNamedSet` expose the created object.
- **Lifecycle/scope**: created by the parser, realized during `Query#resolve`.
- **Notes/gotchas**: `Formula#getFormatExp` infers the member's FORMAT_STRING
  from its expression when none is given (stored as `Property.FORMAT_EXP_PARSED`);
  the search honors `FormatAwareFunDef` (fork PATCH) so functions like
  `Min`/`Max` can steer which argument's format wins.

### Exp / ExpBase

- **Purpose**: THE expression contract of the AST: `Exp#getType`,
  `Exp#accept(Validator)` (resolution, may return a replacement node),
  `Exp#accept(ExpCompiler)` (compilation), `Exp#unparse`. `ExpBase` is the
  skeleton implementation every node extends.
- **Extends/Implements**: `ExpBase` extends `QueryPart` implements `Exp`.
- **Key collaborators**: concrete nodes are `Id`, `Literal` (here) and the
  `mondrian.mdx` classes; `Validator` rewrites the tree; `ExpCompiler`
  (see [../packages/calc.md](../packages/calc.md)) turns it into `Calc`s.
- **Lifecycle/scope**: built at parse time, rewritten in place at resolve time,
  inert after compilation.
- **Notes/gotchas**: an expression's `Type` is only reliable after validation.

### Id

- **Purpose**: a multi-part identifier (`[Store].[USA].[CA]`) before binding —
  a list of `Id.Segment`s.
- **Extends/Implements**: extends `ExpBase`, implements `Cloneable`.
- **Key collaborators**: `Id.NameSegment` (with `Id.Quoting` UNQUOTED/QUOTED)
  and `Id.KeySegment` (the `&[key]` form, possibly composite); consumed by
  `IdBatchResolver`, `Util#lookup`, `NameResolver`, and `SchemaReader` lookups.
- **Lifecycle/scope**: parse-time only — resolution replaces every `Id` with a
  bound leaf (`MemberExpr`, `LevelExpr`, …) or a symbol `Literal`.
- **Notes/gotchas**: `Id.Segment`s are the currency of name handling engine-wide
  (member lookup, schema paths), not just of the AST.

### Validator / ValidatorImpl

- **Purpose**: the resolution context of stage 3 of the
  [query lifecycle](../query-lifecycle.md): validates expressions, requires
  types, resolves function calls, registers parameters.
- **Extends/Implements**: `ValidatorImpl` implements `Validator`.
- **Key collaborators**: `ValidatorImpl#getDef` asks
  `FunTable#getResolvers(name, syntax)` and costs candidate overloads via the
  conversion list each `Resolver#resolve` fills;
  `Validator#canConvert` delegates to `TypeUtil#canConvert`.
- **Lifecycle/scope**: one per `Query#resolve` run (created by
  `Query#createValidator`); stack-based — it tracks the chain of expressions
  currently being validated.
- **Notes/gotchas**: change function overload behavior in the function's
  `Resolver`, not here.

### Evaluator

- **Purpose**: THE evaluation context interface: the current member of every
  hierarchy, plus `#evaluateCurrent`, cached-result access
  (`#getCachedResult(ExpCacheDescriptor)`), non-empty mode, iteration state,
  and native-evaluation hooks. Everything in `mondrian.calc` and
  `mondrian.olap.fun` runs against it.
- **Extends/Implements**: interface; implemented by
  `mondrian.rolap.RolapEvaluator`.
- **Key collaborators**: `SchemaReader` (via `Evaluator#getSchemaReader`),
  `Member` (context), the `Calc` tree that receives it.
- **Lifecycle/scope**: one root evaluator per query execution; child contexts
  are cheap — `Evaluator#savepoint` / `Evaluator#restore(int)` save and restore
  state without copying.
- **Threading**: confined to the executing query thread.
- **Notes/gotchas**: fork PATCH adds `Evaluator#isDirty`, exposing the cell
  reader's dirty state (were any cell values missing/lied about this pass?).

### SchemaReader

- **Purpose**: THE metadata-access interface: members of a level, children of
  a member, default members, compound-name lookup — always filtered through the
  current `Role`.
- **Extends/Implements**: interface; implemented in `rolap`
  (`RolapSchemaReader` and the decorator stack), decorate via
  `DelegatingSchemaReader`.
- **Key collaborators**: `Role` (what is visible), member readers in `rolap`
  (how members are fetched/cached), `NativeEvaluator` via
  `SchemaReader#getNativeSetEvaluator`.
- **Lifecycle/scope**: obtained per connection/cube (`Connection#getSchemaReader`,
  `Cube#getSchemaReader`); stateless views over schema-lifetime caches.
- **Notes/gotchas**: fork PATCH adds `SchemaReader#getLevelMemberByUniqueKey`
  (member by key within a level) and `SchemaReader#hasMemberChildren`
  (existence check without materializing children). Access control is enforced
  by the reader stack, not by call sites.

### Connection / ConnectionBase

- **Purpose**: a native connection to a schema: parse statements, obtain
  `SchemaReader` and `CacheControl`, hold the active `Role`.
- **Extends/Implements**: `ConnectionBase` implements `Connection`; the
  concrete class is `mondrian.rolap.RolapConnection`.
- **Key collaborators**: `DriverManager` (creation), `MondrianServer`
  (registration), `mondrian.parser.MdxParserValidator`
  (`ConnectionBase#createParser` — hardcoded to the JavaCC implementation).
- **Lifecycle/scope**: one per open connection; usually reached through the
  olap4j adapter (`mondrian.olap4j`), which is what the mondrian-olap JRuby gem
  drives.
- **Notes/gotchas**: `ConnectionBase#parseStatement` is where parsing enters,
  passing the schema's `FunTable` (consulted *during* parsing for property and
  reserved-word decisions) and the `strictValidation` flag;
  `Connection#parseExpression` parses schema-defined formulas outside a query.

### MondrianServer

- **Purpose**: an engine instance: connection/statement registry, version
  info, ownership of the `AggregationManager` (and through it the whole
  segment-cache pipeline).
- **Extends/Implements**: abstract class; implemented by
  `mondrian.server.MondrianServerImpl`.
- **Key collaborators**: obtain via `MondrianServer#forConnection` or
  `MondrianServer#forId`; `MondrianServer#getAggregationManager` hands the
  aggregation subsystem to `rolap`.
- **Lifecycle/scope**: JVM-wide "default" instance per registry, plus optional
  per-connection instances; outlives connections.
- **Notes/gotchas**: see [../packages/server.md](../packages/server.md) for the
  statement/execution side.

### MondrianPropertiesBase (and generated `MondrianProperties`)

- **Purpose**: engine configuration. `MondrianProperties` (generated from
  `MondrianProperties.xml` by `mondrian.util.PropertyUtil`) exposes one typed
  field per property; `MondrianPropertiesBase` (hand-written) loads
  `mondrian.properties` from file/classpath plus system properties.
- **Extends/Implements**: extends `org.eigenbase.util.property.TriggerableProperties`.
- **Key collaborators**: read everywhere as
  `MondrianProperties.instance().SomeProperty.get()`; property objects support
  change **triggers** so long-lived components can react without polling.
- **Lifecycle/scope**: process-wide singleton.
- **Notes/gotchas**: to add a property, edit `MondrianProperties.xml` and
  rebuild — never the generated class.

### Util

- **Purpose**: the utility grab-bag used throughout the engine: identifier
  parsing/quoting (`Util#parseIdentifier`, `#quoteMdxIdentifier`), name lookup
  (`Util#lookup`), connect-string parsing (`Util.PropertyList`), assertions,
  collection and string helpers.
- **Extends/Implements**: extends eigenbase `XOMUtil`.
- **Key collaborators**: everything; notably the validator/resolution path and
  `SchemaReader` implementations.
- **Notes/gotchas**: `Util#nullValue` is the engine-wide MDX null sentinel (a
  boxed `FunUtil.DoubleNull`) — test with `Util#isNull`, never with `== null`
  alone.

### CacheControl

- **Purpose**: the programmatic cache-flush API: build cell regions
  (`#createMemberRegion`, `#createCrossjoinRegion`, …) and flush them
  (`CacheControl#flush(CellRegion)`), flush member sets
  (`#flush(MemberSet)`), flush whole schemas (`#flushSchemaCache`,
  `#flushSchema`), plus member-cache edit operations.
- **Extends/Implements**: interface; implemented by
  `mondrian.rolap.CacheControlImpl`.
- **Key collaborators**: obtained from `Connection#getCacheControl`; drives the
  segment index and member caches described in
  [../topics/caching.md](../topics/caching.md).
- **Lifecycle/scope**: cheap facade; the caches it manipulates are
  schema/server-scoped.
- **Notes/gotchas**: flushing cells and flushing members are different
  operations with different consistency caveats — see the caching topic doc.

### Role / RoleImpl

- **Purpose**: access control. A `Role` answers `getAccess` for every schema
  element and gates what a `SchemaReader` exposes; `Role.HierarchyAccess`
  carries per-hierarchy detail (top/bottom level, rollup policy, member
  grants).
- **Extends/Implements**: `RoleImpl` implements `Role`.
- **Key collaborators**: `Access` (the grades), `Schema#lookupRole`,
  `Connection` (the active role), enforcement in `rolap`'s
  `RestrictedMemberReader`.
- **Lifecycle/scope**: `RoleImpl` instances are built from schema `<Role>` XML
  at schema load and shared; combine with `UnionRoleImpl`, adapt with
  `DelegatingRole`.
- **Notes/gotchas**: access decisions happen in the reader stack — code that
  bypasses `SchemaReader` bypasses access control.

### FunTable

- **Purpose**: the registry of MDX functions the validator consults:
  `FunTable#getResolvers(name, syntax)`, plus the two parse-time queries
  `FunTable#isProperty` and `FunTable#isReserved`.
- **Extends/Implements**: interface; implementations layer:
  `BuiltinFunTable` → `GlobalFunTable` (classpath UDFs) →
  `mondrian.rolap.RolapSchema.RolapSchemaFunctionTable` (schema UDFs — the one
  actually consulted).
- **Key collaborators**: `ValidatorImpl#getDef` (lookup),
  `MdxParserImpl#createCall` (property test during parsing), `Resolver`s.
- **Lifecycle/scope**: built once per layer (`FunTableImpl#init` indexes
  resolvers by upper-cased name + `Syntax`); effectively immutable afterwards.
- **Notes/gotchas**: passing a custom fun table to
  `ConnectionBase#parseStatement` changes what *parses*, not just what
  resolves.

### FunDef

- **Purpose**: one resolved MDX function definition: syntax, return/parameter
  categories, `FunDef#createCall` (validate args, build the
  `ResolvedFunCall`), and `FunDef#compileCall` (emit the `Calc` that runs at
  execute time).
- **Extends/Implements**: interface; `mondrian.olap.fun.FunDefBase` is the
  default implementation.
- **Key collaborators**: produced by a `Resolver`; stored in
  `ResolvedFunCall`; consumed by `BetterExpCompiler`.
- **Lifecycle/scope**: shared and stateless — after compilation the `FunDef`
  is inert; all evaluation state lives in the returned `Calc`.
- **Notes/gotchas**: `FunDef#unparse` controls how the call renders when a
  query is unparsed.

---

**OlapElement / OlapElementBase** — the root contract of every catalog object
(dimension, hierarchy, level, member, named set): name, unique name, caption,
description, visibility, and child lookup (`OlapElement#lookupChild`).
`OlapElementBase` is the abstract base with caption/visibility plumbing.
Everything in the metadata model below extends this pair.

**Schema** — a catalog: cubes, shared dimensions, roles, parameters, and the
schema-level `FunTable` (`Schema#getFunTable`). Key methods: `Schema#lookupCube`,
`#lookupRole`, `#getCubes`, `#getSharedDimensions`. Implemented by
`mondrian.rolap.RolapSchema`, which is expensive to build and shared through
the schema pool (see [../topics/schema-loading.md](../topics/schema-loading.md)).

**Cube / CubeBase** — a cube: its dimensions, its `SchemaReader`
(`Cube#getSchemaReader(Role)`), and lookup of levels/members within the cube.
`CubeBase` supplies the dimension array and lookup helpers. Concrete:
`mondrian.rolap.RolapCube` (regular and virtual).

**Dimension / DimensionBase** — a dimension of a cube: its hierarchies and its
`DimensionType` (standard/time/measures; `Dimension#isMeasures`).
`DimensionBase` is the skeleton. Concrete: `RolapDimension` /
`RolapCubeDimension`.

**Hierarchy / HierarchyBase** — a set of members organized into levels; owns
`hasAll`/all-member, the default member (`Hierarchy#getDefaultMember`), and
level array. `HierarchyBase` holds the shared naming/level plumbing. Concrete:
`RolapHierarchy` / `RolapCubeHierarchy`.

**Level / LevelBase** — one level of a hierarchy: depth, `LevelType` (regular
or time role), member/property formatters, approximate cardinality
(`Level#getApproxRowCount`). `LevelBase` is the skeleton. Concrete:
`RolapLevel` / `RolapCubeLevel`.

**Member / MemberBase** — a point on a hierarchy: parent/level navigation,
`Member#isCalculated`, property access (`Member#getPropertyValue`), ordinal
and comparison support, and the `Member.MemberType` enum (REGULAR, ALL,
MEASURE, FORMULA, NULL, UNKNOWN). `MemberBase` implements the common
navigation. Concrete: `RolapMember` and friends; calculated members are
`RolapCalculatedMember`.

**NamedSet / SetBase** — a named set of members or tuples, defined at schema
level or by a WITH SET clause; `NamedSet#getExp` returns the defining
expression. `SetBase` is the standard implementation. Query-scoped named sets
evaluate once per query via `Evaluator#getNamedSetEvaluator` (see
`mondrian.mdx.NamedSetExpr`).

**Result / ResultBase** — a finished query result: `Result#getAxes`,
`Result#getCell(int[])`, the slicer axis, and `Result#print` for debugging.
`ResultBase` holds the axis array and iteration skeleton. Concrete:
`mondrian.rolap.RolapResult`.

**Axis / Position** — one result axis is a list of `Position`s; a `Position`
*is* a `List<Member>` (one tuple on the axis). Concrete: `RolapAxis` and its
position lists, which are `TupleList`-backed (see
[../packages/calc.md](../packages/calc.md)).

**Cell** — one cell of a `Result`: raw value, formatted value, cell
properties, error state, and drill-through (`Cell#canDrillThrough`,
`Cell#getDrillThroughSQL`). Concrete: `mondrian.rolap.RolapCell`.

**IdBatchResolver** — pre-pass of `Query#resolve` that collects the query's
`Id`s (grouped by parent) and resolves them against the `SchemaReader` in
batches, so member lookup that requires SQL is done in few statements instead
of per identifier. Returns a map the validator then substitutes while walking
the tree.

**Parameter / ParameterImpl** — a query parameter created by the
`Parameter(name, type, default[, description])` MDX function and referenced by
`ParamRef(name)`; scopes range from statement to schema
(`Parameter.Scope`). `ParameterImpl` also implements the compilable hook that
turns a parameter into a slot the evaluator can read/assign
(`Evaluator#getParameterValue`). Schema-defined parameters are
`mondrian.rolap.RolapSchemaParameter`.

**EnumeratedValues** — pre-Java-5 enum helper: a set of named/ordinal
`EnumeratedValues.Value` constants with lookup by name or ordinal. Still backs
`Category` and `Property`; don't use it for new code — use real enums.

**Category** — integer codes for expression categories (`Category.Member`,
`.Set`, `.Numeric`, `.String`, `.Logical`, `.Tuple`, `.DateTime`, …, plus the
`Category.Constant`/`Expression` modifier bits and `Category.Mask`). These are
the third piece of every function signature flag string (see
[../packages/olap-fun.md](../packages/olap-fun.md)); the class-based
counterpart is `mondrian.olap.type`.

**Syntax** — enum describing how a call is written: `Function`, `Property`,
`Method`, `Infix`, `Prefix`, `Postfix`, `Braces`, `Parentheses`, `Case`,
`Cast`, `Internal`, `Empty`. Part of the function-lookup key
(`FunTable#getResolvers(name, syntax)`); `Syntax#unparse` renders each shape.

### Tier 3: remaining `mondrian.olap` types

| Type | Kind | Summary |
|---|---|---|
| `Access` | enum | Access-right grades used by `Role`: NONE, CUSTOM, RESTRICTED, ALL, ALL_DIMENSIONS. |
| `Aggregator` | interface | Aggregation-operator contract ("sum", "count", …); implemented by `mondrian.rolap.RolapAggregator`. |
| `Annotated` | interface | A schema element that carries a map of `Annotation`s. |
| `Annotation` | interface | User-defined name/value metadata on a schema element. |
| `AxisOrdinal` | interface | Axis codes (SLICER, COLUMNS, ROWS, …); standard values in `AxisOrdinal.StandardAxisOrdinal`. |
| `CellFormatter` | interface | Deprecated alias of `mondrian.spi.CellFormatter`. |
| `CellProperty` | class | One CELL PROPERTIES clause entry in a `Query` (a `QueryPart`). |
| `CubeAccess` | class | Legacy cube/slicer permission checker; largely superseded by `Role`. |
| `DelegatingRole` | class | Decorator base implementing `Role` by delegating to an underlying role. |
| `DelegatingSchemaReader` | class | Decorator base implementing `SchemaReader` by delegation; the reader stack (caching, access control) is built from these. |
| `DimensionType` | enum | Kinds of dimension: StandardDimension, TimeDimension, MeasuresDimension. (Same simple name as `mondrian.olap.type.DimensionType` — different thing.) |
| `DrillThrough` | class | DRILLTHROUGH statement node wrapping a `Query`. |
| `DriverManager` | class | `DriverManager#getConnection(connectString, catalogLocator)` builds a `mondrian.rolap.RolapConnection` from a `mondrian:` connect string. |
| `Explain` | class | EXPLAIN PLAN FOR statement node wrapping the explained statement. |
| `ExpCacheDescriptor` | class | Key for the expression-result cache (`Evaluator#getCachedResult`); used by the `Cache()` function and `RankFunDef`. |
| `FormatAwareFunDef` | interface | (fork PATCH) Lets a `FunDef`/UDF steer which argument's format string a calculated member inherits (consumed by `Formula#getFormatExp`). |
| `FunCall` | interface | Function-application expression contract; concrete kinds are `mondrian.mdx.UnresolvedFunCall` / `ResolvedFunCall`. |
| `IdentifierVisitor` | class | `MdxVisitorImpl` that collects the `Id` nodes of an expression tree (identifier gathering for batch resolution). |
| `InvalidArgumentException` | class | `MondrianException`: an argument is invalid. |
| `InvalidHierarchyException` | class | `MondrianException`: a hierarchy is invalid because it has no members. |
| `LevelType` | enum | Kinds of level: Regular, Null, and the time roles (TimeYears, TimeQuarters, TimeMonths, …). (Distinct from `mondrian.olap.type.LevelType`.) |
| `Literal` | class | Constant AST node (numeric, string, symbol, null); one of the few node kinds the parser emits directly. |
| `MatchType` | enum | Match modes for member lookup by unique name (EXACT, BEFORE, AFTER, …); used by `SchemaReader#lookupMemberChildByName` and friends. |
| `MemberFormatter` | interface | Deprecated alias of `mondrian.spi.MemberFormatter`. |
| `MemberProperty` | class | Property-value pair inside a WITH MEMBER clause (member property or solve order). |
| `MemoryLimitExceededException` | class | Limit family: memory ceiling exceeded during query execution. |
| `MondrianException` | class | Root of engine runtime exceptions; most concrete errors are generated into `mondrian.resource.MondrianResource` — grep message text there. |
| `NameResolver` | class | Resolves a list of identifier segments to an `OlapElement` with longest-match namespace semantics; used by olap4j-side lookup. |
| `Namer` | interface | Hook for retrieving localized attribute names. |
| `NativeEvaluationUnsupportedException` | class | Thrown when native evaluation was enabled but unsupported and `AlertNativeEvaluationUnsupported` is ERROR. |
| `NativeEvaluator` | interface | Handle for a set operation evaluated as SQL instead of in memory; obtained via `SchemaReader#getNativeSetEvaluator` (see [../topics/native-evaluation.md](../topics/native-evaluation.md)). |
| `Property` | class | Definition of a member/cell property (an `EnumeratedValues.BasicValue`) — standard MDX properties plus schema-defined ones; not engine configuration. |
| `PropertyFormatter` | interface | Deprecated alias of `mondrian.spi.PropertyFormatter`. |
| `QueryCanceledException` | class | Limit family: query canceled by the user (via `Execution`/`CacheControl`). |
| `QueryPart` | class | Base class of every AST node; implements `Walkable`. |
| `QueryTimeoutException` | class | Limit family: query exceeded its allowed time and was auto-canceled. |
| `QueryTiming` | class | Accumulates per-phase timing during execution; surfaced through profiling/monitoring. |
| `ResourceLimitExceededException` | class | Limit family: a resource limit (result/iteration size) was exceeded. |
| `ResultLimitExceededException` | class | Abstract base of the limit-exceeded exception family. |
| `ResultStyleException` | class | Compiler could not implement an expression in any requested `ResultStyle`. |
| `Scanner` | class | Hand-written lexer for the legacy CUP parser — dead code apart from two comment-delimiter lookups (see [../packages/mdx-parser.md](../packages/mdx-parser.md)). |
| `SolveOrderMode` | enum | Strategies for calculated-member solve-order precedence (`MondrianProperties#SolveOrderMode`). |
| `StringScanner` | class | String-input `Scanner` subclass; dead CUP-path code. |
| `UnionRoleImpl` | class | `Role` that unions several roles (superset of their privileges). |
| `Walkable` | interface | Marker for objects `Walker` can tree-walk. |
| `Walker` | class | Prefix-order tree iterator over `Walkable`s (pre-visitor era; new code uses `mondrian.mdx.MdxVisitor`). |

---

## Package `mondrian.olap.type`

The MDX type system. The `Validator` computes a `Type` for every `Exp`; the
compiler uses it to pick `Calc` flavors, and `Resolver`s use it (through
`TypeUtil`) to cost implicit conversions.

### Type

- **Purpose**: the type of an MDX expression: `Type#usesDimension`,
  `Type#usesHierarchy`, `Type#getDimension`/`#getHierarchy`/`#getLevel`
  (the schema element the type is pinned to, if any), and
  `Type#computeCommonType` for unifying branches (e.g. of an `Iif`).
- **Extends/Implements**: root interface of the package.
- **Key collaborators**: `Validator` (assigns), `TypeUtil` (manipulates),
  `ExpCompiler` (dispatches on it), `Category` (the ordinal-code counterpart).
- **Lifecycle/scope**: immutable value objects created during validation.
- **Notes/gotchas**: a metadata type "pinned" to an element (e.g.
  `MemberType.forMember`) is more specific than the unpinned form — conversion
  logic cares about the difference.

### TypeUtil

- **Purpose**: the static workhorse: `TypeUtil#canConvert` (the implicit
  conversion matrix + cost recording used in overload resolution),
  `#computeCommonType`, category↔type mapping (`#typeToCategory`), and
  hierarchy/dimension extraction helpers (`#toMemberType`,
  `#typeToHierarchy`, …).
- **Extends/Implements**: static utility class.
- **Key collaborators**: `ValidatorImpl#canConvert`, every `Resolver`.
- **Notes/gotchas**: changing conversion rules here changes which function
  overloads *any* query resolves to — treat as engine-wide semantics.

---

**MemberType** — the type of a member-valued expression, optionally pinned to
a concrete hierarchy/level/member (`MemberType#forMember`, `#forLevel`, …).
The scalar value of a member expression is what `Evaluator#evaluateCurrent`
produces, so `MemberType#getValueType` matters for implicit member→scalar
conversion.

**SetType / TupleType** — composite types: `TupleType` is an ordered list of
element types (one per tuple slot); `SetType` wraps an element type (member or
tuple) for set expressions. `SetType#getArity` / `TupleType#elementTypes`
drive crossjoin arity math and axis tuple width.

**ScalarType / NumericType** — `ScalarType` is the base for value types and
the "any scalar" type itself; `NumericType` marks numeric expressions
(`DecimalType` refines it with fixed precision/scale for level key types).

### Tier 3: remaining `mondrian.olap.type` types

| Type | Kind | Summary |
|---|---|---|
| `BooleanType` | class | Type of a boolean expression. |
| `CubeType` | class | Type of an expression representing a cube. |
| `DateTimeType` | class | Type of a date/time/timestamp expression. |
| `DecimalType` | class | `NumericType` with fixed precision and scale. |
| `DimensionType` | class | Type of an expression representing a dimension (not the `mondrian.olap.DimensionType` enum). |
| `EmptyType` | class | Type of an empty expression (e.g. an omitted argument). |
| `HierarchyType` | class | Type of an expression representing a hierarchy. |
| `LevelType` | class | Type of an expression representing a level (not the `mondrian.olap.LevelType` enum). |
| `NullType` | class | Type of the null literal/expression. |
| `StringType` | class | Type of a string expression. |
| `SymbolType` | class | Type of a symbol keyword argument (e.g. `BASC`, `EXCLUDEEMPTY`). |

---

## Package `mondrian.olap.fun`

The MDX function library and its resolver machinery. How registration,
signature flag strings, and compilation work — plus the recipe for adding a
function — is in [../packages/olap-fun.md](../packages/olap-fun.md).

### Resolver

- **Purpose**: converts a function name + `Syntax` + concrete argument
  expressions into a `FunDef`, recording any implicit conversions so the
  validator can cost competing overloads.
- **Extends/Implements**: interface; skeletons below (`ResolverBase`,
  `SimpleResolver`, `MultiResolver`).
- **Key collaborators**: registered in a `FunTable`; invoked from
  `ValidatorImpl#getDef`; produces `FunDef`s.
- **Lifecycle/scope**: stateless singletons, usually static fields on the
  `FunDef` class they create.
- **Notes/gotchas**: `Resolver#getReservedWords` is how symbol arguments
  (`ASC`, `RECURSIVE`, …) become reserved during validation of that call.

### BuiltinFunTable

- **Purpose**: the singleton registry of every built-in MDX function and
  operator; `BuiltinFunTable#defineFunctions` (~2000 lines) registers them via
  three idioms — dedicated `FunDef` class + static resolver, anonymous inline
  `FunDefBase`, anonymous inline `MultiResolver`.
- **Extends/Implements**: extends `FunTableImpl`.
- **Key collaborators**: wrapped by `GlobalFunTable` and the per-schema
  function table; VBA/Excel functions are manufactured by
  `JavaFunDef#scan(Vba.class)` / `#scan(Excel.class)` rather than written as
  `FunDef`s.
- **Lifecycle/scope**: `BuiltinFunTable#instance()` — process-wide, built once.
- **Notes/gotchas**: (fork PATCH) names listed in the
  `mondrian.olap.fun.skipJavaFunDefs` system property are skipped during
  Vba/Excel registration (`BuiltinFunTable#parseSkipFunctions`, read from
  `System` directly, deliberately not via `MondrianProperties`), so a schema
  UDF can shadow a Vba/Excel name without an ambiguous-match error. Scalar
  operators (`+`, `AND`, comparisons, …) have no class of their own — grep this
  class.

### FunDefBase

- **Purpose**: the default `FunDef` implementation and superclass of nearly
  every function: decodes signature flag strings in its constructor,
  `#createCall` validates arguments and builds the `ResolvedFunCall`,
  `#getResultType` infers the result type from the first argument (via
  `#castType`; override when wrong), `#compileCall` throws — every concrete
  function must override it.
- **Extends/Implements**: extends `FunUtil`, implements `FunDef`.
- **Key collaborators**: `Resolver`s construct these (often via the
  dummy-`FunDef` reflection protocol); `ExpCompiler` calls `#compileCall`.
- **Lifecycle/scope**: stateless; one instance per resolved signature.
- **Notes/gotchas**: because it extends `FunUtil`, functions call `FunUtil`'s
  static helpers unqualified — grep both classes when tracing a helper.

### FunUtil

- **Purpose**: the package's static toolbox, used from almost every compiled
  `Calc`: `#newEvalException`; the aggregate kernels (`#sum`, `#avg`, `#min`,
  `#max`, `#var`, `#stdev`, `#percentile`, `#quartile`, `#correlation`,
  `#covariance`) and `#evaluateSet`; member navigation (`#ancestor`,
  `#cousin`, `#periodsToDate`, `#memberRange`, `#compareHierarchically`);
  tuple utilities; `StrToMember`/`StrToSet` parsing (`#parseMember`,
  `#parseTupleList`); signature-flag decoding; `#createDummyFunDef`.
- **Extends/Implements**: extends `Util`.
- **Key collaborators**: every `FunDef` (via the `FunDefBase` inheritance
  trick), `sort.Sorter` for ordering.
- **Notes/gotchas**: (fork PATCH) `#min`/`#max` dispatch on
  `instanceof Date` at runtime so `Min`/`Max` work over DateTime expressions.
  Sorting moved out of this class upstream — it lives in the `sort`
  subpackage.

---

**FunTableImpl / GlobalFunTable / CustomizedFunctionTable** — the `FunTable`
implementations. `FunTableImpl` is the common base: `#init` runs
`defineFunctions` and indexes `Resolver`s by upper-cased name + `Syntax`.
`GlobalFunTable` layers classpath-wide UDFs (discovered via
`META-INF/services/mondrian.spi.UserDefinedFunction`,
`GlobalFunTable#lookupUdfImplClasses`) on top of `BuiltinFunTable`.
`CustomizedFunctionTable` exposes a chosen subset of built-ins — used by the
`NativizeSet` machinery, not by normal queries.

**MultiResolver / ReflectiveMultiResolver / SimpleResolver / ResolverBase** —
the resolver skeletons. `SimpleResolver` wraps a single fixed `FunDef` (what
`builder.define(FunDef)` creates). `MultiResolver` tries each signature flag
string in turn and delegates to an abstract `#createFunDef`;
`ReflectiveMultiResolver` implements that by reflectively invoking the target
class's `(FunDef dummyFunDef)` constructor — the standard pattern for
dedicated `FunDef` classes. `ResolverBase` is the skeleton for hand-written
resolvers with irregular matching (`CastFunDef`, the two `IsEmpty` syntaxes).

**UdfResolver** — adapts a `mondrian.spi.UserDefinedFunction` into the
resolver world. Its inner `UdfFunDef` maps declared `Type`s to categories and
pre-compiles each argument (scalar, plus list/iterable for set args) into
`Argument` wrappers, instantiating a fresh UDF object per call site because
UDFs may keep state. (fork PATCH) Returns a UDF-produced `TupleList` as-is
(MONDRIAN-2661) and forwards `FormatAwareFunDef` from the wrapped UDF.

**JavaFunDef** — an MDX function implemented by a public static Java method;
`JavaFunDef#scan` manufactures one per method of `Vba`/`Excel`, with
`JavaFunDef.FunctionName`/`.Signature`/`.Description` annotations overriding
defaults, and `#compileCall` compiling each argument to the `Calc` matching
the Java parameter type. (fork PATCH) Treats the MDX null sentinel as Java
null and coerces `BigDecimal`→`double` for `double` parameters.

**AbstractAggregateFunDef** — base of the aggregate functions: evaluates the
set argument with non-empty mode off (`FunUtil#evaluateSet` semantics) and
handles the unrelated-dimensions policy via `#processUnrelatedDimensions`.
Subclasses: `AggregateFunDef`, `SumFunDef`, `AvgFunDef`, `CountFunDef`,
`MinMaxFunDef`, `MedianFunDef`, `PercentileFunDef`, the Stdev/Var family,
`NthQuartileFunDef`.

**AggregateFunDef** — `Aggregate(<Set>[, <Numeric Expression>])`: rolls a set
up using the current measure's own aggregator; rejects distinct-count rollup
in-memory. (fork PATCH) Can route single-dimension stored-member rollups
through the distinct-count SQL path when in-memory rollup is not possible —
see [../topics/fork-changes.md](../topics/fork-changes.md).

**CrossJoinFunDef** — `CrossJoin`/`*`/`NonEmptyCrossJoin` base: compiles to
list/iterable calcs whose evaluation first offers the call to native SQL
evaluation (`SchemaReader#getNativeSetEvaluator`), and otherwise applies
in-memory product with non-empty optimization (`nonEmptyList` pruning).
One of the hottest paths in the engine.

**FilterFunDef** — `Filter(<Set>, <Condition>)`: native-evaluation hook first,
then in-memory iteration evaluating the boolean calc per tuple with
savepoint/restore. Result style (list vs iterable, mutable vs not) matters
here more than in most functions.

**TopBottomCountFunDef** — `TopCount`/`BottomCount`: native hook first;
in-memory path uses the partial sorter (`sort.Sorter#stablePartialSort`)
rather than a full sort.

**SetFunDef** — the `{…}` braces operator: concatenates/flattens its
arguments into one set, with `IterCalc`/`ListCalc` variants and arity checks;
also home of the internal calcs that wrap single members/tuples as sets.

**MinMaxFunDef** — `Min`/`Max`. Substantially rewritten in this fork
(fork PATCH): supports DateTime as well as numeric expressions (calculated
members are statically typed Numeric, so date support relies on runtime type
detection in `#extremeValue`; the two-arg DateTime variant compiles an
`AbstractDateTimeCalc`), and implements `FormatAwareFunDef` so the value
argument's format string is inherited.

### Tier 3: `mondrian.olap.fun` function classes and support

**Set construction and transformation:**

| Class | Summary |
|---|---|
| `TupleFunDef` | The `(…)` tuple constructor. |
| `RangeFunDef` | The `<Member> : <Member>` range operator (members between a pair, inclusive). |
| `NonEmptyCrossJoinFunDef` | `NonEmptyCrossJoin` — `CrossJoinFunDef` subclass that always crossjoins non-empty. |
| `UnionFunDef` | `Union` (and infix `+` on sets), with ALL/DISTINCT handling. |
| `IntersectFunDef` | `Intersect`, with ALL handling. |
| `ExceptFunDef` | `Except` — set difference. |
| `DistinctFunDef` | `Distinct` — removes duplicate tuples. |
| `OrderFunDef` | `Order` — sorts by key expressions/flags via `sort.Sorter`; supports multiple sort keys. |
| `UnorderFunDef` | `Unorder` — declares order irrelevant (currently a pass-through). |
| `HierarchizeFunDef` | `Hierarchize` — hierarchical (pre/post) ordering. |
| `SubsetFunDef` | `Subset(<Set>, <Start>[, <Count>])`. |
| `HeadTailFunDef` | `Head` and `Tail`. |
| `TopBottomPercentSumFunDef` | `TopPercent`/`BottomPercent`/`TopSum`/`BottomSum`. |
| `DescendantsFunDef` | `Descendants`, with the SELF/BEFORE/AFTER/LEAVES flag algebra. |
| `LevelMembersFunDef` | `<Level>.Members`. |
| `AddCalculatedMembersFunDef` | `AddCalculatedMembers` — adds visible calculated members to a set. |
| `ExtractFunDef` | `Extract(<Set>, <Hierarchy>…)` — projects tuple slots. |
| `GenerateFunDef` | `Generate` — set-mapped iteration (set or string-concat form). |
| `ExistsFunDef` | `Exists(<Set>, <Set>)` — tuples that exist with one another. |
| `ExistingFunDef` | `Existing <Set>` — limits a set to the current context. |
| `DrilldownLevelFunDef` | `DrilldownLevel`. |
| `DrilldownLevelTopBottomFunDef` | `DrilldownLevelTop`/`DrilldownLevelBottom`; has the native-evaluation hook. |
| `DrilldownMemberFunDef` | `DrilldownMember`, with the RECURSIVE flag. |
| `ToggleDrillStateFunDef` | `ToggleDrillState`. |
| `StrToSetFunDef` | `StrToSet` — parses MDX set syntax at evaluate time. |
| `StrToTupleFunDef` | `StrToTuple`. |
| `SetToStrFunDef` | `SetToStr`. |
| `TupleToStrFunDef` | `TupleToStr`. |
| `SetItemFunDef` | `<Set>.Item(<Index>)` / `.Item(<String>…)`. |
| `TupleItemFunDef` | `<Tuple>.Item(<Index>)`. |
| `NativizeSetFunDef` | `NativizeSet` — forces native SQL evaluation of large sets by internally rewriting MDX (see [../topics/native-evaluation.md](../topics/native-evaluation.md)). |
| `VisualTotalsFunDef` | `VisualTotals` — substitutes visually-totaled parent members. |
| `CacheFunDef` | `Cache(<Exp>)` — evaluates its argument once per context via `ExpCacheDescriptor`. |
| `AsFunDef` | The inline `AS` set-alias operator. |

**Member and time navigation:**

| Class | Summary |
|---|---|
| `AncestorFunDef` | `Ancestor(<Member>, <Level>\|<Distance>)`. |
| `AncestorsFunDef` | `Ancestors(<Member>, <Level>\|<Distance>)`. |
| `LeadLagFunDef` | `<Member>.Lead(n)` / `.Lag(n)`. |
| `ParallelPeriodFunDef` | `ParallelPeriod`. |
| `OpeningClosingPeriodFunDef` | `OpeningPeriod` / `ClosingPeriod`. |
| `PeriodsToDateFunDef` | `PeriodsToDate`. |
| `XtdFunDef` | `Ytd`/`Qtd`/`Mtd`/`Wtd` — four resolvers over one time-level-typed `PeriodsToDate` core. |
| `LastPeriodsFunDef` | `LastPeriods(n[, <Member>])`. |
| `HierarchyCurrentMemberFunDef` | `<Hierarchy>.CurrentMember`. |
| `MemberOrderKeyFunDef` | `<Member>.OrderKey` — wraps the member's order key (see `sort.OrderKey`). |
| `NamedSetCurrentFunDef` | `<Named Set>.Current`. |
| `NamedSetCurrentOrdinalFunDef` | `<Named Set>.CurrentOrdinal`. |
| `StrToMemberFunDef` | `StrToMember` — parses a member unique name at evaluate time. |
| `ValidMeasureFunDef` | `ValidMeasure` — forces a measure valid in a virtual cube by clearing unrelated-dimension context. |

**Metadata property functions:**

| Class | Summary |
|---|---|
| `HierarchyDimensionFunDef` | `<Hierarchy>.Dimension`. |
| `DimensionDimensionFunDef` | `<Dimension>.Dimension` (identity). |
| `LevelDimensionFunDef` | `<Level>.Dimension`. |
| `MemberDimensionFunDef` | `<Member>.Dimension`. |
| `DimensionsNumericFunDef` | `Dimensions(<Numeric Expression>)` — dimension by ordinal. |
| `DimensionsStringFunDef` | `Dimensions(<String Expression>)` — dimension by name. |
| `LevelHierarchyFunDef` | `<Level>.Hierarchy`. |
| `MemberHierarchyFunDef` | `<Member>.Hierarchy`. |
| `MemberLevelFunDef` | `<Member>.Level`. |

**Aggregation, statistics and ranking:**

| Class | Summary |
|---|---|
| `SumFunDef` | `Sum(<Set>[, <Numeric Expression>])`. |
| `AvgFunDef` | `Avg`. |
| `CountFunDef` | `Count`, with EXCLUDEEMPTY/INCLUDEEMPTY. |
| `MedianFunDef` | `Median`. |
| `PercentileFunDef` | `Percentile`. |
| `StdevFunDef` | `Stdev` (alias `Stddev`) — sample standard deviation. |
| `StdevPFunDef` | `StdevP`/`StddevP` — population standard deviation. |
| `VarFunDef` | `Var` (alias `Variance`) — sample variance. |
| `VarPFunDef` | `VarP`/`VarianceP` — population variance. |
| `CovarianceFunDef` | `Covariance`/`CovarianceN` (plain `FunDefBase`, not an `AbstractAggregateFunDef`). |
| `CorrelationFunDef` | `Correlation`. |
| `LinReg` | Base for the five linear-regression functions (`LinRegPoint`, `LinRegSlope`, `LinRegIntercept`, `LinRegVariance`, `LinRegR2`). |
| `RankFunDef` | `Rank(<Tuple>, <Set>[, <Exp>])`; caches the sorted set per context via `ExpCacheDescriptor`. |

**Conditionals, tests and scalar helpers:**

| Class | Summary |
|---|---|
| `IifFunDef` | `Iif` — typed overloads unify both branches' types. |
| `CaseTestFunDef` | Searched `CASE WHEN … THEN … END`. |
| `CaseMatchFunDef` | Matched `CASE <Exp> WHEN … END`. |
| `CoalesceEmptyFunDef` | `CoalesceEmpty` — first non-empty value. |
| `IsEmptyFunDef` | `IsEmpty(<Exp>)` and postfix `IS EMPTY`. |
| `IsFunDef` | Infix `IS` — object identity (member/tuple/hierarchy…). |
| `IsNullFunDef` | Postfix `IS NULL` on members. |
| `CastFunDef` | `Cast(<Exp> AS <Type>)`; hand-written `ResolverBase` because the target type is a symbol. |
| `FormatFunDef` | `Format(<Exp>, <Format String>)` via `mondrian.util.Format`. |
| `PropertiesFunDef` | `<Member>.Properties(<String>[, TYPED])`. |

**Pseudo-functions and support classes:**

| Class | Summary |
|---|---|
| `ParameterFunDef` | Pseudo-function describing `Parameter`/`ParamRef` calls; the validator converts them into `Parameter` objects. |
| `ParenthesesFunDef` | The `( … )` grouping operator as a function. |
| `ValueFunDef` | Internal pseudo-function that evaluates a member/tuple to its cell value. |
| `FunInfo` | Function metadata (name, description, signatures) for introspection. |
| `MemberExtractingVisitor` | `MdxVisitorImpl` collecting non-measure base members from an expression (used by `ValidMeasure` and native analysis). |
| `ResolvedFunCallFinder` | `MdxVisitorImpl` that detects whether a given `ResolvedFunCall` occurs in an expression. |
| `MondrianEvaluationException` | Thrown while evaluating a cell expression — what `FunUtil#newEvalException` produces. |

### Subpackage `mondrian.olap.fun.sort`

**Sorter** — the static sorting toolkit used by `Order`, `TopCount`,
`Hierarchize`, and member-child ordering: `Sorter#sortMembers` /
`#sortTuples`, partial (top-n) sorting via `#stablePartialSort`,
`#hierarchizeMemberList` / `#hierarchizeTupleList`, and the `Sorter.Flag`
direction enum. (fork PATCH) Guava replaced with Caffeine/JDK equivalents, and
two added entry points: `#sortSiblings` (sorting member children) and
`#sortParentChildMembers` (children of several parents).

| Class | Summary |
|---|---|
| `TupleComparator` | `Comparator<List<Member>>` base for tuple comparators; nested `TupleExpComparator` adds evaluator + calc context. |
| `TupleExpMemoComparator` | Tuple comparator that memoizes sort-key evaluation per tuple. (fork PATCH) Guava cache → Caffeine; Caffeine doesn't wrap exceptions, so the cell-batching sentinel propagates unwrapped. |
| `MemberComparator` | `Comparator<Member>` with break-hierarchy and within-hierarchy variants for `Order`. |
| `HierarchizeComparator` | `Comparator<Member>` arranging members in prefix/postfix hierarchical order. |
| `HierarchizeTupleComparator` | `TupleComparator` arranging tuples in hierarchical order. |
| `HierarchicalTupleComparator` | `TupleExpMemoComparator` comparing tuples hierarchically by an expression value. |
| `HierarchicalTupleKeyComparator` | `TupleExpMemoComparator` comparing member `OrderKey`s hierarchically. |
| `OrderKey` | Comparable wrapper around a member's order key. (fork PATCH) Null order keys compare explicitly and collate before non-null keys. |
| `Quicksorter` | Quicksort functional used by `Sorter#partialSort` for top-n ordering. |
| `SortKeySpec` | One `Order` sort key: a key `Calc` plus its direction `Sorter.Flag`. |

### Subpackage `mondrian.olap.fun.vba`

| Class | Summary |
|---|---|
| `Vba` | Public-static-method implementations of the VBA function library (`DateAdd`, `Format`, `InStr`, …), scanned by `JavaFunDef#scan`. (fork PATCH) Date functions accept `Object` and coerce via `Vba#castToDate` — calculated members are statically typed Numeric, so a `Date` arrives as `Object` — plus numeric-edge-case fixes (MONDRIAN-2730 among them). |
| `Excel` | Public-static-method implementations of the Excel worksheet function library (`Power`, `Sqrt`, `Pi`, trigonometry, …), scanned the same way. |

### Subpackage `mondrian.olap.fun.extra`

Non-standard extension functions, registered by default:

| Class | Summary |
|---|---|
| `CachedExistsFunDef` | `CachedExists` — an `Exists` variant that caches subtotal tuple projections (used for subtotal/Top-N style calculations). |
| `CalculatedChildFunDef` | `<Member>.CalculatedChild(<String>)`. |
| `NthQuartileFunDef` | `FirstQ`/`ThirdQ` — first/third quartile aggregate extensions. |

---

## Package `mondrian.mdx`

Typed AST node classes for parsed MDX expressions. The parser emits only
`Id`, `UnresolvedFunCall`, `Literal` and the statement skeleton; the bound
leaves appear during resolution. Full narrative:
[../packages/mdx-parser.md](../packages/mdx-parser.md).

### UnresolvedFunCall / ResolvedFunCall

- **Purpose**: the two function-application nodes. `UnresolvedFunCall` is
  every operator/function/property access *as parsed* — a name + `Syntax` +
  args. `ResolvedFunCall` is its validated replacement: args + return `Type` +
  the chosen `FunDef`.
- **Extends/Implements**: both extend `ExpBase`, implement `FunCall`.
- **Key collaborators**: `UnresolvedFunCall#accept(Validator)` performs
  function resolution (via `FunUtil` and the `FunTable`);
  `ResolvedFunCall#accept(ExpCompiler)` delegates to `FunDef#compileCall`.
- **Lifecycle/scope**: after `Query#resolve` no `UnresolvedFunCall` remains in
  the tree.
- **Notes/gotchas**: `UnresolvedFunCall#accept(ExpCompiler)` throws — an
  unresolved call must never reach compilation. (fork PATCH)
  `ResolvedFunCall` carries a `timingName` field used by profiling to label
  calculated-member evaluation with the member's unique name.

---

**MemberExpr / LevelExpr / HierarchyExpr / DimensionExpr / NamedSetExpr** —
the bound leaves: direct usage of a schema object as an expression, created
during resolution (`Util#createExpr`), never by the parser. `NamedSetExpr` is
also what a WITH SET reference resolves to; it compiles to an evaluation
through `Evaluator#getNamedSetEvaluator`, so a named set is computed once per
query and supports `.Current`/`.CurrentOrdinal`.

**MdxVisitor / MdxVisitorImpl** — the visitor over the whole parse tree
(visit overloads for `Query`, `QueryAxis`, `Formula`, both fun-call kinds,
`Id`, the bound leaves, `ParameterExpr`, `Literal`). `MdxVisitorImpl` returns
null everywhere and auto-descends; call `#turnOffVisitChildren` inside a
`visit` to prune (the flag resets after each node). Used heavily by the
native-evaluation analyzers and `Query` internals.

### Tier 3: remaining `mondrian.mdx` types

| Type | Kind | Summary |
|---|---|---|
| `ParameterExpr` | class | Usage of a `Parameter` as an expression; unparses as `Parameter(…)` on first occurrence, `ParamRef(…)` afterwards. |
| `QueryPrintWriter` | class | `PrintWriter` used when unparsing a query; tracks which `Parameter`s have already printed (enables the `Parameter`/`ParamRef` split above). |

---

## Package `mondrian.parser`

The MDX parser entry points. The actual parser classes (`MdxParserImpl`,
`MdxParserImplTokenManager`, `ParseException`, `Token`, …) are **generated**
by JavaCC from `parser/MdxParser.jj` into `src/generated/java/` at build time
and are not cataloged individually — see
[../packages/mdx-parser.md](../packages/mdx-parser.md) for the grammar,
generation wiring, and the legacy CUP remnants.

**MdxParserValidator / JavaccParserValidatorImpl** — `MdxParserValidator` is
the parsing interface: `#parseInternal` (whole statement) and
`#parseExpression` (bare expression), plus the nested `QueryPartFactory`
(`#makeQuery`, `#makeDrillThrough`, `#makeExplain`) whose default
implementation is `Parser.FactoryImpl` (defined inside `olap/Parser.cup`).
`JavaccParserValidatorImpl` is the live implementation: it constructs a fresh
`MdxParserImpl` per parse (no threading concerns), invokes the
`statementEof()`/`expressionEof()` production, and rewrites JavaCC's
`ParseException` into a `MondrianException` in the classic
"Syntax error at line L, column C" format (`#convertException`). Instantiated
by `ConnectionBase#createParser`.

### Tier 3: remaining `mondrian.parser` types

| Type | Kind | Summary |
|---|---|---|
| `MdxParserValidatorImpl` | class | CUP-backed twin of `JavaccParserValidatorImpl`; unreachable dead code (`ConnectionBase#createParser` is hardcoded to the JavaCC path). |
