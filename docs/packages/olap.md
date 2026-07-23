# Package guide: `mondrian.olap`

*The abstract OLAP model — the interfaces the rest of the engine implements and
programs against — plus the `Query` AST, the validator, and engine-wide
configuration. Big picture: [../architecture.md](../architecture.md); control
flow: [../query-lifecycle.md](../query-lifecycle.md). The function library
subpackage `mondrian.olap.fun` has its own guide, [olap-fun.md](olap-fun.md);
neighbors: [mdx-parser.md](mdx-parser.md), [calc.md](calc.md),
[rolap.md](rolap.md).*

## Role

`mondrian.olap` is the **"what"** of the engine: it defines the logical
multidimensional model (`Connection`, `Schema`, `Cube`, `Hierarchy`, `Member`,
…) as interfaces and abstract base classes, while `mondrian.rolap` is the
**"how"** that implements them against relational tables. When you hold a
`Member`, the concrete object is almost always a `Rolap*` class — but nearly
all code outside `rolap` is written against the types in this package.

Three other things live here besides the model:

- the **query AST** (`Query`, `QueryAxis`, `Formula`, `Exp`) plus its
  validator — the parser in `mondrian.parser`/`mondrian.mdx` produces these
  objects (see [mdx-parser.md](mdx-parser.md));
- the two contracts the whole evaluation layer programs against:
  **`Evaluator`** (current cell context) and **`SchemaReader`**
  (role-aware metadata access);
- **engine-wide services and configuration**: `MondrianServer`,
  `MondrianProperties`, `CacheControl`, `Util`.

The package has ~110 hand-written types (excluding `fun/`) plus a handful of
generated ones. The clusters below are the map.

## Metadata model

The logical schema objects. Everything is an `OlapElement` (has a name, unique
name, caption, description; supports child lookup); most elements are also
`Annotated`. Each interface has a `*Base` skeleton that `rolap` subclasses.

| Type | Role |
|---|---|
| `OlapElement` / `OlapElementBase` | Root interface of every catalog object; lookup, naming, visibility |
| `Schema` | A catalog: cubes, shared dimensions, roles; `Schema#lookupCube`, `#lookupRole` |
| `Cube` / `CubeBase` | A cube; entry to its dimensions and its `SchemaReader` |
| `Dimension` / `DimensionBase`, `DimensionType` | Dimension and its kind (standard/time/measures) |
| `Hierarchy` / `HierarchyBase` | Set of members organized into levels; default member, all-member |
| `Level` / `LevelBase`, `LevelType` | One level of a hierarchy; `LevelType` marks time-level roles (years/months/…) |
| `Member` / `MemberBase` | A point on a hierarchy; parent/children, `MemberType` enum (regular, measure, calculated, all, null) |
| `NamedSet` / `SetBase` | Schema- or query-level named set |
| `Annotated`, `Annotation` | Arbitrary user metadata on schema elements |
| `Property` | Definition of a member/cell property (an `EnumeratedValues.BasicValue`; not to be confused with configuration properties) |
| `MemberFormatter`, `CellFormatter`, `PropertyFormatter` | Deprecated aliases for the formatter SPIs in `mondrian.spi` |
| `MatchType` | Match modes for member lookup by unique name (EXACT, BEFORE, AFTER, …) |
| `Aggregator` | Aggregation operator contract ("sum", "count", …); implemented by `mondrian.rolap.RolapAggregator` |
| `NameResolver`, `Namer` | Segment-list → `OlapElement` resolution; localized-name hooks |

## Query model (the AST)

A parsed MDX statement is a tree of `QueryPart`s. The parser builds it
(`Parser.FactoryImpl#makeQuery` is the factory the JavaCC grammar calls into);
`Query#resolve` binds, validates, and compiles it in the constructor, so a
prepared `Query` is immediately executable and reusable.

| Type | Role |
|---|---|
| `Query` | The statement: axes, slicer, WITH formulas; owns `Query.axisCalcs` / `Query.slicerCalc` after compilation |
| `QueryPart` | Base class of every AST node; implements `Walkable` |
| `QueryAxis` | One axis: set expression, NON EMPTY flag, `AxisOrdinal` |
| `AxisOrdinal` | Axis codes (SLICER, COLUMNS, ROWS, …), implemented by `AxisOrdinal.StandardAxisOrdinal` |
| `Formula` | A WITH MEMBER / WITH SET clause; also infers the member's format string (see `FormatAwareFunDef` below) |
| `Exp` / `ExpBase` | The expression interface: `#getType`, `#accept(Validator)`, `#accept(ExpCompiler)` — concrete nodes live in `mondrian.mdx` |
| `FunCall` | Interface for a function applied to operands (resolved/unresolved variants in `mondrian.mdx`) |
| `Id` | Multi-part identifier (`[Store].[USA].[CA]`) before binding; its `Id.Segment`s are used everywhere names are handled |
| `Literal` | Constant value node (numeric, string, symbol, null) |
| `Parameter` / `ParameterImpl` | Query parameter (`Parameter()` / `ParamRef()` MDX functions); scopes from statement to schema |
| `CellProperty`, `MemberProperty` | CELL PROPERTIES clause; property-value pairs inside WITH MEMBER |
| `DrillThrough`, `Explain` | Non-SELECT statement kinds wrapping a `Query` |
| `QueryTiming` | Accumulates phase timings during execution (reported via profiling) |
| `ExpCacheDescriptor` | Key for the expression-result cache (`Evaluator#getCachedResult`; used by the `Cache()` function and `RankFunDef`) |
| `SolveOrderMode` | Strategy enum for calculated-member precedence (`MondrianProperties#SolveOrderMode`) |
| `Walkable`, `Walker`, `IdentifierVisitor` | Generic prefix-order tree walking; identifier collection uses `mondrian.mdx.MdxVisitor` instead |

## Validation and function resolution

Stage 3 of the [query lifecycle](../query-lifecycle.md): turning names into
schema objects and `UnresolvedFunCall`s into typed `ResolvedFunCall`s.

| Type | Role |
|---|---|
| `Validator` / `ValidatorImpl` | Resolution context: validates expressions, requires types, and performs function lookup via `ValidatorImpl#getDef` |
| `IdBatchResolver` | Pre-pass of `Query#resolve`: gathers `Id`s and resolves them against the `SchemaReader` in batches (member lookup may need SQL — per-identifier resolution would be pathological) |
| `FunTable` | The registry of MDX functions the validator consults (`FunTable#getResolvers`); layered implementations live in `mondrian.olap.fun` and `RolapSchema` — see [olap-fun.md](olap-fun.md) |
| `FunDef` | One resolved function definition: validates args, computes result type, and compiles itself to a `Calc` (`FunDef#compileCall`) |
| `FormatAwareFunDef` | (fork PATCH) lets a `FunDef` or UDF steer which argument's format string a calculated member inherits (used by `Formula`'s format inference) |
| `Syntax` | How a call is written: Function, Property, Method, Infix, Prefix, Braces, Parentheses, … — part of the function-lookup key |
| `Category` | Ordinal codes for expression categories (Member, Set, Numeric, …), used in `FunDef` signatures; the class-based counterpart is `mondrian.olap.type` below |

## Evaluation and result contracts

The interfaces the compiled-expression layer ([calc.md](calc.md)) and the
function library program against — implemented in `rolap`
(`RolapEvaluator`, `RolapResult`, `RolapCell`, …).

| Type | Role |
|---|---|
| `Evaluator` | THE evaluation context: current member of every hierarchy, savepoint/restore, `#evaluateCurrent`, cached-result access; also non-empty and native-evaluation hooks. This fork adds a cell-reader accessor (fork PATCH, see `Evaluator` source) |
| `SchemaReader` | THE metadata-access interface: members of a level, children of a member, default members, name lookup — always through the current `Role`. Fork adds `SchemaReader#getLevelMemberByUniqueKey` and `SchemaReader#hasMemberChildren` (fork PATCH) |
| `DelegatingSchemaReader` | Decorator base class; the reader stack pattern (caching, access control) is built from these |
| `NativeEvaluator` | Handle for a set operation that will be evaluated as SQL instead of in memory (`SchemaReader#getNativeSetEvaluator`) |
| `Result` / `ResultBase` | A finished query result: axes + cells by coordinate |
| `Axis`, `Position` | One result axis; a `Position` is one tuple on it (a `List<Member>`) |
| `Cell` | One result cell: value, formatted value, drill-through |

## Access control

A `Role` restricts what a `SchemaReader` exposes and what a query may touch.
Grants go down the model: schema → cube → hierarchy (with
`Role.HierarchyAccess` detail: top/bottom level, rollup policy) → member.

| Type | Role |
|---|---|
| `Access` | Enum of grades: NONE, CUSTOM, RESTRICTED, ALL, ALL_DIMENSIONS |
| `Role` | The access-control interface; per-element `getAccess` plus `canAccess` |
| `RoleImpl` | Standard mutable implementation built from schema `<Role>` XML |
| `UnionRoleImpl` | Union of several roles (superset of their privileges) |
| `DelegatingRole` | Decorator base for wrapping/adjusting an existing role |
| `CubeAccess` | Legacy cube/slicer permission checker (largely superseded by `Role`) |

## Engine services and configuration

| Type | Role |
|---|---|
| `Connection` / `ConnectionBase` | Native connection interface: parse/execute statements, `#getSchemaReader`, `#getCacheControl`, role selection. `ConnectionBase#parseStatement` is where parsing enters (see [mdx-parser.md](mdx-parser.md)) |
| `DriverManager` | `DriverManager#getConnection(connectString, catalogLocator)` — builds a `mondrian.rolap.RolapConnection` from a `mondrian:` connect string |
| `MondrianServer` | An engine instance: statement/connection registry, `AggregationManager` ownership, version info; obtain via `MondrianServer#forConnection` or `#forId` |
| `CacheControl` | Programmatic cache flushing (cell regions and member sets); obtained from `Connection#getCacheControl`. See `../topics/caching.md` |
| `MondrianProperties` (generated) / `MondrianPropertiesBase` | Engine configuration singleton — see "Generated code" below |
| `Util` | The grab-bag used everywhere: name parsing/quoting, `Util#nullValue`, assertions, collection helpers, connect-string parsing (`Util.PropertyList`) |
| `EnumeratedValues` | Pre-Java-5 enum helper still backing `Category` and `Property` |
| `MondrianException` | Root of engine exceptions; most concrete exceptions are **generated** into `mondrian.resource.MondrianResource` from `MondrianResource.xml`, so grep the message text there, not in this package |
| `ResultLimitExceededException` + subclasses | Limit family: `ResourceLimitExceededException`, `MemoryLimitExceededException`, `QueryCanceledException`, `QueryTimeoutException`, `NativeEvaluationUnsupportedException` |
| `InvalidArgumentException`, `InvalidHierarchyException`, `ResultStyleException` | Direct `MondrianException` subclasses for bad arguments, empty hierarchies, and un-satisfiable calc result styles |

### Generated code in this package

Two XML files in the source tree are inputs to code generation
(output lands in `src/generated/java/mondrian/olap/`, wired in
`mondrian/build.xml` invoked from the module `pom.xml`):

- **`Mondrian.xml` → `MondrianDef`** — the schema-XML object model (`<Schema>`,
  `<Cube>`, `<Dimension>`, …) generated by the eigenbase-xom `XOMGenTask`
  (`build.xml` target `def`). `RolapSchema` loading walks `MondrianDef` objects;
  to add a schema XML attribute, edit `Mondrian.xml` and rebuild.
- **`MondrianProperties.xml` → `MondrianProperties`** (plus the
  `mondrian.properties.template` documentation file) — generated by
  `mondrian.util.PropertyUtil` (`build.xml` target `generate.properties`).
  Each property becomes a typed field (`org.eigenbase.util.property.BooleanProperty`,
  `IntegerProperty`, …) supporting change **triggers**;
  `MondrianPropertiesBase` (hand-written) loads `mondrian.properties` from
  file/classpath and system properties. Access pattern:
  `MondrianProperties.instance().SomeProperty.get()`.
- **`Parser.cup` → `Parser`/`ParserSym`** — the *old* CUP-generated parser,
  still generated because `Parser.FactoryImpl` (the `Query` factory used by the
  live JavaCC parser) lives on it; the CUP parse path itself, with `Scanner` /
  `StringScanner` (the hand-written lexer in this package), is dead code.

## The type system: `mondrian.olap.type`

Every `Exp` has a `Type`; the `Validator` computes them and the compiler
([calc.md](calc.md)) uses them to pick `Calc` flavors and implicit conversions
(the conversion costing lives in `mondrian.olap.fun.Resolver`s). The subpackage
is small and worth reading whole:

| Type | Role |
|---|---|
| `Type` | The interface: `#usesDimension`, `#usesHierarchy`, `#computeCommonType` |
| `MemberType`, `LevelType`, `HierarchyType`, `DimensionType`, `CubeType` | Metadata-valued expression types, each optionally pinned to a concrete schema element |
| `TupleType`, `SetType` | Composite types: a tuple of member types; a set of members or tuples |
| `ScalarType` + `NumericType`/`DecimalType`, `StringType`, `BooleanType`, `DateTimeType`, `SymbolType`, `NullType`, `EmptyType` | Scalar value types |
| `TypeUtil` | The workhorse: conversion checks (`TypeUtil#canConvert`), common-type computation, hierarchy extraction |

## Key relationships

- **`Query` owns everything about one statement**: `QueryAxis[]` + slicer,
  `Formula[]`, and after `Query#resolve` also the compiled `Query.axisCalcs`
  and `Query.slicerCalc`. Resolution runs in the constructor — there is no
  window where a `Query` exists unvalidated.
- **`Validator` ↔ `FunTable`**: `ValidatorImpl#getDef` asks
  `FunTable#getResolvers(name, syntax)`; resolvers arbitrate overloads and
  implicit conversions and emit a `FunDef`. Swap or extend function behavior at
  the `FunTable` layer, not in the validator.
- **`Evaluator` and `SchemaReader` are the two interfaces the calc layer sees.**
  A `Calc` gets an `Evaluator`, and through it (`Evaluator#getSchemaReader`)
  role-filtered metadata. Nothing in `mondrian.calc` or `mondrian.olap.fun`
  should touch `rolap` types directly.
- **`MondrianProperties.instance()`** is the global configuration read
  everywhere; property objects support triggers, so long-lived components can
  react to changes instead of re-reading.
- **`Role` gates `SchemaReader`**: `Schema#lookupRole` /
  `Connection#getRole` determine which decorated reader stack you get; access
  control is enforced by the readers, not by call sites.

## Where to look when…

| Task | Start at |
|---|---|
| Add/understand a connect-string property | `mondrian.rolap.RolapConnectionProperties` (enum, in `rolap`) + `ConnectionBase`, `DriverManager` |
| Add an engine configuration property | `MondrianProperties.xml` → rebuild → read via `MondrianProperties.instance()` |
| Add a schema XML element/attribute | `Mondrian.xml` (→ generated `MondrianDef`), then the `RolapSchema` loader ([../topics/schema-loading.md](../topics/schema-loading.md)) |
| Change how a function call is validated/overload-resolved | `ValidatorImpl#getDef`, the function's `Resolver` in `mondrian.olap.fun` ([olap-fun.md](olap-fun.md)) |
| Change identifier → member resolution | `IdBatchResolver`, `SchemaReader`, `Util#lookup`; member SQL is in `rolap` ([../topics/member-resolution.md](../topics/member-resolution.md)) |
| Trace where a thrown error message comes from | `mondrian.resource.MondrianResource` (generated) + the `MondrianException` family here |
| Flush caches programmatically | `CacheControl` via `Connection#getCacheControl` ([../topics/caching.md](../topics/caching.md)) |
| Adjust access control behavior | `Role`, `RoleImpl`, `Role.HierarchyAccess`; enforcement in `rolap`'s `RestrictedMemberReader` |
| Understand a type-mismatch validation error | `mondrian.olap.type.TypeUtil#canConvert` and the `Category` codes in the function signature |
