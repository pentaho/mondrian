# `mondrian.olap.fun` — the MDX function library

*Every built-in MDX function and operator — from `CrossJoin` to `+` — lives in
this package, together with the resolver machinery that binds call syntax to a
`FunDef` during [query-lifecycle stage 3](../query-lifecycle.md) and the
`compileCall` implementations that emit `Calc` trees in stage 4 (see
packages/calc.md). The small `mondrian.udf` package, covered at the end, holds
the bundled user-defined functions. Several set functions defer to native SQL
evaluation — see [topics/native-evaluation.md](../topics/native-evaluation.md).*

## Role in the engine

The package is touched exactly twice per query, both at prepare time:

1. **Resolution.** `ValidatorImpl#getDef` asks the connection's `FunTable` for
   the `Resolver`s registered under an (upper-cased name, `Syntax`) pair;
   each candidate `Resolver#resolve` inspects the argument types and either
   returns a `FunDef` (recording any implicit type conversions, which the
   validator costs to pick the best overload) or `null`. The winning `FunDef`
   is stored in the `ResolvedFunCall` AST node.
2. **Compilation.** `BetterExpCompiler` walks the resolved tree and calls
   `FunDef#compileCall(ResolvedFunCall, ExpCompiler)` on every function node.
   Each `FunDef` compiles its arguments to typed `Calc`s
   (`compiler.compileMember`, `compileList`, `compileInteger`, …) and returns
   the `Calc` that will actually run at execute time. After compilation the
   `FunDef` is inert — all evaluation happens in the returned `Calc`.

The function tables are layered (`FunTableImpl` is the common base; it indexes
`Resolver`s by name+syntax in `FunTableImpl#init`):

- **`BuiltinFunTable`** — singleton registry of every built-in function,
  populated by `BuiltinFunTable#defineFunctions`.
- **`GlobalFunTable`** — wraps `BuiltinFunTable` and adds classpath-wide UDFs
  discovered through `java.util.ServiceLoader`-style lookup of
  `mondrian.spi.UserDefinedFunction` implementations
  (`GlobalFunTable#lookupUdfImplClasses`).
- **`RolapSchema.RolapSchemaFunctionTable`** (in `mondrian.rolap`) — per-schema
  table adding the `<UserDefinedFunction>` elements of the schema XML on top of
  `GlobalFunTable`. This is the table the validator actually consults.
- **`CustomizedFunctionTable`** — a cut-down table exposing a chosen subset of
  built-ins; used by the `NativizeSet` machinery, not by normal queries.

## How functions are registered

`BuiltinFunTable#defineFunctions` is ~2000 lines of `builder.define(...)`
calls using three idioms, in decreasing order of ceremony:

- **Dedicated `FunDef` class + static resolver field.** Anything with
  overloads or nontrivial compilation gets its own file and registers a
  `ReflectiveMultiResolver` constant, e.g.
  `builder.define(SumFunDef.Resolver)` where the field is
  `new ReflectiveMultiResolver("Sum", "Sum(<Set>[, <Numeric Expression>])",
  "Returns the sum of…", new String[] {"fnx", "fnxn"}, SumFunDef.class)`.
  The string array lists the accepted overload signatures (format below);
  `ReflectiveMultiResolver#createFunDef` reflectively invokes the class's
  `(FunDef dummyFunDef)` constructor with a dummy `FunDef` carrying the
  matched signature. `AncestorFunDef`, `CrossJoinFunDef`,
  `TopBottomCountFunDef`, `DescendantsFunDef` all follow this pattern.
- **Anonymous `FunDefBase` subclass, inline.** Single-signature functions —
  and nearly all scalar operators (`+ - * /`, comparisons, `AND`/`OR`/`NOT`,
  string concatenation, `Len`, `UCase`, …) plus simple properties like
  `<Hierarchy>.Levels(<Numeric Expression>)` — are defined in place:
  `builder.define(new FunDefBase("Levels", "Returns the level whose position…",
  "mlhn") { public Calc compileCall(...) {...} })`. There is no separate class
  to grep for these; search `BuiltinFunTable` itself.
- **Anonymous `MultiResolver`, inline** — the rare overloaded-but-inline case
  (`$AggregateChildren`), overriding `MultiResolver#createFunDef` directly.

Finally the VBA/Excel compatibility functions are not written as `FunDef`s at
all: `defineFunctions` loops over `JavaFunDef.scan(Vba.class)` and
`JavaFunDef.scan(Excel.class)`, which reflect over every public static method
and manufacture a `JavaFunDef` per method (see the VBA section). **(fork
PATCH)** Names listed in the `mondrian.olap.fun.skipJavaFunDefs` system
property (read live via `BuiltinFunTable#parseSkipFunctions`, deliberately not
through `MondrianProperties`) are skipped, so a schema UDF may take over a
Vba/Excel name without an ambiguous-match error — details in
[topics/fork-changes.md](../topics/fork-changes.md) §1.7.

### Signature flag strings

The compact strings like `"fnxn"` encode syntax, return type, and parameter
types, one character each, decoded by `FunUtil#decodeSyntacticType`,
`#decodeReturnCategory`, and `#decodeParameterCategories`. First character:
syntax (`f` function, `m` method, `p` property, `i` infix, `P` prefix, `Q`
postfix). Second: return `Category` (`n` numeric, `x` set, `m` member, `l`
level, `h` hierarchy, `S` string, `b` boolean, `v` value, …). The rest: one
category per parameter. So `SumFunDef`'s `"fnxn"` reads: function syntax,
numeric return, taking a set and a numeric expression.

## The machinery classes

- **`FunDefBase`** — default `FunDef` implementation and superclass of nearly
  every function here. Decodes flag strings in its constructor; `#createCall`
  validates arguments and builds the `ResolvedFunCall`; `#getResultType`
  derives the result type from the first argument via the `#castType` helper
  (functions with other rules override it); `#compileCall` throws "not
  implemented" — every concrete function must override it. It extends
  `FunUtil`, so all of `FunUtil`'s static helpers are in scope unqualified.
- **`Resolver` / `ResolverBase` / `SimpleResolver` / `MultiResolver` /
  `ReflectiveMultiResolver`** — `SimpleResolver` wraps a single fixed `FunDef`
  (that is what `builder.define(FunDef)` creates); `MultiResolver` tries each
  signature string in turn and delegates to an abstract `#createFunDef`;
  `ReflectiveMultiResolver` implements `#createFunDef` by reflection;
  `ResolverBase` is the skeleton for hand-written resolvers with irregular
  matching (`CastFunDef.Resolver`, `IsEmptyFunDef`'s two syntaxes, …).
- **`FunUtil`** — the package's static grab-bag, used from almost every
  `Calc`: evaluation-error factory `#newEvalException`; the statistical
  kernels behind the aggregate functions (`#sum`, `#avg`, `#min`, `#max`,
  `#var`, `#stdev`, `#percentile`, `#quartile`, `#correlation`,
  `#covariance`, plus `#evaluateSet` which materializes a set's values with
  `Evaluator#setNonEmpty(false)`); member navigation (`#ancestor`, `#cousin`,
  `#periodsToDate`, `#memberRange`, `#compareHierarchically`,
  `#compareSiblingMembers`); tuple utilities (`#tupleContainsNullMember`,
  `#equalTuple`, `#makeNullMember`); string→object parsing for
  `StrToMember`/`StrToSet` (`#parseMember`, `#parseTupleList`); signature
  decoding; and `#createDummyFunDef` for the `MultiResolver` protocol.
  **(fork PATCH)** `#min`/`#max` dispatch on `instanceof Date` at runtime so
  `Min`/`Max` work over DateTime expressions. Sorting is *not* here anymore —
  upstream 9.x moved it to the `sort` subpackage.
- **Special-case pseudo-functions.** `ParameterFunDef` describes
  `Parameter`/`ParamRef` calls only transiently — the validator converts them
  into `mondrian.olap.Parameter` objects. `CastFunDef` implements
  `CAST(<expr> AS <type>)` with a hand-written `ResolverBase` (the target type
  is a symbol, not an expression). `ValueFunDef` and `ParenthesesFunDef` are
  internal wrappers (evaluate a member/tuple; the `( … )` operator).

## Map of the package

Roughly 111 files; the groups below cover all of them except the machinery
already described.

**Set construction and transformation** — each compiles to list/iterable
`Calc`s and is where most query cost lives:

| Classes | Functions |
|---|---|
| `SetFunDef`, `TupleFunDef`, `RangeFunDef` | `{…}` braces, `(…)` tuple constructor, `<Member> : <Member>` |
| `CrossJoinFunDef`, `NonEmptyCrossJoinFunDef`, `UnionFunDef`, `IntersectFunDef`, `ExceptFunDef`, `DistinctFunDef` | products and set algebra (`*` infix included) |
| `FilterFunDef`, `TopBottomCountFunDef`, `TopBottomPercentSumFunDef`, `OrderFunDef`, `UnorderFunDef`, `HierarchizeFunDef`, `SubsetFunDef`, `HeadTailFunDef` | filtering, ranking, ordering, slicing |
| `DescendantsFunDef`, `LevelMembersFunDef`, `AddCalculatedMembersFunDef`, `ExtractFunDef`, `GenerateFunDef`, `ExistsFunDef`, `ExistingFunDef` | hierarchy expansion and set comprehension |
| `DrilldownLevelFunDef`, `DrilldownLevelTopBottomFunDef`, `DrilldownMemberFunDef`, `ToggleDrillStateFunDef` | drill state manipulation |
| `StrToSetFunDef`, `StrToTupleFunDef`, `SetToStrFunDef`, `TupleToStrFunDef`, `SetItemFunDef`, `TupleItemFunDef` | string↔set/tuple conversion and indexing |
| `NativizeSetFunDef`, `VisualTotalsFunDef`, `CacheFunDef`, `AsFunDef` | `NativizeSet` (force native evaluation of large sets), `VisualTotals`, `Cache`, inline `AS` aliasing |

**Member and time navigation:**

| Classes | Functions |
|---|---|
| `AncestorFunDef`, `AncestorsFunDef`, `LeadLagFunDef` | `Ancestor(s)`, `Lead`/`Lag`; `Cousin`, `FirstChild`, `LastSibling` etc. are inline in `BuiltinFunTable` over `FunUtil` helpers |
| `ParallelPeriodFunDef`, `OpeningClosingPeriodFunDef`, `PeriodsToDateFunDef`, `XtdFunDef`, `LastPeriodsFunDef` | time-series navigation (`Ytd`/`Qtd`/`Mtd`/`Wtd` are four resolvers on `XtdFunDef`) |
| `HierarchyCurrentMemberFunDef`, `MemberOrderKeyFunDef`, `NamedSetCurrentFunDef`, `NamedSetCurrentOrdinalFunDef`, `StrToMemberFunDef`, `ValidMeasureFunDef` | context properties and lookups |
| `HierarchyDimensionFunDef`, `DimensionDimensionFunDef`, `LevelDimensionFunDef`, `MemberDimensionFunDef`, `DimensionsNumericFunDef`, `DimensionsStringFunDef`, `LevelHierarchyFunDef`, `MemberHierarchyFunDef`, `MemberLevelFunDef` | metadata property functions (`.Dimension`, `.Hierarchy`, `.Level`, `Dimensions(n)`) |

**Aggregation and statistics** — all extend `AbstractAggregateFunDef`
(which evaluates the set argument non-empty-off and, via
`#processUnrelatedDimensions`, handles unrelated-dimension policy) except
where noted, and delegate the math to `FunUtil`:

| Classes | Functions |
|---|---|
| `AggregateFunDef` | `Aggregate` — rolls up by the measure's own aggregator; **(fork PATCH)** can route single-dimension rollups through the distinct-count SQL path when in-memory rollup is disabled (see fork-changes §1.7) |
| `SumFunDef`, `AvgFunDef`, `CountFunDef`, `MinMaxFunDef`, `MedianFunDef`, `PercentileFunDef` | the usual aggregates; `MinMaxFunDef` is substantially rewritten **(fork PATCH)** to support DateTime values and implements `FormatAwareFunDef` |
| `StdevFunDef`, `StdevPFunDef`, `VarFunDef`, `VarPFunDef`, `CovarianceFunDef`, `CorrelationFunDef`, `LinReg` | statistics; `LinReg` is the base for the five `LinRegXxx` resolvers |
| `RankFunDef`, `CoalesceEmptyFunDef`, `IifFunDef`, `CaseTestFunDef`, `CaseMatchFunDef`, `IsEmptyFunDef`, `IsFunDef`, `IsNullFunDef` | ranking, conditionals, emptiness tests (not aggregates; grouped here by usage) |

**Scalar operators and simple functions** — arithmetic, comparison, logical
operators, and small string/numeric functions have **no class of their own**:
they are anonymous `FunDefBase` subclasses inside
`BuiltinFunTable#defineFunctions`. `FormatFunDef` and `PropertiesFunDef` are
the notable named exceptions.

**VBA/Excel compatibility** — `vba/Vba.java` and `vba/Excel.java` are plain
classes of public static methods implementing the VBA and Excel worksheet
function libraries (`DateAdd`, `Format`, `InStr`, `Power`, `Sqrt(Pi)` …).
`JavaFunDef#scan` reflects over them; annotations
(`JavaFunDef.FunctionName`, `.Signature`, `.Description`) override defaults
derived from the method name and parameter types, and `JavaFunDef#compileCall`
compiles each argument to the `Calc` matching the Java parameter type.
**(fork PATCH)** The Vba date functions accept `Object` and coerce via
`Vba#castToDate` (calculated members are statically typed Numeric, so a `Date`
arrives as an `Object`), `JavaFunDef` treats the MDX null sentinel as Java
null and coerces `BigDecimal`→`double`, and registration honors
`skipJavaFunDefs` — all cataloged in
[topics/fork-changes.md](../topics/fork-changes.md) §1.7.

**The `sort` subpackage** — `Sorter` is the static sorting toolkit used by
`Order`, `TopCount`, `Hierarchize`, and member-child ordering:
`#sortMembers` / `#sortTuples`, partial (top-n) sorting via `#stablePartialSort`
and `Quicksorter`, `#hierarchizeMemberList` / `#hierarchizeTupleList`, plus
fork-added `#sortSiblings` / `#sortParentChildMembers` **(fork PATCH)**.
`SortKeySpec` carries one `Order` key expression + direction flags;
`MemberComparator`, `TupleComparator`, and the `Hierarchize*Comparator`s do
the comparing; `OrderKey` wraps a member's order key **(fork PATCH: null order
keys collate first)**. `TupleExpMemoComparator` memoizes sort-key evaluation
per tuple in a **Caffeine** cache **(fork PATCH** — replaced Guava; Caffeine
doesn't wrap exceptions, so `CellRequestQuantumExceededException` propagates
to the phase loop without unwrapping**)**.

**Extensions and support** — `extra/` holds non-standard functions registered
by default: `CachedExistsFunDef` (an `Exists` variant that caches subtotal
tuple projections), `CalculatedChildFunDef`, `NthQuartileFunDef`
(`FirstQ`/`ThirdQ`). Support classes: `FunInfo` (function metadata for
introspection), `MemberExtractingVisitor` / `ResolvedFunCallFinder` (AST
visitors used by `ValidMeasure` and native analysis), and
`MondrianEvaluationException` (what `FunUtil#newEvalException` throws).

## Native SQL evaluation hook

`CrossJoinFunDef`, `NonEmptyCrossJoinFunDef`, `FilterFunDef`,
`TopBottomCountFunDef`, and `DrilldownLevelTopBottomFunDef` compile to `Calc`s
whose evaluate method first asks
`SchemaReader#getNativeSetEvaluator(call.getFunDef(), call.getArgs(),
evaluator, this)`; if the registry (`RolapNativeRegistry`) can express the
call as SQL, the returned `NativeEvaluator#execute` produces the tuple list
directly from the database and the in-memory implementation is skipped. The
check happens per evaluation, not at compile time. How the SQL is built, what
disqualifies a call, and the role of `NativizeSetFunDef` are covered in
[topics/native-evaluation.md](../topics/native-evaluation.md).

## User-defined functions and `mondrian.udf`

A UDF implements `mondrian.spi.UserDefinedFunction`: name, syntax, parameter
`Type`s, `getReturnType`, and `execute(Evaluator, Argument[])`. `UdfResolver`
adapts one into the resolver world — its inner `UdfFunDef` maps the declared
`Type`s to categories, and its `compileCall` pre-compiles each argument (as
scalar, and additionally as list/iterable for set arguments) into
`Argument` wrappers, instantiating a fresh UDF object per call site because
UDFs may keep state. Registration paths: schema
`<UserDefinedFunction>` elements → `RolapSchema.RolapSchemaFunctionTable`;
classpath-wide via `META-INF/services/mondrian.spi.UserDefinedFunction` →
`GlobalFunTable`. **(fork PATCH)** `UdfResolver` returns a UDF-produced
`TupleList` as-is (MONDRIAN-2661) and forwards `FormatAwareFunDef` from the
wrapped UDF.

The bundled `mondrian.udf` package (all registered through the services file):

| Class | Function |
|---|---|
| `CurrentDateMemberUdf` | `CurrentDateMember(<Hierarchy>, <format>[, <find>])` — member for today's date, with BEFORE/AFTER/EXACT matching |
| `CurrentDateMemberExactUdf` | `CurrentDateMember` two-argument exact-match variant |
| `CurrentDateStringUdf` | `CurrentDateString(<format>)` — today's date as a formatted string |
| `InUdf` | `IN` — membership of a tuple in a set |
| `InverseNormalUdf` | `InverseNormal` — inverse normal distribution |
| `LastNonEmptyUdf` | `LastNonEmpty(<Set>, <Measure>)` — last member with a non-empty value |
| `MatchesUdf` | `MATCHES` — regex match **(fork PATCH: null-safe, stringifies non-String input)** |
| `NullValueUdf` | `NullValue()` — the null value |
| `ValUdf` | `Val(<String>)` — VBA-style string→number |

## Adding a function (recipe)

1. Decide the shape: overloads or nontrivial typing → a dedicated class
   extending `FunDefBase` (or `AbstractAggregateFunDef` for aggregates);
   a single simple signature → an anonymous `FunDefBase` in
   `BuiltinFunTable#defineFunctions`; user-extensible → a
   `mondrian.spi.UserDefinedFunction` instead.
2. For a dedicated class, add the `(FunDef dummyFunDef)` constructor and a
   static resolver field: `new ReflectiveMultiResolver(name, signature,
   description, new String[] {…flag strings…}, YourFunDef.class)`.
3. Implement `#compileCall`: compile each argument with the matching
   `ExpCompiler#compileXxx`, return an `Abstract*Calc` (from
   `mondrian.calc.impl`) whose evaluate method does the work — always pass the
   child calcs to the constructor so dependency analysis and profiling see
   them. Override `#getResultType` if the default first-argument inference is
   wrong.
4. Reuse `FunUtil` helpers for math/navigation and `sort.Sorter` for ordering;
   throw errors via `FunUtil#newEvalException`; never swallow
   `MondrianEvaluationException` or the cell-batching sentinels (see
   [query-lifecycle.md](../query-lifecycle.md) stage 5).
5. Register it: `builder.define(YourFunDef.Resolver)` in
   `BuiltinFunTable#defineFunctions`, next to its functional group.
6. Mark any deviation from upstream code with `// PATCH:` comments, per
   repository convention.
7. Test through the JRuby suite, not the legacy Java tests: add cases to the
   relevant `test/*_test.rb` (MDX queries through the gem against FoodMart),
   rebuild the JAR with `mise package`, and run `rake test` (see AGENTS.md).
