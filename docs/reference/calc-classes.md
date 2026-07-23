# Class Reference — `mondrian.calc` and `mondrian.calc.impl`

*Lookup catalog for the compiled-expression packages: every type in
`mondrian.calc` and `mondrian.calc.impl`, tiered by importance — Tier 1 gets a
full entry, Tier 2 a paragraph, Tier 3 one line. Grep for a class name rather
than reading top to bottom. For the narrative of how these classes cooperate
(compilation, ResultStyle negotiation, dependency tracking), see
[../packages/calc.md](../packages/calc.md).*

This fork carries **no `// PATCH:` markers** anywhere under `mondrian/calc` —
both packages match upstream Mondrian 9.3 behavior.

---

## Tier 1 — Core types

### Calc

- **Purpose**: The root contract for every compiled expression — the physical,
  strongly typed counterpart of the logical `mondrian.olap.Exp` AST. One method
  matters at runtime: `Calc#evaluate(Evaluator)` computes the value in the
  current dimensional context.
- **Extends / implements**: Nothing; root interface of the family. Typed
  subinterfaces (`MemberCalc`, `ListCalc`, `DoubleCalc`, …) add unboxed
  `evaluateXxx` methods — see Tier 2.
- **Key collaborators**: `mondrian.olap.Evaluator` (the mutable cell context it
  reads), `ExpCompiler` (produces it), `CalcWriter` (serializes it for the
  explain plan via `Calc#accept`), `ResultStyle` (via `Calc#getResultStyle`).
- **Lifecycle / scope**: Built once per prepared `Query` during `Query#resolve`
  (stage 4 of [../query-lifecycle.md](../query-lifecycle.md)); one tree per
  axis, slicer, and WITH formula; lives as long as the prepared statement.
- **Threading**: Implementations must be stateless — all mutable evaluation
  state belongs in the `Evaluator`. Calc trees are re-executed and shared
  across phases of one execution.
- **Notes**: `Calc#dependsOn(Hierarchy)` is the dependency-analysis contract
  ("could my result change if that hierarchy's current member changed?") that
  feeds expression caching and `AbstractCalc#simplifyEvaluator`.
  `Calc#isWrapperFor` / `Calc#unwrap` give JDBC-style unwrapping of decorated
  calcs (used by the profiler).

### ExpCompiler

- **Purpose**: Mediates `Exp → Calc` compilation. One generic entry point
  (`ExpCompiler#compile`), a style-constrained variant
  (`ExpCompiler#compileAs`), and typed convenience methods (`compileMember`,
  `compileList(exp, mutable)`, `compileIter`, `compileScalar`, …). Carries the
  list of `ResultStyle`s the current consumer accepts
  (`ExpCompiler#getAcceptableResultStyles`).
- **Extends / implements**: Interface; implemented by `AbstractExpCompiler`
  (and the `DelegatingExpCompiler` decorator base).
- **Key collaborators**: `Validator` and `Evaluator` (construction inputs),
  `FunDef#compileCall` (the function library produces most calcs; the compiler
  visits `ResolvedFunCall`s and dispatches to it), `ParameterSlot` (via
  `ExpCompiler#registerParameter`).
- **Lifecycle / scope**: One instance per compilation pass, created by
  `Query#createCompiler` through `ExpCompiler.Factory#getExpCompiler`;
  discarded when compilation ends.
- **Threading**: Used single-threaded during statement preparation; not shared.
- **Notes**: The nested `ExpCompiler.Factory` (a `mondrian.util.ObjectFactory`)
  returns `BetterExpCompiler` by default; the implementation can be overridden
  via the `MondrianProperties#ExpCompilerClass` system property or
  `Factory#setThreadLocalClassName` — both are test hooks.

### ResultStyle

- **Purpose**: Enum of the forms in which a compiled expression delivers its
  result: `ANY`, `MUTABLE_LIST`, `LIST`, `ITERABLE`, `VALUE`,
  `VALUE_NOT_NULL`. This is the currency of the producer/consumer negotiation
  that decides whether set expressions stream or materialize.
- **Extends / implements**: Plain enum in `mondrian.calc`.
- **Key collaborators**: `ExpCompiler#getAcceptableResultStyles` (consumer
  side), `Calc#getResultStyle` (producer side),
  `mondrian.olap.ResultStyleException` (thrown at compile time when no
  acceptable style can be produced).
- **Lifecycle / scope**: Static; also defines shared `List<ResultStyle>`
  constants used as negotiation vocabularies (`ResultStyle.ITERABLE_ONLY`,
  `MUTABLELIST_ONLY`, `LIST_ONLY`, `ANY_LIST`, `ITERABLE_LIST_MUTABLELIST`, …).
- **Threading**: Immutable.
- **Notes**: See [../packages/calc.md](../packages/calc.md) "ResultStyle" for
  the negotiation protocol and which entry points fix which constraint.

### TupleList

- **Purpose**: A materialized set of tuples. Extends `List<List<Member>>` and
  adds tuple-aware operations: `TupleList#get(slice, index)`,
  `TupleList#slice(column)`, `TupleList#cloneList(capacity)` (deep copy when
  capacity < 0), `addTuple`, `project`, `addCurrent(cursor)`,
  arity-preserving `subList`, `withPositionCallback`, and `TupleList#fix()`
  (reifies tuples so sorting/filtering the list cannot corrupt them).
- **Extends / implements**: `List<List<Member>>`, `TupleIterable`.
- **Key collaborators**: `TupleCollections` (factories and adapters),
  `TupleCursor` / `TupleIterator` (lazy traversal), the `calc.impl`
  implementations (`UnaryTupleList`, `ArrayTupleList`, `ListTupleList`,
  `DelegatingTupleList` — Tier 2), `mondrian.rolap.RolapAxis` (wraps the final
  axis list).
- **Lifecycle / scope**: Created during set evaluation; axis results live for
  the duration of the `Result`.
- **Threading**: Implementations are not synchronized; confine a list to the
  evaluating thread.
- **Notes**: All set calcs traffic in tuples even for single-hierarchy sets —
  arity-1 sets use `UnaryTupleList`, not a separate "member list" type.

### AbstractCalc (`mondrian.calc.impl`)

- **Purpose**: Root implementation base for calcs. Stores the source `Exp`, its
  `Type`, and the child `Calc[]`; supplies default `dependsOn` (propagates from
  children via `AbstractCalc#anyDepends`), default result style
  `ResultStyle.VALUE`, and the explain-plan machinery
  (`AbstractCalc#accept` → `CalcWriter#visitCalc` with `getName` /
  `collectArguments`).
- **Extends / implements**: Implements `Calc`.
- **Key collaborators**: `CalcWriter`; `mondrian.rolap.RolapEvaluator` (in
  `AbstractCalc#simplifyEvaluator`, which resets hierarchies the calc does not
  depend on to their default member so expression-cache keys collapse).
- **Lifecycle / scope**: Superclass of nearly every calc in the engine
  (directly or via the typed bases and `GenericCalc`).
- **Threading**: Holds only immutable compile-time state; subclasses must keep
  it that way.
- **Notes**: Static helpers `anyDependsButFirst` and `butDepends` implement the
  dependency-subtraction pattern for functions that set context themselves
  (e.g. `Aggregate({Set}, expr)`). Passing children in the constructor is
  load-bearing: it feeds `dependsOn`, the profiler, and the plan output.
  `getName` derives plan-node names from the `ResolvedFunCall` or `NamedSetExpr`
  when the class is anonymous.

### GenericCalc (`mondrian.calc.impl`)

- **Purpose**: Adapter that implements *all* the value-typed calc interfaces
  over a single `evaluate(Evaluator)`, casting the result on demand with a
  descriptive error message (`GenericCalc#evaluateString`, `#evaluateInteger`,
  …) on type mismatch. Numeric methods map Java `null` to the MDX null
  sentinels `FunUtil.IntegerNull` / `FunUtil.DoubleNull`.
- **Extends / implements**: Extends `AbstractCalc`; implements `TupleCalc`,
  `StringCalc`, `IntegerCalc`, `DoubleCalc`, `BooleanCalc`, `DateTimeCalc`,
  `VoidCalc`, `MemberCalc`, `LevelCalc`, `HierarchyCalc`, `DimensionCalc`.
- **Key collaborators**: Base class of `ConstantCalc`, `ValueCalc`,
  `MemberValueCalc`, `MemberArrayValueCalc`, `TupleValueCalc`, `CacheCalc`,
  `AbstractVoidCalc`.
- **Lifecycle / scope**: Same as any calc — compile once, evaluate many.
- **Threading**: Stateless.
- **Notes**: Use sparingly — a calc implementing one specific typed interface
  avoids the per-cell cast. The set-side analog is `GenericIterCalc` (Tier 3).

### AbstractExpCompiler (`mondrian.calc.impl`)

- **Purpose**: The working implementation of `ExpCompiler`.
  `AbstractExpCompiler#compile` is just `exp.accept(this)` (visitor pattern);
  the typed `compileXxx` methods implement the **implicit conversions**:
  hierarchy → member via `HierarchyCurrentMemberFunDef.FixedCalcImpl` /
  `.CalcImpl`, dimension → hierarchy (constant, or the private
  `DimensionHierarchyCalc`), member/level → hierarchy via the corresponding
  `FunDef.CalcImpl`s, member → scalar via `MemberValueCalc#create`, tuple →
  scalar via `TupleValueCalc` then `TupleValueCalc#optimize`, numeric coercions
  as anonymous `Abstract*Calc` wrappers, and constant folding through
  `ConstantCalc`.
- **Extends / implements**: Implements `ExpCompiler`.
- **Key collaborators**: `Evaluator`, `Validator`, the `mondrian.olap.fun`
  CalcImpls listed above, `DummyExp` (types for synthesized calcs),
  `IterableListCalc` (via `AbstractExpCompiler#toList`, the iterable→list
  fallback in `compileList`).
- **Lifecycle / scope**: One per compilation pass; owns the `Parameter` →
  `ParameterSlotImpl` map populated by
  `AbstractExpCompiler#registerParameter`, which also compiles each
  parameter's default-value expression (after registering the slot, to avoid
  cycles).
- **Threading**: Mutable (`resultStyles` is swapped in `compileAs`, the
  parameter map grows); single-threaded use only.
- **Notes**: `compileMember` rejects `NullType` expressions with a
  `NullNotSupported` error. `ParameterSlotImpl`, the only `ParameterSlot`
  implementation, is a private static inner class here.

### BetterExpCompiler (`mondrian.calc.impl`)

- **Purpose**: The default compiler, "enhanced" with two conversions on top of
  `AbstractExpCompiler`: `BetterExpCompiler#compileTuple` accepts a member
  expression and wraps it as a 1-ary tuple, and
  `BetterExpCompiler#compileList` with `mutable=true` protects producers that
  only offer an immutable `LIST` by wrapping them in the private
  `CopyListCalc`, which deep-copies via `TupleList#cloneList(-1)` at
  evaluation time.
- **Extends / implements**: Extends `AbstractExpCompiler`.
- **Key collaborators**: Instantiated by `ExpCompiler.Factory#getDefault`;
  decorated by `RolapUtil#createProfilingCompiler` and
  `RolapUtil#createDependencyTestingCompiler` (both via
  `DelegatingExpCompiler`).
- **Lifecycle / scope / Threading**: As `AbstractExpCompiler`.
- **Notes**: If you need compiler-level behavior for every expression, this
  hierarchy is the place; per-function strategies belong in
  `FunDef#compileCall`.

### ExpCacheDescriptor (`mondrian.olap` — cross-listed)

- **Purpose**: The key object of the expression result cache: pairs a compiled
  `Calc` with the ordinals of the hierarchies it `dependsOn`
  (`ExpCacheDescriptor#getDependentHierarchyOrdinals`, computed by walking
  `Evaluator#getMembers` at construction).
- **Extends / implements**: Plain class; lives in `mondrian.olap`, not
  `mondrian.calc` — listed here because the calc package is meaningless
  without it.
- **Key collaborators**: `CacheCalc` (delegates evaluation to
  `Evaluator#getCachedResult(descriptor)`),
  `mondrian.rolap.RolapEvaluator#getCachedResult` (builds the cache key from
  the expression plus only the dependent hierarchies' current members),
  `CacheFunDef`, `RankFunDef`, and compound-slicer aggregation in
  `RolapResult`.
- **Lifecycle / scope**: One per cached expression, created at compile/prepare
  time (one constructor compiles the expression itself via a fresh
  `BetterExpCompiler`).
- **Threading**: Effectively immutable after construction.
- **Notes**: Accuracy of `dependsOn` directly sizes the cache key — see
  "Dependency tracking" in [../packages/calc.md](../packages/calc.md).

---

## Tier 2 — Supporting types

### `mondrian.calc` interfaces

**MemberCalc, TupleCalc, HierarchyCalc, LevelCalc, DimensionCalc** — the
OLAP-element-typed `Calc` subinterfaces. Each adds one unboxed method —
`MemberCalc#evaluateMember`, `TupleCalc#evaluateTuple` (returns `Member[]`),
`HierarchyCalc#evaluateHierarchy`, `LevelCalc#evaluateLevel`,
`DimensionCalc#evaluateDimension` — so callers avoid casting `Object` per
cell. Compiled via the corresponding `ExpCompiler#compileXxx` method, which
also performs the implicit conversions between these types.

**StringCalc, IntegerCalc, DoubleCalc, BooleanCalc, DateTimeCalc, VoidCalc** —
the scalar-typed `Calc` subinterfaces: `evaluateString`, `evaluateInteger`
(`int`), `evaluateDouble` (`double`), `evaluateBoolean`, `evaluateDateTime`
(`java.util.Date`), `evaluateVoid` (side effects only). MDX null is carried in
the primitive channels as the `FunUtil.IntegerNull` / `FunUtil.DoubleNull`
sentinels, so arithmetic calcs must check for them rather than for `null`.

**IterCalc, ListCalc** — the set-valued calcs. `IterCalc#evaluateIterable`
yields a lazy `TupleIterable` (result style `ITERABLE`); `ListCalc` extends
`IterCalc` and adds `ListCalc#evaluateList` yielding a materialized
`TupleList` (`LIST` or `MUTABLE_LIST`). Which one a function produces — and
whether an adapter (`IterableListCalc`) is inserted — is decided by the
ResultStyle negotiation at compile time.

**ParameterSlot** — per-query storage for one MDX `Parameter`: a unique index,
the compiled default-value calc (`ParameterSlot#getDefaultValueCalc`), the
assigned value and its assigned/unset state, and a cached default value. Slots
are created by `ExpCompiler#registerParameter`; the only implementation is
`ParameterSlotImpl`, a private static inner class of `AbstractExpCompiler`
(not a separate source file). At execution time,
`RolapResult.RolapResultEvaluatorRoot#getParameterValue` evaluates the default
calc lazily for unset parameters and caches the result in the slot, using a
sentinel to detect cyclic parameter definitions.

### `mondrian.calc.impl` — calc base classes

**AbstractBooleanCalc, AbstractDateTimeCalc, AbstractDimensionCalc,
AbstractDoubleCalc, AbstractHierarchyCalc, AbstractIntegerCalc,
AbstractIterCalc, AbstractLevelCalc, AbstractListCalc, AbstractMemberCalc,
AbstractStringCalc, AbstractTupleCalc, AbstractVoidCalc** — one abstract base
per typed calc interface, all extending `AbstractCalc` (except
`AbstractVoidCalc`, which extends `GenericCalc`): the subclass implements only
the typed `evaluateXxx` method and the base's `evaluate` delegates to it. A
`FunDef#compileCall` usually returns an anonymous subclass of one of these,
passing the compiled argument calcs as children for dependency analysis.
`AbstractListCalc` takes a `mutable` constructor flag deciding
`MUTABLE_LIST` vs `LIST` (default constructor is mutable, and its
`evaluateIterable` simply returns `evaluateList`); `AbstractIterCalc` reports
`ITERABLE`; both refine `getType` to `SetType`.

### `mondrian.calc.impl` — tuple collections

**AbstractTupleList, AbstractEndToEndTupleList, UnaryTupleList, ArrayTupleList,
ListTupleList, DelegatingTupleList** and **TupleCollections**
(`mondrian.calc`) — the `TupleList` implementation family. `AbstractTupleList`
(package-visible base) holds the arity and a `mutable` flag and derives
`iterator` / `tupleIterator` / `tupleCursor` from one
`tupleIteratorInternal`; its `fix()` copies into a `DelegatingTupleList`.
`AbstractEndToEndTupleList` stores tuples flattened end-to-end
(`{A1,B1,A2,B2}` for arity 2) for memory density and locality; its concrete
forms are `ArrayTupleList` (backing flat `Member[]`; growth is bounded by
`MondrianProperties#ResultLimit`) and `ListTupleList` (backing flat
`List<Member>`). `UnaryTupleList` is the arity-1 representation — a thin
wrapper over a `List<Member>`, exposed as `slice(0)`. `DelegatingTupleList`
wraps an actual list-of-lists; it is also what `fix()` and `project` return.
`TupleCollections` is the static toolbox: `TupleCollections#createList(arity)`
picks the right implementation (`DelegatingTupleList` for arity 0,
`UnaryTupleList` for 1, `ArrayTupleList` otherwise), plus `emptyList`,
`unmodifiableList`, `iterator(cursor)`, `slice`, `asMemberArrayIterable` /
`asMemberArrayList`, `asTupleList`, and
`TupleCollections#materialize(iterable, mutable)` — used by `RolapResult` to
turn an evaluated axis iterable into the final `RolapAxis`.

### `mondrian.calc.impl` — notable concrete calcs

**ConstantCalc** — a folded constant (plan name "Literal"). Precomputes the
`int` / `double` renditions at construction, `dependsOn` is always false, and
`getResultStyle` reports `VALUE_NOT_NULL` unless the value is null. Static
factories `constantInteger`, `constantDouble`, `constantString`,
`constantBoolean`, `constantNull`, `constantMember`, `constantLevel`,
`constantHierarchy`, `constantDimension` are used throughout the compiler and
function library.

**ValueCalc** — the smallest calc in the engine: `ValueCalc#evaluate` is
`evaluator.evaluateCurrent()` — "read the current cell" — and `dependsOn`
returns true for every hierarchy. The leaf through which base-measure reads
reach `FastBatchingCellReader` (stage 5 of the query lifecycle).

**MemberValueCalc / MemberArrayValueCalc / TupleValueCalc** — the
"set context, then read the cell" family. `MemberValueCalc#create` is the
factory: 0 member expressions → `ValueCalc`, 1 → `MemberValueCalc`, n →
`MemberArrayValueCalc`. All evaluate their member/tuple argument(s), push the
result into the context under `Evaluator#savepoint` / `#restore`, optionally
return null for non-joining dimensions in virtual cubes (the `nullCheck` flag,
via `Evaluator#needToReturnNullForUnrelatedDimension`), then call
`evaluateCurrent`. Their `dependsOn` subtracts the hierarchies the argument
expressions pin (via `Type#usesHierarchy`). `TupleValueCalc#optimize` rewrites
a literal tuple constructor (`TupleFunDef.CalcImpl`) into the cheaper
`MemberValueCalc.create` form. Caveat: `MemberArrayValueCalc` reuses a
per-instance `Member[]` scratch buffer across evaluations, so it assumes the
single-threaded evaluation model.

**CacheCalc** — a `GenericCalc` whose `evaluate` is one line:
`evaluator.getCachedResult(key)`, where the key is an `ExpCacheDescriptor`
(Tier 1). The underlying calc appears as its child so plan output and
dependency analysis see through the cache.

**DelegatingExpCompiler** — the decorator base for compilers. Forwards every
`compileXxx` to the wrapped compiler, offers the
`DelegatingExpCompiler#afterCompile(exp, calc, mutable)` hook, and — the
subtle part — wraps every child expression in a private `WrapExp` whose
`accept(ExpCompiler)` re-enters the *decorator*, so the decoration isn't lost
after the first level of recursion. Its two users are installed by
`Query#createCompiler`: `RolapUtil#createProfilingCompiler` and
`RolapUtil#createDependencyTestingCompiler`.

**DummyExp** — a placeholder `Exp` that exists only to carry a
`mondrian.olap.type.Type`; every other method throws
`UnsupportedOperationException`. Used whenever the compiler synthesizes a calc
that has no source AST node (implicit conversions, `CopyListCalc`,
`IterableListCalc`, …) but still needs `Calc#getType` to work.

---

## Tier 3 — Everything else

### `mondrian.calc`

| Type | Kind | One line |
|---|---|---|
| `CalcWriter` | class | Visitor that serializes a calc tree to indented text — the query "plan" output; driven by `AbstractCalc#accept`, with a profiling mode that merges per-calc timing arguments. |
| `ParameterCompilable` | interface | Extension to `mondrian.olap.Parameter` adding `compile(ExpCompiler)`; implemented by `ParameterImpl` and `RolapSchemaParameter`, invoked from `ParameterExpr#accept(ExpCompiler)`. |
| `TupleCursor` | interface | Cheap forward-only tuple traversal (`forward()`, `current()`, `member(column)`, `setContext(Evaluator)`, `currentToArray`) that avoids allocating a `List<Member>` per step. |
| `TupleIterable` | interface | Extension to `Iterable<List<Member>>` adding `tupleCursor()` / `tupleIterator()`, `getArity()`, and `slice(column)`; the lazy form of a set result. |
| `TupleIterator` | interface | Extension to `java.util.Iterator<List<Member>>` that is also a `TupleCursor`; the bridge between the cheap-cursor and standard-iterator protocols. |

### `mondrian.calc.impl`

| Type | Kind | One line |
|---|---|---|
| `AbstractTupleCursor` | class | Base `TupleCursor` holding the arity; subclasses supply `forward()` / `current()` and inherit `setContext`, `member`, `currentToArray`. |
| `AbstractTupleIterable` | class | Base `TupleIterable`; subclasses implement only `tupleCursor()` and get `iterator()` / `tupleIterator()` via `TupleCollections#iterator`. |
| `AbstractTupleIterator` | class | Base `TupleIterator` on top of `AbstractTupleCursor`; implements `hasNext` / `next` in terms of `forward()`. |
| `GenericIterCalc` | class | Set-side analog of `GenericCalc`: implements both `ListCalc` and `IterCalc` over one `evaluate`, materializing an iterable into a `TupleList` when a list is demanded. |
| `IterableListCalc` | class | The iterable→list materializing adapter (`AbstractListCalc` over an `IterCalc`); inserted by `AbstractExpCompiler#compileList` when a producer can only stream. |
