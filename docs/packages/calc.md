# `mondrian.calc` — Compiled Expression Calculators

*This is stage 4 of [query-lifecycle.md](../query-lifecycle.md) in depth: how MDX
expression trees become executable `Calc` objects. The producers of calcs — the
MDX function library — are covered in [olap-fun.md](olap-fun.md); the consumer —
`RolapResult` driving calcs against a `RolapEvaluator` — in [rolap.md](rolap.md).*

## Role: compile, don't interpret

Mondrian never interprets a parsed MDX expression at runtime. During
`Query#resolve` (statement preparation), every axis, slicer, and WITH formula
expression is compiled from its logical form (`mondrian.olap.Exp` AST) into a
physical form: a tree of `mondrian.calc.Calc` objects. A `Calc` is a small,
strongly typed, stateless evaluator — "give me an `Evaluator` (the current cell
context) and I return my value." Compilation happens once per prepared query;
evaluation happens once per cell (or axis position), millions of times for a
large result.

The compile step is where evaluation *strategy* is chosen: implicit conversions
(`[Store]` the hierarchy → its current member → the current cell value) become
explicit wrapper calcs, constants are folded into `ConstantCalc`, and set
expressions negotiate whether they will be delivered as materialized lists or
lazy iterables (see ResultStyle below).

The package splits cleanly: `mondrian.calc` holds the interfaces (`Calc` and
its typed subinterfaces, `ExpCompiler`, `ResultStyle`, the tuple collection
types), `mondrian.calc.impl` the implementations (abstract calc base classes,
the compilers, tuple list implementations). This fork carries **no `// PATCH:`
markers anywhere under `mondrian/calc`** — the package matches upstream
Mondrian 9.3 behavior.

## The `Calc` interface family

`Calc` itself (in `mondrian.calc.Calc`) has a small contract:

- `evaluate(Evaluator)` — compute the value in the given dimensional context;
- `dependsOn(Hierarchy)` — does the result change if that hierarchy's current
  member changes? (see Dependency tracking below);
- `getType()` — the `mondrian.olap.type.Type` of the result;
- `getResultStyle()` — how a set result is delivered;
- `accept(CalcWriter)` — serializes the calc tree; this is what query
  profiling prints as the "plan";
- `isWrapperFor` / `unwrap` — JDBC-style unwrapping for decorated calcs.

Typed subinterfaces add an unboxed/typed evaluate method so callers avoid
casting on every cell:

| Interface | Typed method | Yields |
|---|---|---|
| `MemberCalc` | `evaluateMember` | `Member` |
| `TupleCalc` | `evaluateTuple` | `Member[]` |
| `HierarchyCalc` | `evaluateHierarchy` | `Hierarchy` |
| `LevelCalc` | `evaluateLevel` | `Level` |
| `DimensionCalc` | `evaluateDimension` | `Dimension` |
| `StringCalc` | `evaluateString` | `String` |
| `IntegerCalc` | `evaluateInteger` | `int` |
| `DoubleCalc` | `evaluateDouble` | `double` |
| `BooleanCalc` | `evaluateBoolean` | `boolean` |
| `DateTimeCalc` | `evaluateDateTime` | `java.util.Date` |
| `VoidCalc` | `evaluateVoid` | nothing (side effects) |
| `IterCalc` | `evaluateIterable` | `TupleIterable` |
| `ListCalc` (extends `IterCalc`) | `evaluateList` | `TupleList` |

Set-valued expressions always yield *tuples*, even for single-hierarchy sets:
there are no separate "member list" calcs. The tuple collection model lives in
`mondrian.calc`:

- `TupleIterable` / `TupleIterator` / `TupleCursor` — lazy traversal.
  `TupleCursor` is the cheap protocol (`forward()`, `member(column)`,
  `setContext(Evaluator)`) that avoids allocating a `List<Member>` per step.
- `TupleList` — a materialized set; extends `List<List<Member>>` and adds
  `slice(column)`, `project`, `cloneList`, `addCurrent(cursor)`, `fix()`.
- Implementations in `calc.impl`: `UnaryTupleList` (arity 1, wraps a member
  list), `ArrayTupleList` and `ListTupleList` (tuples stored end-to-end in one
  flat array/list — both extend `AbstractEndToEndTupleList`), and
  `DelegatingTupleList` (backed by an actual list-of-lists).
- `TupleCollections` — static helpers: `createList(arity)`, `emptyList`,
  `unmodifiableList`, `asTupleList`, and `materialize(iterable, mutable)`,
  which `RolapResult` uses to turn an evaluated axis iterable into the final
  `RolapAxis`.

## ResultStyle: how set results are delivered

`ResultStyle` (an enum in `mondrian.calc`) answers "in what form does this calc
hand back its result": `LIST` (a list the caller must not mutate),
`MUTABLE_LIST` (a list the caller may sort/filter in place), `ITERABLE` (lazy;
possibly never materialized), `VALUE` / `VALUE_NOT_NULL` (scalars), and `ANY`
(caller doesn't care). The point is to avoid materializing huge sets: an axis
whose tuples are consumed once, in order, can stream through `ITERABLE`, while
`Order()` genuinely needs a `MUTABLE_LIST` it can sort.

Negotiation is compiler-mediated. The compiler carries a list of styles the
*consumer* will accept (`ExpCompiler#getAcceptableResultStyles`, preferred
first); the *producer* (a `FunDef`'s `compileCall`) inspects that list and
picks an implementation. Entry points fix the constraint:
`ExpCompiler#compileList(exp, mutable)` requests `MUTABLELIST_ONLY` or
`LIST_ONLY` and, if the expression can only be compiled as an iterable, wraps
it in an `IterableListCalc` (materialize-on-demand adapter);
`ExpCompiler#compileIter` requests `ITERABLE_ONLY`, falling back to `ANY`.
Axes are compiled via `QueryAxis#compile` with the query's default style —
`ITERABLE` (`Query#resultStyle`) — which is why axis evaluation streams until
`RolapResult` materializes it. A producer that cannot satisfy any acceptable
style throws `mondrian.olap.ResultStyleException` ("Producer expected … but
Consumer wanted …") — see `SetFunDef`, `CrossJoinFunDef`, `SumFunDef` for the
checking pattern.

## Compilation: `ExpCompiler` and friends

`ExpCompiler` (interface, `mondrian.calc`) mediates `Exp → Calc`. Its surface
is one generic method plus typed convenience methods: `compile(Exp)`,
`compileAs(exp, resultType, preferredResultStyles)`, and `compileMember` /
`compileLevel` / `compileHierarchy` / `compileDimension` / `compileTuple` /
`compileInteger` / `compileDouble` / `compileBoolean` / `compileString` /
`compileDateTime` / `compileList` / `compileIter` / `compileScalar`, plus
`registerParameter` (assigns each MDX `Parameter` a `ParameterSlot`).

The mechanics are the visitor pattern: `AbstractExpCompiler#compile` is just
`exp.accept(this)`, and each AST node dispatches back — crucially,
`ResolvedFunCall#accept(ExpCompiler)` is `funDef.compileCall(this, compiler)`.
So the function library, not the compiler, produces almost every calc; the
compiler's own job is **implicit conversions**. In `AbstractExpCompiler`:

- hierarchy → member: `HierarchyCurrentMemberFunDef.FixedCalcImpl` (hierarchy
  known at compile time) or `.CalcImpl` — the injected `<Hierarchy>.CurrentMember`;
- dimension → hierarchy: constant if the dimension has a unique default
  hierarchy, else a `DimensionHierarchyCalc`; member → hierarchy/level via
  `MemberHierarchyFunDef.CalcImpl` / `MemberLevelFunDef.CalcImpl`;
- member → scalar (`compileScalar`): `MemberValueCalc#create` — the compiled
  form of "set this member as context, then read the current cell";
  tuple → scalar: `TupleValueCalc` (then `#optimize`);
- numeric coercions (double→int, int→double, number→boolean) as small
  anonymous `Abstract*Calc` wrappers; constants folded via `ConstantCalc`.

`BetterExpCompiler` — the default, instantiated by
`ExpCompiler.Factory#getDefault` (overridable via the
`mondrian.calc.ExpCompiler` property / a thread-local, both test hooks) — adds
two things on top: `compileTuple` accepts a member expression and wraps it as
a 1-ary tuple, and `compileList(exp, mutable=true)` protects producers that
only offer an immutable `LIST` by wrapping them in a private `CopyListCalc`
(clones via `TupleList#cloneList` at evaluation time).

Compiler **decorators** plug in through `DelegatingExpCompiler`, which forwards
every compile method and offers an `afterCompile(exp, calc, mutable)` hook (it
re-wraps child expressions so recursion stays inside the decorator — see the
`WrapExp` comment). Two live users, both installed by `Query#createCompiler`:
`RolapUtil#createProfilingCompiler` (wraps calcs so the profiler can report
per-calc timing/counts) and `RolapUtil#createDependencyTestingCompiler` (a
test harness that verifies `dependsOn` claims by perturbing supposedly
irrelevant context, enabled by `MondrianProperties#TestExpDependencies`).

## `calc.impl` base classes worth knowing

- `AbstractCalc` — the root. Stores the source `Exp`, its `Type`, and the
  child `Calc[]`; default `dependsOn` is `anyDepends(getCalcs(), hierarchy)`;
  default result style `VALUE`. Also provides `anyDependsButFirst` and
  `butDepends` (for functions like `Aggregate({Set}, expr)` that *set* some
  hierarchies' context themselves) and `simplifyEvaluator` (resets context of
  non-depended-on hierarchies to their default member so cache keys collapse).
  `accept(CalcWriter)` + `collectArguments` produce the explain-plan output.
- `AbstractMemberCalc`, `AbstractDoubleCalc`, `AbstractListCalc`,
  `AbstractIterCalc`, … — one per typed interface; a `FunDef#compileCall`
  usually returns an anonymous subclass of one of these. `AbstractListCalc`
  takes a `mutable` flag that decides `LIST` vs `MUTABLE_LIST`;
  `AbstractIterCalc` reports `ITERABLE`.
- `GenericCalc` — implements *all* the typed interfaces over a single
  `evaluate(Evaluator)`, casting the result as needed (with a decent error
  message on type mismatch). `GenericIterCalc` is the set-side analog
  (implements both `ListCalc` and `IterCalc`; materializes an iterable into a
  list when a list is demanded). Use sparingly — a specific calc is cheaper.
- `ConstantCalc` — a folded constant; `dependsOn` is always false, result
  style `VALUE_NOT_NULL`/`VALUE`; static factories `constantInteger`,
  `constantMember`, `constantNull`, etc.
- `ValueCalc` / `MemberValueCalc` / `MemberArrayValueCalc` / `TupleValueCalc` —
  the "read the current cell" family. `ValueCalc#evaluate` is literally
  `evaluator.evaluateCurrent()` (and `dependsOn` everything); the others first
  push members into the context under `Evaluator#savepoint` / `#restore`.
  These are the leaves through which every base-measure read reaches
  `FastBatchingCellReader` in stage 5 of the query lifecycle.
- `CacheCalc` + `mondrian.olap.ExpCacheDescriptor` — expression result
  caching. The descriptor pairs a compiled calc with the ordinals of the
  hierarchies it `dependsOn`; `CacheCalc#evaluate` delegates to
  `Evaluator#getCachedResult(descriptor)`, and `RolapEvaluator#getCachedResult`
  builds a cache key from the expression plus *only the dependent hierarchies'
  current members*, computing and storing on miss. Results computed while the
  cell cache had misses are stored as provisional (invalid) so a later phase
  recomputes them. This mechanism backs the MDX `Cache()` function
  (`CacheFunDef`), `RankFunDef`, and compound-slicer aggregation in
  `RolapResult`.
- `IterableListCalc` — the iterable→list materializing adapter mentioned
  above; `DummyExp` — a placeholder `Exp` used when the compiler synthesizes a
  calc that has no source AST node but still needs a `Type`.

## Dependency tracking

`Calc#dependsOn(Hierarchy)` is a static analysis contract: "if only that
hierarchy's current member changed, could my result change?" Defaults
propagate up from children (`AbstractCalc#anyDepends`), and functions that
*set* context themselves (Filter, Aggregate, tuple value reads) subtract the
hierarchies they control (`anyDependsButFirst`). Accuracy matters in both
directions: claiming too much shrinks expression-cache hit rates (the
`ExpCacheDescriptor` key grows) and defeats `simplifyEvaluator`; claiming too
little returns stale cached values for the wrong context — which is exactly
what the dependency-testing compiler exists to catch.

## How a `FunDef` becomes a `Calc`

The unary minus operator, registered inline in `BuiltinFunTable#defineFunctions`,
shows the entire idiom — compile the arguments to typed child calcs, close over
them in an anonymous typed calc:

```java
builder.define(new FunDefBase("-", "Returns the negative of a number.", "Pnn") {
    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
        final DoubleCalc calc = compiler.compileDouble(call.getArg(0));
        return new AbstractDoubleCalc(call, new Calc[] {calc}) {
            public double evaluateDouble(Evaluator evaluator) {
                final double v = calc.evaluateDouble(evaluator);
                return (v == DoubleNull) ? DoubleNull : -v;
            }
        };
    }
});
```

Passing `new Calc[] {calc}` as the children is not decoration — it feeds the
default `dependsOn`, the profiler, and the explain plan. Every function in
[olap-fun.md](olap-fun.md) is a variation on this pattern, differing mainly in
which `compileXxx` it calls for arguments and which result styles it offers.

## Practical notes

- **New implicit conversions** belong in `AbstractExpCompiler` (the
  `compileMember`/`compileScalar`/... type-dispatch chains), not in individual
  functions; new *strategies* for an existing function belong in its
  `FunDef#compileCall`. Type-level conversion legality is decided earlier, at
  validation (see stage 3 of the query lifecycle) — the compiler assumes the
  types already line up.
- **ResultStyle mismatches** surface as `mondrian.olap.ResultStyleException`
  at compile time (statement preparation), not at evaluation. If you write a
  `compileCall` that inspects `getAcceptableResultStyles`, cover every style
  you claim to support and throw via `ResultStyleException#generate` otherwise.
- **Thread-safety / reuse:** a `Calc` tree is built once per prepared `Query`
  (`Query.axisCalcs`, `Query.slicerCalc`) and must be stateless — all mutable
  evaluation state (current members, savepoints, caches) lives in the
  `Evaluator`. Calcs closing over per-evaluation mutable fields are a bug, not
  a style issue: prepared statements are re-executable and formula calcs are
  shared across phases of the same execution.
