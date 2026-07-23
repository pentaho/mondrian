# Native Set Evaluation: pushing NonEmptyCrossJoin, Filter and TopCount into SQL

*How Mondrian decides that a set expression — a NON EMPTY crossjoin, a
`Filter`, a `TopCount` — can be computed as one constrained SQL statement
instead of being iterated in memory, how that SQL is built and executed, and
why a given query silently falls back to in-memory evaluation. Builds on
[../query-lifecycle.md](../query-lifecycle.md) (stage 5); the member readers
that receive the results are covered in
[member-resolution.md](member-resolution.md), and the SQL assembly layer in
[sql-generation.md](sql-generation.md).*

## 1. The idea and the hook

A crossjoin of two large levels is a cartesian product: `[Product].[Name].Members ×
[Customer].[Name].Members` may be 10⁵ × 10⁴ tuples, of which only a few
thousand have fact data. Evaluating that in memory — materialize both member
lists, cross them, then probe every cell for emptiness — is hopeless. But NON
EMPTY semantics *is* a join to the fact table: the surviving tuples are exactly
the combinations that have a fact row under the current slicer. So the whole
set operation can be one SQL statement — dimension tables joined to the fact
table, WHERE conditions from the evaluation context — and the same is true for
`Filter` over a measure condition (a `HAVING` clause) and `TopCount` over a
measure (an `ORDER BY` plus a row limit).

The hook is not at compile time but at evaluation time. The compiled `Calc`
bodies of the candidate functions all start with the same probe — see
`CrossJoinFunDef.CrossJoinIterCalc#evaluateIterable` (and its list variant),
`NonEmptyCrossJoinFunDef#compileCall`, `FilterFunDef.BaseIterCalc#evaluateIterable`
/ `BaseListCalc#evaluateList`, `TopBottomCountFunDef#compileCall`, and
`DrilldownLevelTopBottomFunDef#compileCall`:

```java
NativeEvaluator nativeEvaluator =
    schemaReader.getNativeSetEvaluator(call.getFunDef(), call.getArgs(), evaluator, this);
if (nativeEvaluator != null) {
    return (TupleList) nativeEvaluator.execute(ResultStyle.LIST);
}
// ... otherwise the ordinary in-memory implementation runs
```

`RolapSchemaReader#getNativeSetEvaluator` first checks
`Evaluator#nativeEnabled` (seeded in the `RolapEvaluator` constructor from
`EnableNativeNonEmpty || EnableNativeCrossJoin`, and toggled by
`NativizeSetFunDef` around its subtrees), then delegates to the schema's
`RolapNativeRegistry#createEvaluator`. The contract, defined on the abstract
`RolapNative` factory: return a `mondrian.olap.NativeEvaluator` if the call
*with these arguments in this context* can be done in SQL, otherwise return
`null` and let the interpreter compute the result. A returned evaluator is the
inner class `RolapNativeSet.SetEvaluator`; its `execute(ResultStyle)` produces
the finished `TupleList`/`TupleIterable`.

## 2. The registry and its enable flags

`RolapNativeRegistry` (one per `RolapSchema`, field `RolapSchema.nativeRegistry`)
is a chain-of-responsibility keyed by upper-cased function name:

| Key | Implementation | Enabled by |
|---|---|---|
| `NONEMPTYCROSSJOIN`, `CROSSJOIN` | `RolapNativeCrossJoin` | `EnableNativeCrossJoin` (`mondrian.native.crossjoin.enable`, default true) |
| `TOPCOUNT` | `RolapNativeTopCount` | `EnableNativeTopCount` (`mondrian.native.topcount.enable`, default true) |
| `FILTER` | `RolapNativeFilter` | `EnableNativeFilter` (`mondrian.native.filter.enable`, default true) |

Functions not in the map (including `BottomCount`: `RolapNativeTopCount`
contains a `BottomCount` branch, but no registry entry ever routes that name
to it, so `BottomCount` always runs in memory) get `null` immediately. There
is no `RolapNativeSum` in this fork.

Two related properties live *outside* the registry:

- **`EnableNativeNonEmpty`** (`mondrian.native.nonempty.enable`) does not gate
  a `RolapNative` — it gates `SqlConstraintFactory`, which decides whether
  ordinary member reads (`member.Children`, `level.Members`) performed during
  NON EMPTY evaluation use a `SqlContextConstraint` (context-constrained SQL)
  instead of the unconstrained default. Same machinery, different entry point;
  see [member-resolution.md](member-resolution.md).
  `SqlConstraintFactory#useDefaultTupleConstraint` also skips the constrained
  path when the level cardinality is below `LevelPreCacheThreshold` — for
  small levels it is cheaper to cache all members once than to run a
  per-context SQL query.
- **`AlertNativeEvaluationUnsupported`** (`mondrian.native.unsupported.alert`,
  default `OFF`) controls `RolapUtil#alertNonNative`: `WARN` logs, `ERROR`
  throws `NativeEvaluationUnsupportedException`. Alerts fire only where native
  evaluation was plausible but blocked by a limitation — and for crossjoins
  only when the query author wrote an explicit `NonEmptyCrossJoin`
  (`RolapNativeCrossJoin#alertCrossJoinNonNative`), rate-limited per query by
  `Query#shouldAlertForNonNative`.

## 3. Applicability analysis: will my query run natively?

The heart of the decision is turning the set argument into an array of
`CrossJoinArg`s — "light constraints", one per hierarchy, each knowing its
level and how to contribute SQL (`CrossJoinArg#addConstraint`). Two concrete
kinds exist:

- `DescendantsCrossJoinArg` — `level.Members` (member == null),
  `member.Children`, `Descendants(member, level)`; constrains by level (and
  optionally by ancestor member).
- `MemberListCrossJoinArg` — an explicit enumeration `{m1, m2, …}`, which
  becomes an `IN` predicate.

`CrossJoinArgFactory#checkCrossJoinArg` recognizes exactly these input shapes
(recursing through `{}` braces, named sets, `NativizeSet`, and nested
`CrossJoin`/`NonEmptyCrossJoin` calls via `#checkCrossJoin`):
`member.Children`, `level.Members`, `Descendants(member, level|depth)`, member
enumerations, and `Filter(set, <membership predicate>)` — where the predicate
is `CurrentMember IS m` / `CurrentMember IN {…}` (optionally through
`Ancestor(...)`, `NOT`, `AND`, parentheses; see `#checkFilterPredicate`).
A recognized dimension filter contributes a *second* array of predicate-only
`CrossJoinArg`s that constrain the SQL without appearing in the SELECT list.

What disqualifies an argument (each returns `null`, which vetoes the whole
expression unless `ExpandNonNative` applies):

- **Calculated members.** A calculated member in `member.Children` /
  `Descendants` input, or in an enumeration when the native class demands
  `restrictMemberTypes()` (`Filter` and `TopCount` do; `CrossJoin` does not —
  it tolerates them and filters afterwards). Parent-child leaf members are the
  exception (`MemberListCrossJoinArg#create`).
- **Non-"simple" levels** — `RolapLevel#isSimple` excludes the All level and
  parent-child levels (an enumeration of *only* parent-child leaves is allowed).
- **Role-based access control.** Children/level-members under a hierarchy with
  `CUSTOM` access and `RollupPolicy.FULL`, or any non-`ALL` access for
  `Descendants` (`CrossJoinArgFactory#checkMemberChildren` / `#checkLevelMembers`
  / `#checkDescendants`).
- **Oversized enumerations** — more members than `MaxConstraints` (the `IN`
  list limit; `MemberListCrossJoinArg#isArgSizeSupported`).
- **Mixed levels** in one enumeration.

If a crossjoin input is not natively recognizable, `checkCrossJoin` can
*evaluate it in memory* and wrap the resulting member list as a
`MemberListCrossJoinArg` (`#expandNonNative`) — but only when
`ExpandNonNative` is set (default false) or the set is trivially cheap
(`#isCheapSet`: literal members plus `LastChild`/`FirstChild`/`Lag`), and with
cycle protection via `RolapEvaluator#getActiveNativeExpansions`.

On top of argument analysis, each `RolapNative` subclass applies its own gate
in `createEvaluator`:

- **`RolapNativeCrossJoin`** requires: the evaluator in NON EMPTY mode
  (`Evaluator#isNonEmpty` — a plain `CrossJoin` is only native on a NON EMPTY
  axis; the fact-table join would silently drop empty tuples otherwise); not
  *all* args degenerate (all-All-member or all-empty enumerations, any
  calc-member enumeration poisons the count); no conflict between calculated
  measures and the crossjoin hierarchies
  (`SqlConstraintUtils#measuresConflictWithMembers`); for virtual cubes,
  `Query#nativeCrossJoinVirtualCube()` must still be true (cleared at
  validation time by `FunUtil#checkNativeCompatible` when the query applies a
  member-producing function to the `[Measures]` dimension — e.g. `AllMembers`,
  ranges, `Lag` — whose results can't be known without executing it);
  and `SqlContextConstraint#isValidContext` with the referenced levels.
- **`RolapNativeFilter`** requires `isValidContext(evaluator, strict=true)` in
  its *virtual-cube-disallowing* form — **native Filter and TopCount never run
  against virtual cubes** (`SqlContextConstraint#isValidContext(context,
  disallowVirtualCube=true, …)`); the condition must compile to SQL through
  `RolapNativeSql#generateFilterCondition`; the context must not contain
  non-expandable calculated members; and a final dry-run
  (`FilterConstraint#isSuported` [sic]) generates the constraint into a test
  `SqlQuery` and checks `SqlQuery#isSupported` — which
  `SqlConstraintUtils#generateSingleValueInExpr` clears when a member
  predicate would need an `IN` list longer than `MaxConstraints` on a dialect
  without `Dialect#supportsUnlimitedValueList`.
- **`RolapNativeTopCount`** requires the same strict, non-virtual context; the
  count must be a `Literal` (a computed count falls back); the 3rd-argument
  ranking expression must compile via `RolapNativeSql#generateTopCountOrderBy`;
  and the two-argument form (no ranking expression) is only valid for a single
  crossjoin arg without calc members (`TopCountConstraint#isValid` — joining
  multiple dimensions through the fact table would drop empty tuples that the
  2-arg form must keep).

`RolapNativeSql` defines the translatable expression language: numeric
literals, stored measures (rendered as their aggregate, e.g.
`sum("store_sales")`, possibly remapped onto an aggregate table), calculated
members expanded through their formula, `+ - * /`, `IIF`, comparisons
`< <= > >= = <>`, `IsEmpty` (→ `IS NULL`), `AND/OR/NOT`, parentheses, and
`MATCHES` when `Dialect#allowsRegularExpressionInWhereClause`. Anything
else — `CASE`, string functions, `Val()`, member navigation inside the
condition — makes `compile` return null and vetoes native evaluation. All
referenced stored measures must live in the same star
(`RolapNativeSql#saveStoredMeasure`).

Finally there is a cost check: `RolapNativeSet#isPreferInterpreter` skips
native evaluation when *all* args prefer the interpreter — a
`MemberListCrossJoinArg` always does outside a join context (the members are
already known), and inside one when it holds only calculated members. This is
why `Filter({m1, m2}, …)` or `TopCount({m1, m2}, …)` over a plain enumeration
is never native: there is nothing SQL could prune.

## 4. Execution: SetConstraint → SqlTupleReader → member caches

When all gates pass, the native class saves the evaluator state
(`RolapEvaluator#savepoint`), calls `RolapNativeSet#overrideContext` — resetting
the hierarchies used by the args to their All member (or an access-restricted
substitute) so the outer context can't contradict the args, and pinning one
stored measure into context so the fact-table star can be reached — and builds
a **`SetConstraint`**, the `TupleConstraint` that carries everything into SQL
generation:

- `SetConstraint extends SqlContextConstraint`: the superclass captures the
  evaluation context at construction (`SqlContextConstraint#<init>` pushes the
  evaluator, expands multi-position slicers and supported calculated members,
  and folds role restrictions and virtual-cube base cubes into the cache key).
  At SQL time, `SqlContextConstraint#addConstraint` →
  `SqlConstraintUtils#addContextConstraint` turns the current member of every
  non-All hierarchy into WHERE predicates — the same mechanism that makes a
  cell request; the slicer becomes SQL for free.
- `SetConstraint#addConstraint` additionally invokes each `CrossJoinArg`'s own
  `addConstraint` (ancestor/`IN` predicates), skipping args whose enumeration
  contains calculated members — those are joined in Java afterwards.
- For a plain `CrossJoin` (not the explicit `NonEmptyCrossJoin` function),
  `RolapNativeCrossJoin#buildConstraint` additionally harvests recognizable
  set expressions from *all* query axes
  (`CrossJoinArgFactory#buildConstraintFromAllAxes`, deduplicated per
  hierarchy) and folds them in as extra predicates, so the SQL is constrained
  by what the other axes display.
- `isJoinRequired` decides whether the fact table participates: for a
  crossjoin, always when there is more than one arg or the context demands it;
  for `Filter`, when the condition references a measure or the evaluator is
  non-empty (`FilterConstraint#isJoinRequired` walks the expression with an
  `MdxVisitor`); for `TopCount`, exactly when a ranking expression exists.

Subclass decoration: `FilterConstraint#addConstraint` adds the compiled
condition as a `HAVING` clause; `TopCountConstraint#addConstraint` adds the
ranking expression to the SELECT list and `ORDER BY` (asc for `BottomCount`,
desc for `TopCount`, with nullability deduced — a distinct-count measure is
never null — via `#deduceNullability`).

`RolapNativeSet.SetEvaluator#execute` then drives
`SqlTupleReader` (see [member-resolution.md](member-resolution.md) for the
reader itself, [sql-generation.md](sql-generation.md) for `SqlQuery`):

1. One `Target` per `CrossJoinArg` level (`SetEvaluator#addLevel`); args whose
   enumerations contain calc members become *enum targets* whose members are
   crossed in Java with the SQL rows instead of being fetched.
2. `SqlTupleReader#readMembers` (single arg) or `#readTuples` (crossjoin)
   generates the statement in `#makeLevelMembersSql` /
   `#generateSelectForLevels`: all target levels' key/ordinal/caption/property
   columns in one SELECT, constraint SQL added around them, an `AggStar`
   chosen if an aggregate table covers the needed columns
   (`SqlTupleReader#chooseAggStar`, honored only when
   `TupleConstraint#supportsAggTables` — `TopCountConstraint` refuses agg
   tables for the 2-arg form). For **virtual cubes** the targets are grouped
   by base cube and the per-cube SELECTs are combined with `UNION`
   (`#groupTargets`; crossjoin-native against virtual cubes is the one case
   `RolapNativeCrossJoin` permits, using the base cubes recorded by
   `SqlContextConstraint#isValidContext`).
3. Row consumption (`Target#internalAddRow`) resolves each key against the
   hierarchy's member cache and creates missing members through the
   `MemberBuilder` — so natively fetched members land in the same
   schema-lifetime caches ordinary member resolution uses. Deliberately *not*
   cached are children lists: `SetConstraint#getMemberChildrenConstraint`
   returns null, because a fact-constrained crossjoin sees only a subset of
   any parent's children and caching that subset would poison later reads.
4. `maxRows` (set to the count for TopCount) is applied as JDBC
   `Statement#setMaxRows` (`RolapUtil#executeQuery` → `SqlStatement#execute`),
   not as a dialect `LIMIT` clause; `ResultLimit` and
   `Util#checkCJResultLimit` guard runaway result sizes.
5. Post-processing: `SetEvaluator#filterInaccessibleTuples` re-applies role
   visibility (hidden ragged members, `Access.CUSTOM` hierarchies) that SQL
   could not express — results were cached *below* the role-aware readers.

NON EMPTY interplay: native evaluation is the "pushed-down" branch of NON
EMPTY axis handling. `RolapResult` sets `Evaluator#setNonEmpty` per axis, which
both unlocks `RolapNativeCrossJoin` and switches ordinary member reads to
`SqlContextConstraint`s; whatever emptiness SQL could not prove (calculated
members ignored by a non-strict constraint) is cleaned up afterwards by
`RolapConnection.NonEmptyResult`. That layering is also the source of the
documented semantic looseness: a native result may legitimately contain *more*
tuples than a strict in-memory evaluation at the constraint level, relying on
the later filter. For TopCount in a non-NON-EMPTY context,
`SetEvaluator#setCompleteWithNullValues` pads a short fact-joined result with
a second, exclusion-constrained query (`MemberExcludeConstraint`) so the count
is honored even when fewer than N members have data — note the padded members
are appended, and the class javadoc of `RolapNativeSet` carries a standing
upstream TODO that native result ordering can differ from enumeration order.

## 5. The native result cache

Each `RolapNativeSet` subclass instance owns a
`SmartCache<Object, TupleList>` (field `RolapNativeSet.cache`, a
`SoftSmartCache` by default; `useHardCache` switches it for tests). Since the
three natives live in the schema's registry, the cache is **per schema, per
function family**, shared across connections and queries.

The key (`SetEvaluator#executeList`) is: the tuple reader's cache key —
`SqlTupleReader#getCacheKey` wraps `TupleConstraint#getCacheKey`, which for a
`SetConstraint` includes the expanded context members, slicer tuples, role
restrictions, the args themselves, and for Filter/TopCount the condition
string, ascending flag, count, and non-empty flag — plus the args, `maxRows`,
and the caller's `Role` (roles are in the key because caching happens beneath
the role-applying readers). A hit skips SQL entirely (listener event
`foundInCache`); with enum targets only the SQL-sourced partial result is
cached and the enumerated members are re-crossed per query. Population is
skipped when `DisableCaching` is set.

Flushing: `RolapNativeRegistry#flushAllNativeSetCache` clears all three caches;
it is invoked from `CacheControlImpl#flush(MemberSet)` before member-cache
surgery. There is no finer-grained invalidation — any member flush drops every
native set result for the schema.

## 6. Observability and typical fallbacks

- `LOGGER.debug` on category `mondrian.rolap.RolapNativeSet` prints
  `using native crossjoin` / `using native filter` / `using native topcount`
  when an evaluator is created; `mondrian.rolap.sql.CrossJoinArgFactory` logs
  `using native dimension filter` for recognized membership predicates.
- The SQL itself appears on the `mondrian.sql` debug logger, tagged
  `SqlTupleReader.readTuples ...` (the `SqlStatement` locus), which is the
  reliable way to confirm a query ran natively.
- Set `AlertNativeEvaluationUnsupported=WARN` (or `ERROR`) to surface blocked
  cases; remember it only fires for explicit `NonEmptyCrossJoin` and TopCount
  paths that chose to alert.

The most common silent fallbacks, in rough order: the axis is not NON EMPTY
(crossjoin); a calculated member appears in the set argument (Filter/TopCount)
or everywhere (crossjoin); the Filter/TopCount expression uses a function
`RolapNativeSql` cannot compile; the cube is virtual (Filter/TopCount always;
crossjoin when unsupported `[Measures]` functions were used); the slicer
contains an unsupported calculated member (`SqlContextConstraint#isValidContext`);
access control (`RollupPolicy.FULL` on a custom-access hierarchy); an
enumeration exceeding `MaxConstraints`; a non-literal TopCount count; or the
set is a plain member enumeration that SQL could not prune anyway
(`isPreferInterpreter`).

## 7. Fork PATCHes in this area

Grep `PATCH:` for the authoritative list; the ones that affect native
evaluation:

- **`SqlTupleReader#generateSelectForLevels` / `#makeLevelMembersSql`** — the
  context constraint is applied *before* the level SELECT columns so level
  tables are joined last (excluding `TopCountConstraint`, whose `SUM(...)`
  ORDER BY expression must stay after the level columns); per-base-cube
  SELECTs that produced no constraint for a virtual-cube UNION are skipped
  instead of emitted unconstrained, and if no SELECT is generated at all an
  empty-tuple SQL is returned (`#sqlForEmptyTuple`). This prevents unrelated
  base cubes of a virtual cube from flooding the UNION with unconstrained rows.
- **`SqlContextConstraint#addedConstraint`** (fork field) records whether
  `addConstraint` actually added anything, which is what the SELECT-skipping
  logic above consults; `SqlConstraintUtils#addContextConstraint` was changed
  to return that boolean.
- **`SqlContextConstraint#findVirtualCubeBaseCubes`** — when a query references
  only calculated measures, the cube's default stored measure is added so a
  base cube can still be found (upstream returned no base cubes and vetoed
  native evaluation for such virtual-cube queries).
- **`NonEmptyCrossJoinFunDef`** — profiling timers
  (`Evaluator#getTiming` mark/end) around evaluation, so native vs in-memory
  crossjoin time shows up in query profiles.

The native classes themselves (`RolapNative*`, `CrossJoinArg*`) are unpatched
upstream code in this fork.
