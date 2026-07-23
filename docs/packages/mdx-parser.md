# mondrian.mdx / mondrian.parser — the MDX parser and AST

*How MDX text becomes a `mondrian.olap.Query` whose expressions are
`mondrian.mdx` nodes. This is the detail behind stage 2 of
[query-lifecycle.md](../query-lifecycle.md); the `Query`/`Validator` object
model is in [olap.md](olap.md), and what happens to the AST after resolution —
compilation into `Calc` trees — is in [calc.md](calc.md).*

## Role

`mondrian.parser` turns an MDX string into a `mondrian.olap.QueryPart` —
usually a `Query`, but the grammar also accepts `DRILLTHROUGH` and
`EXPLAIN PLAN FOR` statements (`DrillThrough`, `Explain`). `mondrian.mdx`
supplies the typed expression nodes that populate the query's axes, slicer,
and WITH formulas. Parsing happens at **statement-prepare time**, not execute
time: `MondrianOlap4jStatement#parseQuery` calls the native
`ConnectionBase#parseStatement` inside a `Locus.execute` block, and the
`Query` constructor immediately resolves and compiles itself, so a prepared
statement never re-parses.

The two packages split cleanly: `mondrian.parser` knows syntax,
`mondrian.mdx` knows structure. Neither knows the schema — binding names to
cubes/members is resolution (stage 3), not parsing.

## The parser side

### Grammar and generated sources

The live grammar is **`parser/MdxParser.jj`** (JavaCC, options
`IGNORE_CASE`, `UNICODE_INPUT`, non-static). It is *not* compiled directly:
at every build the Maven `generate-sources` phase (the antrun execution in
`mondrian/pom.xml` invoking the `parser` target of `mondrian/build.xml`) runs
JavaCC 5.0 over it and emits `MdxParserImpl`, `MdxParserImplTokenManager`,
`MdxParserImplConstants`, `ParseException`, `Token`, etc. into
`mondrian/src/generated/java/mondrian/parser/`. That directory is
**gitignored and deleted by `mvn clean`** — generated sources are never
checked in; `build-helper-maven-plugin` adds `src/generated/java` to the
compile source roots. So after editing the grammar, a plain `mise package`
regenerates everything; there is no separate "regenerate parser" step to
forget.

### The legacy CUP parser — mostly dead, one live limb

`olap/Parser.cup` is the pre-2011 JavaCUP grammar. The same build `parser`
target still runs JavaCUP over it (generating `Parser` and `ParserSym` into
`src/generated/java/mondrian/olap/`), and it cannot be deleted, because
**`Parser.FactoryImpl` — the default `QueryPartFactory` — is a nested class
defined inside `Parser.cup`** and therefore lives on the CUP-generated
`Parser` class. `JavaccParserValidatorImpl`'s no-arg constructor instantiates
`new Parser.FactoryImpl()`, so every query parse touches the CUP-generated
class, even though its parse tables are never exercised.

Everything else on the CUP path is dead code:

- `ConnectionBase#createParser` is hardcoded
  `true ? new JavaccParserValidatorImpl() : new MdxParserValidatorImpl()` —
  the CUP-backed `MdxParserValidatorImpl` is unreachable.
- `olap/Scanner.java` and `olap/StringScanner.java` (the CUP lexer) are
  reachable only from that dead path, plus two static comment-delimiter
  lookups in the legacy `mondrian.tui.CmdRunner`.

### Class cast

| Class | Role |
|---|---|
| `MdxParserValidator` | Interface: `#parseInternal` (whole statement) and `#parseExpression` (bare expression). Defines the nested `QueryPartFactory` (`#makeQuery`, `#makeDrillThrough`, `#makeExplain`). |
| `JavaccParserValidatorImpl` | The live implementation. Builds an `MdxParserImpl`, invokes its `statementEof()` / `expressionEof()` production, and rewrites JavaCC's `ParseException` into a `MondrianException` in the old JavaCUP "Syntax error at line L, column C, token 'T'" format (see `#convertException`). |
| `MdxParserImpl` (generated) | The recursive-descent parser. Grammar actions build `mondrian.mdx` / `mondrian.olap` nodes directly and call the `QueryPartFactory` for the top-level statement objects. |
| `MdxParserImplTokenManager` (generated) | The lexer. |
| `Parser.FactoryImpl` (in `olap/Parser.cup`) | Default `QueryPartFactory`; `#makeQuery` wraps the slicer expression in a `QueryAxis` and constructs the `Query`. Subclass it (and pass it to `JavaccParserValidatorImpl#<init>`) to substitute your own `Query` subclass — see `MdxParserValidator.QueryPartFactory` javadoc. |
| `MdxParserValidatorImpl` | Dead CUP-backed twin of `JavaccParserValidatorImpl`. |

`ConnectionBase#parseStatement` supplies two parse-time inputs:

- **`funTable`** (defaulting to `Schema#getFunTable`) is consulted *during
  parsing* in exactly two places: `MdxParserImpl#createCall` calls
  `FunTable#isProperty` to decide whether `x.Foo` is a property call
  (`UnresolvedFunCall` with `Syntax.Property`) or just another `Id` segment,
  and `Id#accept(Validator)` later uses `FunTable#isReserved` for keyword
  literals like `ASC`/`DESC`. Passing a custom fun table changes what parses.
- **`strictValidation`** is not used by the grammar itself — it is stored on
  the `Query`, where `Query#ignoreInvalidMembers` returns false when it is
  set, overriding the `IgnoreInvalidMembers` /
  `IgnoreInvalidMembersDuringQuery` properties. The olap4j path always passes
  `false`.

`Connection#parseExpression` (same machinery, `expressionEof()` production)
is how schema-defined formulas get parsed outside a query: calculated
members/named sets in the schema XML (`RolapSchema`), parameter default
expressions (`RolapSchemaParameter`), and the internal MDX rewriting done by
`NativizeSetFunDef`.

## The AST node side

`mondrian.olap.Exp` is the expression interface; `mondrian.olap.ExpBase` the
base class; literals are `mondrian.olap.Literal` — all in `mondrian.olap`,
not `mondrian.mdx` (see [olap.md](olap.md)). The `mondrian.mdx` package holds
the composite and leaf node types:

| Class | Role |
|---|---|
| `UnresolvedFunCall` | Every operator, function, method call, `{...}`, `(...)`, and property access as parsed — just a name + `Syntax` + args. Its `#accept(Validator)` resolves it; its `#accept(ExpCompiler)` throws: an unresolved call must never reach compilation. |
| `ResolvedFunCall` | The validated replacement: args + return `Type` + the chosen `FunDef`. `#accept(ExpCompiler)` delegates to `FunDef#compileCall`. Carries a fork-added `timingName` field used by profiling to label calculated members (fork PATCH). |
| `Id` | A not-yet-bound identifier: a list of `Id.Segment`s. `Id.NameSegment` is a name with a `Quoting` (`UNQUOTED`, `QUOTED`); `Id.KeySegment` is the `&[key]` form (`Quoting.KEY`), possibly composite (`&[k1]&[k2]`), built by the grammar's `keyIdentifier()` production from `AMP_QUOTED_ID` tokens. |
| `MemberExpr`, `LevelExpr`, `HierarchyExpr`, `DimensionExpr`, `NamedSetExpr` | Bound leaves: direct usages of a schema object as an expression. Created during resolution (`Util#createExpr`), never by the parser. `NamedSetExpr` is also what a WITH SET reference resolves to; at compile time it evaluates through `Evaluator#getNamedSetEvaluator` so a named set is computed once per query. |
| `ParameterExpr` | Usage of a `Parameter`. `Parameter(...)` calls define one; `ParamRef(...)` references it; validation unifies both onto one `Parameter` object via `SchemaReader#getParameter` / the validator's parameter registry. |
| `MdxVisitor` / `MdxVisitorImpl` | Visitor over the whole parse tree (13 `visit` overloads: `Query`, `QueryAxis`, `Formula`, both fun-call kinds, `Id`, the five bound leaves, `ParameterExpr`, `Literal`). `MdxVisitorImpl` returns null everywhere and auto-descends; call `#turnOffVisitChildren` inside a `visit` to prune (the flag resets after each node). Used heavily by the native-evaluation analyzers and `Query` internals. |
| `QueryPrintWriter` | `PrintWriter` used when unparsing a whole query; tracks which `Parameter`s have printed so the first occurrence unparses as `Parameter(...)` and later ones as `ParamRef(...)`. |

The parser emits only `Id`, `UnresolvedFunCall`, `Literal`, and the
statement/axis/formula skeleton (`Query`, `QueryAxis`, `Formula`,
`MemberProperty`, cell properties). Every other node type appears during
resolution.

## What resolution does to the tree

`Query#resolve` (stage 3 of [query-lifecycle.md](../query-lifecycle.md))
rewrites the tree in place via `Exp#accept(Validator)`: each `Id` is looked
up against the schema (`Util#lookup`, pre-batched by `IdBatchResolver`) and
replaced by the matching bound leaf — `MemberExpr`, `LevelExpr`,
`HierarchyExpr`, `DimensionExpr`, or `NamedSetExpr` (single unquoted segments
matching `FunTable#isReserved` become symbol `Literal`s, e.g. `BDESC`); each
`UnresolvedFunCall` picks a `FunDef` through `FunUtil#resolveFunArgs` and
becomes the `ResolvedFunCall` returned by `FunDef#createCall` (which may
itself rewrite further — some fun-defs substitute a different expression).
After resolution the tree contains no `Id` or `UnresolvedFunCall` nodes, and
only then is it compilable.

## Unparsing

Every `Exp` can print itself back as MDX: `Exp#unparse(PrintWriter)`,
aggregated by `Query#unparse` and wrapped by `Util#unparse`.
`Query#toString` calls `resolve()` first, so what you get back is the
**resolved** form — unique names, canonical function rendering
(`ResolvedFunCall#unparse` delegates to `FunDef#unparse`) — not the original
text; don't expect round-trip fidelity of whitespace, comments, or the exact
identifiers the user typed. Consumers in the engine: MDX debug logging
(`RolapConnection#execute` and the `mondrian.mdx` logger), execution
monitoring (`Execution#getMdx`), and error messages. Parameters unparse
through `QueryPrintWriter` as described above.

## Practical notes

- **Changing syntax** touches three places: the grammar (`MdxParser.jj` —
  add tokens/productions), possibly a new AST node in `mondrian.mdx` (extend
  `ExpBase`, implement `#accept(Validator)`, `#accept(ExpCompiler)`,
  `#accept(MdxVisitor)` — which also means a new `MdxVisitor` method — and
  `#unparse`), and, for new top-level statement kinds, the
  `QueryPartFactory` interface plus `Parser.FactoryImpl` in `Parser.cup`.
- **Regeneration is automatic**: `mise package` cleans and regenerates
  `src/generated`; never edit generated files. If you change `Parser.cup`,
  note the build passes `expect="63"` to JavaCUP (63 tolerated conflicts) —
  keep the CUP grammar compiling even though only `FactoryImpl` matters.
- **Testing**: per `AGENTS.md`, use the JRuby integration tests (`rake
  test`), not the legacy Java suite. Grammar-level behavior is exercised by
  executing MDX strings through the gem (`test/connection_test.rb` and
  friends); a parse failure surfaces as `Mondrian::OLAP::Error` wrapping
  `mondrian.olap.MondrianException` ("Syntax error at line …").
- The parser is per-parse-call state (`MdxParserImpl` is constructed fresh in
  `JavaccParserValidatorImpl#parseInternal`), so there are no threading
  concerns here; `STATIC=false` in the grammar options is what makes that
  possible.
