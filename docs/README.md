# Mondrian Engine Documentation

Developer and AI-agent oriented documentation for this Mondrian OLAP engine fork
(based on Pentaho Mondrian 9.3.0.0, maintained as the core Java library for the
[mondrian-olap](https://github.com/rsim/mondrian-olap) JRuby gem).

The goal of these documents is to explain **how the engine is structured and how
data flows through it when an MDX query is executed** — the semantic layer that
javadocs alone don't provide. They complement the source code; they do not
replace reading it.

## Start here

Read in this order:

1. **[architecture.md](architecture.md)** — the big picture: layers, packages,
   the main runtime objects and who owns whom.
2. **[query-lifecycle.md](query-lifecycle.md)** — the spine document. Traces one
   MDX statement end to end: parse → resolve → compile → axis evaluation →
   batched cell loading → result. Every other document hangs off this one.
3. The package and topic documents below, as needed.

## Document map

### Explanation

| Document | Covers | Status |
|---|---|---|
| [architecture.md](architecture.md) | Layers, package map, runtime object ownership, core design patterns | ✅ |
| [query-lifecycle.md](query-lifecycle.md) | End-to-end execution path of an MDX statement | ✅ |

### Package guides (`packages/`)

One document per significant package: what lives there, the central classes,
and how the package relates to its neighbors.

| Document | Package(s) | Status |
|---|---|---|
| [packages/olap.md](packages/olap.md) | `mondrian.olap` — connection/query/schema object model | ✅ |
| [packages/olap-fun.md](packages/olap-fun.md) | `mondrian.olap.fun` — the MDX function library | ✅ |
| [packages/mdx-parser.md](packages/mdx-parser.md) | `mondrian.mdx`, `mondrian.parser` — AST nodes and JavaCC parser | ✅ |
| [packages/calc.md](packages/calc.md) | `mondrian.calc` — compiled expression calculators | ✅ |
| [packages/rolap.md](packages/rolap.md) | `mondrian.rolap` — the relational OLAP engine | ✅ |
| [packages/rolap-agg.md](packages/rolap-agg.md) | `mondrian.rolap.agg` — cell batching, segments, SQL loading | ✅ |
| [packages/rolap-aggmatcher.md](packages/rolap-aggmatcher.md) | `mondrian.rolap.aggmatcher` — aggregate table recognition | ✅ |
| [packages/spi.md](packages/spi.md) | `mondrian.spi` — dialects and other service provider interfaces | ✅ |
| [packages/server.md](packages/server.md) | `mondrian.server` — statements, executions, Locus, monitoring | ✅ |
| [packages/olap4j.md](packages/olap4j.md) | `mondrian.olap4j` — the olap4j driver adapter | ✅ |
| [packages/xmla.md](packages/xmla.md) | `mondrian.xmla` — XML for Analysis endpoint | ✅ |
| [packages/util.md](packages/util.md) | `mondrian.util` — utility classes worth knowing about | ✅ |

### Topic deep dives (`topics/`)

Cross-cutting subsystems that span packages.

| Document | Covers | Status |
|---|---|---|
| [topics/schema-loading.md](topics/schema-loading.md) | Schema XML → `RolapSchema`, schema pool, star construction | ✅ |
| [topics/member-resolution.md](topics/member-resolution.md) | MemberReader chain, `SqlMemberSource`, member caches | ✅ |
| [topics/cell-batching.md](topics/cell-batching.md) | `FastBatchingCellReader` → `CellRequest` → `Segment` pipeline | ✅ |
| [topics/sql-generation.md](topics/sql-generation.md) | `SqlQuery`, query specs, predicates, `Dialect` abstraction | ✅ |
| [topics/caching.md](topics/caching.md) | Member caches vs segment caches vs `CacheControl` flushing | ✅ |
| [topics/native-evaluation.md](topics/native-evaluation.md) | Native NonEmpty/TopCount/Filter/CrossJoin SQL pushdown | ✅ |
| [topics/aggregate-tables.md](topics/aggregate-tables.md) | Aggregate table matching and usage | ✅ |
| [topics/fork-changes.md](topics/fork-changes.md) | Catalog of `PATCH:` deltas vs upstream Mondrian 9.3 | ✅ |

### Class reference (`reference/`)

Javadoc-style catalog, one document per package area, tiered by importance:
core classes get full entries (purpose, collaborators, lifecycle, threading
notes), supporting classes a paragraph, the long tail one line each.

| Document | Covers | Status |
|---|---|---|
| [reference/olap-classes.md](reference/olap-classes.md) | `mondrian.olap`, `olap.type`, `olap.fun`, `mdx`, `parser` | ✅ |
| [reference/rolap-classes.md](reference/rolap-classes.md) | `mondrian.rolap`, `rolap.sql`, `rolap.cache`, `rolap.format` | ✅ |
| [reference/rolap-agg-classes.md](reference/rolap-agg-classes.md) | `mondrian.rolap.agg`, `rolap.aggmatcher` | ✅ |
| [reference/calc-classes.md](reference/calc-classes.md) | `mondrian.calc`, `calc.impl` | ✅ |
| [reference/other-classes.md](reference/other-classes.md) | `spi`, `server`, `olap4j`, `xmla`, `util`, `udf`, misc | ✅ |

## Conventions used in these documents

- Code references use `ClassName#methodName` form, not line numbers (line
  numbers rot; names are greppable). Unqualified class names live under
  `mondrian/src/main/java/mondrian/`.
- "This fork" means this repository. Deviations from upstream Pentaho Mondrian
  are marked `PATCH` in the source (`// PATCH:` comments) and are called out in
  the documents where they affect behavior.
- Documents describe the code as it **is**, verified against the source. If you
  find a statement that no longer matches the code, fix the document in the
  same change that altered the behavior.

## For AI coding agents

- Load `architecture.md` for orientation, then `query-lifecycle.md` if the task
  touches query execution, then only the specific package/topic documents
  relevant to the task. Each document is self-contained enough to be read alone.
- The class reference documents are lookup tables — grep them for a class name
  rather than reading them whole.
