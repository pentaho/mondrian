# `mondrian.xmla` — XML for Analysis endpoint

*The SOAP/HTTP face of the engine: `mondrian.xmla` implements the XML for
Analysis (XML/A) protocol — `Discover` for metadata rowsets, `Execute` for MDX —
on top of olap4j connections handed out by a [server](server.md)-level
`ConnectionFactory`. See [../architecture.md](../architecture.md) for where this
sits in the layer map and [olap4j.md](olap4j.md) for the API it consumes.*

**The mondrian-olap JRuby gem does not use this path.** The gem talks to
`mondrian.olap4j` directly in-process; nothing here is on its execution path.
This package matters only if you embed Mondrian behind an XML/A-speaking client
(Excel, ADOMD.NET, etc.) in a servlet container. Treat this document as
orientation for reading, not as a tuning surface.

## Request flow

An HTTP POST carrying a SOAP envelope arrives at a servlet: `XmlaServlet` is
the abstract base (its `XmlaServlet#doPost` drives the phases),
`impl/DefaultXmlaServlet` supplies the DOM-based SOAP parsing
(`DefaultXmlaServlet#unmarshallSoapMessage`, `#handleSoapBody`,
`#marshallSoapMessage`), and `impl/MondrianXmlaServlet` is the concrete entry
point that boots a `MondrianServer` from a datasources XML file
(`MondrianServer#createWithRepository` with a `UrlRepositoryContentFinder`) and
uses that server as the `XmlaHandler.ConnectionFactory`. The parsed body
becomes a `DefaultXmlaRequest`, which `XmlaHandler#process` dispatches on
`XmlaRequest#getMethod`:

- **DISCOVER** → `XmlaHandler#discover` looks up the requested rowset in the
  `RowsetDefinition` enum (one constant per XML/A schema rowset —
  `MDSCHEMA_CUBES`, `DBSCHEMA_TABLES`, …, each defining its columns and
  sort order), instantiates the matching `Rowset` subclass via
  `RowsetDefinition#getRowset`, and calls `Rowset#unparse`, which populates
  rows from olap4j metadata (`Rowset#populateImpl`, applying the request's
  restrictions) and streams them out.
- **EXECUTE** → `XmlaHandler#execute` obtains an `OlapConnection` from the
  `ConnectionFactory`, runs the statement (`XmlaHandler#executeQuery`, or
  `#executeDrillThroughQuery` for drillthrough), wraps the resulting olap4j
  `CellSet` in an `MDDataSet_Multidimensional` or `MDDataSet_Tabular`
  (drillthrough uses `TabularRowSet`), and serializes it to the SOAP body via
  that object's `unparse(SaxWriter)`.

All XML output goes through the `SaxWriter` abstraction — SAX-like events in,
document out — so the same serialization code can produce XML
(`impl/DefaultSaxWriter`) or JSON (`impl/JsonSaxWriter`).

## Main types

| Type | Role |
|---|---|
| `XmlaHandler` | Protocol brain: dispatches Discover/Execute, owns statement execution and `MDDataSet` serialization; servlet-independent (also reused by test harnesses) |
| `XmlaHandler.ConnectionFactory` | How the handler gets `OlapConnection`s; implemented by `MondrianServer`, keyed by databaseName/catalogName/roleName |
| `XmlaHandler.XmlaExtra` | Escape hatch for Mondrian-specific abilities beyond the olap4j API (cache flush, `SCHEMA_MEMBERS` internals, …) |
| `XmlaRequest` / `XmlaResponse` | Request/response abstractions between servlet and handler; defaults in `impl/DefaultXmlaRequest`, `impl/DefaultXmlaResponse` |
| `XmlaConstants` | Namespaces, fault codes, `Method`/`Format`/`Content` enums shared across the package |
| `RowsetDefinition` | Enum of every Discover rowset: column definitions, ordering, and the factory (`#getRowset`) for its populator |
| `Rowset` | Base class of the per-rowset populators; subclasses live as inner classes of `RowsetDefinition` |
| `PropertyDefinition`, `Enumeration` | Enum of XML/A properties (`DataSourceInfo`, `Format`, …) and the value enumerations Discover reports |
| `SaxWriter` / `impl/DefaultSaxWriter` / `impl/JsonSaxWriter` | Output abstraction and its XML/JSON implementations |
| `XmlaUtil` | Helpers: element/text extraction, name encoding, `XmlaUtil#getMetadataRowset` for calling Discover programmatically |
| `XmlaException` | Carries SOAP fault code/string alongside the cause; thrown throughout the handler |
| `XmlaRequestCallback` | Hook to pull auth/context out of the HTTP request and SOAP header; `impl/AuthenticatingXmlaRequestCallback` is an abstract authenticating base |
| `XmlaServlet` → `impl/DefaultXmlaServlet` → `impl/MondrianXmlaServlet` | Servlet inheritance chain: phase skeleton → SOAP/DOM plumbing → Mondrian engine bootstrap |
| `impl/DynamicDatasourceXmlaServlet` | `MondrianXmlaServlet` variant using `mondrian.server.DynamicContentFinder` to pick up datasources-config changes without redeploy |
| `impl/Olap4jXmlaServlet` | Serves XML/A over any olap4j driver (driver class + connection string from servlet config, DBCP-pooled), not just Mondrian |

## `mondrian.web`

A small legacy companion, unrelated to XML/A except for also being
servlet-hosted: `web/servlet/MdxQueryServlet` accepts an MDX query over HTTP
and renders the result as an HTML table, and `web/taglib` is a JSP tag library
(`QueryTag`, `TransformTag`, `ResultCache`, `DomBuilder`, `ApplResources`,
`Listener`) that runs a query, caches the result in the HTTP session, and
renders it through an XSLT stylesheet. It predates olap4j — the taglib drives
`mondrian.olap.Connection` directly — and exists for the demo webapp. Nothing
else in the engine depends on it.

## Maintenance status in this fork

There are no `PATCH:` markers under `mondrian/xmla` or `mondrian/web` — both
packages are essentially upstream-as-is (Pentaho Mondrian 9.3.0.0). Since the
mondrian-olap gem bypasses them entirely, they receive no active maintenance
here; expect upstream behavior, upstream bugs, and no fork-specific tests.
