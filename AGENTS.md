# mondrian-olap-java

This file provides guidance to AI coding agents working with this repository.

## Overview

mondrian-olap-java is a fork of the Mondrian OLAP Java engine, maintained to provide the core Java library for the [mondrian-olap](https://github.com/rsim/mondrian-olap) JRuby gem. This fork includes patches and enhancements specific to the mondrian-olap use case, built on top of Pentaho Mondrian 9.3.0.0.

## Codebase Structure

### Top-Level Layout

- `pom.xml` - Parent Maven POM that defines shared properties, dependency versions, and modules
- `mondrian/` - The core Mondrian OLAP engine (main module)
- `mondrian/pom.xml` - Module-specific Maven POM with dependencies and build configuration
- `mondrian/src/main/java/mondrian/` - Java source code for the Mondrian engine
- `mondrian/src/main/java/mondrian/resource/` - Generated resource bundles
- `mondrian/target/` - Maven build output (compiled classes, JARs)
- `lib-repo/` - Local Maven repository for dependencies not available in public repositories
- `bin/` - Shell scripts for running and testing Mondrian
- `demo/` - Demo files

### Key Java Packages

- `mondrian.olap` - Core OLAP model (connections, schemas, cubes, members, queries)
- `mondrian.rolap` - Relational OLAP implementation (the primary engine)
- `mondrian.mdx` - MDX expression model
- `mondrian.parser` - MDX parser
- `mondrian.calc` - Calculated member and expression evaluation
- `mondrian.udf` - User-defined functions
- `mondrian.spi` - Service provider interfaces (dialect, data source, etc.)
- `mondrian.util` - Utility classes
- `mondrian.olap4j` - olap4j API integration
- `mondrian.xmla` - XML for Analysis (XMLA) interface
- `mondrian.server` - Server and session management
- `mondrian.resource` - Localized message resources

### Important Classes

- `mondrian.olap.MondrianProperties` - Configuration properties for the engine
- `mondrian.rolap.RolapConnection` - Main connection implementation
- `mondrian.rolap.RolapSchema` - Schema loading and management
- `mondrian.rolap.RolapCube` - Cube implementation
- `mondrian.rolap.SqlMemberSource` - SQL-based member retrieval from database
- `mondrian.rolap.RolapEvaluator` - Expression evaluation engine
- `mondrian.spi.Dialect` - Database dialect abstraction for multi-database support

## Common Workflows

### Building

1. Build the project with Maven: `mvn package` (or `mise run package` if mise is configured).
2. The output JAR is produced in `mondrian/target/`.
3. After building, the JAR files should be copied to the mondrian-olap gem's `lib/mondrian/jars/` directory.

### Making Changes

1. **Bug fixes and enhancements** - Modify Java source files under `mondrian/src/main/java/mondrian/`.
2. **SQL generation changes** - Look at dialect classes in `mondrian/spi/` and SQL generation in `mondrian/rolap/sql/`.
3. **Member retrieval and caching** - Key files are in `mondrian/rolap/`, especially `SqlMemberSource`, `SmartMemberReader`, and cache-related classes.
4. **UDF changes** - User-defined functions are in `mondrian/udf/` and registered via `mondrian/olap/fun/`.
5. **Expression evaluation** - Calculator and evaluator classes are in `mondrian/calc/`.

### Testing

- After making changes, build the JAR, copy it to the mondrian-olap gem, and run the gem's test suite.

## Technology Stack

- **Java** 8+ (source and target compatibility 1.8), Java 11 recommended for building (see `mise.local.toml`)
- **Maven** for build management
- **olap4j** 1.2.0 - Open Java API for OLAP
- **Caffeine** 2.9.3 - Caching library (replaced Guava in this fork)
- **commons-lang3** - Upgraded from legacy commons-lang in this fork
- **Log4j 2** 2.17.1 - Logging
- **Databases**: PostgreSQL, MySQL, Oracle, Microsoft SQL Server, ClickHouse, and other JDBC-compatible databases via dialect abstraction

## Code Style and Guidelines

### General

- Use meaningful semantic names for variables, methods, and classes.
- Write comments to explain why something is done, not what is done.
- Write comments only when it is not obvious from the code.
- Start full sentence comments with a capital letter.
- Keep methods small and focused on a single task.
- Validate correct spelling for variable, method, class names, as well as for comments.

### Java-Specific

- Follow existing Mondrian code conventions and formatting style.
- Use Java 8 compatible syntax (no features from Java 9+).
- Mark patches and modifications to upstream Mondrian code with `// PATCH:` comments to distinguish them from original code.
- When modifying existing methods, preserve the original method signature where possible to maintain API compatibility with the mondrian-olap gem.
- Be mindful of thread safety — Mondrian is used in concurrent environments.

### Fork-Specific Considerations

- This is a maintained fork, not the upstream Mondrian project. Changes should focus on what is needed for the mondrian-olap JRuby gem.
- Notable fork patches include: replacing Guava with Caffeine, upgrading commons-lang to commons-lang3, removing Pentaho maven repository dependency, and various bug fixes for member retrieval and SQL generation.
- Dependencies not available in public Maven repositories are stored in `lib-repo/`.

### mise

- mise may be used to manage Java versions.
- If mise is available then prefix java, mvn, ruby, rake calls with `mise exec --` to initialize the correct environment.
