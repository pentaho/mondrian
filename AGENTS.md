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

1. Build the project: `mise run package` (runs `mvn package -DskipTests`).
2. The output JAR is produced in `mondrian/target/`.
3. After building, the JAR files should be copied to the mondrian-olap gem's `lib/mondrian/jars/` directory.

> Note: bare `mvn package` runs the legacy Java test suite, which requires a preloaded FoodMart database. Use `mise run package` for a fast test-skipping build, or `mise run java_test` to explicitly run the legacy Java tests.

### Making Changes

1. **Bug fixes and enhancements** - Modify Java source files under `mondrian/src/main/java/mondrian/`.
2. **SQL generation changes** - Look at dialect classes in `mondrian/spi/` and SQL generation in `mondrian/rolap/sql/`.
3. **Member retrieval and caching** - Key files are in `mondrian/rolap/`, especially `SqlMemberSource`, `SmartMemberReader`, and cache-related classes.
4. **UDF changes** - User-defined functions are in `mondrian/udf/` and registered via `mondrian/olap/fun/`.
5. **Expression evaluation** - Calculator and evaluator classes are in `mondrian/calc/`.

### Testing

This repository includes JRuby-based integration tests using the mondrian-olap gem to test the built Mondrian JAR against a FoodMart database.
When modifying existing Mondrian Java classes or adding new functionality then use JRuby tests instead of modifying legacy Java tests.

#### Setup

1. Build the Mondrian JAR: `mise run package`
2. Install gems: `bundle install`
3. Create the FoodMart database and user: `rake db:create_foodmart` (default user/password: `foodmart`/`foodmart`, database: `foodmart`)
4. Load FoodMart data: `rake db:load_foodmart`

#### Running Tests

- Run all tests (default MySQL): `rake test`
- Run with a specific database: `rake test:mysql`, `rake test:postgresql`, `rake test:oracle`, `rake test:sqlserver`, `rake test:clickhouse`
- Run a single test file: `ruby -Itest test/connection_test.rb`

#### Environment Variables

- `MONDRIAN_DRIVER` - Database driver (`mysql`, `postgresql`, `oracle`, `sqlserver`, `clickhouse`). Default: `mysql`
- `DATABASE_HOST` - Database hostname. Default: `localhost`
- `DATABASE_PORT` - Database port. Default: driver-specific
- `DATABASE_USER` - Database username. Default: `foodmart`
- `DATABASE_PASSWORD` - Database password. Default: `foodmart`
- `DATABASE_NAME` - Database name. Default: `foodmart`
- Override any `DATABASE_*` environment variable with a driver-specific version using `${MONDRIAN_DRIVER}_DATABASE_*` (e.g. `MYSQL_DATABASE_HOST`, `POSTGRESQL_DATABASE_HOST`).
- `MONDRIAN_OLAP_PATH` - Path to local mondrian-olap gem (e.g. `../mondrian-olap`). If not set, uses git master branch.
- `${MONDRIAN_DRIVER}_ADMIN_USER` - Admin username for database creation/drop (e.g. `MYSQL_ADMIN_USER`, `POSTGRESQL_ADMIN_USER`). Default: driver-specific (`root`, `postgres`, `system`, `sa`, `default`).
- `${MONDRIAN_DRIVER}_ADMIN_PASSWORD` - Admin password for database creation/drop (e.g. `MYSQL_ADMIN_PASSWORD`, `POSTGRESQL_ADMIN_PASSWORD`). Default: driver-specific.

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

### Testing using JRuby and Minitest

- Use Minitest for JRuby-based testing with the minitest-hooks gem for lifecycle hooks.
- Use Minitest Spec-style syntax with `describe` and `it` blocks. Nest `describe` blocks for logical grouping.
- Use `before` and `after` hooks for per-test setup and teardown.
- Use `before(:all)` and `after(:all)` hooks (from minitest-hooks) for expensive setup shared across all tests
  in a `describe` block, such as establishing database connections or defining schemas.
- Use standard Minitest assertions: `assert_equal`, `assert_nil`, `assert_empty`, `assert_kind_of`, `assert_match`.
- Use `assert_raises` with a block for exception testing, for example,
  `error = assert_raises(Mondrian::OLAP::Error) { action }`.
- Use `refute` and `refute_nil` for negation assertions.
- Prefer `assert_equal true, method?` and `assert_equal false, method?` for boolean methods
  instead of simple `assert` and `refute` assertions.
- Use instance variables (`@olap`, `@schema`, `@cube`) for shared state between hooks and tests.
- Conditionally skip tests for specific database drivers using `unless` guards, for example,
  `unless %w(oracle).include?(MONDRIAN_DRIVER)`.
- Test files are located in `test/` directory and follow the naming pattern `*_test.rb`.
- Test helper and database configuration are in `test/test_helper.rb`.

### mise

- mise may be used to manage Ruby and Java versions.
- If mise is available then prefix java, mvn, ruby, rake calls with `mise exec --` to initialize the correct environment.
