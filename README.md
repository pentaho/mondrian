# mondrian-olap-java

A fork of the Mondrian OLAP Java engine, maintained to provide the core Java library for the [mondrian-olap](https://github.com/rsim/mondrian-olap) JRuby gem. This fork includes patches and enhancements specific to the mondrian-olap use case, built on top of Pentaho Mondrian 9.3.0.0.

## Technology Stack

- **Java** 8+ (source and target compatibility 1.8), Java 11 recommended for building
- **Maven** for build management
- **olap4j** 1.2.0 - Open Java API for OLAP
- **Caffeine** 2.9.3 - Caching library (replaced Guava in this fork)
- **commons-lang3** - Upgraded from legacy commons-lang in this fork
- **Log4j 2** 2.17.1 - Logging
- **Databases**: PostgreSQL, MySQL, Oracle, Microsoft SQL Server, ClickHouse, and other JDBC-compatible databases via dialect abstraction

## Building

1. Build the project with Maven: `mvn package`
2. The output JAR is produced in `mondrian/target/`
3. After building, the JAR files should be copied to the mondrian-olap gem's `lib/mondrian/jars/` directory

## Testing for mondrian-olap

After making changes, build the JAR, copy it to the mondrian-olap gem, and run the gem's test suite.

## Testing in this repository

    # Run the Ruby tests
    mise run test

    # Run the original Java test suite
    mise run test_java

    # Run a single Java test
    mise run single_java_test NativeFilterMatchingTest


## Fork-Specific Patches

Notable patches in this fork include:
- Replacing Guava with Caffeine for caching
- Upgrading commons-lang to commons-lang3
- Removing Pentaho maven repository dependency
- Various bug fixes for member retrieval and SQL generation

Dependencies not available in public Maven repositories are stored in `lib-repo/`.
