#!/bin/bash

set -ev

# Download latest ClickHouse JDBC driver (all-in-one jar with dependencies)
# Check https://github.com/ClickHouse/clickhouse-java/releases for latest version
CLICKHOUSE_JDBC_VERSION=0.9.6
wget https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/${CLICKHOUSE_JDBC_VERSION}/clickhouse-jdbc-${CLICKHOUSE_JDBC_VERSION}-all.jar
cp clickhouse-jdbc-${CLICKHOUSE_JDBC_VERSION}-all.jar test/support/jars/

# Download SLF4J dependencies (required for ClickHouse JDBC driver logging)
SLF4J_VERSION=2.0.17
wget https://repo1.maven.org/maven2/org/slf4j/slf4j-api/${SLF4J_VERSION}/slf4j-api-${SLF4J_VERSION}.jar
wget https://repo1.maven.org/maven2/org/slf4j/slf4j-simple/${SLF4J_VERSION}/slf4j-simple-${SLF4J_VERSION}.jar
cp slf4j-api-${SLF4J_VERSION}.jar test/support/jars/
cp slf4j-simple-${SLF4J_VERSION}.jar test/support/jars/
