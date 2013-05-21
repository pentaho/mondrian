#!/bin/bash
#
# This software is subject to the terms of the Eclipse Public License v1.0
# Agreement, available at the following URL:
# http://www.eclipse.org/legal/epl-v10.html.
# You must accept the terms of that agreement to use this software.
#
# Copyright (C) 2008-2012 Pentaho and others
# All Rights Reserved.
#
# Sample scripts to load Mondrian's database for various databases.

case $(uname) in
Linux|Darwin) PS=: ;;
*) PS=\; ;;
esac

outputQuoted=true

export CP="lib/mondrian.jar"
export CP="${CP}${PS}lib/olap4j.jar"
export CP="${CP}${PS}lib/log4j.jar"
export CP="${CP}${PS}lib/commons-logging.jar"
export CP="${CP}${PS}lib/commons-collections.jar"
export CP="${CP}${PS}lib/eigenbase-properties.jar"
export CP="${CP}${PS}lib/eigenbase-xom.jar"
export CP="${CP}${PS}lib/eigenbase-resgen.jar"

usage() {
    echo "Usage: loadFoodMart.sh [ --help | --db <database> ]"
    echo
    echo "Populate FoodMart database, calling the MondrianFoodMartLoader"
    echo "program with a typical set of arguments. This script does not aim to"
    echo "be 100% customizable from the command line; you will almost certainly"
    echo "have to edit it with the URL and options of your database. But for"
    echo "each database engine, the command given here will be reasonably"
    echo "close to what you need."
    echo
    echo "Options:"
    echo "   --help  Prints this help"
    echo "   --db <database> Loads into target database, where <database> is"
    echo "           one of: ${dbs}"
}

error() {
    echo "Error: $1"
    echo
    usage
}

oracle() {
    # Assume ORACLE_HOME is set, e.g.:
    #  export ORACLE_HOME=G:/oracle/product/10.1.0/Db_1
    # For JDBC driver, try 'ojdbc6.jar' on JDK1.6;
    # try 'ojdbc5.jar' on JDK1.5;
    # try 'ojdbc14.jar' on JDK1.4 or Oracle 10 and earlier.
    java -cp "${CP}${PS}${ORACLE_HOME}/jdbc/lib/ojdbc6.jar" \
        mondrian.test.loader.MondrianFoodMartLoader \
        -verbose -aggregates -tables -data -indexes \
        -dataset=${dataset} \
        -jdbcDrivers=oracle.jdbc.OracleDriver \
        -inputFile="$inputFile" \
        -afterFile="$afterFile" \
        -outputQuoted=${outputQuoted} \
        -outputJdbcURL="jdbc:oracle:thin:${datasetLower}/foodmart@//localhost:1521/XE"
}

# Load into Oracle, creating dimension tables first, then trickling data into
# fact tables.
oracleTrickle() {
    java -cp "${CP}${PS}${ORACLE_HOME}/jdbc/lib/ojdbc6.jar" \
        mondrian.test.loader.MondrianFoodMartLoader \
        -verbose -tables -indexes -data -exclude=sales_fact_1997 \
        -dataset=${dataset} \
        -jdbcDrivers=oracle.jdbc.OracleDriver \
        -inputJdbcURL="jdbc:oracle:thin:${datasetLower}/foodmart@//localhost:1521/XE" \
        -outputQuoted=${outputQuoted} \
        -outputJdbcURL="jdbc:oracle:thin:slurpmart/slurpmart@//localhost:1521/XE"

    # Write 10 rows each second into the sales fact table.
    java -cp "${CP}${PS}${ORACLE_HOME}/jdbc/lib/ojdbc6.jar" \
        mondrian.test.loader.MondrianFoodMartLoader \
        -verbose -tables -indexes -data -pauseMillis=100 -include=sales_fact_1997 \
        -dataset=${dataset} \
        -jdbcDrivers=oracle.jdbc.OracleDriver \
        -inputJdbcURL="jdbc:oracle:thin:${datasetLower}/foodmart@//localhost:1521/XE" \
        -outputQuoted=${outputQuoted} \
        -outputJdbcBatchSize=100 \
        -outputJdbcURL="jdbc:oracle:thin:slurpmart/slurpmart@//localhost:1521/XE"
}

mysql() {
    java -cp "${CP}${PS}/usr/local/mysql-connector-java-3.1.12/mysql-connector-java-3.1.12-bin.jar" \
        mondrian.test.loader.MondrianFoodMartLoader \
        -verbose -aggregates -tables -data -indexes \
        -dataset=${dataset} \
        -jdbcDrivers=com.mysql.jdbc.Driver \
        -inputFile="$inputFile" \
        -afterFile="$afterFile" \
        -outputQuoted=${outputQuoted} \
        -outputJdbcURL="jdbc:mysql://localhost/${datasetLower}?user=foodmart&password=foodmart"
}

infobright() {
    # As mysql, but '-indexes' option removed because infobright doesn't support them.
    java -cp "${CP}${PS}/usr/local/mysql-connector-java-3.1.12/mysql-connector-java-3.1.12-bin.jar" \
        mondrian.test.loader.MondrianFoodMartLoader \
        -verbose -aggregates -tables -data \
        -dataset=${dataset} \
        -jdbcDrivers=com.mysql.jdbc.Driver \
        -inputFile="$inputFile" \
        -afterFile="$afterFile" \
        -outputQuoted=${outputQuoted} \
        -outputJdbcURL="jdbc:mysql://localhost/${datasetLower}?user=foodmart&password=foodmart&characterEncoding=UTF-8"
}

# Load PostgreSQL.
#
# To install postgres and its JDBC driver on ubuntu:
#   $ sudo apt-get install postgresql libpg-java
# Then change postgres password, create a user and database:
#   $ sudo -u postgres psql postgres
#   # ALTER USER postgres WITH ENCRYPTED PASSWORD '<password>';
#   # \q
#   $ sudo -u postgres  createuser -D -A -P foodmart
#   $ sudo -u postgres createdb -O foodmart foodmart
postgresql() {
    java -verbose -cp "${CP}${PS}/usr/share/java/postgresql.jar" \
        mondrian.test.loader.MondrianFoodMartLoader \
        -verbose -tables -data -indexes \
        -dataset=${dataset} \
        -jdbcDrivers="org.postgresql.Driver" \
        -inputFile="$inputFile" \
        -afterFile="$afterFile" \
        -outputQuoted=${outputQuoted} \
        -outputJdbcURL="jdbc:postgresql://localhost/${datasetLower}" \
        -outputJdbcUser=foodmart \
        -outputJdbcPassword=foodmart
}

# Load farrago (a LucidDB variant)
farrago() {
    java -cp "${CP}${PS}../farrago/classes" \
        mondrian.test.loader.MondrianFoodMartLoader \
        -verbose -aggregates -tables -data -indexes \
        -dataset=${dataset} \
        -jdbcDrivers=net.sf.farrago.client.FarragoVjdbcClientDriver \
        -inputFile="$inputFile" \
        -afterFile="$afterFile" \
        -outputQuoted=${outputQuoted} \
        -outputJdbcURL="jdbc:farrago:rmi://localhost"
}

# Load firebird
#
# $ /firebird/bin/isql -u SYSDBA -p masterkey
# Use CONNECT or CREATE DATABASE to specify a database
# SQL> CREATE DATABASE '/mondrian/foodmart.gdb';
# SQL> QUIT;
#
firefird() {
    java -cp "${CP}${PS}../farrago/classes" \
        mondrian.test.loader.MondrianFoodMartLoader \
        -verbose -aggregates -tables -data -indexes \
        -dataset=${dataset} \
        -jdbcDrivers="org.firebirdsql.jdbc.FBDriver" \
        -inputFile="$inputFile" \
        -afterFile="$afterFile" \
        -outputQuoted=${outputQuoted} \
        -outputJdbcUser=SYSDBA \
        -outputJdbcPassword=masterkey \
        -outputJdbcURL="jdbc:firebirdsql:localhost/3050:/mondrian/foodmart.gdb"
}

# Generate a JSON file.
json() {
    rm -rf foo
    mkdir foo
    (
        cd foo
        jar xvf ../lib/mondrian-data-foodmart-json.jar
        find . -type f | xargs perl -p -i -e 's/%VERSION%/0.1/'
    )
    java -cp "${CP}" \
        mondrian.test.loader.MondrianFoodMartLoader \
        -verbose -data \
        -dataset=${dataset} \
        -inputFile="$inputFile" \
        -outputQuoted=${outputQuoted} \
        -outputDirectory=foo \
        -outputFormat=json
    (
        cd foo
        jar cvf /tmp/mondrian-data-foodmart-json.jar .
    )
    echo Created /tmp/mondrian-data-foodmart-json.jar
}

# Load LucidDB
#
# Install LucidDB per instructions at
# http://pub.eigenbase.org/wiki/LucidDbGettingStarted, start sqlline, then run
# the following:
# 
# jdbc:luciddb:http://localhost> create schema foodmart;
# jdbc:luciddb:http://localhost> create user "foodmart" identified by 'foodmart';
# jdbc:luciddb:http://localhost> grant execute on specific procedure
#                              >   applib.estimate_statistics_for_schema_no_samplingrate
#                              >   to "foodmart";
luciddb() {
    export LUCIDDB_HOME=/usr/local/luciddb
    java -cp "${CP}${PS}${LUCIDDB_HOME}/plugin/LucidDbClient.jar" \
        mondrian.test.loader.MondrianFoodMartLoader \
        -verbose -aggregates -tables -data -indexes -analyze \
        -dataset=${dataset} \
        -jdbcDrivers=org.luciddb.jdbc.LucidDbClientDriver \
        -inputFile="$inputFile" \
        -afterFile="$afterFile" \
        -outputQuoted=${outputQuoted} \
        -outputJdbcURL="jdbc:luciddb:http://localhost;schema=${dataset}" \
        -outputJdbcUser="foodmart" \
        -outputJdbcPassword="foodmart"
}

# Load monetdb
#
# 1. Build from source (because required patches are not in a release
# yet).
#
# sudo apt-get install libssl-dev pkg-config libpcre* libxml2-dev
# curl -O http://monetdb.cwi.nl/testweb/web/45868:949c8b8db28d/MonetDB-11.13.4.tar.bz2
# tar xvfj MonetDB-11.13.4.tar.bz2
# cd MonetDB-11.13.4
# ./configure
# make
# sudo make install
#
# 2. Create and start database.
#
# sudo mkdir /var/local/monetdb
# sudo chown ${USER} /var/local/monetdb
# monetdbd create /var/local/monetdb
# monetdbd start /var/local/monetdb
# monetdb create foodmart
# monetdb start foodmart
# monetdb release foodmart
monetdb() {
    java -ea -esa -cp "${CP}${PS}lib/monetdb-jdbc.jar" \
        mondrian.test.loader.MondrianFoodMartLoader \
        -verbose -tables -data -indexes \
        -dataset=${dataset} \
        -jdbcDrivers=nl.cwi.monetdb.jdbc.MonetDriver \
        -inputFile="$inputFile" \
        -afterFile="$afterFile" \
        -outputQuoted=${outputQuoted} \
        -outputJdbcBatchSize=1000 \
        -outputJdbcURL="jdbc:monetdb://localhost/${dataset}" \
        -outputJdbcUser="monetdb" \
        -outputJdbcPassword="monetdb"
}

# Load Teradata.
# You'll have to download drivers and put them into the drivers folder.
# Note that we do not use '-aggregates'; we plan to use aggregate
# join indexes instead of explicit aggregate tables.
teradata() {
    java -cp "${CP}${PS}drivers/terajdbc4.jar${PS}drivers/tdgssjava.jar${PS}drivers/tdgssconfig.jar" \
        mondrian.test.loader.MondrianFoodMartLoader \
        -verbose -tables -data -indexes \
        -dataset=${dataset} \
        -jdbcDrivers=com.ncr.teradata.TeraDriver \
        -inputFile="$inputFile" \
        -afterFile="$afterFile" \
        -outputQuoted=${outputQuoted} \
        -outputJdbcURL="jdbc:teradata://localhost/${datasetLower}" \
        -outputJdbcUser="tduser" \
        -outputJdbcPassword="tduser"
}

# Load Hsqldb.
hsqldb() {
    rm -rf demo/hsqldb/${datasetLower}.*
    java -Xmx512M -ea -esa -cp "${CP}${PS}lib/hsqldb.jar" \
        mondrian.test.loader.MondrianFoodMartLoader \
        -verbose -tables -data -indexes \
        -dataset=${dataset} \
        -jdbcDrivers=org.hsqldb.jdbcDriver \
        -inputFile="$inputFile" \
        -afterFile="$afterFile" \
        -outputQuoted=${outputQuoted} \
        -outputJdbcBatchSize=1 \
        -outputJdbcURL="jdbc:hsqldb:file:demo/hsqldb/${datasetLower}" \
        -outputJdbcUser="sa" \
        -outputJdbcPassword=""
}

dbs="\
farrago \
hsqldb \
infobright \
luciddb \
monetdb \
mysql \
oracle \
oracleTrickle \
postgresql \
teradata \
"

db=
dataset=FOODMART
while [ $# -gt 0 ]; do
    case "$1" in
    (--help) usage; exit 0;;
    (--db) shift; db="$1"; shift;;
    (--dataset) shift; dataset="$1"; shift;;
    (*) error "Unknown argument '$1'"; exit 1;;
    esac
done

cd $(dirname $0)/..

inputFile=
afterFile=
case "$dataset" in
(FOODMART)
    inputFile=jar:file:lib/mondrian-data-foodmart.jar!/data.sql
    afterFile=jar:file:lib/mondrian-data-foodmart.jar!/after.sql
    ;;
(ADVENTUREWORKS)
    inputFile=jar:file:lib/mondrian-data-adventureworks.jar!/data.sql
    ;;
(ADVENTUREWORKS_DW)
    inputFile=jar:file:lib/mondrian-data-adventureworks-dw.jar!/data.sql
    ;;
(*)
    echo "Unknown dataset '$dataset'"
    exit 1
    ;;
esac

datasetLower=$(echo $dataset | tr A-Z a-z)

case "$db" in
('') error "You must specify a database."; exit 1;;
(farrago) farrago;;
(firebird) firebird;;
(hsqldb) hsqldb;;
(infobright) infobright;;
(json) json;;
(luciddb) luciddb;;
(monetdb) monetdb;;
(mysql) mysql;;
(oracle) oracle;;
(oracleTrickle) oracleTrickle;;
(postgresql) postgresql;;
(teradata) teradata;;
(*) error "Unknown database '$db'."; exit 1;;
esac

# End loadFoodMart.sh

