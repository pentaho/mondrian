#!/bin/bash
# $Id$
# This software is subject to the terms of the Eclipse Public License v1.0
# Agreement, available at the following URL:
# http://www.eclipse.org/legal/epl-v10.html.
# Copyright (C) 2008-2010 Julian Hyde and others.
# All Rights Reserved.
# You must accept the terms of that agreement to use this software.
#
# Sample scripts to load Mondrian's database for various databases.

case $(uname) in
Linux) PS=: ;;
*) PS=\; ;;
esac

export CP="lib/mondrian.jar"
export CP="${CP}${PS}lib/log4j.jar"
export CP="${CP}${PS}lib/commons-logging.jar"
export CP="${CP}${PS}lib/eigenbase-properties.jar"
export CP="${CP}${PS}lib/eigenbase-xom.jar"
export CP="${CP}${PS}lib/eigenbase-resgen.jar"

oracle() {
    #export ORACLE_HOME=G:/oracle/product/10.1.0/Db_1
    java -cp "${CP}${PS}${ORACLE_HOME}/jdbc/lib/ojdbc14.jar" \
         mondrian.test.loader.MondrianFoodMartLoader \
         -verbose -aggregates -tables -data -indexes \
         -jdbcDrivers=oracle.jdbc.OracleDriver \
         -inputFile=demo/FoodMartCreateData.sql \
         -outputJdbcURL="jdbc:oracle:thin:foodmart/foodmart@//localhost:1521/XE"
}

# Load into Oracle, creating dimension tables first, then trickling data into
# fact tables.
oracleTrickle() {
    java -cp "${CP}${PS}${ORACLE_HOME}/jdbc/lib/ojdbc14.jar" \
         mondrian.test.loader.MondrianFoodMartLoader \
         -verbose -tables -indexes -data -exclude=sales_fact_1997 \
         -jdbcDrivers=oracle.jdbc.OracleDriver \
         -inputJdbcURL="jdbc:oracle:thin:foodmart/foodmart@//localhost:1521/XE" \
         -outputJdbcURL="jdbc:oracle:thin:slurpmart/slurpmart@//localhost:1521/XE"

    # Write 10 rows each second into the sales fact table.
    java -cp "${CP}${PS}${ORACLE_HOME}/jdbc/lib/ojdbc14.jar" \
         mondrian.test.loader.MondrianFoodMartLoader \
         -verbose -tables -indexes -data -pauseMillis=100 -include=sales_fact_1997 \
         -jdbcDrivers=oracle.jdbc.OracleDriver \
         -inputJdbcURL="jdbc:oracle:thin:foodmart/foodmart@//localhost:1521/XE" \
         -outputJdbcBatchSize=100 \
         -outputJdbcURL="jdbc:oracle:thin:slurpmart/slurpmart@//localhost:1521/XE"
}

mysql() {
    java -cp "${CP}${PS}/usr/local/mysql-connector-java-3.1.12/mysql-connector-java-3.1.12-bin.jar" \
         mondrian.test.loader.MondrianFoodMartLoader \
         -verbose -aggregates -tables -data -indexes \
         -jdbcDrivers=com.mysql.jdbc.Driver \
         -inputFile=demo/FoodMartCreateData.sql \
         -outputJdbcURL="jdbc:mysql://localhost/foodmart?user=foodmart&password=foodmart"
}

infobright() {
    # As mysql, but '-indexes' option removed because infobright doesn't support them.
    java -cp "${CP}${PS}/usr/local/mysql-connector-java-3.1.12/mysql-connector-java-3.1.12-bin.jar" \
         mondrian.test.loader.MondrianFoodMartLoader \
         -verbose -aggregates -tables -data \
         -jdbcDrivers=com.mysql.jdbc.Driver \
         -inputFile=demo/FoodMartCreateData.sql \
         -outputJdbcURL="jdbc:mysql://localhost/foodmart?user=foodmart&password=foodmart&characterEncoding=UTF-8"
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
         -jdbcDrivers="org.postgresql.Driver" \
         -inputFile=demo/FoodMartCreateData.sql \
         -outputJdbcURL="jdbc:postgresql://localhost/foodmart" \
         -outputJdbcUser=foodmart \
         -outputJdbcPassword=foodmart
}

# Load farrago (a LucidDB variant)
farrago() {
    java -cp "${CP}${PS}../farrago/classes" \
        mondrian.test.loader.MondrianFoodMartLoader \
        -verbose -aggregates -tables -data -indexes \
        -jdbcDrivers=net.sf.farrago.client.FarragoVjdbcClientDriver \
        -inputFile=demo/FoodMartCreateData.sql \
        -outputJdbcURL="jdbc:farrago:rmi://localhost"
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
        -jdbcDrivers=org.luciddb.jdbc.LucidDbClientDriver \
        -inputFile=demo/FoodMartCreateData.sql \
        -outputJdbcURL="jdbc:luciddb:http://localhost;schema=FOODMART" \
        -outputJdbcUser="foodmart" \
        -outputJdbcPassword="foodmart"
}

# Load Teradata.
# You'll have to download drivers and put them into the drivers folder.
# Note that we do not use '-aggregates'; we plan to use aggregate
# join indexes instead of explicit aggregate tables.
teradata() {
    java -cp "${CP}${PS}drivers/terajdbc4.jar${PS}drivers/tdgssjava.jar${PS}drivers/tdgssconfig.jar" \
        mondrian.test.loader.MondrianFoodMartLoader \
        -verbose -tables -data -indexes \
        -jdbcDrivers=com.ncr.teradata.TeraDriver \
        -inputFile=demo/FoodMartCreateData.sql \
        -outputJdbcURL="jdbc:teradata://localhost/foodmart" \
        -outputJdbcUser="tduser" \
        -outputJdbcPassword="tduser"
}

# Load Hsqldb.
hsqldb() {
    rm -rf demo/hsqldb/foodmart.*
    java -Xmx512M -ea -esa -cp "${CP}${PS}lib/hsqldb.jar" \
        mondrian.test.loader.MondrianFoodMartLoader \
        -verbose -tables -data -indexes \
        -jdbcDrivers=org.hsqldb.jdbcDriver \
        -inputFile=demo/FoodMartCreateData.sql \
        -outputJdbcBatchSize=1 \
        -outputJdbcURL="jdbc:hsqldb:file:demo/hsqldb/foodmart" \
        -outputJdbcUser="sa" \
        -outputJdbcPassword=""
}

cd $(dirname $0)/..
hsqldb

# End loadFoodMart.sh
