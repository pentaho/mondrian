#!/bin/bash

declare -r DIR=$(dirname $0)

LOCATION="$PWD/$DIR"
MONDRIAN_HOME="$LOCATION/.."
MONDRIAN_LIB="$MONDRIAN_HOME/lib"
MONDRIAN_TEST_LIB="$MONDRIAN_HOME/testlib"

CLASSPATH=$MONDRIAN_LIB/mondrian.jar
CLASSPATH=$CLASSPATH:$MONDRIAN_LIB/log4j-1.2.9.jar
CLASSPATH=$CLASSPATH:$MONDRIAN_LIB/eigenbase-xom.jar
CLASSPATH=$CLASSPATH:$MONDRIAN_LIB/eigenbase-resgen.jar
CLASSPATH=$CLASSPATH:$MONDRIAN_LIB/eigenbase-properties.jar
CLASSPATH=$CLASSPATH:$MONDRIAN_LIB/javacup.jar
CLASSPATH=$CLASSPATH:$MONDRIAN_LIB/commons-dbcp.jar
CLASSPATH=$CLASSPATH:$MONDRIAN_LIB/commons-collections.jar
CLASSPATH=$CLASSPATH:$MONDRIAN_LIB/commons-pool.jar
CLASSPATH=$CLASSPATH:$MONDRIAN_LIB/commons-math-1.0.jar
CLASSPATH=$CLASSPATH:$MONDRIAN_LIB/commons-vfs.jar
CLASSPATH="$CLASSPATH:$MONDRIAN_LIB/commons-logging.jar"

CLASSPATH=$CLASSPATH:$MONDRIAN_TEST_LIB/mysql-connector-java-3.1.7-bin.jar
CLASSPATH=$CLASSPATH:$MONDRIAN_TEST_LIB/ojdbc14.jar
CLASSPATH=$CLASSPATH:$MONDRIAN_TEST_LIB/junit.jar
CLASSPATH=$CLASSPATH:$MONDRIAN_TEST_LIB/derby.jar

CLASSPATH="$CLASSPATH:$MONDRIAN_HOME/testclasses"

export CLASSPATH

CATALOG_URL="file://localhost${MONDRIAN_HOME}/demo/FoodMart.mondrian.xml"

# oracle 
#JDBC_DRIVER=oracle.jdbc.OracleDriver
#JDBC_URL=jdbc:oracle:thin:foodmart/foodmart@EDGEPSW01:1521:pstest

# mysql 
JDBC_DRIVER=com.mysql.jdbc.Driver
JDBC_URL="jdbc:mysql://localhost/foodmart?user=foodmart&password=foodmart"

# Derby
#JDBC_DRIVER=org.apache.derby.jdbc.EmbeddedDriver
#JDBC_URL="jdbc:derby:demo/derby/foodmart;JdbcUser=sa;JdbcPassword=sa"

P="-Dmondrian.jdbcDrivers=$JDBC_DRIVER"
P="$P -Dmondrian.foodmart.jdbcURL=$JDBC_URL"
# P="$P -Dmondrian.test.foodmart.catalogURL=$CATALOG_URL"
P="$P -Dmondrian.foodmart.catalogURL=$CATALOG_URL"
P="$P -Dmondrian.test.connectString=Provider=mondrian;Jdbc='$JDBC_URL';Catalog='$CATALOG_URL'"

P="$P -Dlog4j.configuration=file://localhost${MONDRIAN_HOME}/log4j.properties"

JAVA=$( which java )
if [[ -n "$JAVA_HOME" ]]; then
    JAVA="$JAVA_HOME"/bin/java
fi

if [[ ! -e $JAVA ]]; then
    echo "java executable $JAVA not found"
    exit 1
fi

MAIN_CLASS=mondrian.test.SimpleTestRunner

$JAVA -cp $CLASSPATH  $P $MAIN_CLASS  $@

