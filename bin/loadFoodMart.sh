#!/bin/bash
# $Id$
# Scripts to load Mondrian's database for various databases.

case $(uname) in
Linux) PS=: ;;
*) PS=\; ;;
esac

oracle() {
    #export ORACLE_HOME=G:/oracle/product/10.1.0/Db_1
    java -cp "lib/mondrian.jar${PS}lib/log4j.jar${PS}lib/eigenbase-properties.jar${PS}lib/eigenbase-xom.jar${PS}lib/eigenbase-resgen.jar${PS}${ORACLE_HOME}/jdbc/lib/ojdbc14.jar" \
         mondrian.test.loader.MondrianFoodMartLoader \
         -verbose -aggregates -tables -data -indexes \
         -jdbcDrivers=oracle.jdbc.OracleDriver \
         -inputFile=demo/FoodMartCreateData.sql \
         -outputJdbcURL="jdbc:oracle:thin:foodmart/foodmart@//localhost:1521/XE"
}

mysql() {
    java -cp "lib/mondrian.jar${PS}lib/log4j.jar${PS}lib/eigenbase-properties.jar${PS}lib/eigenbase-xom.jar${PS}lib/eigenbase-resgen.jar${PS}/usr/local/mysql-connector-java-3.1.12/mysql-connector-java-3.1.12-bin.jar" \
         mondrian.test.loader.MondrianFoodMartLoader \
         -verbose -aggregates -tables -data -indexes \
         -jdbcDrivers=com.mysql.jdbc.Driver \
         -inputFile=demo/FoodMartCreateData.sql \
         -outputJdbcURL="jdbc:mysql://localhost/foodmart?user=foodmart&password=foodmart"
}

farrago() {
    java -cp "lib/mondrian.jar${PS}lib/log4j.jar${PS}lib/eigenbase-xom.jar${PS}lib/eigenbase-resgen.jar${PS}lib/eigenbase-properties.jar${PS}../farrago/classes" \
        mondrian.test.loader.MondrianFoodMartLoader \
        -verbose -aggregates -tables -data -indexes \
        -jdbcDrivers=net.sf.farrago.client.FarragoVjdbcClientDriver \
        -inputFile=demo/FoodMartCreateData.sql \
        -outputJdbcURL="jdbc:farrago:rmi://localhost"
}

# Load Teradata.
# You'll have to download drivers and put them into the drivers folder.
# Note that we do not use '-aggregates'; we plan to use aggregate
# join indexes instead of explicit aggregate tables.
teradata() {
    java -cp "lib/mondrian.jar${PS}lib/log4j.jar${PS}lib/eigenbase-xom.jar${PS}lib/eigenbase-resgen.jar${PS}lib/eigenbase-properties.jar${PS}drivers/terajdbc4.jar${PS}drivers/tdgssjava.jar${PS}drivers/tdgssconfig.jar" \
        mondrian.test.loader.MondrianFoodMartLoader \
        -verbose -tables -data -indexes \
        -jdbcDrivers=com.ncr.teradata.TeraDriver \
        -inputFile=demo/FoodMartCreateData.sql \
        -outputJdbcURL="jdbc:teradata://localhost/foodmart" \
        -outputJdbcUser="tduser" \
        -outputJdbcPassword="tduser"
}

cd $(dirname $0)
teradata

# End loadFoodMart
