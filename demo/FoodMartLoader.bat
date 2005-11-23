
rem example script for MondrianFoodMartLoader

@echo off
set CP=../lib/commons-dbcp.jar;../lib/commons-collections.jar;../lib/commons-pool.jar
set CP=%CP%;../lib/eigenbase-properties.jar;../lib/eigenbase-resgen.jar;../lib/eigenbase-xom.jar
set CP=%CP%;../lib/javacup.jar;../lib/mondrian.jar
set CP=%CP%;../lib/log4j-1.2.9.jar
set CP=%CP%;../testlib/mysql-connector-java-3.1.11-bin.jar
set CP=%CP%;../testlib/postgresql-driver-jdbc3-74-214.jar
set CP=%CP%;../testlib/jtds-1.2.jar



java -Xms100m -Xmx500m -cp "%CP%" -Dlog4j.configuration=file:///C:/Temp/runnerLog4j.xml mondrian.test.loader.MondrianFoodMartLoader -tables -data -indexes -inputFile="C:/Documents and Settings/swood/My Documents/perforce/mondrian/demo/FoodMartCreateData.sql" -jdbcDrivers=net.sourceforge.jtds.jdbc.Driver -outputJdbcURL="jdbc:jtds:sqlserver://localhost:1433/FoodMart;tds=8.0;lastupdatecount=true" -outputJdbcUser=sa -outputJdbcPassword=password