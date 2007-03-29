@echo off

rem base Mondrian JARs

set CP=../lib/commons-dbcp.jar;../lib/commons-collections.jar;../lib/commons-pool.jar
set CP=%CP%;../lib/eigenbase-properties.jar;../lib/eigenbase-resgen.jar;../lib/eigenbase-xom.jar
set CP=%CP%;../lib/javacup.jar;../lib/log4j-1.2.9.jar;../lib/mondrian.jar
set CP=%CP%;../lib/jlfgr-1_0.jar;../lib/jmi.jar;lib/mof.jar;../lib/commons-math-1.0.jar
set CP=%CP%;../lib/commons-vfs.jar;../lib/commons-logging.jar

rem Workbench GUI code and resources

set CP=%CP%;../lib/workbench.jar

rem add all needed JDBC drivers to the classpath

set CP=%CP%;../testlib/mysql-connector-java-3.1.11-bin.jar
set CP=%CP%;../testlib/postgresql-driver-jdbc3-74-214.jar

rem set the log4j.properties system property "-Dlog4j.properties=<.properties or .xml file>"
rem in the java command below to adjust workbench logging

java -Xms100m -Xmx500m -cp "%CP%" mondrian.gui.Workbench