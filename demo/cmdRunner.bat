@echo off
set CP=../lib/commons-dbcp.jar;../lib/commons-collections.jar;../lib/commons-pool.jar
set CP=%CP%;../lib/eigenbase-properties.jar;../lib/eigenbase-resgen.jar;../lib/eigenbase-xom.jar
set CP=%CP%;../lib/javacup.jar;../lib/log4j-1.2.9.jar;../lib/mondrian.jar
set CP=%CP%;../testlib/mysql-connector-java-3.1.16-ga-bin.jar
set CP=%CP%;../testlib/postgresql-driver-jdbc3-74-214.jar

java -Xms100m -Xmx500m -cp "%CP%" -Dlog4j.configuration=file:///C:/Temp/wip/log4j.xml mondrian.tui.CmdRunner -p CmdRunner.properties -f CmdRunner.cmdr
