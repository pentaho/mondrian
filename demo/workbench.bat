rem @echo off

rem Schema Workbench launch script


rem base Mondrian JARs need to be included

set CP=lib/commons-dbcp.jar;lib/commons-collections.jar;lib/commons-pool.jar
set CP=%CP%;lib/eigenbase-properties.jar;lib/eigenbase-resgen.jar;lib/eigenbase-xom.jar
set CP=%CP%;lib/javacup.jar;lib/log4j-1.2.12.jar;lib/mondrian.jar
set CP=%CP%;lib/jlfgr-1_0.jar;lib/jmi.jar;lib/mof.jar;lib/commons-math-1.0.jar
set CP=%CP%;lib/commons-vfs.jar;lib/commons-logging.jar

rem Workbench GUI code and resources

set CP=%CP%;lib/workbench.jar

rem Have a .schemaWorkbench directory for local 

for /F "delims=/" %%i in ('echo %USERPROFILE%') do set ROOT=%%~si

if not exist %ROOT%\.schemaWorkbench mkdir %ROOT%\.schemaWorkbench
if not exist %ROOT%\.schemaWorkbench\log4j.xml copy log4j.xml %ROOT%\.schemaWorkbench
if not exist %ROOT%\.schemaWorkbench\mondrian.properties copy ..\mondrian.properties %ROOT%\.schemaWorkbench

rem put mondrian.properties on the classpath for it to be picked up

set CP=%ROOT%/.schemaWorkbench;%CP%

rem or
rem set the log4j.properties system property 
rem "-Dlog4j.properties=path to <.properties or .xml file>"
rem in the java command below to adjust workbench logging

rem add all needed JDBC drivers to the classpath

for %%i in ("drivers\*.jar") do call "cpappend.bat" %%i

java -Xms100m -Xmx500m -cp "%CP%" -Dlog4j.configuration=file:///%ROOT%\.schemaWorkbench\log4j.xml mondrian.gui.Workbench