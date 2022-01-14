@echo off

REM ***************************************************************************
REM This software is subject to the terms of the Eclipse Public License v1.0
REM Agreement, available at the following URL:
REM http://www.eclipse.org/legal/epl-v10.html.
REM You must accept the terms of that agreement to use this software.
REM
REM Copyright (c) 2007 - ${copyright.year} ${project.organization.name}. All rights reserved.
REM ***************************************************************************

cd /D %~dp0

rem Schema Workbench launch script

rem base Mondrian JARs need to be included

set CP=lib/*;plugins/*;drivers/*;lib/

rem Have a .schemaWorkbench directory for local 

for /F "delims=/" %%i in ('echo %USERPROFILE%') do set ROOT=%%~si

if not exist %ROOT%\.schemaWorkbench mkdir %ROOT%\.schemaWorkbench
if not exist %ROOT%\.schemaWorkbench\log4j2.xml copy log4j2.xml %ROOT%\.schemaWorkbench
if not exist %ROOT%\.schemaWorkbench\mondrian.properties copy mondrian.properties %ROOT%\.schemaWorkbench

rem put mondrian.properties on the classpath for it to be picked up

set CP=%ROOT%/.schemaWorkbench;%CP%

rem or
rem set the log4j.properties system property 
rem "-Dlog4j.properties=path to <.properties or .xml file>"
rem in the java command below to adjust workbench logging

set PENTAHO_JAVA=java
call "%~dp0set-pentaho-env.bat"

set ISJAVA11=0
pushd "%_PENTAHO_JAVA_HOME%"
if exist java.exe goto USEJAVAFROMPENTAHOJAVAHOME
cd bin
if exist java.exe goto USEJAVAFROMPENTAHOJAVAHOME
popd
pushd "%_PENTAHO_JAVA_HOME%\jre\bin"
if exist java.exe goto USEJAVAFROMPATH
goto USEJAVAFROMPATH
:USEJAVAFROMPENTAHOJAVAHOME
FOR /F %%a IN ('.\java.exe -version 2^>^&1^|%windir%\system32\find /C "version ""11."') DO (SET /a ISJAVA11=%%a)
popd
GOTO VERSIONCHECKDONE
:USEJAVAFROMPATH
FOR /F %%a IN ('java -version 2^>^&1^|%windir%\system32\find /C "version ""11."') DO (SET /a ISJAVA11=%%a)
popd
:VERSIONCHECKDONE

SET JAVA_LOCALE_COMPAT=
IF NOT %ISJAVA11% == 1 GOTO :SKIPLOCALE
set JAVA_LOCALE_COMPAT=-Djava.locale.providers=COMPAT,SPI
:SKIPLOCALE

echo %JAVA_LOCALE_COMPAT%
"%_PENTAHO_JAVA%" -Xms1024m -Xmx2048m %JAVA_LOCALE_COMPAT% -cp "%CP%" -Dlog4j.configurationFile=file:///%ROOT%\.schemaWorkbench\log4j2.xml mondrian.gui.Workbench

rem End workbench.bat
