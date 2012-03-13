@echo off
setlocal

rem This software is subject to the terms of the Eclipse Public License v1.0
rem Agreement, available at the following URL:
rem http://www.eclipse.org/legal/epl-v10.html.
rem You must accept the terms of that agreement to use this software.
rem
rem Copyright (C) 2001-2005 Julian Hyde and others
rem All Rights Reserved.

set SRCROOT=%~dp0

if exist "%HOME_DRIVE%" goto homeDriveOk
set HOME_DRIVE=E
:homeDriveOk

if exist "%JAVA_HOME%" goto javaHomeOk
set JAVA_HOME=%HOME_DRIVE%:/j2sdk1.4.1_01
if exist "%JAVA_HOME%" goto javaHomeOk
echo JAVA_HOME (%JAVA_HOME%) does not exist
goto end
:javaHomeOk

echo Using JAVA_HOME %JAVA_HOME%

set PATH=%JAVA_HOME%/bin;%PATH%

if exist "%ANT_HOME%" goto antHomeOk
set ANT_HOME=%HOME_DRIVE%:/apache-ant-1.6.1
if exist "%ANT_HOME%" goto antHomeOk
echo ANT_HOME (%ANT_HOME%) does not exist
goto end
:antHomeOk

if exist "%JUNIT_HOME%" goto junitHomeOk
set JUNIT_HOME=%HOME_DRIVE%:/junit3.8.1
if exist "%JUNIT_HOME%" goto junitHomeOk
echo JUNIT_HOME (%JUNIT_HOME%) does not exist
goto end
:junitHomeOk

if exist "%CATALINA_HOME%" goto catalinaHomeOk
set CATALINA_HOME=%HOME_DRIVE%:/jakarta-tomcat-5.0.25
if exist "%CATALINA_HOME%" goto catalinaHomeOk
echo CATALINA_HOME (%CATALINA_HOME%) does not exist
goto end
:catalinaHomeOk

set CLASSPATH=%SRCROOT%classes;%SRCROOT%lib/javacup.jar;%SRCROOT%lib/eigenbase-xom.jar;%SRCROOT%lib/eigenbase-resgen.jar

rem To use Oracle, uncomment the next line and modify appropriately
rem set ORACLE_HOME=%HOME_DRIVE%:/oracle/ora81
if "%ORACLE_HOME%" == "" goto oracleHomeNotSet
if exist "%ORACLE_HOME%" goto oracleHomeOk
echo ORACLE_HOME (%ORACLE_HOME%) does not exist
goto end
:oracleHomeOk
set CLASSPATH=%CLASSPATH%;%ORACLE_HOME%/jdbc/lib/classes12.zip
:oracleHomeNotSet

rem To use MySQL, uncomment the next 2 lines and modify appropriately
rem set MYSQL_HOME=%HOME_DRIVE%:/MySQL
rem set CLASSPATH=%CLASSPATH%;%MYSQL_HOME%/lib/mm.mysql-2.0.4-bin.jar

rem To use Weblogic, uncomment the next line and modify appropriately.
rem set WEBLOGIC_HOME=%HOME_DRIVE%:/bea/wlserver6.1

%ANT_HOME%\bin\ant %1 %2 %3 %4 %5 %6 %7 %8 %9
:end
endlocal
rem end build.bat
