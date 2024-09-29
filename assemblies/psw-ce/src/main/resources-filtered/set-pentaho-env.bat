REM ******************************************************************************
REM
REM Pentaho
REM
REM Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
REM
REM Use of this software is governed by the Business Source License included
REM in the LICENSE.TXT file.
REM
REM Change Date: 2028-08-13
REM ******************************************************************************

if not "%PENTAHO_JAVA%" == "" goto gotPentahoJava
set __LAUNCHER=java.exe
goto checkPentahoJavaHome

:gotPentahoJava
set __LAUNCHER=%PENTAHO_JAVA%
goto checkPentahoJavaHome

:checkPentahoJavaHome
if exist "%~1\bin\%__LAUNCHER%" goto gotValueFromCaller
if not "%PENTAHO_JAVA_HOME%" == "" goto gotPentahoJavaHome
if exist "%~dp0jre\bin\%__LAUNCHER%" goto gotJreCurrentFolder
if exist "%~dp0java\bin\%__LAUNCHER%" goto gotJavaCurrentFolder
if exist "%~dp0..\jre\bin\%__LAUNCHER%" goto gotJreOneFolderUp
if exist "%~dp0..\java\bin\%__LAUNCHER%" goto gotJavaOneFolderUp
if exist "%~dp0..\..\jre\bin\%__LAUNCHER%" goto gotJreTwoFolderUp
if exist "%~dp0..\..\java\bin\%__LAUNCHER%" goto gotJavaTwoFolderUp
if not "%JAVA_HOME%" == "" goto gotJdkHome
if not "%JRE_HOME%" == "" goto gotJreHome
goto gotPath

:gotPentahoJavaHome
echo DEBUG: Using PENTAHO_JAVA_HOME
set _PENTAHO_JAVA_HOME=%PENTAHO_JAVA_HOME%
set _PENTAHO_JAVA=%_PENTAHO_JAVA_HOME%\bin\%__LAUNCHER%
goto end

:gotJreCurrentFolder
echo DEBUG: Found JRE at the current folder
set _PENTAHO_JAVA_HOME=%~dp0jre
set _PENTAHO_JAVA=%_PENTAHO_JAVA_HOME%\bin\%__LAUNCHER%
goto end

:gotJavaCurrentFolder
echo DEBUG: Found JAVA at the current folder
set _PENTAHO_JAVA_HOME=%~dp0java
set _PENTAHO_JAVA=%_PENTAHO_JAVA_HOME%\bin\%__LAUNCHER%
goto end

:gotJreOneFolderUp
echo DEBUG: Found JRE one folder up
set _PENTAHO_JAVA_HOME=%~dp0..\jre
set _PENTAHO_JAVA=%_PENTAHO_JAVA_HOME%\bin\%__LAUNCHER%
goto end

:gotJavaOneFolderUp
echo DEBUG: Found JAVA one folder up
set _PENTAHO_JAVA_HOME=%~dp0..\java
set _PENTAHO_JAVA=%_PENTAHO_JAVA_HOME%\bin\%__LAUNCHER%
goto end

:gotJreTwoFolderUp
echo DEBUG: Found JRE two folder up
set _PENTAHO_JAVA_HOME=%~dp0..\..\jre
set _PENTAHO_JAVA=%_PENTAHO_JAVA_HOME%\bin\%__LAUNCHER%
goto end

:gotJavaTwoFolderUp
echo DEBUG: Found JAVA two folder up
set _PENTAHO_JAVA_HOME=%~dp0..\..\java
set _PENTAHO_JAVA=%_PENTAHO_JAVA_HOME%\bin\%__LAUNCHER%
goto end

:gotJdkHome
echo DEBUG: Using JAVA_HOME
set _PENTAHO_JAVA_HOME=%JAVA_HOME%
set _PENTAHO_JAVA=%_PENTAHO_JAVA_HOME%\bin\%__LAUNCHER%
goto end

:gotJreHome
echo DEBUG: Using JRE_HOME
set _PENTAHO_JAVA_HOME=%JRE_HOME%
set _PENTAHO_JAVA=%_PENTAHO_JAVA_HOME%\bin\%__LAUNCHER%
goto end

:gotValueFromCaller
echo DEBUG: Using value (%~1) from calling script
set _PENTAHO_JAVA_HOME=%~1
set _PENTAHO_JAVA=%_PENTAHO_JAVA_HOME%\bin\%__LAUNCHER%
goto end

:gotPath
echo WARNING: Using java from path
set _PENTAHO_JAVA_HOME=
set _PENTAHO_JAVA=%__LAUNCHER%

goto end

:end

echo DEBUG: _PENTAHO_JAVA_HOME=%_PENTAHO_JAVA_HOME%
echo DEBUG: _PENTAHO_JAVA=%_PENTAHO_JAVA%
