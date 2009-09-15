rem ---------------------------------------------------------------------------
rem Finds a suitable Java
rem
rem Looks in well-known locations to find a suitable Java then sets two 
rem environment variables for use in other bat files. The two environment
rem variables are:
rem 
rem * _PENTAHO_JAVA_HOME - absolute path to Java home
rem * _PENTAHO_JAVA - absolute path to Java launcher (e.g. java.exe)
rem 
rem The order of the search is as follows:
rem 
rem 1. environment variable PENTAHO_JAVA_HOME - path to Java home
rem 2. environment variable JAVA_HOME - path to Java home
rem 3. environment variable JRE_HOME - path to Java home
rem 4. argument #1 - path to Java home
rem 
rem If a suitable Java is found at one of these locations, then 
rem _PENTAHO_JAVA_HOME is set to that location and _PENTAHO_JAVA is set to the 
rem absolute path of the Java launcher at that location. If none of these 
rem locations are suitable, then _PENTAHO_JAVA_HOME is set to empty string and 
rem _PENTAHO_JAVA is set to java.exe.
rem 
rem Finally, there is one final optional environment variable: PENTAHO_JAVA.
rem If set, this value is used in the construction of _PENTAHO_JAVA. If not 
rem set, then the value java.exe is used. 
rem ---------------------------------------------------------------------------

if not "%PENTAHO_JAVA%" == "" goto gotPentahoJava
set __LAUNCHER=java.exe
goto checkPentahoJavaHome

:gotPentahoJava
set __LAUNCHER=%PENTAHO_JAVA%
goto checkPentahoJavaHome

:checkPentahoJavaHome
if not "%PENTAHO_JAVA_HOME%" == "" goto gotPentahoJavaHome
if not "%JAVA_HOME%" == "" goto gotJdkHome
if not "%JRE_HOME%" == "" goto gotJreHome
goto tryValueFromCaller 

:gotPentahoJavaHome
echo DEBUG: Using PENTAHO_JAVA_HOME
set _PENTAHO_JAVA_HOME=%PENTAHO_JAVA_HOME%
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

:tryValueFromCaller
if not !%1!==!! goto gotValueFromCaller
goto :gotPath

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