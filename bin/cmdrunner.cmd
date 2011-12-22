@echo off
rem
rem
rem cmdrunner.cmd
rem
rem Must set location of the cmdrunner.jar
rem
rem $Id: $
rem

set JAVA_COMMAND=%JAVA_HOME%\bin\java

set CMD_RUNNER_JAR=..\lib\cmdrunner.jar

%JAVA_COMMAND% -jar %CMD_RUNNER_JAR% %*
