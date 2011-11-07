@echo off
rem $Id$
rem Called recursively from 'ant release' to build the files which can only be
rem built under JDK 1.6.

rem Change the following line to point to your JDK 1.6 home.
rem set JAVA_HOME=C:\jdk1.6.0_01

rem Change the following line to point to your ant home.
rem set ANT_HOME=C:\open\thirdparty\ant

rem set PATH=%JAVA_HOME%\bin;%PATH%
"%ANT_HOME%\bin\ant" -Dskip.download=true compile.java

rem End buildJdk16.bat

