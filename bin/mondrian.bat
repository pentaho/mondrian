@rem $Id$
@rem This software is subject to the terms of the Common Public License
@rem Agreement, available at the following URL:
@rem http://www.opensource.org/licenses/cpl.html.
@rem (C) Copyright 2001-2003 Kana Software, Inc. and others.
@rem All Rights Reserved.
@rem You must accept the terms of that agreement to use this software.

@set SRCROOT=%~dp0\..
@set LIB=%SRCROOT%\lib
@rem set JAVA_HOME=C:/jdk1.3.1_02
@if exist "%JAVA_HOME%/bin/javac.exe" goto javaOk
@echo JAVA_HOME (%JAVA_HOME%) is not set correctly
@goto end
:javaOk

@set CLASSPATH=%LIB%\mondrian.jar
@set CLASSPATH=%CLASSPATH%;%LIB%\ant.jar
@set CLASSPATH=%CLASSPATH%;%LIB%\optional.jar
@set CLASSPATH=%CLASSPATH%;%LIB%\xercesImpl.jar
@set CLASSPATH=%CLASSPATH%;%LIB%\xml-apis.jar
@set CLASSPATH=%CLASSPATH%;%LIB%\junit.jar

%JAVA_HOME%\bin\java -classpath "%CLASSPATH%" -Dant.home="%SRCROOT%" org.apache.tools.ant.Main -buildfile runtime.xml %1 %2 %3 %4 %5 %6 %7 %8 %9

:end
@rem End mondrian.bat

