@rem $Id$
@rem This software is subject to the terms of the Common Public License
@rem Agreement, available at the following URL:
@rem http://www.opensource.org/licenses/cpl.html.
@rem (C) Copyright 2001-2002 Kana Software, Inc. and others.
@rem All Rights Reserved.
@rem You must accept the terms of that agreement to use this software.

@set SRCROOT=%~dp0
@set HOME_DRIVE=Z
@set ANT_HOME=%HOME_DRIVE%:\jakarta-ant-1.4.1
@set TOMCAT_HOME=%HOME_DRIVE%:\jakarta-tomcat-4.0.3
@set ORACLE_HOME=%HOME_DRIVE%:/oracle/ora81
@set XALAN_HOME=%HOME_DRIVE%:/xalan-j_2_3_1
@set JUNIT_HOME=%HOME_DRIVE%:/junit3.7
@if exist "%TOMCAT_HOME%\common\lib\xalan.jar" goto x100
@echo You must copy xalan.jar from XALAN to TOMCAT/common/lib
@goto end
:x100
@if exist "%TOMCAT_HOME%\common\lib\xml-apis.jar" goto x200
@echo You must copy xml-apis.jar from XALAN to TOMCAT/common/lib
@goto end
:x200
set CLASSPATH=%SRCROOT%/classes;%SRCROOT%/lib/javacup.jar;%TOMCAT_HOME%/common/lib/xerces.jar;%SRCROOT%/lib/boot.jar;%ORACLE_HOME%/jdbc/lib/classes12.zip
@%ANT_HOME%\bin\ant %1 %2 %3 %4 %5 %6 %7 %8 %9
:end
@rem end build.bat
