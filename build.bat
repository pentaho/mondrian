@rem $Id$
@rem This software is subject to the terms of the Common Public License
@rem Agreement, available at the following URL:
@rem http://www.opensource.org/licenses/cpl.html.
@rem (C) Copyright 2001-2002 Kana Software, Inc. and others.
@rem All Rights Reserved.
@rem You must accept the terms of that agreement to use this software.

@set SRCROOT=%~dp0
@set HOME_DRIVE=Z

@set ANT_HOME=%HOME_DRIVE%:\jakarta-ant-1.5
@if exist "%ANT_HOME%" goto x010
@echo ANT_HOME (%ANT_HOME%) does not exist
@goto end
:x010

@set XALAN_HOME=%HOME_DRIVE%:/xalan-j_2_4_D1
@if exist "%XALAN_HOME%" goto x030
@echo XALAN_HOME (%XALAN_HOME%) does not exist
@goto end
:x030

@set JUNIT_HOME=%HOME_DRIVE%:/junit3.7
@if exist "%JUNIT_HOME%" goto x040
@echo JUNIT_HOME (%JUNIT_HOME%) does not exist
@goto end
:x040

@set TOMCAT_HOME=%HOME_DRIVE%:\jakarta-tomcat-4.0.4
@if exist "%TOMCAT_HOME%" goto x050
@echo TOMCAT_HOME (%TOMCAT_HOME%) does not exist
@goto end
:x050

@if exist "%TOMCAT_HOME%\common\lib\xalan.jar" goto x100
@echo You must copy xalan.jar from XALAN/bin to TOMCAT/common/lib
@goto end
:x100

@if exist "%TOMCAT_HOME%\common\lib\xml-apis.jar" goto x200
@echo You must copy xml-apis.jar from XALAN/bin to TOMCAT/common/lib
@goto end
:x200

@set CLASSPATH=%SRCROOT%/classes;%SRCROOT%/lib/javacup.jar;%SRCROOT%/lib/boot.jar;%XALAN_HOME%/bin/xml-apis.jar;%XALAN_HOME%/lib/xercesImpl.jar

@rem To use Oracle, uncomment this line and modify appropriately
@rem set ORACLE_HOME=%HOME_DRIVE%:/oracle/ora81
@if "%ORACLE_HOME%" == "" goto x300
@if exist "%ORACLE_HOME%" goto x290
@echo ORACLE_HOME (%ORACLE_HOME%) does not exist
@goto end
:x290
@set CLASSPATH=%CLASSPATH%;%ORACLE_HOME%/jdbc/lib/classes12.zip
:x300

@%ANT_HOME%\bin\ant %1 %2 %3 %4 %5 %6 %7 %8 %9
:end
@rem end build.bat
