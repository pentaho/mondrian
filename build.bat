@rem $Id$
@rem This software is subject to the terms of the Common Public License
@rem Agreement, available at the following URL:
@rem http://www.opensource.org/licenses/cpl.html.
@rem (C) Copyright 2001-2003 Kana Software, Inc. and others.
@rem All Rights Reserved.
@rem You must accept the terms of that agreement to use this software.

@set SRCROOT=%~dp0

@if exist "%HOME_DRIVE%" goto x001
@set HOME_DRIVE=E
:x001

@if exist "%JAVA_HOME%" goto x002
@set JAVA_HOME=%HOME_DRIVE%:/j2sdk1.4.1_01
@if exist "%JAVA_HOME%" goto x002
@echo JAVA_HOME (%JAVA_HOME%) does not exist
@goto end
:x002

@set PATH=%JAVA_HOME%/bin;%PATH%

@if exist "%ANT_HOME%" goto x010
@set ANT_HOME=%HOME_DRIVE%:\jakarta-ant-1.5
@if exist "%ANT_HOME%" goto x010
@echo ANT_HOME (%ANT_HOME%) does not exist
@goto end
:x010

@if exist "%XALAN_HOME%" goto x030
@set XALAN_HOME=%HOME_DRIVE%:/xalan-j_2_4_1
@if exist "%XALAN_HOME%" goto x030
@echo XALAN_HOME (%XALAN_HOME%) does not exist
@goto end
:x030

@if exist "%JUNIT_HOME%" goto x040
@set JUNIT_HOME=%HOME_DRIVE%:/junit3.7
@if exist "%JUNIT_HOME%" goto x040
@echo JUNIT_HOME (%JUNIT_HOME%) does not exist
@goto end
:x040

@if exist "%CATALINA_HOME%" goto x050
@set CATALINA_HOME=%HOME_DRIVE%:\jakarta-tomcat-4.1.18
@if exist "%CATALINA_HOME%" goto x050
@echo CATALINA_HOME (%CATALINA_HOME%) does not exist
@goto end
:x050

@set CLASSPATH=%SRCROOT%/classes;%SRCROOT%/lib/javacup.jar;%SRCROOT%/lib/boot.jar;%XALAN_HOME%/bin/xml-apis.jar;%XALAN_HOME%/bin/xercesImpl.jar;%JUNIT_HOME%/junit.jar

@rem To use Oracle, uncomment the next line and modify appropriately
@rem set ORACLE_HOME=%HOME_DRIVE%:/oracle/ora81
@if "%ORACLE_HOME%" == "" goto x300
@if exist "%ORACLE_HOME%" goto x290
@echo ORACLE_HOME (%ORACLE_HOME%) does not exist
@goto end
:x290
@set CLASSPATH=%CLASSPATH%;%ORACLE_HOME%/jdbc/lib/classes12.zip
:x300

@rem To use MySQL, uncomment the next 2 lines and modify appropriately
@rem set MYSQL_HOME=%HOME_DRIVE%:/MySQL
@rem set CLASSPATH=%CLASSPATH%;%MYSQL_HOME%/lib/mm.mysql-2.0.4-bin.jar

@rem To use Weblogic, uncomment the next line and modify appropriately.
@rem set WEBLOGIC_HOME=%HOME_DRIVE%:/bea/wlserver6.1

@%ANT_HOME%\bin\ant %1 %2 %3 %4 %5 %6 %7 %8 %9
:end
@rem end build.bat
