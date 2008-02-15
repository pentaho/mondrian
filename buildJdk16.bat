# $Id$
# Called recursively from 'ant release' to build the files which can only be
# built under JDK 1.6.

# Change the following line to point to your JDK 1.6 home.
set JAVA_HOME=C:\jdk1.6.0_01
set PATH=%JAVA_HOME%\bin;%PATH%
ant compile.java

# End buildJdk16.bat

