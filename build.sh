#!/usr/bin/ksh
# $Id$
# This software is subject to the terms of the Common Public License
# Agreement, available at the following URL:
# http://www.opensource.org/licenses/cpl.html.
# Copyright (C) 2003 Julian Hyde
# All Rights Reserved.
# You must accept the terms of that agreement to use this software.

export SRCROOT=$(cd $(dirname $0); pwd)
export PREFIX="E:" # E.g. "/usr" on Unix, "E:" on Windows
case $(uname) in
Windows_NT)
  export PS=";" ;;
*)
  export PS=":" ;;
esac
export JAVA_HOME="${PREFIX}/j2sdk1.4.1_01"
export PATH="${JAVA_HOME}/bin${PS}${PATH}"

export ANT_HOME="${PREFIX}/jakarta-ant-1.5"
if [ ! -d "${ANT_HOME}" ]; then
  echo "ANT_HOME (${ANT_HOME}) does not exist"
  exit 1
fi

export XALAN_HOME="${PREFIX}/xalan-j_2_4_1"
if [ ! -d "${XALAN_HOME}" ]; then
  echo "XALAN_HOME (${XALAN_HOME}) does not exist"
  exit 1
fi

export JUNIT_HOME="${PREFIX}/junit3.7"
if [ ! -d "${JUNIT_HOME}" ]; then
  echo "JUNIT_HOME (${JUNIT_HOME}) does not exist"
  exit 1
fi

export CATALINA_HOME="${PREFIX}/jakarta-tomcat-4.1.18"
if [ ! -d "${CATALINA_HOME}" ]; then
  echo "CATALINA_HOME (${CATALINA_HOME}) does not exist"
  exit 1
fi

export CLASSPATH="${SRCROOT}/classes${PS}${SRCROOT}/lib/javacup.jar${PS}${SRCROOT}/lib/boot.jar${PS}${XALAN_HOME}/bin/xml-apis.jar${PS}${XALAN_HOME}/bin/xercesImpl.jar${PS}${JUNIT_HOME}/junit.jar"

# To use Oracle, uncomment the next line and modify appropriately
# set ORACLE_HOME="${PREFIX}/oracle/ora81"
if [ "${ORACLE_HOME}" ]; then
  if [ ! -d "${ORACLE_HOME}" ]; then
    echo "ORACLE_HOME (${ORACLE_HOME}) does not exist"
  fi
  export CLASSPATH="${CLASSPATH}${PS}${ORACLE_HOME}/jdbc/lib/classes12.zip"
fi

# To use MySQL, uncomment the next 2 lines and modify appropriately
# set MYSQL_HOME=${PREFIX}/MySQL
# set CLASSPATH=${CLASSPATH}${PS}${MYSQL_HOME}/lib/mm.mysql-2.0.4-bin.jar

# To use Weblogic, uncomment the next line and modify appropriately.
# set WEBLOGIC_HOME=${PREFIX}/bea/wlserver6.1

${ANT_HOME}/bin/ant "$@"
# end build.sh
