#!/bin/bash
#
# This software is subject to the terms of the Eclipse Public License v1.0
# Agreement, available at the following URL:
# http://www.eclipse.org/legal/epl-v10.html.
# You must accept the terms of that agreement to use this software.
#
# Copyright (C) 2003-2005 Julian Hyde
# Copyright (C) 2005-2005 Pentaho
# All Rights Reserved.

export SRCROOT=$(cd $(dirname $0); pwd)
export PREFIX="E:" # E.g. "/usr" on Unix, "E:" on Windows
case $(uname) in
Windows_NT)
  export PS=";" ;;
*)
  export PS=":" ;;
esac
function foo() {
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


export CLASSPATH="${SRCROOT}/classes${PS}${SRCROOT}/lib/javacup.jar${PS}${SRCROOT}/lib/mondrian-xom.jar${PS}${SRCROOT}/lib/mondrian-resource.jar${PS}${OH_LIB}/xml-apis.jar${PS}${XALAN_HOME}/bin/xercesImpl.jar${PS}${JUNIT_HOME}/junit.jar"


}

OH_HOME=/home/emberson/OH
OH_LIB=$OH_HOME/lib

CLASSPATH="${SRCROOT}/classes
CLASSPATH=$CLASSPATH${PS}${SRCROOT}/lib/javacup.jar
CLASSPATH=$CLASSPATH${PS}${SRCROOT}/lib/mondrian-xom.jar
CLASSPATH=$CLASSPATH${PS}${SRCROOT}/lib/mondrian-resource.jar
CLASSPATH=$CLASSPATH${PS}${OH_LIB}/dom3-xml-apis-2.6.2.jar
CLASSPATH=$CLASSPATH${PS}${OH_LIB}/dom3-xercesImpl-2.6.2.jar
CLASSPATH=$CLASSPATH${PS}${OH_HOME}/build/lib/junit.jar"
export CLASSPATH

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
