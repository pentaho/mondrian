#!/usr/bin/ksh
# $Id$
# This software is subject to the terms of the Common Public License
# Agreement, available at the following URL:
# http://www.opensource.org/licenses/cpl.html.
# (C) Copyright 2003-2003 Julian Hyde
# All Rights Reserved.
# You must accept the terms of that agreement to use this software.

export SRCROOT=$(cd $(dirname $0)/..; pwd)
export LIB=${SRCROOT}/lib
case $(uname) in
Windows_NT)
  export PS=";" ;;
*)
  export PS=":" ;;
esac
# export JAVA_HOME=C:/jdk1.3.1_02
if [ ! -f  "${JAVA_HOME}/bin/javac.exe" -a \
    ! -f "${JAVA_HOME}/bin/javac" ]; then
  echo "JAVA_HOME (${JAVA_HOME}) is not set correctly"
  exit 1
fi

export CLASSPATH="${LIB}/ant.jar${PS}${LIB}/optional.jar${PS}${LIB}/xercesImpl.jar${PS}${LIB}/xml-apis.jar${PS}${LIB}/junit.jar"
echo $CLASSPATH
${JAVA_HOME}/bin/java -classpath "${CLASSPATH}" -Dant.home="${SRCROOT}" org.apache.tools.ant.Main -buildfile runtime.xml "$@"

# End mondrian.sh

