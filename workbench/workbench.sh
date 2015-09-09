#!/bin/bash
#
# This software is subject to the terms of the Eclipse Public License v1.0
# Agreement, available at the following URL:
# http://www.eclipse.org/legal/epl-v10.html.
# You must accept the terms of that agreement to use this software.
#
# Copyright (C) 2007-2010 Pentaho and others
# All Rights Reserved.
#
# Launch Mondrian Schema Workbench on Linux, UNIX or Cygwin

# Platform specific path-separator.

# first look in directory of the script for lib, then 
# look up one folder if lib does not exist

MONDRIAN_HOME=`cd \`dirname $0\`; pwd`
if test ! -d "$MONDRIAN_HOME/lib"; then
    MONDRIAN_HOME="`cd \`dirname $0\`/..; pwd`"
fi
case `uname` in
Windows_NT|CYGWIN*)
    export PS=";"
    export MONDRIAN_HOME=`cygpath -m $MONDRIAN_HOME`
    ;;
*)
    export PS=":"
    ;;
esac

CP="${MONDRIAN_HOME}/lib/commons-collections.jar"
CP="${CP}${PS}${MONDRIAN_HOME}/lib/commons-pool.jar"
CP="${CP}${PS}${MONDRIAN_HOME}/lib/commons-dbcp.jar"
CP="${CP}${PS}${MONDRIAN_HOME}/lib/commons-io.jar"
CP="${CP}${PS}${MONDRIAN_HOME}/lib/commons-lang.jar"
CP="${CP}${PS}${MONDRIAN_HOME}/lib/eigenbase-properties.jar"
CP="${CP}${PS}${MONDRIAN_HOME}/lib/eigenbase-resgen.jar"
CP="${CP}${PS}${MONDRIAN_HOME}/lib/eigenbase-xom.jar"
CP="${CP}${PS}${MONDRIAN_HOME}/lib/javacup.jar"
CP="${CP}${PS}${MONDRIAN_HOME}/lib/log4j.jar"
CP="${CP}${PS}${MONDRIAN_HOME}/lib/mondrian.jar"
CP="${CP}${PS}${MONDRIAN_HOME}/lib/olap4j.jar"
CP="${CP}${PS}${MONDRIAN_HOME}/lib/jlfgr.jar"
CP="${CP}${PS}${MONDRIAN_HOME}/lib/commons-math.jar"
CP="${CP}${PS}${MONDRIAN_HOME}/lib/commons-vfs.jar"
CP="${CP}${PS}${MONDRIAN_HOME}/lib/commons-logging.jar"

# Workbench GUI code and resources
CP="${CP}${PS}${MONDRIAN_HOME}/lib/workbench.jar"

# local directory is ~/.schemaWorkbench

if test ! -d ${HOME}/.schemaWorkbench; then
    mkdir ${HOME}/.schemaWorkbench
fi

# copy mondrian.properties and log4j.xml if necessary
if test ! -e ${HOME}/.schemaWorkbench/mondrian.properties; then
    cp mondrian.properties ${HOME}/.schemaWorkbench/mondrian.properties
fi

if test ! -e ${HOME}/.schemaWorkbench/log4j.xml; then
    cp log4j.xml ${HOME}/.schemaWorkbench/log4j.xml
fi

CP="${CP}${PS}${HOME}/.schemaWorkbench"


# or
# set the log4j.properties system property 
# "-Dlog4j.properties=path to <.properties or .xml file>"
# in the java command below to adjust workbench logging

# add all needed JDBC drivers to the classpath
for i in `ls ${MONDRIAN_HOME}/drivers/*.jar 2> /dev/null`; do
    CP="${CP}${PS}${i}"
done

# add all needed plugins to the classpath
for i in `ls ${MONDRIAN_HOME}/plugins/*.jar 2> /dev/null`; do
    CP="${CP}${PS}${i}"
done

#echo $CP

JAVA_FLAGS="-Xms1024m -Xmx2048m"
#JAVA_FLAGS="-verbose $JAVA_FLAGS"

# Standard pentaho environment. Script lives in workbench directory in a
# development environment, MONDRIAN_HOME otherwise.
if test -x "$MONDRIAN_HOME/workbench/set-pentaho-env.sh"; then
    . "$MONDRIAN_HOME/workbench/set-pentaho-env.sh"
else
    . "$MONDRIAN_HOME/set-pentaho-env.sh"
fi
setPentahoEnv

exec "$_PENTAHO_JAVA" $JAVA_FLAGS -cp "$CP" mondrian.gui.Workbench

# End workbench.sh
