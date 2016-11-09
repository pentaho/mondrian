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

CP="${MONDRIAN_HOME}/lib/*"
CP="${CP}${PS}${MONDRIAN_HOME}/plugins/*"
CP="${CP}${PS}${MONDRIAN_HOME}/drivers/*"

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
