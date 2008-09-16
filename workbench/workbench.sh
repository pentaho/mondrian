#!/bin/bash
# $Id$
# Launch Mondrian Schema Workbench on Linux, UNIX or Cygwin

# Platform specific path-separator.
MONDRIAN_HOME=$(cd $(dirname $0)/..; pwd)
case $(uname) in
Windows_NT|CYGWIN*)
    export PS=";"
    export MONDRIAN_HOME=$(cygpath -m $MONDRIAN_HOME)
    ;;
*)
    export PS=":"
    ;;
esac

CP="${MONDRIAN_HOME}/lib/commons-collections.jar"
CP="${CP}${PS}${MONDRIAN_HOME}/lib/commons-pool.jar"
CP="${CP}${PS}${MONDRIAN_HOME}/lib/eigenbase-properties.jar"
CP="${CP}${PS}${MONDRIAN_HOME}/lib/eigenbase-resgen.jar"
CP="${CP}${PS}${MONDRIAN_HOME}/lib/eigenbase-xom.jar"
CP="${CP}${PS}${MONDRIAN_HOME}/lib/javacup.jar"
CP="${CP}${PS}${MONDRIAN_HOME}/lib/log4j.jar"
CP="${CP}${PS}${MONDRIAN_HOME}/lib/mondrian.jar"
CP="${CP}${PS}${MONDRIAN_HOME}/lib/jlfgr.jar"
CP="${CP}${PS}${MONDRIAN_HOME}/lib/commons-math.jar"
CP="${CP}${PS}${MONDRIAN_HOME}/lib/commons-vfs.jar"
CP="${CP}${PS}${MONDRIAN_HOME}/lib/commons-logging.jar"

# Workbench GUI code and resources
CP="${CP}${PS}${MONDRIAN_HOME}/lib/workbench.jar"

# local directory is ~/.schemaWorkbench

if test ! -d ~/.schemaWorkbench; then
    mkdir ~/.schemaWorkbench
fi

# copy mondrian.properties and log4j.xml if necessary
if test ! -e ~/.schemaWorkbench/mondrian.properties; then
    cp mondrian.properties ~/.schemaWorkbench/mondrian.properties
fi

if test ! -e ~/.schemaWorkbench/log4j.xml; then
    cp log4j.xml ~/.schemaWorkbench/log4j.xml
fi

CP="${CP}${PS}~/.schemaWorkbench"


# or
# set the log4j.properties system property 
# "-Dlog4j.properties=path to <.properties or .xml file>"
# in the java command below to adjust workbench logging

# add all needed JDBC drivers to the classpath
for i in `ls drivers/*.jar 2> /dev/null`; do
    CP="${CP}${PS}${i}"
done

# add all needed plugins to the classpath
for i in `ls plugins/*.jar 2> /dev/null`; do
    CP="${CP}${PS}${i}"
done

#echo $CP

JAVA_FLAGS="-Xms100m -Xmx500m"
#JAVA_FLAGS="-verbose $JAVA_FLAGS"

exec java $JAVA_FLAGS -cp "$CP" mondrian.gui.Workbench

# End workbench.sh
