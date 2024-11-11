# ******************************************************************************
#
# Pentaho
#
# Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
#
# Use of this software is governed by the Business Source License included
# in the LICENSE.TXT file.
#
# Change Date: 2029-07-20
# ******************************************************************************


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
CP="${CP}${PS}${MONDRIAN_HOME}/lib/"

# local directory is ~/.schemaWorkbench

if test ! -d ${HOME}/.schemaWorkbench; then
    mkdir ${HOME}/.schemaWorkbench
fi

# copy mondrian.properties and log4j.xml if necessary
if test ! -e ${HOME}/.schemaWorkbench/mondrian.properties; then
    cp mondrian.properties ${HOME}/.schemaWorkbench/mondrian.properties
fi

if test ! -e ${HOME}/.schemaWorkbench/log4j2.xml; then
    cp log4j2.xml ${HOME}/.schemaWorkbench/log4j2.xml
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

JAVA_LOCALE_COMPAT=""
JAVA_ADD_OPENS=""
if $($_PENTAHO_JAVA -version 2>&1 | grep "version \"11\..*" > /dev/null )
then
  JAVA_LOCALE_COMPAT="-Djava.locale.providers=COMPAT,SPI"
fi

JAVA_ADD_OPENS="$JAVA_ADD_OPENS --add-opens=java.base/sun.net.www.protocol.jar=ALL-UNNAMED"
JAVA_ADD_OPENS="$JAVA_ADD_OPENS --add-opens=java.base/java.lang=ALL-UNNAMED"
JAVA_ADD_OPENS="$JAVA_ADD_OPENS --add-opens=java.base/java.io=ALL-UNNAMED"
JAVA_ADD_OPENS="$JAVA_ADD_OPENS --add-opens=java.base/java.lang.reflect=ALL-UNNAMED"
JAVA_ADD_OPENS="$JAVA_ADD_OPENS --add-opens=java.base/java.net=ALL-UNNAMED"
JAVA_ADD_OPENS="$JAVA_ADD_OPENS --add-opens=java.base/java.security=ALL-UNNAMED"
JAVA_ADD_OPENS="$JAVA_ADD_OPENS --add-opens=java.base/java.util=ALL-UNNAMED"
JAVA_ADD_OPENS="$JAVA_ADD_OPENS --add-opens=java.base/sun.net.www.protocol.file=ALL-UNNAMED"
JAVA_ADD_OPENS="$JAVA_ADD_OPENS --add-opens=java.base/sun.net.www.protocol.ftp=ALL-UNNAMED"
JAVA_ADD_OPENS="$JAVA_ADD_OPENS --add-opens=java.base/sun.net.www.protocol.http=ALL-UNNAMED"
JAVA_ADD_OPENS="$JAVA_ADD_OPENS --add-opens=java.base/sun.net.www.protocol.https=ALL-UNNAMED"
JAVA_ADD_OPENS="$JAVA_ADD_OPENS --add-opens=java.base/sun.reflect.misc=ALL-UNNAMED"
JAVA_ADD_OPENS="$JAVA_ADD_OPENS --add-opens=java.management/javax.management=ALL-UNNAMED"
JAVA_ADD_OPENS="$JAVA_ADD_OPENS --add-opens=java.management/javax.management.openmbean=ALL-UNNAMED"
JAVA_ADD_OPENS="$JAVA_ADD_OPENS --add-opens=java.naming/com.sun.jndi.ldap=ALL-UNNAMED"
JAVA_ADD_OPENS="$JAVA_ADD_OPENS --add-opens=java.base/java.math=ALL-UNNAMED"
JAVA_ADD_OPENS="$JAVA_ADD_OPENS --add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
JAVA_ADD_OPENS="$JAVA_ADD_OPENS --add-opens=java.base/java.nio=ALL-UNNAMED"
JAVA_ADD_OPENS="$JAVA_ADD_OPENS --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED"

exec "$_PENTAHO_JAVA" $JAVA_ADD_OPENS $JAVA_FLAGS $JAVA_LOCALE_COMPAT -cp "$CP" mondrian.gui.Workbench

# End workbench.sh
