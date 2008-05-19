#!/bin/bash
# do not use ksh, use bash
#####################################################################
#
# runcmd.sh
#
# Must set location of the cmdrunner.jar
#
# $Id$
#####################################################################

declare -r DIR=$(dirname $0)

LOCATION="$PWD/$DIR"
MONDRIAN_HOME="$LOCATION/.."
MONDRIAN_LIB="$MONDRIAN_HOME/lib"
MONDRIAN_TEST_LIB="$MONDRIAN_HOME/testlib"


CLASSPATH="$MONDRIAN_LIB/mondrian.jar"
CLASSPATH="$CLASSPATH:$MONDRIAN_LIB/commons-dbcp.jar"
CLASSPATH="$CLASSPATH:$MONDRIAN_LIB/commons-collections.jar"
CLASSPATH="$CLASSPATH:$MONDRIAN_LIB/commons-pool.jar"
CLASSPATH="$CLASSPATH:$MONDRIAN_LIB/eigenbase-resgen.jar"
CLASSPATH="$CLASSPATH:$MONDRIAN_LIB/eigenbase-xom.jar"
CLASSPATH="$CLASSPATH:$MONDRIAN_LIB/javacup.jar"
CLASSPATH="$CLASSPATH:$MONDRIAN_LIB/log4j.jar"
CLASSPATH="$CLASSPATH:$MONDRIAN_LIB/servlet-api.jar"
CLASSPATH="$CLASSPATH:$MONDRIAN_LIB/eigenbase-properties.jar"
CLASSPATH="$CLASSPATH:$MONDRIAN_LIB/commons-math.jar"
CLASSPATH="$CLASSPATH:$MONDRIAN_LIB/commons-vfs.jar"
CLASSPATH="$CLASSPATH:$MONDRIAN_LIB/commons-logging.jar"
CLASSPATH="$CLASSPATH:$MONDRIAN_LIB/derby.jar"

# now pick up jdbc jars
for j in $MONDRIAN_TEST_LIB/*.jar
do  
    CLASSPATH="$CLASSPATH:$j"
done

export CLASSPATH

MAIN=mondrian.tui.CmdRunner

java \
    -Dlog4j.configuration=file://$MONDRIAN_HOME/log4j.properties \
    -cp $CLASSPATH $MAIN "$@"

