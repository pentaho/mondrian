#!/bin/bash
# do not use ksh, use bash
#####################################################################
#
# cmdrunner.sh
#
# Must set location of the cmdrunner.jar
#
# $Id$
#####################################################################

declare -r DIR=$(dirname $0)

#
# attempt to find jar
#
if [[ -e "$DIR"/../lib/cmdrunner.jar ]] ; then
    CMD_RUNNER_JAR="$DIR"/../lib/cmdrunner.jar
elif [[ -e "$DIR"/lib/cmdrunner.jar ]] ; then
    CMD_RUNNER_JAR="$DIR"/lib/cmdrunner.jar
elif [[ -e ./cmdrunner.jar ]] ; then
    CMD_RUNNER_JAR=./cmdrunner.jar
else 
    echo "Can not locate cmdrunner.jar"
    exit 1
fi

java -jar "$CMD_RUNNER_JAR" "$@"
