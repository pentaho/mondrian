#!/bin/bash
# do not use ksh, use bash
#####################################################################
#
# cmdrunner.sh
#
# Relies on java -cp "LIB/*" syntax to avoid re-jarring
# No guessing where the path is.  Must be run from the root mondrian
# folder via the command: $ bin/cr.sh
#
# $Id$
#####################################################################

CP="$CLASSPATH:lib/*"
CP="$CP:$(awk -F= '/^driver.classpath=/ { print $2; }' mondrian.properties)"
CP="$CP:$HADOOP_HOME/share/hadoop/common/*"
CP="$CP:$HADOOP_HOME/share/hadoop/common/lib/*"
CP="$CP:$HADOOP_HOME/share/hadoop/hdfs/*"
CP="$CP:$TAJO_HOME/*"
CP="$CP:$TAJO_HOME/lib/*"
java -cp "$CP" mondrian.tui.CmdRunner "$@"
