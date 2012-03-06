# $Id$
# Called recursively from 'ant release' to build the files which can only be
# built under a particular JDK version.
#
# Usage:
#   buildOnJdk.sh <jdk version> <ant args>
#
# For example:
#   buildOnJdk.sh jdk1.6 compile.java
########################################################################
jdkVersion=$1
shift

########################################################################
# If you are building Mondrian for a single JDK (most likely the
# case), leave the following lines commented.
#
# If you are building a release, you will need to compile different
# parts of Mondrian's source code under different JDKs.  Uncomment the
# following lines to ensure that JAVA_HOME is assigned correctly for
# each JDK version.
#
########################################################################
#case "$jdkVersion" in
#(*) export JAVA_HOME=/usr/lib/jvm/${jdkVersion};;
#esac
#
#case $(uname) in
#Darwin)
#    JDK_VERSION=`echo "${jdkVersion}" | sed 's/jdk\([0-9]\.[0-9]\)/\1/'`
#    export JAVA_HOME=/System/Library/Frameworks/JavaVM.framework/Versions/${JDK_VERSION}/Home;;
#*)
#    export JAVA_HOME=/usr/lib/jvm/${jdkVersion};;
#esac
########################################################################
# From this point, you don't need to modify anything... we hope.

########################################################################
# Validate that JAVA_HOME exists
if [ ! -d $JAVA_HOME ]; then
   echo "$0: Invalid JAVA_HOME $JAVA_HOME; skipping compile."
   exit 1
fi

########################################################################
# Setting the PATH env variable
export PATH=$JAVA_HOME/bin:$PATH

########################################################################
# Validating that Ant is present
if [ ! -f "/opt/ant1.7/bin/ant" ]; then
    ANT_BIN=`which ant`
    if [ -z "$ANT_BIN" ]; then
        echo "$0: Unable to locate ant binary; skipping compile."
        exit 1
    else
        echo Using JAVA_HOME: $JAVA_HOME
        echo Using Ant arguments: -Drequested.java.version="$jdkVersion" $@
        $ANT_BIN -Drequested.java.version="$jdkVersion" "$@"
    fi
else
    echo Using JAVA_HOME: $JAVA_HOME
    echo Using Ant arguments: -Drequested.java.version="$jdkVersion" $@
    /opt/ant1.7/bin/ant -Drequested.java.version="$jdkVersion" "$@"
fi

########################################################################
# End buildOnJdk.sh
