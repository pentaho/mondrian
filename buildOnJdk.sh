# $Id$
# Called recursively from 'ant release' to build the files which can only be
# built under a particular JDK version.
#
# Usage:
#   buildOnJdk.sh <jdk version> <ant args>
#
# For example:
#   buildOnJdk.sh jdk1.6 compile.java

jdkVersion=$1
shift

# If you are building Mondrian for a single JDK (most likely the
# case), leave the following lines commented.
#
# If you are building a release, you will need to compile different
# parts of Mondrian's source code under different JDKs.  Uncomment the
# following lines to ensure that JAVA_HOME is assigned correctly for
# each JDK version.
#
#case "$jdkVersion" in
#(*) export JAVA_HOME=/usr/lib/jvm/${jdkVersion};;
#esac

if [ ! -d "$JAVA_HOME" ]; then
    echo "$0: Invalid JAVA_HOME $JAVA_HOME; skipping compile."
    exit 1
fi

export PATH=$JAVA_HOME/bin:$PATH

echo Using JAVA_HOME: $JAVA_HOME
echo Using Ant arguments: -Drequested.java.version="$jdkVersion" $@

ant -Drequested.java.version="$jdkVersion" "$@"

# End buildOnJdk.sh
