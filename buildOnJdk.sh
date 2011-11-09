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

# Change the following line to point each JDK's home.
case "$jdkVersion" in
(*) export JAVA_HOME=/usr/lib/jvm/${jdkVersion};;
esac

if [ ! -d "$JAVA_HOME" ]; then
    echo "$0: Invalid JAVA_HOME $JAVA_HOME; skipping compile."
    exit 1
fi

export PATH=$JAVA_HOME/bin:$PATH

echo Using JAVA_HOME: $JAVA_HOME
echo Using Ant arguments: $@

ant "$@"

# End buildOnJdk.sh
