#!/bin/bash
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

# Version number without jdk prefix. E.g. '1.6'.
versionSansJdk=`echo "${jdkVersion}" | sed 's/jdk\([0-9]\.[0-9]\)/\1/'`

# Chooses a JAVA_HOME to match the required JDK version.
#
# If you are building Mondrian for a single JDK (most likely the case), it
# doesn't matter if this method cannot find a valid JAVA_HOME for the JDK
# version. It will keep the existing JAVA_HOME, ant will report that the JDK
# version is not the one required, and will not compile any files. You will
# still end up with a valid build for all the classes needed by your JDK
# version.
#
# If you are building a release, you must compile different parts of Mondrian's
# source code under different JDKs.  You will need to do one of the following:
#
# 1. Specify environment variables JAVA_HOME_15, JAVA_HOME_16 and JAVA_HOME_17
# before invoking 'ant'.
#
# 2. Install a JDK in (or create a symbolic link as) one of the standard
# locations: /usr/lib/jvm/jdk1.x on Linux/Unix/Cygwin; or
# /System/Library/Frameworks/JavaVM.framework/Versions/1.x/Home on MacOS.
#
# 3. Customize this method with the correct JDK location.
#
chooseJavaHome() {
    # If JAVA_HOME_xx is specified in the environment, use it.
    case "$jdkVersion" in
    (jdk1.5)
        if [ -d "$JAVA_HOME_15" ]; then
            export JAVA_HOME="$JAVA_HOME_15"
            return
        fi
        ;;
    (jdk1.6)
        if [ -d "$JAVA_HOME_16" ]; then
            export JAVA_HOME="$JAVA_HOME_16"
            return
        fi
        ;;
    (jdk1.7)
        if [ -d "$JAVA_HOME_17" ]; then
            export JAVA_HOME="$JAVA_HOME_17"
            return
        fi
        ;;
    esac

    # 2. Look in default location based on the operating system and JDK
    # version. If the directory exists, we presume that it is a valid JDK, and
    # override JAVA_HOME.
    defaultJavaHome=
    case $(uname) in
    (Darwin)
        defaultJavaHome=/System/Library/Frameworks/JavaVM.framework/Versions/${versionSansJdk}/Home;;
    (*)
        defaultJavaHome=`readlink -f /usr/lib/jvm/${jdkVersion}`;;
    esac

    if [ -d "$defaultJavaHome" ]; then
        export JAVA_HOME="$defaultJavaHome"
        return
    fi

    # 3. If JDK is installed in a non-standard location, customize here.
    #
    #case ${jdkVersion} in
    #(jdk1.5) export JAVA_HOME=...; return ;;
    #(jdk1.6) export JAVA_HOME=...; return ;;
    #(jdk1.7) export JAVA_HOME=...; return ;;
    #esac

    # 4. Leave JAVA_HOME unchanged. If it does not match the required version,
    # ant will no-op.
}

chooseJavaHome

if [ ! -d "$JAVA_HOME" ]; then
    echo "$0: Invalid JAVA_HOME $JAVA_HOME; skipping compile."
    exit 1
fi

export PATH=$JAVA_HOME/bin:$PATH

echo Using JAVA_HOME: $JAVA_HOME
echo Using Ant arguments: -Drequested.java.version="$jdkVersion" $@

ant -Drequested.java.version="$jdkVersion" "$@"

# End buildOnJdk.sh
