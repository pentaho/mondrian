# $Id$
# Called recursively from 'ant release' to build the files which can only be
# built under JDK 1.6.

# Change the following line to point to your JDK 1.6 home.
export JAVA_HOME=/usr/local/jdk1.6
export PATH=$JAVA_HOME/bin:$PATH
ant -Dskip.download compile.java

# End buildJdk16.sh
