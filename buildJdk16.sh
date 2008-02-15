# $Id$
# Called recursively from 'ant release' to build the files which can only be
# built under JDK 1.6.

# Change the following line to point to your JDK 1.6 home.
export JAVA_HOME=/usr/local/jdk1.6.0_01
export PATH=$JAVA_HOME/bin:$PATH
ant compile.java

echo "done buidJdk16.sh"

touch /tmp/foo.txt

# End buildJdk16.sh
