#!/bin/sh

# mondrian jars
CP="lib/commons-dbcp.jar"
CP=$CP:"lib/commons-collections.jar"
CP=$CP:"lib/commons-pool.jar"
CP=$CP:"lib/eigenbase-properties.jar"
CP=$CP:"lib/eigenbase-resgen.jar"
CP=$CP:"lib/eigenbase-xom.jar"
CP=$CP:"lib/javacup.jar"
CP=$CP:"lib/log4j.jar"
CP=$CP:"lib/mondrian.jar"
CP=$CP:"lib/jlfgr.jar"
CP=$CP:"lib/commons-math.jar"
CP=$CP:"lib/commons-vfs.jar"
CP=$CP:"lib/commons-logging.jar"

# Workbench GUI code and resources

CP=$CP:"lib/workbench.jar"

# local directory is ~/.schemaWorkbench

if test ! -d ~/.schemaWorkbench; then
mkdir ~/.schemaWorkbench
fi

# copy mondrian.properties and log4j.xml if necessary

if test ! -e ~/.schemaWorkbench/mondrian.properties; then
cp mondrian.properties ~/.schemaWorkbench/mondrian.properties
fi

if test ! -e ~/.schemaWorkbench/log4j.xml; then
cp log4j.xml ~/.schemaWorkbench/log4j.xml
fi

CP=$CP:~/.schemaWorkbench


# or
# set the log4j.properties system property 
# "-Dlog4j.properties=path to <.properties or .xml file>"
# in the java command below to adjust workbench logging

# add all needed JDBC drivers to the classpath

for i in `ls drivers/*.jar 2> /dev/null`; do
CP=$CP:$i
done

# add all needed plugins to the classpath

for i in `ls plugins/*.jar 2> /dev/null`; do
CP=$CP:$i
done

#echo $CP

java -Xms100m -Xmx500m -cp "$CP" mondrian.gui.Workbench

# End workbench.sh
