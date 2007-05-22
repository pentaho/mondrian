#!/bin/sh

# mondrian jars
CP="lib/commons-dbcp.jar:lib/commons-collections.jar:lib/commons-pool.jar"
CP=$CP:"lib/eigenbase-properties.jar:lib/eigenbase-resgen.jar:lib/eigenbase-xom.jar"
CP=$CP:"lib/javacup.jar:lib/log4j-1.2.12.jar:lib/mondrian.jar"
CP=$CP:"lib/jlfgr-1_0.jar:lib/jmi.jar:lib/mof.jar:lib/commons-math-1.0.jar"
CP=$CP:"lib/commons-vfs.jar:lib/commons-logging.jar"

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

for i in `ls drivers/*.jar`; do
CP=$CP:$i
done

#echo $CP

java -Xms100m -Xmx500m -cp "$CP" -Dlog4j.configuration=file:~/.schemaWorkbench/log4j.xml mondrian.gui.Workbench

