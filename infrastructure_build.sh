#!/bin/bash

PROJECT_LOC=`pwd`

echo "Setting up the environment variables"

export JAVA_HOME_15=/home/shazra/jdk1.5.0_22
export JAVA_HOME_16=/home/shazra/jdk1.6.0_45
export JAVA_HOME_17=/home/shazra/jdk1.7.0_72
export JAVA_HOME=/home/shazra/jdk1.5.0_22
export ANT_HOME=/usr/bin/ant

cd $PROJECT_LOC
echo "Current directory" `pwd`
ant war

if [ $? -eq 0 ]; then
	echo "Building Mondrian is successful"
fi

echo "--------- Download tomcat----------"
wget "http://mirror.dkd.de/apache/tomcat/tomcat-8/v8.0.21/bin/apache-tomcat-8.0.21-deployer.tar.gz"

tar -xvzf apache-tomcat-8.0.21-deployer.tar.gz




