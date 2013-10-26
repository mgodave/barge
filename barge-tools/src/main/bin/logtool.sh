#!/bin/bash

# Find Java
if [ "$JAVA_HOME" = "" ] ; then
    JAVA="java -server"
else
    JAVA="$JAVA_HOME/bin/java -server"
fi

PREFIX=$( echo `dirname $0`/.. )
LIB_DIR=$PREFIX/lib

# Set Java options
if [ "$JAVA_OPTIONS" = "" ] ; then
    JAVA_OPTIONS=" \
    -XX:+UseConcMarkSweepGC \
    -d64"
fi

export BASE_DIR=$*

# Launch the application
cd $PREFIX

export PREFIX
export CLASSPATH=$( echo $LIB_DIR/*.jar . | sed 's/ /:/g')
exec $JAVA $JAVA_OPTIONS org.robotninjas.barge.tools.LogTool $*