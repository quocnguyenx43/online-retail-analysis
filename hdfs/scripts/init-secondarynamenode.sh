#!/bin/bash

HADOOP_HOME=/opt/hadoop
HADOOP_DATA_DIR=/tmp/hadoop/data/

# Format Secondary NameNode
rm -rf $HADOOP_DATA_DIR/secondaryNameNode/*
chown -R hadoop:hadoop $HADOOP_DATA_DIR/secondaryNameNode
chmod 755 $HADOOP_DATA_DIR/secondaryNameNode

# Start Secondary NameNode
echo "Starting SecondaryNameNode..."
$HADOOP_HOME/bin/hdfs secondarynamenode
