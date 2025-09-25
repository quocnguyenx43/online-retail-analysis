#!/bin/bash

HADOOP_HOME=/opt/hadoop
HADOOP_DATA_DIR=/tmp/hadoop/data/

# Format HDFS
rm -rf $HADOOP_DATA_DIR/dataNode/*
chown -R hadoop:hadoop $HADOOP_DATA_DIR/dataNode
chmod 755 $HADOOP_DATA_DIR/dataNode

# Start DataNode
echo "Starting DataNode..."
$HADOOP_HOME/bin/hdfs datanode
