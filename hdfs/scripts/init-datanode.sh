#!/bin/bash

HADOOP_HOME=/opt/hadoop
HADOOP_DATA_DIR=/tmp/hadoop/data/

rm -rf $HADOOP_DATA_DIR/dataNode/*
chown -R hadoop:hadoop $HADOOP_DATA_DIR/dataNode
chmod 755 $HADOOP_DATA_DIR/dataNode
$HADOOP_HOME/bin/hdfs datanode
