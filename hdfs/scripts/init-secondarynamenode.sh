#!/bin/bash

HADOOP_HOME=/opt/hadoop
HADOOP_DATA_DIR=/tmp/hadoop/data/

rm -rf $HADOOP_DATA_DIR/secondaryNameNode/*
chown -R hadoop:hadoop $HADOOP_DATA_DIR/secondaryNameNode
chmod 755 $HADOOP_DATA_DIR/secondaryNameNode
$HADOOP_HOME/bin/hdfs secondarynamenode
