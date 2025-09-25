#!/bin/bash

HADOOP_HOME=/opt/hadoop
HADOOP_DATA_DIR=/tmp/hadoop/data/

if [ ! -d "$HADOOP_DATA_DIR/nameNode/current" ]; then
    echo "Formatting NameNode..."
    $HADOOP_HOME/bin/hdfs namenode -format
    rm -rf $HADOOP_DATA_DIR/nameNode/*
    chown -R hadoop:hadoop $HADOOP_DATA_DIR/nameNode
    chmod 755 $HADOOP_DATA_DIR/nameNode
fi

$HADOOP_HOME/bin/hdfs namenode
