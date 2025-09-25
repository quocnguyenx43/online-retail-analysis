#!/bin/bash

HADOOP_HOME=/opt/hadoop
HADOOP_DATA_DIR=/tmp/hadoop/data/

# Format HDFS
$HADOOP_HOME/bin/hdfs namenode -format -force -nonInteractive
rm -rf $HADOOP_DATA_DIR/nameNode/*
chown -R hadoop:hadoop $HADOOP_DATA_DIR/nameNode
chmod 755 $HADOOP_DATA_DIR/nameNode

# Format NameNode (only if no metadata exists yet)
if [ ! -f "$HADOOP_DATA_DIR/nameNode/current/VERSION" ]; then
    echo "Formatting NameNode..."
    $HADOOP_HOME/bin/hdfs namenode -format -force -nonInteractive
else
    echo "NameNode already formatted. Skipping format."
fi

# Start NameNode
echo "Starting NameNode..."
$HADOOP_HOME/bin/hdfs namenode
