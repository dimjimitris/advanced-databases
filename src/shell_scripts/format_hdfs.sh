#!/bin/bash

# stop all services if running
$SPARK_HOME/sbin/stop-history-server.sh && stop-yarn.sh && stop-dfs.sh

# remove all data
rm -rf $HOME/opt/data/hdfs/* $HOME/opt/data/name/* $HOME/opt/data/tmp/*
ssh okeanos-worker "rm -rf $HOME/opt/data/hdfs/* $HOME/opt/data/name/* $HOME/opt/data/tmp/*"

# format hdfs
hdfs namenode -format

# start hdfs
start-dfs.sh

# create needed directories
hdfs dfs -mkdir /spark.eventLog/

# stop hdfs
stop-dfs.sh
