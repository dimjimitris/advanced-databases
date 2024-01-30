#!/bin/bash

src=$HOME/datasets
if [ $# -eq 1 ]
  then
    src=$1
fi

dst=/user/user/datasets

start-dfs.sh

hdfs dfs -mkdir -p $dst/

hdfs dfs -put $src/* $dst/

stop-dfs.sh
