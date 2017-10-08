#!/usr/bin/env bash
if [ ! -d  "$HOME/spark/spark-2.2.0-bin-hadoop2.7" ]; then
  wget https://d3kbcqa49mib13.cloudfront.net/spark-2.2.0-bin-hadoop2.7.tgz
  mkdir -p $HOME/spark \
    && tar zxf spark-2.2.0-bin-hadoop2.7.tgz -C $HOME/spark
else
  echo "Using cached spark"
fi
