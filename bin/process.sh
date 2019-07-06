#!/bin/bash

source spinner.sh

usage()
{
    echo "usage: $0 input_folder table_name"
    echo "processes the result of generate.sh in [input_folder] (on hdfs), and creates the table [table_name] in HBase and Phoenix"
}

if [[ $# -ne 2 ]]; then
    usage
    exit 1
fi

echo -n "Processing data..."
start_spin
spark-submit --jars /usr/hdp/current/phoenix-client/phoenix-client.jar,/usr/hdp/current/hbase-client/lib/hbase-client.jar,/usr/hdp/current/hbase-client/lib/hbase-common.jar,/usr/hdp/current/hbase-client/lib/hbase-server.jar,/usr/hdp/current/hbase-client/lib/guava-12.0.1.jar,/usr/hdp/current/hbase-client/lib/hbase-protocol.jar,/usr/hdp/current/hbase-client/lib/htrace-core-3.1.0-incubating.jar --class homework.spark.CountTriplets homework.jar $1 $2 > process.log 2>&1
echo "done"