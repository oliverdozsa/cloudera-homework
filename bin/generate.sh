#!/bin/bash

source spinner.sh

usage()
{
    echo "usage: $0 size output_folder"
    echo "generates [size] random firsName,lastName,location,birthDate values to [output_folder] (on hdfs)"
    echo "  size > 0"
    echo "example: $0 100 /tmp/homework"
    echo "if something's not working check the log file 'generate.log'"
}


if [[ $# -ne 2 ]]; then
    usage
    exit 1
fi

if [[ $1 -le 0 ]]; then
    usage
    exit 1
fi

echo -n "Generating random data to $2 on HDFS..."
start_spin
hadoop fs -rm -r $2 > generate_log.txt 2>&1
hadoop jar homework.jar homework.mapreduce.GeneratePeople -Dmapreduce.generatenames.size=$1 $2 >> generate.log 2>&1
if [[ $? -eq 0 ]]; then
    echo "done"
else
    echo "failed! Please check logs."
fi