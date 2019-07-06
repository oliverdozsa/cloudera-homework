#!/bin/bash

source spinner.sh

usage()
{
    echo "usage $0 size output_folder"
    echo "generates size random firsName,lastName,location,birthDate values to output_folder (on hdfs)"
    echo "  size > 0"
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
hadoop jar ./bin/homework.jar homework.mapreduce.GeneratePeople -Dmapreduce.generatenames.size=$1 $2 >> generate_log.txt 2>&1
echo "done"