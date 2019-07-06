#!/bin/bash

source spinner.sh
source query_common.sh

usage()
{
    echo "usage: $0 table_name location"
    echo "queries how many people were born at the given location"
    echo "  size > 0"
    OLD_IFS=$IFS
    IFS=,
    echo "  possible locations = ${locations[*]}"
    IFS=$OLD_IFS
    echo "example: $0 PeopleTable 'New York'"
}

if [[ $# -ne 2 ]]; then
    usage
    exit 1
fi

contains locations "$2"
if [[ $? -eq 0 ]]; then
    echo "Running query..."
    start_spin
    echo "select sum(\"count\") from \"$1\" where \"location\"='$2';" | /usr/hdp/current/phoenix-client/bin/sqlline.py localhost:2181/hbase-unsecure 2>&1 | sed -r "s/\x1B\[([0-9]{1,2}(;[0-9]{1,2})?)?[m|K]//g" | grep -E "(\+.*|\|.*)"
else
    usage
fi

