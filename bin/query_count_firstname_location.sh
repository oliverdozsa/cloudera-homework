#!/bin/bash

source spinner.sh
source query_common.sh

usage()
{
    echo "usage: $0 table_name first_name location"
    echo "queries how many people with the specified first name were born at the given location"
    echo "  size > 0"
    OLD_IFS=$IFS
    IFS=,
    echo "  possible first names = ${first_names[*]}"
    echo "  possible locations = ${locations[*]}"
    IFS=$OLD_IFS
    echo "example: $0 PeopleTable Jaylene 'New York'"
}

if [[ $# -ne 3 ]]; then
    usage
    exit 1
fi

contains locations "$3"
if [[ $? -eq 0 ]]; then
    contains first_names "$2"
    if [[ $? -eq 0 ]]; then
        echo "Running query..."
        start_spin
        echo "SELECT SUM(\"count\") FROM \"$1\" WHERE \"location\"='$3' AND \"firstName\"='$2';" | /usr/hdp/current/phoenix-client/bin/sqlline.py localhost:2181/hbase-unsecure 2>&1 | sed -r "s/\x1B\[([0-9]{1,2}(;[0-9]{1,2})?)?[m|K]//g" | grep -E "(\+.*|\|.*|.*ERROR.*)"
    else
        usage
    fi
else
    usage
fi