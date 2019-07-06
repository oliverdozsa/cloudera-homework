#!/bin/bash

first_names=("Jaylene" "Elizabeth" "Laura" "Jada" "Kyra")
last_names=("Pollard" "Rose" "Frazier" "Rubio")
locations=("New York" "San Francisco")

contains()
{
    arr_name=$1[@]
    val=$2
    arr=("${!arr_name}")

    for s in "${arr[@]}"
    do
        if [[ $2 == ${s} ]]; then
            return 0
        fi
    done

    return 1
}