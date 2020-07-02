#!/bin/bash
if [ "$1" = 1 ]
  then
    if [ "$2" = 1 ] || [ "$2" = 7 ] || [ "$2" = 30 ];
    then
      java -cp project2-1.0.jar it.uniroma2.utils.DataSourceQuery1 | nc -l -N 9091 &
      nc -l -N 9001 | java -cp project2-1.0.jar it.uniroma2.utils.QueryResultsExporter $1 $2
      return
    fi
fi

if [ "$1" = 2 ]
  then
    if [ "$2" = 1 ] || [ "$2" = 7 ];
    then
      java -cp project2-1.0.jar it.uniroma2.utils.DataSourceQuery2 | nc -l -N 9092 &
      nc -l -N 9002 | java -cp project2-1.0.jar it.uniroma2.utils.QueryResultsExporter $1 $2
      return
    fi
fi

echo "Error passing arguments. Please run script with args: <queryNum (1 or 2)> <numDays (query1: 1,7,30 - query2: 1,7)>"
