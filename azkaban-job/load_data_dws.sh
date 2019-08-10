#!/bin/bash

args=$1
dt=
if [ ${#args} == 0 ]
then
dt=`date -d "1 days ago" "+%s"`
else
dt=`date -d "$1" "+%s"`
fi

#1. insert overwrite
echo "load dwd_user"
beeline -u jdbc:hive2://master:10000 -hivevar dt0=${dt} -f /azkaban-job/azkabanflow/dws_insert.sql
