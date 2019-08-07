#!/bin/bash

#run with args.eg: ./load_data.sh 2018-12-25
args=$1
dt=
if [ ${#args} == 0 ]
then
dt=`date -d "1 days ago" "+%Y%m%d"`
else
dt=$1
fi

#1. insert overwrite
echo "load dwd_user"
hive -hivevar param_dt=${dt} -f dwd_insert.sql
