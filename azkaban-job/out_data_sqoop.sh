#!/bin/bash

args=$1
dt=
if [ ${#args} == 0 ]
then
dt=`date -d "1 days ago" "+%Y-%m-%d"`
else
dt=$1
fi

sqoop export \
--connect jdbc:mysql://master:3306/qfbap_dm \
--username root \
--password admin \
--table dm_user_visit \
--export-dir /user/hive/warehouse/qfbap_dm.db/dm_user_visit/dt=${dt}/* \
--fields-terminated-by '\t' \
--null-string 'null' \
--null-non-string '0' \
--lines-terminated-by '\n' \
-m 1