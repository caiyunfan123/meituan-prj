#!/bin/bash

#run with args.eg: ./load_data.sh 2018-12-25
args=$1
dt=
if [ ${#args} == 0 ]
then
dt=`date -d "1 days ago" "+%Y-%m-%d"`
else
dt=$1
fi

#1. run sqoop job
echo "load full data by sqoop.starting...."
sqoop job -exec bap_us_order
sqoop job -exec bap_cart
sqoop job -exec bap_order_delivery
sqoop job -exec bap_order_item
sqoop job -exec bap_user_app_click_log
sqoop job -exec bap_user_pc_click_log
echo "load full data by sqoop.ended...."

#2. load data statments.
echo "run hive load data."
beeline -u jdbc:hive2://master:10000/qfbap_ods -e "load data inpath '/qfbap/ods_tmp/ods_us_order/*' into table qfbap_ods.ods_us_order partition(dt='${dt}');"
beeline -u jdbc:hive2://master:10000/qfbap_ods -e "load data inpath '/qfbap/ods_tmp/ods_cart/*' into table qfbap_ods.ods_cart partition(dt='${dt}');"
beeline -u jdbc:hive2://master:10000/qfbap_ods -e "load data inpath '/qfbap/ods_tmp/ods_order_delivery/*' into table qfbap_ods.ods_order_delivery partition(dt='${dt}');"
beeline -u jdbc:hive2://master:10000/qfbap_ods -e "load data inpath '/qfbap/ods_tmp/ods_order_item/*' into table qfbap_ods.ods_order_item partition(dt='${dt}');"
beeline -u jdbc:hive2://master:10000/qfbap_ods -e "load data inpath '/qfbap/ods_tmp/ods_user_app_click_log/*' into table qfbap_ods.ods_user_app_click_log partition(dt='${dt}');"
beeline -u jdbc:hive2://master:10000/qfbap_ods -e "load data inpath '/qfbap/ods_tmp/ods_user_pc_click_log/*' into table qfbap_ods.ods_user_pc_click_log partition(dt='${dt}');"
echo "run hive load data end."


