#!/bin/bash

###################################
##1、该脚本是创建sqoop的job脚本，原则上只需要执行一次即可，以后不需要执行该脚本，只需要执行sqoop job -exec jobname。
##2、该文件中的全部为非增量表的sqoop的job语句。
##3、如果需要重新运行，则运行方式:load_data_full_sqoop.sh
################################

#1. not full load data of bap_ods_category_code, run every day.
sqoop job --create bap_code_category -- import \
 --connect jdbc:mysql://master:3306/qfbap_ods?dontTrackOpenResources=true\&defaultFetchSize=1000\&useCursorFetch=true \
 --driver com.mysql.jdbc.Driver \
 --username root \
 --password admin \
 --table code_category \
 --delete-target-dir \
 --target-dir /qfbap/ods/ods_code_category \
 --fields-terminated-by '\001'
#2. not full load data of bap_ods_user, run every day.
sqoop job --create bap_user -- import \
 --connect jdbc:mysql://master:3306/qfbap_ods?dontTrackOpenResources=true\&defaultFetchSize=1000\&useCursorFetch=true \
 --driver com.mysql.jdbc.Driver \
 --username root \
 --password admin \
 --table user \
 --delete-target-dir \
 --target-dir /qfbap/ods/ods_user/ \
 --fields-terminated-by '\001'
#3. not full load data of bap_user_extend, run every day.
sqoop job --create bap_user_extend -- import \
 --connect jdbc:mysql://master:3306/qfbap_ods?dontTrackOpenResources=true\&defaultFetchSize=1000\&useCursorFetch=true \
 --driver com.mysql.jdbc.Driver \
 --username root \
 --password admin \
 --table user_extend \
 --delete-target-dir \
 --target-dir /qfbap/ods/ods_user_extend/ \
 --fields-terminated-by '\001'
#4.not full load data of bap_user_addr, run every day.
sqoop job --create bap_user_addr -- import \
 --connect jdbc:mysql://master:3306/qfbap_ods?dontTrackOpenResources=true\&defaultFetchSize=1000\&useCursorFetch=true \
 --driver com.mysql.jdbc.Driver \
 --username root \
 --password admin \
 --table user_addr \
 --delete-target-dir \
 --target-dir /qfbap/ods/ods_user_addr/ \
 --fields-terminated-by '\001'