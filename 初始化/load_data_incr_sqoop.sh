#!/bin/bash

###################################
##1、该脚本是创建sqoop的job脚本，原则上只需要执行一次即可，以后不需要执行该脚本，只需要执行sqoop job -exec jobname。
##2、该文件中的全部为增量表的sqoop的job语句。
##3、如果需要重新运行，则运行方式:load_data_incr_sqoop.sh
################################

#增量将数据导入到hive中(注：业务系统中表有自增的id的就用id来增量导出；否则都用create_date字段来导出)
#1、增量导出ods_order：
sqoop job --delete bap_us_order
sqoop job --create bap_us_order -- import \
--connect jdbc:mysql://master:3306/qfbap_ods?dontTrackOpenResources=true\&defaultFetchSize=1000\&useCursorFetch=true \
--driver com.mysql.jdbc.Driver \
--username root \
--password-file hdfs:///password.file \
--table us_order \
--target-dir /qfbap/ods_tmp/ods_us_order/ \
--fields-terminated-by '\001' \
--check-column order_id \
--incremental append \
--last-value 0 \
;

#2、增量导出ods_cart
sqoop job --delete bap_cart
sqoop job --create bap_cart -- import \
--connect jdbc:mysql://master:3306/qfbap_ods?dontTrackOpenResources=true\&defaultFetchSize=1000\&useCursorFetch=true \
--driver com.mysql.jdbc.Driver \
--username root \
--password-file hdfs:///password.file \
--table cart \
--target-dir /qfbap/ods_tmp/ods_cart/ \
--fields-terminated-by '\001' \
--check-column cart_id \
--incremental append \
--last-value 0 \
;

#3、增量导出ods_order_delivery
sqoop job --delete bap_order_delivery
sqoop job --create bap_order_delivery -- import \
--connect jdbc:mysql://master:3306/qfbap_ods?dontTrackOpenResources=true\&defaultFetchSize=1000\&useCursorFetch=true \
--driver com.mysql.jdbc.Driver \
--username root \
--password-file hdfs:///password.file \
--table order_delivery \
--target-dir /qfbap/ods_tmp/ods_order_delivery/ \
--fields-terminated-by '\001' \
--check-column order_id \
--incremental append \
--last-value 0 \
;

#4、增量导出ods_order_item
sqoop job --delete bap_order_item
sqoop job --create bap_order_item -- import \
--connect jdbc:mysql://master:3306/qfbap_ods?dontTrackOpenResources=true\&defaultFetchSize=1000\&useCursorFetch=true \
--driver com.mysql.jdbc.Driver \
--username root \
--password-file hdfs:///password.file \
--table order_item \
--target-dir /qfbap/ods_tmp/ods_order_item/ \
--fields-terminated-by '\001' \
--check-column order_id \
--incremental append \
--last-value 0 \
;


#5、增量导出ods_user_app_click_log
sqoop job --delete bap_user_app_click_log
 sqoop job --create bap_user_app_click_log -- import \
 --connect jdbc:mysql://master:3306/qfbap_ods?dontTrackOpenResources=true\&defaultFetchSize=1000\&useCursorFetch=true \
 --driver com.mysql.jdbc.Driver \
 --username root \
 --password-file hdfs:///password.file \
 --table user_app_click_log \
 --target-dir /qfbap/ods_tmp/ods_user_app_click_log/ \
 --fields-terminated-by '\001' \
 --check-column user_id \
 --incremental append \
 --last-value 0 \
 ;

#6、增量导出ods_user_pc_click_log
sqoop job --delete bap_user_pc_click_log
 sqoop job --create bap_user_pc_click_log -- import \
 --connect jdbc:mysql://master:3306/qfbap_ods?dontTrackOpenResources=true\&defaultFetchSize=1000\&useCursorFetch=true \
 --driver com.mysql.jdbc.Driver \
 --username root \
 --password-file hdfs:///password.file \
 --table user_pc_click_log \
 --target-dir /qfbap/ods_tmp/ods_user_pc_click_log/ \
 --fields-terminated-by '\001' \
 --check-column user_id \
 --incremental append \
 --last-value 0 \
 ;
