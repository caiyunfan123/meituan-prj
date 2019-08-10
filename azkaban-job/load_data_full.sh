#!/bin/bash

############################
##1、执行该脚本，需要保证sqoop的job已经被创建好(语句在load_data_full_sqoop.txt中,如果没有job，需创建).
sqoop job --list
sqoop job --delete jobname
sqoop job -exec jobname
##2、每天凌晨2点定时执行如下的sqoop的job，全为全量表的数据加载。
##3、如果有问题，需要重新跑job。
##4、运行方式。./load_data_full.sh
################################

#run sqoop job
echo "load full data by sqoop.starting...."
sqoop job -exec bap_code_category
sqoop job -exec bap_user
sqoop job -exec bap_user_extend
sqoop job -exec bap_user_addr
echo "load full data by sqoop.ended...."