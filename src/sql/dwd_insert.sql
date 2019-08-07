-- load data to dwd_user
--这里我使用了之前销售案例中的那三个临时变量，把他们都放到了hive-site.xml中永久化。

insert overwrite table qfbap_dwd.dwd_user 
select 
t.user_id,
t.user_name,
t.user_gender,
t.user_birthday,
t.user_age,
t.constellation,
t.province,
t.city,
t.city_level,
t.e_mail,
t.op_mail,
t.mobile,
t.num_seg_mobile,
t.op_mobile,
t.register_time,
t.login_ip,
t.login_source,
t.request_user,
t.total_score,
t.used_score,
t.is_blacklist,
t.is_married,
t.education,
t.monthly_income,
t.profession,
current_timestamp() dw_date 
from qfbap_ods.ods_user t;

-- load data to user_extend
insert overwrite table qfbap_dwd.dwd_user_extend 
SELECT
user_id, 
user_gender, 
is_pregnant_woman, 
is_have_children, 
is_have_car, 
phone_brand, 
phone_brand_level, 
phone_cnt, 
change_phone_cnt, 
is_maja, 
majia_account_cnt, 
loyal_model, 
shopping_type_model, 
weight, 
height, 
current_timestamp() dw_date
from qfbap_ods.ods_user_extend t;

--------------------------------------------------------------------------
/*这里我使用dt的动态分区插入，原因是：
1.使用静态分区字段，无法传入函数值，只能传固定数值
2.ods层中已经以dt字段分区，dw层应该直接拿来用就行，表示ods层更新的数据dw层也在当天更新。
3.current_timestamp() 已经记录了执行语句的时间，没必要再画蛇添足
4.使用相同的dt来分区，对where dt='${hiveconf:pre_date}'的契合率更高。
...
!!!注意：如果使用动态分区，务必要用where对动态分区的字段值进行限制，否则可能导致严重后果。

下面所有的分区表统一改为读取ods层的dt字段来动态分区*/
--------------------------------------------------------------------------

-- load data to biz_tarde
insert overwrite table qfbap_dwd.dwd_biz_trade  partition(dt)
select
trade_id, 
order_id, 
user_id, 
amount, 
trade_type, 
trade_time, 
current_timestamp() dw_date,
dt
from qfbap_ods.ods_biz_trade
where dt='${hiveconf:pre_date}';

--提供一个清空语句：truncate table qfbap_dwd.dwd_biz_trade

-- load data to cart
insert overwrite table qfbap_dwd.dwd_cart partition(dt)
select
cart_id, 
session_id, 
user_id, 
goods_id, 
goods_num, 
add_time, 
cancle_time, 
sumbit_time, 
create_date,
current_timestamp() dw_dt,
dt
from qfbap_ods.ods_cart
where dt='${hiveconf:pre_date}';

--提供一个清空语句：truncate table qfbap_dwd.dwd_cart

-- load data to code_category
insert overwrite table qfbap_dwd.dwd_code_category
select
first_category_id, 
first_category_name, 
second_category_id, 
second_catery_name, 
third_category_id, 
third_category_name, 
category_id,
current_timestamp() dw_dt
from qfbap_ods.ods_code_category;

-- load data to order-delivery
insert overwrite table qfbap_dwd.dwd_order_delivery partition(dt)
select
order_id, 
order_no, 
consignee, 
area_id, 
area_name, 
address, 
mobile, 
phone, 
coupon_id, 
coupon_money, 
carriage_money, 
create_time, 
update_time, 
addr_id, 
current_timestamp() dw_dt,
dt
from qfbap_ods.ods_order_delivery
where dt='${hiveconf:pre_date}';

--提供一个清空语句：truncate table qfbap_dwd.dwd_order_delivery

-- load data to order_item
insert overwrite table qfbap_dwd.dwd_order_item partition(dt)
select
user_id, 
order_id, 
order_no, 
goods_id, 
goods_no, 
goods_name, 
goods_amount, 
shop_id, 
shop_name, 
curr_price, 
market_price, 
discount, 
cost_price, 
first_cart, 
first_cart_name, 
second_cart, 
second_cart_name, 
third_cart, 
third_cart_name, 
goods_desc, 
current_timestamp() dw_dt,
dt
from qfbap_ods.ods_order_item
where dt='${hiveconf:pre_date}';

--提供一个清空语句：truncate table qfbap_dwd.dwd_order_item

-- load data to order
insert overwrite table qfbap_dwd.dwd_us_order partition(dt)
select
order_id, 
order_no, 
order_date, 
user_id, 
user_name, 
order_money, 
order_type, 
order_status, 
pay_status, 
pay_type, 
order_source, 
update_time, 
current_timestamp() dw_dt,
dt
from qfbap_ods.ods_us_order 
where dt='${hiveconf:pre_date}';

--提供一个清空语句：truncate table qfbap_dwd.dwd_us_order


-- load data to user_app_pv
insert overwrite table qfbap_dwd.dwd_user_app_pv partition(dt)
select
log_id, 
user_id, 
imei, 
log_time, 
hour(log_time) log_hour, 
visit_os, 
os_version, 
app_name, 
app_version, 
device_token, 
visit_ip, 
province, 
city, 
current_timestamp() dw_date,
dt
from qfbap_ods.ods_user_app_click_log
where dt='${hiveconf:pre_date}';

--提供一个清空语句：truncate table qfbap_dwd.dwd_user_app_pv

-- load data to user_pc_pv
insert overwrite table qfbap_dwd.dwd_user_pc_pv partition(dt)
SELECT
max(log_id),
user_id, 
session_id,
cookie_id, 
min(visit_time) in_time, 
max(visit_time) out_time, 
case when min(visit_time) = max(visit_time) then 3 else max(visit_time) -  min(visit_time) end stay_time, 
count(1) pv, 
visit_os, 
browser_name, 
visit_ip, 
province, 
city,
current_timestamp() dw_date,
dt
from qfbap_ods.ods_user_pc_click_log
where dt='${hiveconf:pre_date}'
group by 
user_id, 
cookie_id,
session_id,
visit_os,
browser_name, 
visit_ip, 
province, 
city,
dt;

--提供一个清空语句：truncate table qfbap_dwd.dwd_user_pc_pv

-- insert overwrite table qfbap_dwd.dwd_user_addr
--select 
--user_id, 
--order_addr, 
--user_order_flag, 
--addr_id, 
--arear_id, 
--current_timestamp() dw_date
--from qfbap_ods.ods_user_addr;
