
drop database if exists qfbap_dwd CASCADE;
-- drop database if exists qfbap_dwd CASCADE;
-- drop database if exists qfbap_dws CASCADE;
-- drop database if exists qfbap_dm CASCADE;
-- hive 建库
create database if not exists qfbap_dwd;


use qfbap_dwd;

CREATE TABLE qfbap_dwd.dwd_biz_trade (
  trade_id bigint     ,
  order_id bigint   ,
  user_id bigint   ,
  amount decimal(18,2)  ,
  trade_type tinyint  ,
  trade_time string  ,
  dw_date string
) 
partitioned by (dt string)
location '/qfbap/dwd/dwd_biz_trade'
 ;


CREATE TABLE qfbap_dwd.dwd_cart (
  cart_id bigint     ,
  session_id string  ,
  user_id bigint   ,
  godwd_id bigint   ,
  godwd_num int   ,
  add_time string  ,
  cancle_time string  ,
  sumbit_time string  ,
  create_date string,
   dw_date string
)  
partitioned by (dt string)
location '/qfbap/dwd/dwd_cart'
;



CREATE TABLE qfbap_dwd.dwd_code_category (
  first_category_id int   ,
  first_category_name string  ,
  second_category_id int   ,
  second_catery_name string  ,
  third_category_id int   ,
  third_category_name string  ,
  category_id int     ,
   dw_date string
) 
location '/qfbap/dwd/dwd_code_category'
;



CREATE TABLE qfbap_dwd.dwd_order_delivery (
  order_id bigint   ,
  order_no string  ,
  consignee string  ,
  area_id bigint   ,
  area_name string  ,
  address string  ,
  mobile bigint   ,
  phone string  ,
  coupon_id bigint   ,
  coupon_money decimal(18,2)  ,
  carriage_money decimal(18,2)  ,
  create_time string  ,
  update_time string  ,
  addr_id bigint   ,
   dw_date string
) 
partitioned by (dt string)
location '/qfbap/dwd/dwd_order_delivery'
;



CREATE TABLE qfbap_dwd.dwd_order_item (
  user_id bigint   ,
  order_id bigint   ,
  order_no string  ,
  godwd_id bigint   ,
  godwd_no string  ,
  godwd_name string  ,
  godwd_amount int   ,
  shop_id bigint   ,
  shop_name string  ,
  curr_price decimal(18,2)  ,
  market_price decimal(18,2)  ,
  discount decimal(18,2)  ,
  cost_price decimal(18,2)  ,
  first_cart bigint   ,
  first_cart_name string  ,
  second_cart bigint   ,
  second_cart_name string  ,
  third_cart bigint   ,
  third_cart_name string  ,
  godwd_desc string ,
   dw_date string 
)  
partitioned by (dt string)
location '/qfbap/dwd/dwd_order_item';



CREATE TABLE qfbap_dwd.dwd_us_order (
  order_id bigint     ,
  order_no string  ,
  order_date string  ,
  user_id bigint   ,
  user_name string  ,
  order_money decimal(18,2)  ,
  order_type int   ,
  order_status int   ,
  pay_status int   ,
  pay_type int   ,
  order_source string  ,
  update_time string  ,
   dw_date string
)
partitioned by (dt string)
location '/qfbap/dwd/dwd_us_order';


CREATE TABLE qfbap_dwd.dwd_user (
  user_id bigint     ,
  user_name string  ,
  user_gender tinyint  ,
  user_birthday string  ,
  user_age int   ,
  constellation string  ,
  province string  ,
  city string  ,
  city_level tinyint  ,
  e_mail string  ,
  op_mail string  ,
  mobile bigint   ,
  num_seg_mobile int   ,
  op_Mobile string  ,
  register_time string  ,
  login_ip string  ,
  login_source string  ,
  request_user string  ,
  total_score decimal(18,2)  ,
  used_score decimal(18,2)  ,
  is_blacklist tinyint  ,
  is_married tinyint  ,
  education string  ,
  monthly_income decimal(18,2) ,
  profession string  ,
  dw_date string   
) 
location '/qfbap/dwd/dwd_user';



CREATE TABLE qfbap_dwd.dwd_user_addr (
  user_id bigint   ,
  order_addr string  ,
  user_order_flag tinyint  ,
  addr_id bigint     ,
  arear_id int   ,
   dw_date string
)
location '/qfbap/dwd/dwd_user_addr' ;


CREATE TABLE qfbap_dwd.dwd_user_extend (
  user_id bigint   ,
  user_gender bigint   ,
  is_pregnant_woman tinyint  ,
  is_have_children tinyint  ,
  is_have_car tinyint  ,
  phone_brand string  ,
  phone_brand_level string  ,
  phone_cnt int   ,
  change_phone_cnt int   ,
  is_maja tinyint  ,
  majia_account_cnt int   ,
  loyal_model string  ,
  shopping_type_model string  ,
  weight int   ,
  height int   ,
   dw_date string
)  
location '/qfbap/dwd/dwd_user_extend';




CREATE TABLE qfbap_dwd.dwd_user_app_pv (
  log_id bigint     ,
  user_id bigint   ,
  imei string  ,
  log_time string  ,
  log_hour string,
  visit_os string  ,
  os_version string  ,
  app_name string  ,
  app_version string  ,
  device_token string  ,
  visit_ip string  ,
  province string  ,
  city string  ,
   dw_date string
) 
partitioned by (dt string)
location '/qfbap/dwd/dwd_user_app_pv';



CREATE TABLE qfbap_dwd.dwd_user_pc_pv (
  log_id bigint     ,
  user_id bigint   ,
  session_id string  ,
  cookie_id string  ,
  in_time string,
  out_time string,
  stay_time bigint,
  pv bigint,
  visit_os string  ,
  browser_name string  ,
  visit_ip string  ,
  province string  ,
  city string  ,
  dw_date string
)
partitioned by (dt string)
location '/qfbap/dwd/dwd_user_pc_pv'
;

