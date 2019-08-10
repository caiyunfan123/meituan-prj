create database if not exists qfbap_mid;

-- 用户表
create table if not exists qfbap_mid.mid_user
(
   user_id              bigint   auto_increment,
   user_name            string  ,
   user_gender          tinyint  ,
   user_birthday        datetime  ,
   user_age             int  ,
   constellation        string ,
   province             string  ,
   city                 string ,
   city_level           tinyint  ,
   e_mail               string  ,
   op_mail              string ,
   mobile               bigint  ,
   num_seg_mobile       int  ,
   op_mobile            string ,
   register_time        datetime  ,
   login_ip             string ,
   login_source         string,
   request_user         bigint  ,
   total_score          decimal(18,2) ,
   used_score           decimal(18,2)  ,
   is_blacklist         tinyint  ,
   is_married           tinyint  ,
   education            varchar(128)  ,
   monthly_income       decimal(18,2)  ,
   profession           varchar(128)  ,
   dw_date  timestamp
) partitioned by (dt string)
location '/qfbap/mid/mid_user';


--加载用户信息表数据
insert overwrite table qfbap_mid.mid_user partition(dt='2018-12-20')
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
t.level_name,
t.is_blacklist,
t.is_married,
t.education,
t.monthly_income,
t.profession, 
from_unixtime(unix_timestamp())  dw_date 
from ods.ods_user t where dt='2018-12-20';


-- 用户扩展表
create table qfbap_mid.mid_user_extend
(
   user_id              bigint ,
   user_gender          bigint  ,
   is_pregnant_woman    tinyint  ,
   is_have_children     tinyint  ,
   is_have_car          tinyint  ,
   phone_brand          string,
   phone_brand_level    int  ,
   phone_cnt            int  ,
   change_phone_cnt     int  ,
   is_maja              tinyint  ,
   majia_account_cnt    int  ,
   loyal_model          int  ,
   shopping_type_model  int  ,
   weight               int  ,
   height               int  ,
   dw_date  timestamp
)
partitioned by (dt string)
location '/qfbap/mid/mid_user_extend';

-- 加载用户扩展表数据
insert overwrite table qfbap_mid.mid_user partition(dt='2018-12-20')
select
t.user_id,
t.user_gender,
t.is_pregnant_woman,
t.is_have_children,
t.is_have_car,
t.phone_brand,
t.phone_brand_level,
t.is_maja,
t.majia_account_cnt,
t.loyal_model,
t.shopping_type_model,
t.weight,
t.height
from ods_user_extend t;

create table qfbap_mid.mid_order
(
   order_id             bigint not null auto_increment,
   order_no             varchar(128) not null,
   order_date           datetime not null,
   user_id              bigint not null,
   user_name            varchar(64) not null,
   order_money          decimal(18,2) not null,
   order_type           int not null,
   order_status         int not null,
   pay_status           int not null,
   pay_type             int not null,
   order_source         int not null,
   update_time          datetime not null
)
partitioned by (dt string)
location '/qfbap/mid/mid_order';
;


--订单商品表
create table qfbap_mid.mid_order_item
(
   user_id              bigint,
   order_id             bigint not null,
   order_no             varchar(256) not null,
   goods_id             bigint not null,
   goods_no             varchar(64) not null,
   goods_name           varchar(256) not null,
   goods_amount         int not null,
   shop_id              bigint not null,
   shop_name            varchar(256) not null,
   curr_price           decimal(18,2) not null,
   market_price         decimal(18,2) not null,
   discount             decimal(18,2) not null,
   cost_price           decimal(18,2) not null,
   first_cart           bigint not null,
   first_cart_name      varchar(256) not null,
   second_cart          bigint not null,
   second_cart_name     varchar(256) not null,
   third_cart           bigint not null,
   third_cart_name      varchar(256) not null,
   goods_desc           varchar(256) not null
)
partitioned by (dt string)
location '/qfbap/mid/mid_order_item'
;


--购物车表
create table qfbap_mid.mid_order_cart
(
   cart_id              bigint not null auto_increment,
   session_id           varchar(64) not null,
   user_id              bigint not null,
   goods_id             bigint not null,
   goods_num            int not null,
   add_time             datetime not null,
   cancle_time          datetime not null,
   sumbit_time          datetime not null
)
partitioned by (dt string)
location '/qfbap/mid/mid_order_cart'
;





--用户地址表
create table qfbap_mid.mid_user_addr
(
   user_id              bigint not null,
   order_addr           varchar(512) not null,
   user_order_flag      tinyint not null
)
partitioned by (dt string)
location '/qfbap/mid/mid_user_addr'
;

--订单收货人表
create table qfbap_mid.mid_order_delivery
(
   order_id             bigint not null,
   order_no             varchar(64) not null,
   consignee            varchar(64) not null,
   area_id              bigint not null,
   area_name            varchar(256) not null,
   address              varchar(512) not null,
   mobile               bigint not null,
   phone                varchar(64) not null,
   coupon_id            bigint not null,
   coupon_money         decimal(18,2) not null,
   carriage_money       decimal(18,2) not null,
   create_time          datetime not null,
   update_time          datetime not null
)
partitioned by (dt string)
location '/qfbap/mid/mid_order_delivery'
;


--  类目码表
create table qfbap_mid.mid_category_code
(
   first_category_id    int not null,
   first_category_name  varchar(32) not null,
   second_category_id   int not null,
   second_catery_name   varchar(32) not null,
   third_category_id    int not null,
   third_category_name  varchar(32) not null
)
partitioned by (dt string)
location '/qfbap/mid/mid_category_code'
;