--创建ods层数据库
create database if not exist qfbap_ods;
--创建用户信息表
create table if not exists qfbap_ods.ods_user
(
   user_id              bigint not null auto_increment,
   user_name            varchar(64) not null,
   user_gender          tinyint not null,
   user_birthday        datetime not null,
   user_age             int not null,
   constellation        varchar(8) not null,
   province             varchar(32) not null,
   city                 varchar(32) not null,
   city_level           tinyint not null,
   e_mail               varchar(256) not null,
   op_mail              varchar(32) not null,
   mobile               bigint not null,
   num_seg_mobile       int not null,
   op_Mobile            varchar(64) not null,
   register_time        datetime not null,
   login_ip             varchar(64) not null,
   login_source         varchar(512) not null,
   request_user         bigint not null,
   total_score          decimal(18,2) not null,
   used_score           decimal(18,2) not null,
   is_blacklist         tinyint not null,
   is_married           tinyint not null,
   education            varchar(128) not null,
   monthly_income       decimal(18,2) not null,
   profession           varchar(128) not null,
   primary key (user_id)
);

--创建用户扩展信息表
create table qfbap_ods.ods_user_extend
(
   user_id              bigint not null,
   user_gender          bigint not null,
   is_pregnant_woman    tinyint not null,
   is_have_children     tinyint not null,
   is_have_car          tinyint not null,
   phone_brand          varchar(64) not null,
   phone_brand_level    int not null,
   phone_cnt            int not null,
   change_phone_cnt     int not null,
   is_maja              tinyint not null,
   majia_account_cnt    int not null,
   loyal_model          int not null,
   shopping_type_model  int not null,
   weight               int not null,
   height               int not null,
   primary key (user_id)
);

--用户pc端访问表
create table qfbap_ods.ods_user_pc_click_log
(
   user_id              bigint not null,
   session_id           varchar(128) not null,
   cookie_id            varchar(128) not null,
   visit_time           datetime not null,
   visit_url            varchar(512) not null,
   visit_os             varchar(128) not null,
   browser_name         varchar(64) not null,
   visit_ip             varchar(64) not null,
   province             varchar(32) not null,
   city                 varchar(64) not null,
   page_id              int not null,
   goods_id             bigint not null,
   shop_id              bigint not null,
   primary key (user_id, session_id)
);



--用户app端访问表

create table qfbap_ods.ods_user_app_click_log
(
   user_id              bigint not null,
   imei                 varchar(128) not null,
   log_time             datetime not null,
   visit_os             varchar(64) not null,
   os_version           varchar(32) not null,
   app_name             varchar(256) not null,
   app_version          varchar(32) not null,
   device_token         varchar(256) not null,
   visit_ip             varchar(64) not null,
   province             varchar(32) not null,
   city                 varchar(64) not null,
   primary key (user_id, imei)
);


-- 用户订单表
create table qfbap_ods.ods_order
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
   update_time          datetime not null,
   primary key (order_id)
);


--订单商品表
create table ods_order_item
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
   goods_desc           varchar(256) not null,
   primary key (order_id, goods_id)
);


--购物车表
create table ods_order_cart
(
   cart_id              bigint not null auto_increment,
   session_id           varchar(64) not null,
   user_id              bigint not null,
   goods_id             bigint not null,
   goods_num            int not null,
   add_time             datetime not null,
   cancle_time          datetime not null,
   sumbit_time          datetime not null,
   primary key (cart_id)
);




--用户地址表
create table ods_user_addr
(
   user_id              bigint not null,
   order_addr           varchar(512) not null,
   user_order_flag      tinyint not null,
   primary key (user_id)
);

--订单收货人表
create table ods_order_delivery
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
   update_time          datetime not null,
   primary key (order_id)
);


--  类目码表
create table ods_category_code
(
   first_category_id    int not null,
   first_category_name  varchar(32) not null,
   second_category_id   int not null,
   second_catery_name   varchar(32) not null,
   third_category_id    int not null,
   third_category_name  varchar(32) not null
);


