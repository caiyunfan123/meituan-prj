--dws_user_visit_month1
create table if not exists qfbap_dws.dws_user_visit_month1(
user_id int,
type string,
cnt int,
content string,
in_time date,
rn int
)
partitioned by (dt date)
row format delimited fields terminated by '\t'
stored as textfile;

--假设统计的是昨天的数据，dt=date_sub(current_date,1),where dt >= date_sub(current_date,30)
insert into qfbap_dws.dws_user_visit_month1 partition(dt)
select
t.user_id,
t.type,
t.cnt,
t.content,
t.in_time,
row_number() over(distribute by user_id,type sort by cnt desc) rn,
date_sub(current_date,1) dt
from (
select
  user_id,
  'visit_ip' as type,-- 近30天访问ip
  sum(pv) as cnt,
  visit_ip as content,
  max(in_time) as in_time
from qfbap_dwd.dwd_user_pc_pv
--写<=31的原因：数据最多只会到昨天导入的数据为止，因此过滤0-30天内的数据没问题
where datediff(current_date,in_time)<31
group by 
    user_id,
    visit_ip
union all
select
  user_id,
  'cookie_id' as type, -- 近30天常用cookie
  sum(pv) as cnt,
  cookie_id as content,
  max(in_time) as in_time
from qfbap_dwd.dwd_user_pc_pv
where datediff(current_date,in_time)<31
group by 
    user_id,
    cookie_id
union all 
select
  user_id,
  'browser_name' as type,-- 近30天常用浏览器
  sum(pv) as cnt,
  browser_name as content,
  max(in_time) as in_time
from qfbap_dwd.dwd_user_pc_pv
where datediff(current_date,in_time)<31
group by 
    user_id,
    browser_name
union all
select
  user_id,
  'visit_os' as type, -- 近30天常用操作系统
  sum(pv) as cnt,
  visit_os as content,
  max(in_time) as in_time
from qfbap_dwd.dwd_user_pc_pv
where datediff(current_date,in_time)<31
group by 
    user_id,
    visit_os
) t


--dws_user_basic
drop table if exists qfbap_dws.dws_user_basic;
create table if not exists qfbap_dws.dws_user_basic as
select
   a.user_id           user_id,
   a.user_name         user_name,
   a.user_gender       user_gender,
   a.user_birthday     user_birthday,
   a.user_age          user_age,
   a.constellation     constellation,
   a.province          province,
   a.city              city,
   a.city_level        city_level,
   a.e_mail            e_mail,
   a.op_mail           op_mail,
   a.mobile            mobile,
   a.num_seg_mobile    num_seg_mobile,
   a.op_mobile         op_mobile,
   a.register_time     register_time,
   a.login_ip          login_ip,
   a.login_source      login_source,
   a.request_user      request_user,
   a.total_score       total_score,
   a.used_score        used_score,
   a.is_blacklist      is_blacklist,
   a.is_married        is_married,
   a.education         education,
   a.monthly_income    monthly_income,
   a.profession        profession           ,
   b.is_pregnant_woman is_pregnant_woman    ,
   b.is_have_children  is_have_children     ,
   b.is_have_car       is_have_car          ,
   b.phone_brand       phone_brand          ,
   b.phone_brand_level phone_brand_level    ,
   b.phone_cnt         phone_cnt            ,
   b.change_phone_cnt  change_phone_cnt     ,
   b.is_maja           is_maja              ,
   b.majia_account_cnt majia_account_cnt    ,
   b.loyal_model       loyal_model          ,
   b.shopping_type_model shopping_type_model,
   b.weight            weight               ,
   b.height            height               
from qfbap_ods.ods_user a
left join qfbap_ods.ods_user_extend b on a.user_id = b.user_id