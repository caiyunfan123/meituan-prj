--qfbap_dm.dm_user_visit
--默认传入的日期是昨天，统计的就是昨天

set hivevar:dt=from_unixtime(${hivevar:dt0},'yyyy-MM-dd');

insert overwrite table qfbap_dm.dm_user_visit partition(dt)
select 
   us.user_id user_id
   ,r_pc.latest_pc_visit_date                latest_pc_visit_date              --最近一次访问时间
   ,r_pc.latest_pc_visit_session             latest_pc_visit_session           --最近一次访问的session
   ,r_pc.latest_pc_cookies                   latest_pc_cookies                 --最近一次的coookie
   ,r_pc.latest_pc_pv                        latest_pc_pv                      --最近一次的pc端的pv量
   ,r_pc.latest_pc_browser_name              latest_pc_browser_name            --最近一次访问使用的浏览器
   ,r_pc.latest_pc_visit_os                  latest_pc_visit_os                --最近一次访问使用的操作系统
   ,r_pc.first_pc_visit_date                 first_pc_visit_date               --最早pc端访问的日期
   ,r_pc.first_pc_visit_session              first_pc_visit_session            --最早pc端访问的session
   ,r_pc.first_pc_cookies                    first_pc_cookies                  --最早pc端访问的cookie
   ,r_pc.first_pc_pv                         first_pc_pv                       --最早一次访问的pv
   ,r_pc.first_pc_browser_name               first_pc_browser_name             --最早一次访问使用的浏览器
   ,r_pc.first_pc_visit_os                   first_pc_visit_os                 --最早一次访问的os
   ,r_pc.day7_pc_cnt                         day7_pc_cnt                       --PC连续7天访问次数
   ,r_pc.day15_pc_cnt                        day15_pc_cnt                      --连续15天访问次数
   ,r_pc.month1_pc_cnt                       month1_pc_cnt                     --连续30天访问次数
   ,r_pc.month2_pc_cnt                       month2_pc_cnt                     --连续60天访问的次数
   ,r_pc.month3_pc_cnt                       month3_pc_cnt                     --连续90天访问的次数
   ,r_pc.month1_pc_days                      month1_pc_days                    --近30天pc端访问的次数
   ,r_pc.month1_pc_pv                        month1_pc_pv                      --近30天pc端的pv
   ,r_pc.month1_pc_avg_pv                    month1_pc_avg_pv                  --近30天pc端每天的平均pv
   ,r_pc.month1_pc_hour025_cnt               month1_pc_hour025_cnt             --0到5点的数量
   ,r_pc.month1_pc_hour627_cnt               month1_pc_hour627_cnt             --6到7点的数量
   ,r_pc.month1_pc_hour829_cnt               month1_pc_hour829_cnt             --8到9的数量
   ,r_pc.month1_pc_hour10211_cnt             month1_pc_hour10211_cnt           --10到11的数量
   ,r_pc.month1_pc_hour12213_cnt             month1_pc_hour12213_cnt           --12到13的数量
   ,r_pc.month1_pc_hour14216_cnt             month1_pc_hour14216_cnt           --14到16点的数量
   ,r_pc.month1_pc_hour17219_cnt             month1_pc_hour17219_cnt           --17到19点的数量
   ,r_pc.month1_pc_hour18219_cnt             month1_pc_hour18219_cnt           --18到19点的数量
   ,r_pc.month1_pc_hour20221_cnt             month1_pc_hour20221_cnt           --20到21点的数量
   ,r_pc.month1_pc_hour22223_cnt             month1_pc_hour22223_cnt           --22到23点的数量
   ,coalesce(pc_month1.month1_pc_diff_ip_cnt,0)          month1_pc_diff_ip_cnt             --近30天访问使用的不同ip数量
   ,pc_month1.month1_pc_common_ip            month1_pc_common_ip               --近30天最常用的ip
   ,coalesce(pc_month1.month1_pc_diff_cookie_cnt,0)      month1_pc_diff_cookie_cnt         --近30天使用的cookie的数量
   ,pc_month1.month1_pc_common_cookie        month1_pc_common_cookie           --近30使用最常用的cookie_id
   ,pc_month1.month1_pc_common_browser_name  month1_pc_common_browser_name     --pc最常用浏览器
   ,pc_month1.month1_pc_common_os            month1_pc_common_os               --近30天使用最常用系统
   ,r_app.latest_app_visit_date              latest_app_visit_date             --最近一次app访问的日期
   ,r_app.latest_app_name                    latest_app_name                   --最近一次访问app的名称
   ,r_app.latest_app_visit_os                latest_app_visit_os               --最近一次app访问的操作系统
   ,r_app.first_app_visit_date               first_app_visit_date              --第一次app访问日期
   ,r_app.first_app_name                     first_app_name                    --第一app访问名称
   ,r_app.first_app_visit_os                 first_app_visit_os                --第一次app访问os
   ,r_app.first_visit_ip                     first_app_visit_ip                --app第一次访问ip
   ,r_app.day7_app_cnt                       day7_app_cnt                      --app近7天访问次数
   ,r_app.day15_app_cnt                      day15_app_cnt                     --app近15天访问次数
   ,r_app.month1_app_cnt                     month1_app_cnt                    --app近30天的访问次数
   ,r_app.month2_app_cnt                     month2_app_cnt                    --app近60天的访问次数
   ,r_app.month3_app_cnt                     month3_app_cnt                    --app近90天的访问次数
   ,r_app.month1_app_hour025_cnt             month1_app_hour025_cnt            --app近30天0到5点的访问次数
   ,r_app.month1_app_hour627_cnt             month1_app_hour627_cnt            --app近30天的6到7点的访问次数
   ,r_app.month1_app_hour829_cnt             month1_app_hour829_cnt            --app近30天8到9的访问次数
   ,r_app.month1_app_hour10211_cnt           month1_app_hour10211_cnt          --app近30天10到11访问次数
   ,r_app.month1_app_hour12213_cnt           month1_app_hour12213_cnt          --app近30天12到13点的访问次数
   ,r_app.month1_app_hour14215_cnt           month1_app_hour14215_cnt          --app近30天14到15点的访问次数
   ,r_app.month1_app_hour16217_cnt           month1_app_hour16217_cnt          --app近30天16到17点的访问次数
   ,r_app.month1_app_hour18219_cnt           month1_app_hour18219_cnt          --app近30天18到19点的访问次数
   ,r_app.month1_app_hour20221_cnt           month1_app_hour20221_cnt          --app近30天20到21点的访问次数
   ,r_app.month1_app_hour22223_cnt           month1_app_hour22223_cnt          --app近30天22到23点的访问次数
   ,(case 
      when  r_pc.latest_pc_visit_date>= r_app.latest_app_visit_date 
      then   r_pc.latest_visit_ip
      else   r_app.latest_visit_ip
    end )  latest_visit_ip  ,                                             --最近一次访问的ip
   (case 
      when r_pc.latest_pc_visit_date >= r_app.latest_app_visit_date
      then r_pc.latest_city
      else r_app.latest_city
    end )  latest_city          ,                                             --最近一次访问的城市
   (case
      when r_pc.latest_pc_visit_date >= r_app.latest_app_visit_date
      then r_pc.latest_city
      else r_app.latest_city
    end
    ) latest_province      ,                                            -- 最近一次访问的省份
   (case
      when r_pc.first_pc_visit_date <= r_app.first_app_visit_date
      then r_pc.first_visit_ip
      else r_app.first_visit_ip
    end
    ) first_visit_ip       ,                                             -- 第一次访问的ip
   (case 
      when r_pc.first_pc_visit_date <= r_app.first_app_visit_date
      then r_pc.first_city
      else r_app.first_city
    end
    ) first_city           ,                                            -- 第一次访问的城市
   (case
      when r_pc.first_pc_visit_date <= r_app.first_app_visit_date
      then r_pc.first_province
      else r_app.first_province
    end
    )first_province,
    ${dt} import_dt,
    ${dt} dt
from qfbap_dws.dws_user_basic  us
left join (
                                                -- PC端的统计指标
    select 
      user_id
       ,max(case when pc.rn_desc = 1 then in_time end) latest_pc_visit_date            -- 最近一次访问时间
       ,max(case when pc.rn_desc=1 then visit_ip end) latest_visit_ip                  --最近一次的访问ip
       ,max(case when pc.rn_desc=1 then province end) latest_province
       ,max(case when pc.rn_desc=1 then city end) latest_city
       ,max(case when pc.rn_desc = 1 then session_id end) latest_pc_visit_session      -- 最近一次访问的session
       ,max(case when pc.rn_desc = 1 then cookie_id end ) latest_pc_cookies            -- 最近一次的coookie
       ,max(case when pc.rn_desc = 1 then pv end) latest_pc_pv                         -- 最近一次的pc端的pv量
       ,max(case when pc.rn_desc = 1 then browser_name end ) latest_pc_browser_name    -- 最近一次访问使用的浏览器
       ,max(case when pc.rn_desc = 1 then visit_os end ) latest_pc_visit_os            -- 最近一次访问使用的操作系统
       ,max(case when pc.rn_asc = 1 then in_time end) first_pc_visit_date              -- 最早pc端访问的日期
       ,max(case when pc.rn_asc=1 then visit_ip end) first_visit_ip                    --最近一次的访问ip
       ,max(case when pc.rn_asc=1 then province end) first_province
       ,max(case when pc.rn_asc=1 then city end) first_city
       ,max(case when pc.rn_asc = 1 then session_id end ) first_pc_visit_session       --最早pc端访问的session
       ,max(case when pc.rn_asc = 1 then cookie_id end ) first_pc_cookies              -- 最早pc端访问的cookie
       ,max(case when pc.rn_asc = 1 then pv end ) first_pc_pv                          -- 最早一次访问的pv
       ,max(case when pc.rn_asc = 1 then browser_name end) first_pc_browser_name       --最早一次访问使用的浏览器
       ,max(case when pc.rn_asc = 1 then visit_os end) first_pc_visit_os               -- 最早一次访问的os
       ,coalesce(sum(dt7),0) day7_pc_cnt                                               --连续7天访问次数
       ,coalesce(sum(dt15),0) day15_pc_cnt                                             -- 连续15天访问次数
       ,coalesce(sum(dt30),0) month1_pc_cnt                                            -- 连续30天访问次数
       ,coalesce(sum(dt60),0) month2_pc_cnt                                            -- 连续60天访问的次数
       ,coalesce(sum(dt90),0) month3_pc_cnt                                            -- 连续90天访问的次数
       ,coalesce(count(distinct (case when dt30=1 then substr(in_time,0,8) end)),0) month1_pc_days                                             --近30天pc端访问的次数
       ,sum(case when dt30=1 then pv else 0 end ) month1_pc_pv                                             --近30天pc端的pv
       ,sum(case when dt30=1 then pv else 0 end )/count(distinct(case when dt30=1 then substr(in_time,0,8) end)) month1_pc_avg_pv --近30天pc端每天的平均pv
       ,sum(case when dt30=1 and hr025=1 then 1 else 0 end ) month1_pc_hour025_cnt          --0到5点的数量
       ,sum(case when dt30=1 and hr627=1 then 1 else 0 end ) month1_pc_hour627_cnt          --6到7点的数量
       ,sum(case when dt30=1 and hr829=1 then 1 else 0 end ) month1_pc_hour829_cnt          -- 8到9的数量
       ,sum(case when dt30=1 and hr10211=1 then 1 else 0 end ) month1_pc_hour10211_cnt          -- 10到11的数量
       ,sum(case when dt30=1 and hr12213=1 then 1 else 0 end ) month1_pc_hour12213_cnt          --12到13的数量
       ,sum(case when dt30=1 and hr14215=1 then 1 else 0 end ) month1_pc_hour14216_cnt          -- 14到16点的数量
       ,sum(case when dt30=1 and hr16217=1 then 1 else 0 end ) month1_pc_hour17219_cnt          -- 17到19点的数量
       ,sum(case when dt30=1 and hr18219=1 then 1 else 0 end ) month1_pc_hour18219_cnt          -- 18到19点的数量
       ,sum(case when dt30=1 and hr20221=1 then 1 else 0 end ) month1_pc_hour20221_cnt          -- 20到21点的数量
       ,sum(case when dt30=1 and hr22223=1 then 1 else 0 end ) month1_pc_hour22223_cnt          -- 22到23点的数量
    from (
       select 
          row_number() over(distribute by user_id sort by in_time asc) rn_asc,
          row_number() over(distribute by user_id sort by in_time desc) rn_desc,
          user_id,
          session_id,
          cookie_id,
          visit_os,
          browser_name,
          visit_ip,
          province,
          city,
                                            --因为以当前日期为准，所以应该取到7天前的数据
          (case when datediff(${dt},in_time)<7 then 1 end ) dt7,
          (case when datediff(${dt},in_time)<15 then 1 end ) dt15,
          (case when datediff(${dt},in_time)<30 then 1 end) dt30,
          (case when datediff(${dt},in_time)<60 then 1 end) dt60,
          (case when datediff(${dt},in_time)<90 then 1 end) dt90,
          (case when hour(in_time) between 0 and 5 then 1 end) hr025,
          (case when hour(in_time) between 6 and 7 then 1 end ) hr627,
          (case when hour(in_time) between 8 and 9 then 1 end ) hr829,
          (case when hour(in_time) between 10 and 11 then 1 end ) hr10211,
          (case when hour(in_time) between 12 and 13 then 1 end ) hr12213,
          (case when hour(in_time) between 14 and 15 then 1 end ) hr14215,
          (case when hour(in_time) between 16 and 17 then 1 end ) hr16217,
          (case when hour(in_time) between 18 and 19 then 1 end ) hr18219,
          (case when hour(in_time) between 20 and 21 then 1 end ) hr20221,
          (case when hour(in_time) between 22 and 23 then 1 end ) hr22223,
          in_time,
          out_time,
          stay_time,
          pv
       from qfbap_dwd.dwd_user_pc_pv t
    )  pc
    group by user_id

) r_pc on us.user_id = r_pc.user_id
left join (
        select user_id,
        count(distinct case when type='visit_ip' then content end ) month1_pc_diff_ip_cnt,           --近30天访问使用的不同ip数量
        max(case when rn=1 and type='visit_ip' then content end ) month1_pc_common_ip,                         --近30天最常用的ip
        count(distinct case when rn=1 and type = 'cookie_id' then content end) month1_pc_diff_cookie_cnt,  --近30天使用的cookie的数量
        max(case when rn=1 and type='cookie_id' then content end ) month1_pc_common_cookie,                    --近30使用最常用的cookie_id
        max(case when rn=1 and type='browser_name' then content end ) month1_pc_common_browser_name,
        max(case when rn=1 and type='visit_os' then content end ) month1_pc_common_os                          --近30天使用最常用系统
        from qfbap_dws.dws_user_visit_month1
        where datediff(${dt},in_time)<30
        group by user_id
    ) pc_month1 on us.user_id = pc_month1.user_id
left join (
                                              -- app端的统计指标
   select 
     app.user_id,
     max(case when rn_desc = 1 then log_time end) latest_app_visit_date ,
     max(case when rn_desc=1 then app_name  end) latest_app_name,
     max(case when rn_desc=1 then visit_os end) latest_app_visit_os,
     max(case when rn_desc=1 then visit_ip end) latest_visit_ip,
     max(case when rn_desc=1 then city end) latest_city,
     max(case when rn_desc=1 then province end) latest_province,
     max(case when rn_asc=1 then log_time end ) first_app_visit_date,
     max(case when rn_asc=1 then app_name end) first_app_name,
     max(case when rn_asc=1 then visit_os end) first_app_visit_os,
     max(case when rn_asc=1 then visit_ip end) first_visit_ip,
     max(case when rn_asc=1 then city end) first_city,
     max(case when rn_asc=1 then province end) first_province,
     coalesce(sum(app_dt7),0)  day7_app_cnt,
     coalesce(sum(app_dt15),0) day15_app_cnt,
     coalesce(sum(app_dt30),0) month1_app_cnt,
     coalesce(sum(app_dt60),0) month2_app_cnt,
     coalesce(sum(app_dt90),0) month3_app_cnt,
     sum(case when app_dt30 =1 then app_hr_025 else 0 end) month1_app_hour025_cnt,
     sum(case when app_dt30 =1 then app_hr_627 else 0 end) month1_app_hour627_cnt,
     sum(case when app_dt30 =1 then app_hr_829 else 0 end) month1_app_hour829_cnt,
     sum(case when app_dt30 =1 then app_hr_10211 else 0 end) month1_app_hour10211_cnt,
     sum(case when app_dt30 =1 then app_hr_12213 else 0 end) month1_app_hour12213_cnt,
     sum(case when app_dt30 =1 then app_hr_14215 else 0 end) month1_app_hour14215_cnt  ,
     sum(case when app_dt30 =1 then app_hr_16217 else 0 end) month1_app_hour16217_cnt  ,
     sum(case when app_dt30 =1 then app_hr_18219 else 0 end) month1_app_hour18219_cnt  ,
     sum(case when app_dt30 =1 then app_hr_20221 else 0 end) month1_app_hour20221_cnt  ,
     sum(case when app_dt30 =1 then app_hr_22223 else 0 end) month1_app_hour22223_cnt  
  from (
     select 
        user_id ,
        log_time ,
        log_hour ,
        visit_os ,
        os_version ,
        app_name   ,
        app_version,
        device_token,
        visit_ip,
        province,
        city,
        row_number() over(distribute by user_id sort by  log_time asc) rn_asc,
        row_number() over(distribute by user_id sort by  log_time desc) rn_desc,
        (case when datediff(${dt},log_time)<7 then 1 end) app_dt7,
        (case when datediff(${dt},log_time)<15 then 1 end) app_dt15,
        (case when datediff(${dt},log_time)<30 then 1 end) app_dt30,
        (case when datediff(${dt},log_time)<60 then 1 end) app_dt60,
        (case when datediff(${dt},log_time)<90 then 1 end) app_dt90,
        (case when hour(log_time) between 0 and 5 then 1 end) app_hr_025,
        (case when hour(log_time) between 6 and 7 then 1 end) app_hr_627,
        (case when hour(log_time) between 8 and 9 then 1 end) app_hr_829,
        (case when hour(log_time) between 10 and 11 then 1 end) app_hr_10211,
        (case when hour(log_time) between 12 and 13 then 1 end) app_hr_12213,
        (case when hour(log_time) between 14 and 15 then 1 end) app_hr_14215,
        (case when hour(log_time) between 16 and 17 then 1 end) app_hr_16217,
        (case when hour(log_time) between 18 and 19 then 1 end) app_hr_18219,
        (case when hour(log_time) between 20 and 21 then 1 end) app_hr_20221,
        (case when hour(log_time) between 22 and 23 then 1 end) app_hr_22223
     from qfbap_dwd.dwd_user_app_pv
     ) app
  group by user_id
) r_app on us.user_id = r_app.user_id;