/*----------------------------------------------------------------
        解析qfbap_dm.dm_user_visit
1.在案例已经提供了完整的sql语句的前提下，没有必要再写一遍。

2.因此对该sql语句进行分解解析，完善分区表就算完成学习。

3.以下8个模块是dm_user_visit结果表中所有模块的一个精简提炼，
结果表中的其它指标均是修改自以下9个模块。

4.可以将current_date改为变量，从外界传入，修改后该sql将变为指定任意时间的sql
-----------------------------------------------------------------*/

--1.求最近一次的访问时间
select max(case when pc.rn_desc = 1 then in_time end) from (
    select in_time,
    row_number() over(distribute by user_id sort by in_time desc) rn_desc
	from qfbap_dwd.dwd_user_pc_pv
) t

--2.求pc端最早访问记录
select max(case when pc.rn_asc = 1 then in_time end) from (
    select in_time,
    row_number() over(distribute by user_id sort by in_time asc) rn_asc
	from qfbap_dwd.dwd_user_pc_pv
) t


--3.求最近七天的访问次数(原表注释为连续7天，但这显然不是连续7天而是7天内总登陆次数)
select sum(dt7) from(
	select (datediff(current_date,in_time)<8 then 1 else 0 end) dt7 
	from qfbap_dwd.dwd_user_pc_pv t
)
group by user_id

--4.后续补上连续7天的正确代码

--5.求近30天内0点-5点访问的数量
select count(case when dt30=1 and hr025=1 then 1 end null) from (
	select (case when datediff(current_date,in_time)<31 then 1 end) dt30,
	(case when hour(in_time) between 0 and 5 then 1 end) hr025
	from qfbap_dwd.dwd_user_pc_pv
)
group by user_id

--6.求PC端近30天用户使用不同ip访问的数量
select user_id,
count(distinct content)
from qfbap_dws.dws_user_visit_month1
where datediff(current_date,in_time)<31 and type='visit_ip'
group by user_id

--7.求PC端近30天用户
select user_id,content
from qfbap_dws.dws_user_visit_month1
where datediff(current_date,in_time)<31 and type='visit_ip' and rn_desc=1
group by user_id,content

--8.app端的指标与pc端类似，就不再研究。表：qfbap_dwd.dwd_user_app_pv

--9.最近一次访问的城市
/*
该语句为伪代码，latest_pc_visit_date等字段是基于上面模块所得，具体思路如下：
1.在dwd_user_pc_pv表中求出最近一次pc端的访问时间和访问城市（通过连接用户宽表所得）
2.在dwd_user_app_pv表中求出最近一次app端的访问时间和访问城市（通过连接用户宽表所得）
3.比较两次时间，选择最近时间的表，取出它的访问城市
*/
select us.user_id
(case when r_pc.latest_pc_visit_date>= r_app.latest_app_visit_date 
then r_pc.latest_city else r_app.latest_city end)
from dws_user_basic us 
left join qfbap_dwd.dwd_user_pc_pv r_pc on us.user_id=r_pc.user_id 
left join qfbap_dwd.dwd_user_app_pv r_pc on us.user_id=r_app.user_id