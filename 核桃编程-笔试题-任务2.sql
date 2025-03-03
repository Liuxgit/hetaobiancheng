# 任务2

分析角度
试题中给出的角度：



其他角度
用户年龄（若有）
老师的通话时长分段
老师的通话所在时段




-- 任务2
with u as (
	select user_id,city,enrollment_date,experience_teacher_id
	from users
),
u_info as ( -- 构造数据
	select user_id
	source -- 用户渠道
	,stu_age_bin -- 学生的年龄分桶
	,his_renewal_rate_bin -- 历史续费率分桶
	from user_info
),
c as (
	select call_id,teacher_id,user_id,call_date,call_duration
	from call_logs
),
r as (
	select user_id,finish_status,renewal_status,renewal_date
	from renewals
),
t as (
	select teacher_id,group,
	case
		when group='A' then '实验组'
		when group='B' then '对照组'
		else '其他'
	end as test_group
	from teachers
)
select
uf.source,
uf.stu_age_bin,
uf.his_renewal_rate_bin,
t.test_group,
count(distinct u.user_id) as `用户数`,
count(distinct if(c.user_id is not null,c.user_id,null)) as `被家访用户数`,
count(distinct if(c.user_id is not null,c.user_id,null))/count(distinct u.user_id) as `家访电话覆盖率`,
avg(if(c.user_id is not null,c.call_duration,null)) as `平均通话时长`,
count(distinct if(renewal_status=1,r.user_id,null)) as `续费用户数`,
count(distinct if(renewal_status=1,r.user_id,null))/count(distinct u.user_id) as `整体续费率`
from u
left join u_info as uf on u.user_id=u_info.user_id
join t on u.experience_teacher_id=t.teacher_id
left join c on c.teacher_id=t.teacher_id and c.user_id=u.user_id
left join r on r.user_id=u.user_id
group by uf.source,uf.stu_age_bin,uf.his_renewal_rate_bin,t.test_group
