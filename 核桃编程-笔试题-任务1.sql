-- 任务1
with u as (
	select user_id,city,enrollment_date,experience_teacher_id
	from users
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
t.test_group as `实验分组`,
count(distinct u.user_id) as `用户数`,
count(distinct if(c.user_id is not null,c.user_id,null)) as `被家访用户数`,
count(distinct if(c.user_id is not null,c.user_id,null))/count(distinct u.user_id) as `家访电话覆盖率`,
avg(if(c.user_id is not null,c.call_duration,null)) as `平均通话时长`,
count(distinct if(renewal_status=1,r.user_id,null)) as `续费用户数`,
count(distinct if(renewal_status=1,r.user_id,null))/count(distinct u.user_id) as `整体续费率`
from u
join t on u.experience_teacher_id=t.teacher_id
left join c on c.teacher_id=t.teacher_id and c.user_id=u.user_id
left join r on r.user_id=u.user_id
group by t.test_group
