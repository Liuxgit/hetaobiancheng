#!/usr/bin/env python
# coding: utf-8

from pyspark.sql.types import * 
import pandas as pd
spark = SparkSession.builder.appName("liuxin_dis_model")

sql='''
-- 任务2
with u as (
	select user_id,city,enrollment_date,experience_teacher_id
    -- 构造数据
    ,course_amount -- 课程金额
	from users
),
u_info as ( -- 构造数据
	select user_id
	source -- 用户渠道
	,stu_age_bin -- 学生的年龄分桶
	,his_renewal_rate_bin -- 历史续费率分桶
    ,city_level -- 所在城市级别
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
uf.city_level,
t.test_group,
count(distinct u.user_id) as `用户数`,
count(distinct if(c.user_id is not null,c.user_id,null)) as `被家访用户数`,
count(distinct if(c.user_id is not null,c.user_id,null))/count(distinct u.user_id) as `家访电话覆盖率`,
avg(if(c.user_id is not null,c.call_duration,null)) as `平均通话时长`,
sum(if(c.user_id is not null,c.call_duration,null)) as `通话总时长`,
count(distinct if(renewal_status=1,r.user_id,null)) as `续费用户数`,
count(distinct if(renewal_status=1,r.user_id,null))/count(distinct u.user_id) as `整体续费率`,
sum(course_amount) as `课程金额`
from u
left join u_info as uf on u.user_id=u_info.user_id
join t on u.experience_teacher_id=t.teacher_id
left join c on c.teacher_id=t.teacher_id and c.user_id=u.user_id
left join r on r.user_id=u.user_id
group by uf.source,uf.stu_age_bin,uf.his_renewal_rate_bin,t.test_group,uf.city_level

'''
df = spark.sql(sql)
df = df.toPandas()



# 1、卡方检验


from scipy.stats import chi2_contingency

tmp1 = df[df['test_group']=='实验组'][['用户数','续费用户数']]
tmp2 = df[df['test_group']=='对照组'][['用户数','续费用户数']]

contingency_table = np.array([[tmp1['用户数'].sum(), tmp1['续费用户数'].sum()],
                              [tmp2['用户数'].sum(), tmp2['续费用户数'].sum()]])
chi2, p_value, dof, expected = chi2_contingency(contingency_table)
print("p 值：", p_value)


# 2、对比不同维度的续费率表现


import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

fig, axes = plt.subplots(2, 2, figsize=(18, 6))

# 绘制不同城市下两组数据的续费率
tmp_city = df.groupby(['test_group','city_level'])[['用户数', '续费用户数']].sum().reset_index()
tmp_city['续费率'] = tmp_city['续费用户数'] / tmp_city['用户数']

sns.barplot(x='city_level', y='续费率', hue='test_group', data=tmp_city, ax=axes[0,0])
axes[0].set_title("不同城市级别下两组数据的续费率转化表现")
axes[0].set_xlabel("城市级别")
axes[0].set_ylabel("续费率")

# 渠道维度数据聚合
tmp_source = df.groupby(['test_group','source'])[['用户数', '续费用户数']].sum().reset_index()
tmp_source['续费率'] = tmp_source['续费用户数'] / tmp_source['用户数']

sns.barplot(x='source', y='续费率', hue='test_group', data=tmp_source, ax=axes[0,1])
axes[1].set_title("不同渠道两组数据的续费率转化表现")
axes[1].set_xlabel("渠道")
axes[1].set_ylabel("续费率")

# 学生年龄维度数据聚合
tmp_age = df.groupby(['test_group','stu_age_bin'])[['用户数', '续费用户数']].sum().reset_index()
tmp_age['续费率'] = tmp_age['续费用户数'] / tmp_age['用户数']

sns.barplot(x='stu_age_bin', y='续费率', hue='test_group', data=tmp_age, ax=axes[1,0])
axes[2].set_title("不同学生年龄两组数据的续费率转化表现")
axes[2].set_xlabel("学生年龄")
axes[2].set_ylabel("续费率")


# 续费率维度数据聚合
tmp_age = df.groupby(['test_group','his_renewal_rate_bin'])[['用户数', '续费用户数']].sum().reset_index()
tmp_age['续费率'] = tmp_age['续费用户数'] / tmp_age['用户数']

sns.barplot(x='his_renewal_rate_bin', y='续费率', hue='test_group', data=tmp_age, ax=axes[1,1])
axes[2].set_title("不同历史续费率两组数据的续费率转化表现")
axes[2].set_xlabel("学生年龄")
axes[2].set_ylabel("续费率")

plt.tight_layout()
plt.show()


# 对比电话覆盖率


import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

fig, axes = plt.subplots(2, 2, figsize=(18, 6))

# 绘制不同城市下两组数据的续费率
tmp_city = df.groupby(['test_group','city_level'])[['用户数', '被家访用户数']].sum().reset_index()
tmp_city['电话覆盖率'] = tmp_city['被家访用户数'] / tmp_city['用户数']

sns.barplot(x='city_level', y='电话覆盖率', hue='test_group', data=tmp_city, ax=axes[0,0])
axes[0].set_title("不同城市级别下两组数据的电话覆盖率转化表现")
axes[0].set_xlabel("城市级别")
axes[0].set_ylabel("电话覆盖率")

# 渠道维度数据聚合
tmp_source = df.groupby(['test_group','source'])[['用户数', '被家访用户数']].sum().reset_index()
tmp_source['电话覆盖率'] = tmp_source['被家访用户数'] / tmp_source['用户数']

sns.barplot(x='source', y='电话覆盖率', hue='test_group', data=tmp_source, ax=axes[0,1])
axes[1].set_title("不同渠道两组数据的电话覆盖率转化表现")
axes[1].set_xlabel("渠道")
axes[1].set_ylabel("电话覆盖率")

# 学生年龄维度数据聚合
tmp_age = df.groupby(['test_group','stu_age_bin'])[['用户数', '被家访用户数']].sum().reset_index()
tmp_age['电话覆盖率'] = tmp_age['被家访用户数'] / tmp_age['用户数']

sns.barplot(x='stu_age_bin', y='电话覆盖率', hue='test_group', data=tmp_age, ax=axes[1,0])
axes[2].set_title("不同学生年龄两组数据的电话覆盖率转化表现")
axes[2].set_xlabel("学生年龄")
axes[2].set_ylabel("电话覆盖率")


# 历史续费率维度数据聚合
tmp_age = df.groupby(['test_group','his_renewal_rate_bin'])[['用户数', '被家访用户数']].sum().reset_index()
tmp_age['电话覆盖率'] = tmp_age['被家访用户数'] / tmp_age['用户数']

sns.barplot(x='his_renewal_rate_bin', y='电话覆盖率', hue='test_group', data=tmp_age, ax=axes[1,1])
axes[2].set_title("历史续费率维度两组数据的电话覆盖率转化表现")
axes[2].set_xlabel("学生年龄")
axes[2].set_ylabel("电话覆盖率")

plt.tight_layout()
plt.show()


# 3、构建回归

sql='''
-- 任务2
with u as (
	select user_id,city,enrollment_date,experience_teacher_id
    -- 构造数据
    ,course_amount -- 课程金额
	from users
),
u_info as ( -- 构造数据
	select user_id
	source -- 用户渠道
	,stu_age_bin -- 学生的年龄分桶
	,his_renewal_rate_bin -- 历史续费率分桶
    ,city_level -- 所在城市级别
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
uf.city_level,
t.test_group,
renewal_status
from u
left join u_info as uf on u.user_id=u_info.user_id
join t on u.experience_teacher_id=t.teacher_id
left join c on c.teacher_id=t.teacher_id and c.user_id=u.user_id
left join r on r.user_id=u.user_id
'''
df_l = spark.sql(sql)
df_l = df_l.toPandas()
df_l = df_l[df_l.test_group=='实验组']

import numpy as np
import statsmodels.api as sm
import statsmodels.formula.api as smf

source,
stu_age_bin,
his_renewal_rate_bin,
city_level,
renewal_status


df_l = pd.get_dummies(df_l, columns=['source','stu_age_bin','his_renewal_rate_bin','city_level'], drop_first=True)

cols = list(df_l.columns).remove('renewal_status') 
cols_ = ' + '.join(cols)
formula = 'renewal_status ~ ' + cols_
model = smf.logit(formula=formula, data=df_l)
result = model.fit()

# 输出回归结果摘要
print(result.summary())

# 输出对续费率影响较大的因素（即 p 值 < 0.05 的变量）
significant_factors = result.pvalues[result.pvalues < 0.05]
print("\n显著因素及其 p 值：")
print(significant_factors)



# 4、计算ROI

# 这里假设仅有通话这一成本，且假设通话每分钟成本0.1元
df_roi = df.groupby(['test_group'])[['课程金额', '通话总时长']].sum().reset_index()
df_roi['成本'] = df_roi['通话总时长']*0.1
df_roi['ROI'] = df_roi['课程金额']-df_roi['成本']
roi_ = (df_roi[df_roi.test_group=='实验组']['ROI'].summ()-df_roi[df_roi.test_group=='对照组']['ROI'].summ())/df_roi[df_roi.test_group=='对照组']['ROI'].summ()
print(f'增量ROI={roi_}')




