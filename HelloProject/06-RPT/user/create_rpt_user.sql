--用户数量统计
drop table if exists yp_rpt.rpt_user_count;
create table yp_rpt.rpt_user_count(
    dt string COMMENT '统计日期',
    day_users BIGINT COMMENT '活跃会员数',
    day_new_users BIGINT COMMENT '新增会员数',
    day_new_payment_users BIGINT COMMENT '新增消费会员数',
    payment_users BIGINT COMMENT '总付费会员数',
    users BIGINT COMMENT '总会员数',
    day_users2users decimal(38,4) COMMENT '会员活跃率',
    payment_users2users decimal(38,4) COMMENT '总会员付费率',
    day_new_users2users decimal(38,4) COMMENT '会员新鲜度'
)
COMMENT '用户数量统计报表'
ROW format delimited fields terminated BY '\t'
stored AS orc tblproperties ('orc.compress' = 'SNAPPY');