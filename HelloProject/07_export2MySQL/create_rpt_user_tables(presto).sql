drop table if exists mysql.yp_olap.rpt_user_count;
create table mysql.yp_olap.rpt_user_count(
    dt varchar COMMENT '统计日期',
    day_users BIGINT COMMENT '活跃会员数',
    day_new_users BIGINT COMMENT '新增会员数',
    day_new_payment_users BIGINT COMMENT '新增消费会员数',
    payment_users BIGINT COMMENT '总付费会员数',
    users BIGINT COMMENT '总会员数',
    day_users2users decimal(38,4) COMMENT '会员活跃率',
    payment_users2users decimal(38,4) COMMENT '总会员付费率',
    day_new_users2users decimal(38,4) COMMENT '会员新鲜度'
)
COMMENT '用户数量统计报表';