--用户数量统计
insert into yp_rpt.rpt_user_count
select
    '2019-05-07' dt,
    sum(if(login_date_last='2019-05-07',1,0)) day_users,
    sum(if(login_date_first='2019-05-07',1,0)) day_new_users,
    sum(if(payment_date_first='2019-05-07',1,0)) day_new_payment_users,
    sum(if(payment_count>0,1,0)) payment_users,
    count(*) users,
    if(
        sum(if(login_date_last = '2019-05-07', 1, 0)) = 0,
        null,
        cast(sum(if(login_date_last = '2019-05-07', 1, 0)) as DECIMAL(38,4))
    )/count(*) * 100 day_users2users,
    if(
        sum(if(payment_count>0,1,0)) = 0,
        null,
        cast(sum(if(payment_count>0,1,0)) as DECIMAL(38,4))
    )/count(*) * 100 payment_users2users,
    if(
        sum(if(login_date_first='2019-05-07',1,0)) = 0,
        null,
        cast(sum(if(login_date_first='2019-05-07',1,0)) as DECIMAL(38,4))
    )/sum(if(login_date_last='2019-05-07',1,0)) * 100 day_new_users2users
from yp_dm.dm_user;