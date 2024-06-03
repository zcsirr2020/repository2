
drop table if exists yp_dm.dm_user;
create table yp_dm.dm_user
(
  date_time string COMMENT '统计日期',
  user_id string  comment '用户id',
--登录
  login_date_first string  comment '首次登录时间',
  login_date_last string  comment '末次登录时间',
  login_count bigint comment '累积登录天数',
  login_last_30d_count bigint comment '最近30日登录天数',
--购物车
  cart_date_first string comment '首次加入购物车时间',
  cart_date_last string comment '末次加入购物车时间',
  cart_count bigint comment '累积加入购物车次数',
  cart_amount decimal(38,2) comment '累积加入购物车金额',
  cart_last_30d_count bigint comment '最近30日加入购物车次数',
  cart_last_30d_amount decimal(38,2) comment '最近30日加入购物车金额',
  --订单
  order_date_first string  comment '首次下单时间',
  order_date_last string  comment '末次下单时间',
  order_count bigint comment '累积下单次数',
  order_amount decimal(38,2) comment '累积下单金额',
  order_last_30d_count bigint comment '最近30日下单次数',
  order_last_30d_amount decimal(38,2) comment '最近30日下单金额',
  --支付
  payment_date_first string  comment '首次支付时间',
  payment_date_last string  comment '末次支付时间',
  payment_count bigint comment '累积支付次数',
  payment_amount decimal(38,2) comment '累积支付金额',
  payment_last_30d_count bigint comment '最近30日支付次数',
  payment_last_30d_amount decimal(38,2) comment '最近30日支付金额'
)
COMMENT '用户主题宽表'
ROW format delimited fields terminated BY '\t'
stored AS orc tblproperties ('orc.compress' = 'SNAPPY');




