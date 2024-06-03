insert into yp_dm.dm_sku
select * from (
with t1_all_count as(
   select
      sku_id,sku_name,
      sum(order_count) as order_count,
      sum(order_num) as order_num,
      sum(order_amount) as order_amount,
      sum(payment_count) payment_count,
      sum(payment_num) payment_num,
      sum(payment_amount) payment_amount,
      sum(refund_count) refund_count,
      sum(refund_num) refund_num,
      sum(refund_amount) refund_amount,
      sum(cart_count) cart_count,
      sum(cart_num) cart_num,
      sum(favor_count) favor_count,
      sum(evaluation_good_count)   evaluation_good_count,
      sum(evaluation_mid_count)   evaluation_mid_count,
      sum(evaluation_bad_count)   evaluation_bad_count
   from yp_dws.dws_sku_daycount
   group by sku_id,sku_name
)
,
t2_last_30d as(
   select
      sku_id,sku_name,
       sum(order_count) order_last_30d_count,
       sum(order_num) order_last_30d_num,
       sum(order_amount) as order_last_30d_amount,
       sum(payment_count) payment_last_30d_count,
       sum(payment_num) payment_last_30d_num,
       sum(payment_amount) payment_last_30d_amount,
       sum(refund_count) refund_last_30d_count,
       sum(refund_num) refund_last_30d_num,
       sum(refund_amount) refund_last_30d_amount,
       sum(cart_count) cart_last_30d_count,
       sum(cart_num) cart_last_30d_num,
       sum(favor_count) favor_last_30d_count,
       sum(evaluation_good_count) evaluation_last_30d_good_count,
       sum(evaluation_mid_count) evaluation_last_30d_mid_count,
       sum(evaluation_bad_count) evaluation_last_30d_bad_count
   from yp_dws.dws_sku_daycount
   where dt>=cast(date_add('day', -30, date '2020-05-08') as varchar)
   group by sku_id,sku_name
)

select
  t1.sku_id,  -- string comment 'sku_id',
  t2.order_last_30d_count,-- bigint comment '最近30日被下单次数',
  t2.order_last_30d_num,-- bigint comment '最近30日被下单件数',
  t2.order_last_30d_amount ,-- decimal(38,2) comment '最近30日被下单金额',
  t1.order_count,-- bigint comment '累积被下单次数',
  t1.order_num,-- bigintcomment '累积被下单件数',
  t1.order_amount,-- decimal(38,2) comment '累积被下单金额',

  t2.payment_last_30d_count,--   bigint comment '最近30日被支付次数',
  t2.payment_last_30d_num,-- bigint comment '最近30日被支付件数',
  t2.payment_last_30d_amount,-- decimal(38,2) comment '最近30日被支付金额',
  t1.payment_count ,--   bigint comment '累积被支付次数',
  t1.payment_num ,-- bigint comment '累积被支付件数',
  t1.payment_amount,--   decimal(38,2) comment '累积被支付金额',

  t2.refund_last_30d_count ,-- bigint comment '最近三十日退款次数',
  t2.refund_last_30d_num,-- bigint comment '最近三十日退款件数',
  t2.refund_last_30d_amount,-- decimal(38,2) comment '最近三十日退款金额',
  t1.refund_count,-- bigint comment '累积退款次数',
  t1.refund_num,-- bigint comment '累积退款件数',
  t1.refund_amount,-- decimal(38,2) comment '累积退款金额',

  t2.cart_last_30d_count,-- bigint comment '最近30日被加入购物车次数',
  t2.cart_last_30d_num,-- bigint comment '最近30日被加入购物车件数',
  t1.cart_count,-- bigint comment '累积被加入购物车次数',
  t1.cart_num,-- bigint comment '累积被加入购物车件数',

  t2.favor_last_30d_count,-- bigint comment '最近30日被收藏次数',
  t1.favor_count ,-- bigint comment '累积被收藏次数',

  t2.evaluation_last_30d_good_count,-- bigint comment '最近30日好评数',
  t2.evaluation_last_30d_mid_count,-- bigint comment '最近30日中评数',
  t2.evaluation_last_30d_bad_count,-- bigint comment '最近30日差评数',
  t1.evaluation_good_count,-- bigint comment '累积好评数',
  t1.evaluation_mid_count,-- bigint comment '累积中评数',
  t1.evaluation_bad_count-- bigint comment '累积差评数'

from t1_all_count t1 left join t2_last_30d t2 on t1.sku_id = t2.sku_id
)
