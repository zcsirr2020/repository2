-- insert into hive.yp_dws.dws_sku_daycount -- Presto

-- 一、列截取（在结果表中所显示的字段）
with t0 as (
    select
        dt,
        goods_id as sku_id,
        goods_name as sku_name,
        order_id,
        buy_num,
        total_price,
        order_state,
        refund_id,
        evaluation_id,
        geval_scores,
-- 八、去重
        row_number() over (partition by order_id,goods_id)as rn
    from yp_dwb.dwb_order_detail
),

-- 二、统计被下单次数，被下单件数，被下单金额
t1 as (select dt,
              sku_id,
              sku_name,
              count(order_id)  as order_count, --被下单次数
              sum(buy_num)     as order_num,   --被下单件数
              sum(total_price) as order_amount --被下单金额
       from t0
       where rn = 1
       group by dt, sku_id, sku_name
),

--三、统计被支付次数，被支付件数，被支付金额
t2 as (select dt,
              sku_id,
              sku_name,
              count(order_id)  as payment_count, --被支付次数
              sum(buy_num)     as payment_num,   --被支付件数
              sum(total_price) as payment_amount --被支付金额
       from t0
       where rn = 1 and order_state not in(1,7)
       group by dt, sku_id, sku_name
),

--四、统计被退款次数，被退款件数，被退款金额
t3 as (select dt,
              sku_id,
              sku_name,
              count(order_id)  as refund_count, --被退款次数
              sum(buy_num)     as refund_num,   --被退款件数
              sum(total_price) as refund_amount --被退款金额
       from t0
       where rn = 1 and refund_id is not null
       group by dt, sku_id, sku_name
),

--五、统计被加入购物车次数，被加入购物车件数
----通过分析我们发现，之前的DWB层中并没有关联和购物车相关联的表，我们需要去跨层去DWD层去访问数据
----通过分析发现fact_shop_cart表没有商品名，所以需要和dim_goods商品表进行关联，获取商品名
t4 as (
    select
        substring(sc.create_time,1,10) as dt,
        sc.goods_id as sku_id,
        goods_name as sku_name,
        count(sc.id)as cart_count,   --被加入购物车次数
        sum(sc.buy_num) as cart_num  --被加入购物车件数
    from yp_dwd.fact_shop_cart sc left join yp_dwd.dim_goods g on sc.goods_id = g.id
    where sc.end_date='9999-99-99'
    group by substring(sc.create_time,1,10),sc.goods_id,goods_name
),

--五、统计被收藏次数，被加收藏件数
t5 as (
    select
        substring(gc.create_time,1,10)as dt,
        gc.goods_id as sku_id,
        g.goods_name as sku_name,
        count(gc.id)as favor_count -- 被收藏次数
    from yp_dwd.fact_goods_collect gc left join yp_dwd.dim_goods g on gc.goods_id = g.id
    where gc.end_date ='9999-99-99'
    group by substring(gc.create_time,1,10),gc.goods_id,g.goods_name
),

-- 六、统计 好评数 中评数 差评数
t6 as (
    select
        dt,
        sku_id,
        sku_name,
        count(if(geval_scores>=9,evaluation_id,null))as evaluation_good_count,
        count(if(geval_scores<9 and geval_scores >6,evaluation_id,null)) as evaluation_mid_count,
        count(if(geval_scores <= 6,evaluation_id,null)) as evaluation_bad_count
    from t0
    where rn=1 and evaluation_id is not null
    group by dt,sku_id,sku_name
),


t7 as (
-- 七、 将t1-t6(6张表)进行full join整合
select
    coalesce(t1.dt,t2.dt,t3.dt,t4.dt,t5.dt,t6.dt) as dt,
    coalesce(t1.sku_id,t2.sku_id,t3.sku_id,t4.sku_id, t5.sku_id, t6.sku_id) as sku_id,
    coalesce(t1.sku_name,t2.sku_name ,t3.sku_name,t4.sku_name,t5.sku_name, t6.sku_name) as sku_name,
    coalesce(t1.order_count,0)as order_count,
    coalesce(t1.order_num,0)as order_num,
    coalesce(t1.order_amount,0)as order_amount,
    coalesce(t2.payment_count,0)as payment_count,
    coalesce(t2.payment_num,0)as payment_num,
    coalesce(t2.payment_amount,0)as payment_amount,
    coalesce(t3.refund_count,0)as refund_count,
    coalesce(t3.refund_num,0)as refund_num,
    coalesce(t3.refund_amount,0)as refund_amount,
    coalesce(t4.cart_count,0)as cart_count,
    coalesce(t4.cart_num,0)as cart_num,
    coalesce(t5.favor_count,0)as favor_count,
    coalesce(t6.evaluation_good_count,0) as evaluation_good_count,
    coalesce(t6.evaluation_mid_count,0)as evaluation_mid_count,
    coalesce(t6.evaluation_bad_count,0)as evaluation_bad_count
from t1
    full join t2 on t1.dt = t2.dt and t1.sku_id = t2.sku_id
    full join t3 on t2.dt = t3.dt and t2.sku_id = t3.sku_id
    full join t4 on t3.dt = t4.dt and t3.sku_id = t4.sku_id
    full join t5 on t4.dt = t5.dt and t4.sku_id = t5.sku_id
    full join t6 on t5.dt = t6.dt and t5.sku_id = t6.sku_id
)
-- insert into table yp_dws.dws_sku_daycount -- Hive
-- 八、去重
select
    dt,
    sku_id,
    sku_name,
    sum(order_count) as order_count,
    sum(order_num) as order_num,
    sum(order_amount) as order_amount,
    sum(payment_count) as payment_count,
    sum(payment_num) as payment_num,
    sum(payment_amount) as payment_amount,
    sum(refund_count) as refund_count,
    sum(refund_num) as refund_num,
    sum(refund_amount) as refund_amount,
    sum(cart_count) as cart_count,
    sum(cart_num) as cart_num,
    sum(favor_count) as favor_count,
    sum(evaluation_good_count) as evaluation_good_count,
    sum(evaluation_mid_count) as evaluation_mid_count,
    sum(evaluation_bad_count) as evaluation_bad_count
from t7
group by dt,sku_id,sku_name