
-- insert into yp_dws.dws_user_daycount -- Presto
-- 一、列裁剪
    with column_cropping as (
        select
            dt,
            buyer_id,
            order_id,
            order_amount,
            order_state,
        row_number() over(partition by order_id, goods_id)as rn1  --对脏数据进行过滤
        from yp_dwb.dwb_order_detail
    ),
t0 as (
    select
        *,
    row_number() over (partition by order_id)as rn2   -- 去重
    from column_cropping
    where rn1 = 1
),

---二、统计6张结果表
    -- 1、登录次数
    login_count as (
       select
          login_user as user_id,
          count(id) as login_count,
          dt
      from yp_dwd.fact_user_login
      group by login_user, dt
    ),
    -- 2、收藏店铺数
    store_collect_count as (
       select
           user_id,
           count(store_id) as store_collect_count,
           substring(create_time, 1, 10) as dt
       from yp_dwd.fact_store_collect
       where end_date='9999-99-99'
       group by user_id, substring(create_time, 1, 10)
    ),
    -- 3、收藏商品数
    goods_collect_count as (
    select
        user_id,
        count(goods_id) as goods_collect_count,
        substring(create_time, 1, 10) as dt
     from yp_dwd.fact_goods_collect
     where end_date='9999-99-99'
     group by user_id, substring(create_time, 1, 10)
    ),

    -- 4、加入购物车次数和金额
    cart_count_amount as (
    select
       count(t1.id) as cart_count,
       round(sum(buy_num * goods_price),2) as cart_amount,
       buyer_id as user_id,
       substring(t1.create_time, 1, 10) as dt
    from (select * from yp_dwd.fact_shop_cart where end_date ='9999-99-99') t1
        left join(select *from yp_dwd.dim_goods where end_date ='9999-99-99') t2 on t1.goods_id = t2.id
    group by buyer_id, substring(t1.create_time, 1, 10)
    ),

      -- 5、下单次数、下单金额
    order_count_amount as (
        select
            dt,
            buyer_id as user_id,
            count(if(rn2 =1,order_id,null)) as order_count,
            sum(if(rn2 =1,order_amount,0))as order_amount
        from t0
        group by dt,buyer_id
    ),
     -- 6、支付次数和金额
    payment_count_amount as (
        select
            dt,
            buyer_id as user_id,
            count(if(rn2 =1,order_id,null)) as payment_count,
            sum(if(rn2 =1,order_amount,0))as payment_amount
        from t0
        where order_state not in(1,7)
        group by dt,buyer_id
    ),

-- 三、将6张表进行full join
    fulljoin as (
       select
         coalesce(lc.dt, scc.dt, gcc.dt, cc.dt, oc.dt, pc.dt) dt ,
         coalesce(lc.user_id, scc.user_id, gcc.user_id, cc.user_id, oc.user_id, pc.user_id) user_id ,
         coalesce(login_count,0)  as login_count ,
         coalesce(store_collect_count,0)  as store_collect_count ,
         coalesce(goods_collect_count,0) as goods_collect_count ,
         coalesce(cart_count,0) as cart_count ,
         coalesce(cart_amount,0) as cart_amount ,
         coalesce(order_count,0) as order_count ,
         coalesce(order_amount,0) as order_amount ,
         coalesce(payment_count,0) as payment_count ,
         coalesce(payment_amount,0) as payment_amount
      from login_count lc
         full join store_collect_count scc on lc.dt=scc.dt and lc.user_id=scc.user_id
         full join goods_collect_count gcc on scc.dt=gcc.dt and scc.user_id=gcc.user_id
         full join cart_count_amount cc on gcc.dt=cc.dt and gcc.user_id=cc.user_id
         full join order_count_amount oc on cc.dt=oc.dt and cc.user_id=oc.user_id
         full join payment_count_amount pc on oc.dt=pc.dt and oc.user_id=pc.user_id
    )
-- insert into table yp_dws.dws_user_daycount --Hive
-- 四、将full join后的结果去重，求和
    select
        dt,
        user_id,
        -- 登录次数
        sum(coalesce(login_count,0)) as login_count,
        -- 店铺收藏数
        sum(coalesce(store_collect_count,0)) as store_collect_count,
        -- 商品收藏数
        sum(coalesce(goods_collect_count,0)) as goods_collect_count,
        -- 加入购物车次数和金额
        sum(coalesce(cart_count,0)) as cart_count,
        sum(coalesce(cart_amount,0)) as cart_amount,
        -- 下单次数和金额
        sum(coalesce(order_count,0)) as order_count,
        sum(coalesce(order_amount,0)) as order_amount,
        -- 支付次数和金额
        sum(coalesce(payment_count,0)) as payment_count,
        sum(coalesce(payment_amount,0)) as payment_amount
    from fulljoin
    group by dt, user_id

