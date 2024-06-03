-- insert into hive.yp_dws.dws_sale_daycount --在Hive中查不了这个表 Presto
with temp as (select
--一、从DWB层表中抽取字段----
   --维度抽取
  o.dt as create_date,  --日期
  s.city_id,
  s.city_name,--城市
  s.trade_area_id,
  s.trade_area_name,--商圈
  s.id as store_id,
  s.store_name,--店铺
  g.brand_id,
  g.brand_name, --品牌
  g.max_class_id,
  g.max_class_name,--商品大类
  g.mid_class_id,
  g.mid_class_name,--商品中类
  g.min_class_id,
  g.min_class_name, --商品小类

   --订单量指标
  o.order_id, --订单ID
  o.goods_id, --商品ID


   --金额指标
  o.order_amount,--订单金额
  o.total_price,--商品金额
  o.plat_fee, --平台分润
  o.dispatcher_money,--配送员运费

   --判断条件
  o.order_from,--订单来源：安卓，苹果啥的...
  o.evaluation_id,--评论单ID（如果不为null,表示该订单有评价）
  o.geval_scores, --订单评分（用于计算差评）
  o.delievery_id, --配送单ID(如果不为null，表示是配送单，其他还有可能是自提、商家配送)
  o.refund_id, --退款单ID(如果不为null,表示有退款)

   --二、分组去重
  row_number() over(partition by order_id) as order_rn,
  row_number() over(partition by order_id,g.brand_id) as brand_rn,
  row_number() over(partition by order_id,g.max_class_name) as maxclass_rn,
  row_number() over(partition by order_id,g.max_class_name,g.mid_class_name) as midclass_rn,
  row_number() over(partition by order_id,g.max_class_name,g.mid_class_name,g.min_class_name) as minclass_rn,

   --下面分组加入goods_id
  row_number() over(partition by order_id,g.brand_id,o.goods_id) as brand_goods_rn,
  row_number() over(partition by order_id,g.max_class_name,o.goods_id) as maxclass_goods_rn,
  row_number() over(partition by order_id,g.max_class_name,g.mid_class_name,o.goods_id) as midclass_goods_rn,
  row_number() over(partition by order_id,g.max_class_name,g.mid_class_name,g.min_class_name,o.goods_id) as minclass_goods_rn

from yp_dwb.dwb_order_detail o
   left join yp_dwb.dwb_goods_detail g on o.goods_id = g.id
   left join yp_dwb.dwb_shop_detail s on o.store_id = s.id
)

select
--三、grouping sets 分组字段设置
   --查询出来的字段个数、顺序、类型要和待插入表(dws_sale_daycount)的一致
    case when grouping(city_id) = 0   --如果分组中包含city_id 则grouping为0 那么就返回city_id
    then city_id
    else null end as city_id ,
    case when grouping(city_id) = 0
    then city_name
    else null end as city_name ,
    case when grouping(trade_area_id) = 0--商圈
    then trade_area_id
    else null end as trade_area_id ,
    case when grouping(trade_area_id) = 0
    then trade_area_name
    else null end as trade_area_name ,
    case when grouping(store_id) = 0 --店铺
    then store_id
    else null end as store_id ,
    case when grouping(store_id) = 0
    then store_name
    else null end as store_name ,
    case when grouping(brand_id) = 0 --品牌
    then brand_id
    else null end as brand_id ,
    case when grouping(brand_id) = 0
    then brand_name
    else null end as brand_name ,
    case when grouping(max_class_id) = 0 --大类
    then max_class_id
    else null end as max_class_id ,
    case when grouping(max_class_id) = 0
    then max_class_name
    else null end as max_class_name ,
    case when grouping(mid_class_id) = 0 --中类
    then mid_class_id
    else null end as mid_class_id ,
    case when grouping(mid_class_id) = 0
    then mid_class_name
    else null end as mid_class_name ,
    case when grouping(min_class_id) = 0--小类
    then min_class_id
    else null end as min_class_id ,
    case when grouping(min_class_id) = 0
    then min_class_name
    else null end as min_class_name ,

--五、分组类型（8种统计维度组合）
    case when grouping(city_id, trade_area_id, store_id,store_name) = 0          -- if
    then 'store'                                                                 --日期+ 城市+ 商圏+ 店铺
    when grouping(city_id, trade_area_id ,trade_area_name) = 0                            -- else if
    then 'trade_area'                                                            --日期+ 城市+ 商圏
    when grouping (city_id,city_name) = 0                                        -- else if
    then 'city'                                                                  --日期+ 城市
    when grouping (brand_id,brand_name) = 0
    then 'brand'                                                                 --日期+ 品牌
    when grouping (max_class_id, mid_class_id, min_class_id,min_class_name) = 0                              -- else if
    then 'min_class'                                                              --日期+ 大类+ 中类+ 小类
    when grouping (max_class_id, mid_class_id,mid_class_name) = 0                               -- else if
    then 'mid_class'                                                               --日期+ 大类+ 中类
    when grouping (max_class_id,max_class_name) = 0                                -- else if
    then 'max_class'                                                               --日期+ 大类
    when grouping (create_date) = 0                                                --日期
    then 'all'
    else 'other' end as group_type,

--六、指标计算 注意每个指标都对应着8个分组维度的计算
    --1、销售收入指标 sale_amt
    case when grouping(city_id, trade_area_id, store_id,store_name) =0  --如果分组中包含店铺,则分组为：日期+城市+商圈+店铺
    then sum(if( order_rn = 1 and store_id is not null ,order_amount,0)) --只有分组中标号为1的(去重)，店铺不为空的才参与计算
    --then sum(if( order_rn = 1 and store_id is not null ,coalesce(order_amount,0),0))  --使用coalesce函数更加成熟

    when grouping (city_id, trade_area_id ,trade_area_name) = 0 --日期+城市+商圈
    then sum(if( order_rn = 1 and trade_area_id is not null ,order_amount,0))

    when grouping (city_id,city_name) = 0 --日期+城市
    then sum(if( order_rn = 1 and city_id is not null,order_amount,0))

    when grouping (brand_id,brand_name) = 0 --日期+品牌
    then sum(if(brand_goods_rn = 1 and brand_id is not null,total_price,0))

    when grouping (max_class_id, mid_class_id, min_class_id,min_class_name) = 0 --日期+大类+中类+小类
    then sum(if(minclass_goods_rn = 1 and min_class_id is not null ,total_price,0))

    when grouping (max_class_id, mid_class_id,mid_class_name) = 0 --日期+大类+中类
    then sum(if(midclass_goods_rn = 1 and mid_class_id is not null,total_price,0))

    when grouping (max_class_id,max_class_name) = 0 ----日期+大类
    then sum(if(maxclass_goods_rn = 1 and max_class_id is not null ,total_price,0))

    when grouping (create_date) = 0 --日期
    then sum(if(order_rn=1 and create_date is not null,order_amount,0))
    else null end  as sale_amt,
--七、金额指标统计
   --2、平台收入 plat_amt
    case when grouping(city_id, trade_area_id, store_id,store_name) =0
    then sum(if( order_rn = 1 and store_id is not null ,plat_fee,0))
    when grouping (city_id, trade_area_id ,trade_area_name) = 0
    then sum(if( order_rn = 1 and trade_area_id is not null ,plat_fee,0))
    when grouping (city_id,city_name) = 0
    then sum(if( order_rn = 1 and city_id is not null,plat_fee,0))
    when grouping (brand_id,brand_name) = 0
    then null
    when grouping (max_class_id, mid_class_id, min_class_id,min_class_name) = 0
    then null
    when grouping (max_class_id, mid_class_id,mid_class_name) = 0
    then null
    when grouping (max_class_id,max_class_name) = 0
    then null
    when grouping (create_date) = 0
    then sum(if(order_rn=1 and create_date is not null,plat_fee,0))
    else null end  as plat_amt ,

    -- 3、配送成交额 deliver_sale_amt
    case when grouping(store_id,store_name) =0
    then sum(if( order_rn = 1 and store_id is not null and delievery_id is not null ,dispatcher_money,0))
    when grouping (trade_area_id ,trade_area_name) = 0
    then sum(if( order_rn = 1 and trade_area_id is not null and delievery_id is not null,dispatcher_money,0))
    when grouping (city_id,city_name) = 0
    then sum(if( order_rn = 1 and city_id is not null and delievery_id is not null,dispatcher_money,0))
    when grouping (brand_id,brand_name) = 0
    then null
    when grouping (min_class_id,min_class_name) = 0
    then null
    when grouping (mid_class_id,mid_class_name) = 0
    then null
    when grouping (max_class_id,max_class_name) = 0
    then null
    when grouping (create_date) = 0
    then sum(if(order_rn=1 and create_date is not null and delievery_id is not null ,dispatcher_money,0))
    else null end  as deliver_sale_amt ,

    -- 4、小程序成交额 mini_app_sale_amt
    case when grouping(store_id,store_name) =0
    then sum(if( order_rn = 1 and store_id is not null and order_from='miniapp' ,order_amount,0))
    when grouping (trade_area_id ,trade_area_name) = 0
    then sum(if( order_rn = 1 and trade_area_id is not null and order_from='miniapp',order_amount,0))
    when grouping (city_id,city_name) = 0
    then sum(if( order_rn = 1 and city_id is not null and order_from='miniapp',order_amount,0))
    when grouping (brand_id,brand_name) = 0
    then sum(if(brand_goods_rn = 1 and brand_id is not null and order_from='miniapp',total_price,0))
    when grouping (min_class_id,min_class_name) = 0
    then sum(if(minclass_goods_rn = 1 and min_class_id is not null and order_from='miniapp',total_price,0))
    when grouping (mid_class_id,mid_class_name) = 0
    then sum(if(midclass_goods_rn = 1 and mid_class_id is not null and order_from='miniapp',total_price,0))
    when grouping (max_class_id,max_class_name) = 0
    then sum(if(maxclass_goods_rn = 1 and max_class_id is not null and order_from='miniapp',total_price,0))
    when grouping (create_date) = 0
    then sum(if(order_rn=1 and create_date is not null and order_from='miniapp',order_amount ,0))
    else null end  as mini_app_sale_amt ,

    -- 5、安卓成交额 android_sale_amt
    case when grouping(store_id,store_name) =0
    then sum(if( order_rn = 1 and store_id is not null and order_from='android' ,order_amount,0))
    when grouping (trade_area_id ,trade_area_name) = 0
    then sum(if( order_rn = 1 and trade_area_id is not null and order_from='android',order_amount,0))
    when grouping (city_id,city_name) = 0
    then sum(if( order_rn = 1 and city_id is not null and order_from='android',order_amount,0))
    when grouping (brand_id,brand_name) = 0
    then sum(if(brand_goods_rn = 1 and brand_id is not null and order_from='android',total_price,0))
    when grouping (min_class_id,min_class_name) = 0
    then sum(if(minclass_goods_rn = 1 and min_class_id is not null and order_from='android',total_price,0))
    when grouping (mid_class_id,mid_class_name) = 0
    then sum(if(midclass_goods_rn = 1 and mid_class_id is not null and order_from='android',total_price,0))
    when grouping (max_class_id,max_class_name) = 0
    then sum(if(maxclass_goods_rn = 1 and max_class_id is not null and order_from='android',total_price,0))
    when grouping (create_date) = 0
    then sum(if(order_rn=1 and create_date is not null and order_from='android',order_amount ,0))
    else null end  as android_sale_amt ,

    -- 6、苹果成交额 ios_sale_amt
    case when grouping(store_id,store_name) =0
    then sum(if( order_rn = 1 and store_id is not null and order_from='ios' ,order_amount,0))
    when grouping (trade_area_id ,trade_area_name) = 0
    then sum(if( order_rn = 1 and trade_area_id is not null and order_from='ios',order_amount,0))
    when grouping (city_id,city_name) = 0
    then sum(if( order_rn = 1 and city_id is not null and order_from='ios',order_amount,0))
    when grouping (brand_id,brand_name) = 0
    then sum(if(brand_goods_rn = 1 and brand_id is not null and order_from='ios',total_price,0))
    when grouping (min_class_id,min_class_name) = 0
    then sum(if(minclass_goods_rn = 1 and min_class_id is not null and order_from='ios',total_price,0))
    when grouping (mid_class_id,mid_class_name) = 0
    then sum(if(midclass_goods_rn = 1 and mid_class_id is not null and order_from='ios',total_price,0))
    when grouping (max_class_id,max_class_name) = 0
    then sum(if(maxclass_goods_rn = 1 and max_class_id is not null and order_from='ios',total_price,0))
    when grouping (create_date) = 0
    then sum(if(order_rn=1 and create_date is not null and order_from='ios',order_amount ,0))
    else null end  as ios_sale_amt ,

    -- 7、pc成交额 pcweb_sale_amt
    case when grouping(store_id,store_name) =0
    then sum(if( order_rn = 1 and store_id is not null and order_from='pcweb' ,order_amount,0))
    when grouping (trade_area_id ,trade_area_name) = 0
    then sum(if( order_rn = 1 and trade_area_id is not null and order_from='pcweb',order_amount,0))
    when grouping (city_id,city_name) = 0
    then sum(if( order_rn = 1 and city_id is not null and order_from='pcweb',order_amount,0))
    when grouping (brand_id,brand_name) = 0
    then sum(if(brand_goods_rn = 1 and brand_id is not null and order_from='pcweb',total_price,0))
    when grouping (min_class_id,min_class_name) = 0
    then sum(if(minclass_goods_rn = 1 and min_class_id is not null and order_from='pcweb',total_price,0))
    when grouping (mid_class_id,mid_class_name) = 0
    then sum(if(midclass_goods_rn = 1 and mid_class_id is not null and order_from='pcweb',total_price,0))
    when grouping (max_class_id,max_class_name) = 0
    then sum(if(maxclass_goods_rn = 1 and max_class_id is not null and order_from='pcweb',total_price,0))
    when grouping (create_date) = 0
    then sum(if(order_rn=1 and create_date is not null and order_from='pcweb',order_amount ,0))
    else null end  as pcweb_sale_amt ,

--八、订单量指标统计
       -- 8、订单量 order_cnt
    case when grouping(store_id,store_name) =0
    then count(if(order_rn=1 and store_id is not null , order_id,null))
    when grouping (trade_area_id ,trade_area_name) = 0
    then count(if(order_rn=1 and trade_area_id is not null , order_id,null))
    when grouping (city_id,city_name) = 0
    then count(if(order_rn=1 and city_id is not null , order_id,null))
    when grouping (brand_id,brand_name) = 0
    then count(if(brand_rn=1 and brand_id is not null , order_id,null))
    when grouping (min_class_id,min_class_name) = 0
    then count(if(minclass_rn=1 and min_class_id is not null , order_id,null))
    when grouping (mid_class_id,mid_class_name) = 0
    then count(if(midclass_rn=1 and mid_class_id is not null , order_id,null))
    when grouping (max_class_id,max_class_name) = 0
    then count(if(maxclass_rn=1 and max_class_id is not null , order_id,null))
    when grouping (create_date) = 0
    then count(if(order_rn=1 , order_id,null))
    else null end  as order_cnt ,

    --9、 参评单量 eva_order_cnt
    case when grouping(store_id,store_name) =0
    then count(if(order_rn=1 and store_id is not null and evaluation_id is not null , order_id,null))
    when grouping (trade_area_id ,trade_area_name) = 0
    then count(if(order_rn=1 and trade_area_id is not null and evaluation_id is not null , order_id,null))
    when grouping (city_id,city_name) = 0
    then count(if(order_rn=1 and city_id is not null and evaluation_id is not null , order_id,null))
    when grouping (brand_id,brand_name) = 0
    then count(if(brand_rn=1 and brand_id is not null and evaluation_id is not null , order_id,null))
    when grouping (min_class_id,min_class_name) = 0
    then count(if(minclass_rn=1 and min_class_id is not null and evaluation_id is not null , order_id,null))
    when grouping (mid_class_id,mid_class_name) = 0
    then count(if(midclass_rn=1 and mid_class_id is not null  and evaluation_id is not null, order_id,null))
    when grouping (max_class_id,max_class_name) = 0
    then count(if(maxclass_rn=1 and max_class_id is not null  and evaluation_id is not null, order_id,null))
    when grouping (create_date) = 0
    then count(if(order_rn=1 and evaluation_id is not null, order_id,null))
    else null end  as eva_order_cnt ,
    --10、差评单量 bad_eva_order_cnt
    case when grouping(store_id,store_name) =0
    then count(if(order_rn=1 and store_id is not null and evaluation_id is not null and coalesce(geval_scores,0) <6 , order_id,null))
    when grouping (trade_area_id ,trade_area_name) = 0
    then count(if(order_rn=1 and trade_area_id is not null and evaluation_id is not null and coalesce(geval_scores,0) <6, order_id,null))
    when grouping (city_id,city_name) = 0
    then count(if(order_rn=1 and city_id is not null and evaluation_id is not null and coalesce(geval_scores,0) <6, order_id,null))
    when grouping (brand_id,brand_name) = 0
    then count(if(brand_rn=1 and brand_id is not null and evaluation_id is not null and coalesce(geval_scores,0) <6, order_id,null))
    when grouping (min_class_id,min_class_name) = 0
    then count(if(minclass_rn=1 and min_class_id is not null and evaluation_id is not null and coalesce(geval_scores,0) <6, order_id,null))
    when grouping (mid_class_id,mid_class_name) = 0
    then count(if(midclass_rn=1 and mid_class_id is not null  and evaluation_id is not null and coalesce(geval_scores,0) <6, order_id,null))
    when grouping (max_class_id,max_class_name) = 0
    then count(if(maxclass_rn=1 and max_class_id is not null  and evaluation_id is not null and coalesce(geval_scores,0) <6, order_id,null))
    when grouping (create_date) = 0
    then count(if(order_rn=1 and evaluation_id is not null and coalesce(geval_scores,0) <6, order_id,null))
    else null end  as bad_eva_order_cnt ,

    --11、配送单量 deliver_order_cnt
    case when grouping(store_id,store_name) =0
    then count(if(order_rn=1 and store_id is not null and delievery_id is not null, order_id,null))
    when grouping (trade_area_id ,trade_area_name) = 0
    then count(if(order_rn=1 and trade_area_id is not null and delievery_id is not null, order_id,null))
    when grouping (city_id,city_name) = 0
    then count(if(order_rn=1 and city_id is not null and delievery_id is not null, order_id,null))
    when grouping (brand_id,brand_name) = 0
    then count(if(brand_rn=1 and brand_id is not null and delievery_id is not null, order_id,null))
    when grouping (min_class_id,min_class_name) = 0
    then count(if(minclass_rn=1 and min_class_id is not null and delievery_id is not null, order_id,null))
    when grouping (mid_class_id,mid_class_name) = 0
    then count(if(midclass_rn=1 and mid_class_id is not null  and delievery_id is not null, order_id,null))
    when grouping (max_class_id,max_class_name) = 0
    then count(if(maxclass_rn=1 and max_class_id is not null and delievery_id is not null, order_id,null))
    when grouping (create_date) = 0
    then count(if(order_rn=1 and delievery_id is not null, order_id,null))
    else null end  as deliver_order_cnt ,

    --12、退款单量 refund_order_cnt
    case when grouping(store_id,store_name) =0
    then count(if(order_rn=1 and store_id is not null and refund_id is not null, order_id,null))
    when grouping (trade_area_id ,trade_area_name) = 0
    then count(if(order_rn=1 and trade_area_id is not null and refund_id is not null, order_id,null))
    when grouping (city_id,city_name) = 0
    then count(if(order_rn=1 and city_id is not null and refund_id is not null, order_id,null))
    when grouping (brand_id,brand_name) = 0
    then count(if(brand_rn=1 and brand_id is not null and refund_id is not null, order_id,null))
    when grouping (min_class_id,min_class_name) = 0
    then count(if(minclass_rn=1 and min_class_id is not null and refund_id is not null, order_id,null))
    when grouping (mid_class_id,mid_class_name) = 0
    then count(if(midclass_rn=1 and mid_class_id is not null  and refund_id is not null, order_id,null))
    when grouping (max_class_id,max_class_name) = 0
    then count(if(maxclass_rn=1 and max_class_id is not null and refund_id is not null, order_id,null))
    when grouping (create_date) = 0
    then count(if(order_rn=1 and refund_id is not null, order_id,null))
    else null end  as refund_order_cnt ,

    -- 13、小程序订单量 miniapp_order_cnt
    case when grouping(store_id,store_name) =0
    then count(if(order_rn=1 and store_id is not null and order_from = 'miniapp', order_id,null))
    when grouping (trade_area_id ,trade_area_name) = 0
    then count(if(order_rn=1 and trade_area_id is not null and order_from = 'miniapp', order_id,null))
    when grouping (city_id,city_name) = 0
    then count(if(order_rn=1 and city_id is not null and order_from = 'miniapp', order_id,null))
    when grouping (brand_id,brand_name) = 0
    then count(if(brand_rn=1 and brand_id is not null and order_from = 'miniapp', order_id,null))
    when grouping (min_class_id,min_class_name) = 0
    then count(if(minclass_rn=1 and min_class_id is not null and order_from = 'miniapp', order_id,null))
    when grouping (mid_class_id,mid_class_name) = 0
    then count(if(midclass_rn=1 and mid_class_id is not null  and order_from = 'miniapp', order_id,null))
    when grouping (max_class_id,max_class_name) = 0
    then count(if(maxclass_rn=1 and max_class_id is not null and order_from = 'miniapp', order_id,null))
    when grouping (create_date) = 0
    then count(if(order_rn=1 and order_from = 'miniapp', order_id,null))
    else null end  as miniapp_order_cnt ,

    -- 14、android订单量 android_order_cnt
    case when grouping(store_id,store_name) =0
    then count(if(order_rn=1 and store_id is not null and order_from = 'android', order_id,null))
    when grouping (trade_area_id ,trade_area_name) = 0
    then count(if(order_rn=1 and trade_area_id is not null and order_from = 'android', order_id,null))
    when grouping (city_id,city_name) = 0
    then count(if(order_rn=1 and city_id is not null and order_from = 'android', order_id,null))
    when grouping (brand_id,brand_name) = 0
    then count(if(brand_rn=1 and brand_id is not null and order_from = 'android', order_id,null))
    when grouping (min_class_id,min_class_name) = 0
    then count(if(minclass_rn=1 and min_class_id is not null and order_from = 'android', order_id,null))
    when grouping (mid_class_id,mid_class_name) = 0
    then count(if(midclass_rn=1 and mid_class_id is not null  and order_from = 'android', order_id,null))
    when grouping (max_class_id,max_class_name) = 0
    then count(if(maxclass_rn=1 and max_class_id is not null and order_from = 'android', order_id,null))
    when grouping (create_date) = 0
    then count(if(order_rn=1 and order_from = 'android', order_id,null))
    else null end  as android_order_cnt ,

    -- 15、ios订单量 ios_order_cnt
    case when grouping(store_id,store_name) =0
    then count(if(order_rn=1 and store_id is not null and order_from = 'ios', order_id,null))
    when grouping (trade_area_id ,trade_area_name) = 0
    then count(if(order_rn=1 and trade_area_id is not null and order_from = 'ios', order_id,null))
    when grouping (city_id,city_name) = 0
    then count(if(order_rn=1 and city_id is not null and order_from = 'ios', order_id,null))
    when grouping (brand_id,brand_name) = 0
    then count(if(brand_rn=1 and brand_id is not null and order_from = 'ios', order_id,null))
    when grouping (min_class_id,min_class_name) = 0
    then count(if(minclass_rn=1 and min_class_id is not null and order_from = 'ios', order_id,null))
    when grouping (mid_class_id,mid_class_name) = 0
    then count(if(midclass_rn=1 and mid_class_id is not null  and order_from = 'ios', order_id,null))
    when grouping (max_class_id,max_class_name) = 0
    then count(if(maxclass_rn=1 and max_class_id is not null and order_from = 'ios', order_id,null))
    when grouping (create_date) = 0
    then count(if(order_rn=1 and order_from = 'ios', order_id,null))
    else null end  as ios_order_cnt ,

    --16、pcweb订单量 pcweb_order_cnt
    case when grouping(store_id,store_name) =0
    then count(if(order_rn=1 and store_id is not null and order_from = 'pcweb', order_id,null))
    when grouping (trade_area_id ,trade_area_name) = 0
    then count(if(order_rn=1 and trade_area_id is not null and order_from = 'pcweb', order_id,null))
    when grouping (city_id,city_name) = 0
    then count(if(order_rn=1 and city_id is not null and order_from = 'pcweb', order_id,null))
    when grouping (brand_id,brand_name) = 0
    then count(if(brand_rn=1 and brand_id is not null and order_from = 'pcweb', order_id,null))
    when grouping (min_class_id,min_class_name) = 0
    then count(if(minclass_rn=1 and min_class_id is not null and order_from = 'pcweb', order_id,null))
    when grouping (mid_class_id,mid_class_name) = 0
    then count(if(midclass_rn=1 and mid_class_id is not null  and order_from = 'pcweb', order_id,null))
    when grouping (max_class_id,max_class_name) = 0
    then count(if(maxclass_rn=1 and max_class_id is not null and order_from = 'pcweb', order_id,null))
    when grouping (create_date) = 0
    then count(if(order_rn=1 and order_from = 'pcweb', order_id,null))
    else null end  as pcweb_order_cnt ,

    create_date as dt  --日期

from temp
-- 四、grouping sets分组
group by
  grouping sets(
      create_date, --日期
       (create_date,city_id,city_name),--日期+城市
       (create_date,city_id,city_name,trade_area_id,trade_area_name),--日期+城市+商圈
       (create_date,city_id,city_name,trade_area_id,trade_area_name,store_id,store_name), --日期+城市+商圈+店铺
       (create_date,brand_id,brand_name),--日期+品牌
       (create_date,max_class_id,max_class_name),--日期+大类
       (create_date,max_class_id,max_class_name,mid_class_id,mid_class_name),--日期+大类+中类
       (create_date,max_class_id,max_class_name,mid_class_id,mid_class_name,min_class_id,min_class_name)--日期+大类+中类+小类
   );