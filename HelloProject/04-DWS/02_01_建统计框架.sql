-- insert into hive.yp_dws.dws_sale_daycount
with temp as (select
--一、从DWB层表中抽取字段----
   --维度抽取
  o.dt as create_date,  --日期
  s.city_id,

-- 二、分组去重
   row_number() over(partition by order_id) as order_rn

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

--五、分组类型（8种统计维度组合）
    when grouping (create_date) = 0
    then 'all'
    else 'other' end as group_type,
--六、指标计算 注意每个指标都对应着8个分组维度的计算
    --1、销售收入指标 sale_amt
    case when grouping(store_id,store_name) =0  --如果分组中包含店铺,则分组为：日期+城市+商圈+店铺
    then sum(if( order_rn = 1 and store_id is not null ,order_amount,0)) --只有分组中标号为1的(去重)，店铺不为空的才参与计算
    else null end  as sale_amt,
 --七、金额指标统计
   --2、平台收入 plat_amt
    then sum(if(order_rn=1 and create_date is not null,plat_fee,0))
    else null end  as plat_amt ,

--八、订单量指标统计
    -- 8、订单量 order_cnt
    case when grouping(store_id,store_name) =0
    then count(if(order_rn=1 and store_id is not null , order_id,null))
    else null end  as order_cnt ,

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

