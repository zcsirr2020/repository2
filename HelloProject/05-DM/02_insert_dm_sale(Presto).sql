--一、将dws的统计表和日期维度表dim_date进行关联
insert into yp_dm.dm_sale
with t0 as(
    select
        a.*,
        year_code,         --'年code'       2021
        year_month,        --'年月'         2021-05
        month_code,        --'月份编码’      05
        day_month_num,     --'这个月第几天’   30
        dim_date_id,       --'日期’         2021-05-30
        year_week_name_cn  --'年中第几周”    2021-21
    from yp_dws.dws_sale_daycount a left join yp_dwd.dim_date b on a.dt = b.dim_date_id
)

-- 三、确定维度类型
----  1、时间维度类型
select
    '2022-11-24' as date_time,   -- 统计日期
     case when grouping(dim_date_id) = 0  -- 统计同期
         then 'day'
        when grouping(year_week_name_cn) = 0
         then 'week'
        when grouping(year_month) = 0
         then 'month'
        when grouping(year_code) = 0  -- 这个when可以放在else中
         then 'year'
   end  as time_type,
  year_code,
  year_month,
  month_code,
  day_month_num, --几号
  dim_date_id,
  year_week_name_cn,  --第几周

 ----  2、分组类型
  --'all', --group_type string COMMENT '分组类型：store，trade_area，city，brand，min_class，mid_class，max_class，all',
   CASE WHEN grouping(store_id)=0
        THEN 'store'
        WHEN grouping( trade_area_id)=0
        THEN 'trade_area'
        WHEN grouping(city_id)=0
        THEN 'city'
        WHEN grouping(brand_id)=0
        THEN 'brand'
        WHEN grouping(min_class_id)=0
        THEN 'min_class'
        WHEN grouping(mid_class_id)=0
        THEN 'mid_class'
        WHEN grouping(max_class_id)=0
        THEN 'max_class'
        ELSE 'all'
   END as group_type,

  city_id,
  city_name,
  trade_area_id,
  trade_area_name,
  store_id,
  store_name,
  brand_id,
  brand_name,
  max_class_id,
  max_class_name,
  mid_class_id,
  mid_class_name,
  min_class_id,
  min_class_name,

-- 四、在每一个维度组合中进行指标sum
   sum(sale_amt) as sale_amt,
   sum(plat_amt) as plat_amt,
   sum(deliver_sale_amt) as deliver_sale_amt,
   sum(mini_app_sale_amt) as mini_app_sale_amt,
   sum(android_sale_amt) as android_sale_amt,
   sum(ios_sale_amt) as ios_sale_amt,
   sum(pcweb_sale_amt) as pcweb_sale_amt,
   sum(order_cnt) as order_cnt,
   sum(eva_order_cnt) as eva_order_cnt,
   sum(bad_eva_order_cnt) as bad_eva_order_cnt,
   sum(deliver_order_cnt) as deliver_order_cnt,
   sum(refund_order_cnt) as refund_order_cn,
   sum(miniapp_order_cnt) as miniapp_order_cnt,
   sum(android_order_cnt) as android_order_cnt,
   sum(ios_order_cnt) as ios_order_cnt,
   sum(pcweb_order_cnt) as pcweb_order_cnt
from t0
group by
-- 二、使用grouping sets来实现维度组合
grouping sets (
    --  1、天相关的统计
    --  2021        30          05          2021-05-30
    (year_code, month_code, day_month_num, dim_date_id),
    (year_code, month_code, day_month_num, dim_date_id,city_id,city_name),
    (year_code, month_code, day_month_num, dim_date_id,city_id,city_name,trade_area_id,trade_area_name),
    (year_code, month_code, day_month_num, dim_date_id,city_id,city_name,trade_area_id,trade_area_name,store_id,store_name),
    (year_code, month_code, day_month_num, dim_date_id,brand_id,brand_name),
    (year_code, month_code, day_month_num, dim_date_id,max_class_id,max_class_name),
    (year_code, month_code, day_month_num, dim_date_id,max_class_id,max_class_name,mid_class_id,mid_class_name),
    (year_code, month_code, day_month_num, dim_date_id,max_class_id,max_class_name,mid_class_id,mid_class_name,min_class_id,min_class_name),


    --  2、周相关的统计
    --  2021         2021-22  2021年第22周
    (year_code, year_week_name_cn),
    (year_code, year_week_name_cn,city_id,city_name),
    (year_code, year_week_name_cn,city_id,city_name,trade_area_id,trade_area_name),
    (year_code, year_week_name_cn,city_id,city_name,trade_area_id,trade_area_name,store_id,store_name),
    (year_code, year_week_name_cn,brand_id,brand_name),
    (year_code, year_week_name_cn,max_class_id,max_class_name),
    (year_code, year_week_name_cn,max_class_id,max_class_name,mid_class_id,mid_class_name),
    (year_code, year_week_name_cn,max_class_id,max_class_name,mid_class_id,mid_class_name,min_class_id,min_class_name),

         --  3、月相关的统计
    --  2021         2021-22
    (year_code, year_month,month_code),
    (year_code, year_month,month_code,city_id,city_name),
    (year_code, year_month,month_code,city_id,city_name,trade_area_id,trade_area_name),
    (year_code, year_month,month_code,city_id,city_name,trade_area_id,trade_area_name,store_id,store_name),
    (year_code, year_month,month_code,brand_id,brand_name),
    (year_code, year_month,month_code,max_class_id,max_class_name),
    (year_code, year_month,month_code,max_class_id,max_class_name,mid_class_id,mid_class_name),
    (year_code, year_month,month_code,max_class_id,max_class_name,mid_class_id,mid_class_name,min_class_id,min_class_name),

              --  4、年相关的统计
    --  2021         2021-22
    (year_code),
    (year_code,city_id,city_name),
    (year_code,city_id,city_name,trade_area_id,trade_area_name),
    (year_code,city_id,city_name,trade_area_id,trade_area_name,store_id,store_name),
    (year_code,brand_id,brand_name),
    (year_code,max_class_id,max_class_name),
    (year_code,max_class_id,max_class_name,mid_class_id,mid_class_name),
    (year_code,max_class_id,max_class_name,mid_class_id,mid_class_name,min_class_id,min_class_name)
)