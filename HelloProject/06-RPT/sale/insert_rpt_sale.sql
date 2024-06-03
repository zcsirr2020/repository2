--门店月销售单量排行
insert into yp_rpt.rpt_sale_store_cnt_month
select 
   date_time,
   year_code,
   year_month,
   city_id,
   city_name,
   trade_area_id,
   trade_area_name,
   store_id,
   store_name,
   order_cnt,
   miniapp_order_cnt,
   android_order_cnt,
   ios_order_cnt,
   pcweb_order_cnt
from yp_dm.dm_sale 
where time_type ='month' and group_type='store' and store_id is not null 
order by order_cnt desc;


--日销售曲线
insert into yp_rpt.rpt_sale_day
select 
   date_time,
   year_code,
   month_code,
   day_month_num,
   dim_date_id,
   sale_amt,
   order_cnt
from yp_dm.dm_sale 
where time_type ='day' and group_type='all'
--按照日期排序显示曲线
order by dim_date_id;


--渠道销量占比
insert into yp_rpt.rpt_sale_fromtype_ratio
select 
   date_time,
   time_type,
   year_code,
   year_month,
   dim_date_id,
   
   order_cnt,
	miniapp_order_cnt,
	cast(cast(miniapp_order_cnt as DECIMAL(38,4)) / order_cnt * 100 as DECIMAL(5,2)) as miniapp_order_ratio,
	android_order_cnt,
	cast(cast(android_order_cnt as DECIMAL(38,4)) / order_cnt * 100 as DECIMAL(5,2)) as android_order_ratio,
	ios_order_cnt,
	cast(cast(ios_order_cnt as DECIMAL(38,4)) / order_cnt * 100 as DECIMAL(5,2)) as ios_order_ratio,
	pcweb_order_cnt,
	cast(cast(pcweb_order_cnt as DECIMAL(38,4)) / order_cnt * 100 as DECIMAL(5,2)) as pcweb_order_ratio
from yp_dm.dm_sale
where order_cnt> 0 and group_type = 'all';


