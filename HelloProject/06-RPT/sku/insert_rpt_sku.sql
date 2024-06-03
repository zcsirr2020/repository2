--商品销量TOPN
insert into yp_rpt.rpt_goods_sale_topN
select
    '2019-05-07' dt,
    sku_id,
    payment_count
from
    yp_dws.dws_sku_daycount
where
    dt='2019-05-07'
order by payment_count desc
limit 10;


--商品收藏TOPN
insert into yp_rpt.rpt_goods_favor_topN
select
    '2019-04-11' dt,
    sku_id,
    favor_count
from
    yp_dws.dws_sku_daycount 
where
    dt='2019-04-11'
order by favor_count desc
limit 10;


--商品加入购物车TOPN
insert into yp_rpt.rpt_goods_cart_topN
select
    '2019-04-20' dt,
    sku_id,
    cart_num
from
    yp_dws.dws_sku_daycount
where
    dt='2019-04-20'
order by cart_num desc
limit 10;


--商品退款率TOPN
insert into yp_rpt.rpt_goods_refund_topN
select
    '2019-04-20',
    sku_id,
    cast(
      cast(refund_last_30d_count as DECIMAL(38,4)) / cast(payment_last_30d_count as DECIMAL(38,4))
      * 100
      as DECIMAL(5,2)
   ) refund_ratio
from yp_dm.dm_sku 
where payment_last_30d_count!=0
order by refund_ratio desc
limit 10;

--商品差评率TopN
insert into dp_rpt.rpt_evaluation_bad_topN
select
    '2019-06-11' dt,
    sku_id,
	cast(evaluation_bad_count as DECIMAL(38,4)) / (evaluation_good_count+evaluation_mid_count+evaluation_bad_count)
		* 100 as evaluation_bad_ratio
from
    yp_dws.dws_sku_daycount 
where
    dt='2019-06-11'
    and (evaluation_good_count+evaluation_mid_count+evaluation_bad_count) != 0
order by evaluation_bad_ratio desc
limit 10;
