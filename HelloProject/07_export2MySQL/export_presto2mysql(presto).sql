--促销
insert into mysql.yp_olap.rpt_sale_store_cnt_month
select * from hive.yp_rpt.rpt_sale_store_cnt_month;

insert into mysql.yp_olap.rpt_sale_day
select * from hive.yp_rpt.rpt_sale_day;

insert into mysql.yp_olap.rpt_sale_fromtype_ratio
select * from hive.yp_rpt.rpt_sale_fromtype_ratio;

--商品
insert into mysql.yp_olap.rpt_goods_sale_topN
select * from hive.yp_rpt.rpt_goods_sale_topN;

insert into mysql.yp_olap.rpt_goods_favor_topN
select * from hive.yp_rpt.rpt_goods_favor_topN;

insert into mysql.yp_olap.rpt_goods_cart_topN
select * from hive.yp_rpt.rpt_goods_cart_topN;

insert into mysql.yp_olap.rpt_goods_refund_topN
select * from hive.yp_rpt.rpt_goods_refund_topN;

--用户
insert into mysql.yp_olap.rpt_user_count
select * from hive.yp_rpt.rpt_user_count;