--商品销量TOPN
drop table if exists yp_rpt.rpt_goods_sale_topN;
create table yp_rpt.rpt_goods_sale_topN(
    `dt` string COMMENT '统计日期',
    `sku_id` string COMMENT '商品ID',
    `payment_num` bigint COMMENT '销量'
) COMMENT '商品销量TopN'
ROW format delimited fields terminated BY '\t'
stored AS orc tblproperties ('orc.compress' = 'SNAPPY');

--商品收藏TOPN
drop table if exists yp_rpt.rpt_goods_favor_topN;
create table yp_rpt.rpt_goods_favor_topN(
    `dt` string COMMENT '统计日期',
    `sku_id` string COMMENT '商品ID',
    `favor_count` bigint COMMENT '收藏量'
) COMMENT '商品收藏TopN'
ROW format delimited fields terminated BY '\t' 
stored AS orc tblproperties ('orc.compress' = 'SNAPPY');

--商品加入购物车TOPN
drop table if exists yp_rpt.rpt_goods_cart_topN;
create table yp_rpt.rpt_goods_cart_topN(
    `dt` string COMMENT '统计日期',
    `sku_id` string COMMENT '商品ID',
    `cart_num` bigint COMMENT '加入购物车数量'
) COMMENT '商品加入购物车TopN'
ROW format delimited fields terminated BY '\t' 
stored AS orc tblproperties ('orc.compress' = 'SNAPPY');

--商品退款率TOPN
drop table if exists yp_rpt.rpt_goods_refund_topN;
create table yp_rpt.rpt_goods_refund_topN(
    `dt` string COMMENT '统计日期',
    `sku_id` string COMMENT '商品ID',
    `refund_ratio` decimal(10,2) COMMENT '退款率'
) COMMENT '商品退款率TopN'
ROW format delimited fields terminated BY '\t' 
stored AS orc tblproperties ('orc.compress' = 'SNAPPY');

drop table if exists yp_rpt.rpt_evaluation_bad_topN;
create table yp_rpt.rpt_evaluation_bad_topN(
    `dt` string COMMENT '统计日期',
    `sku_id` string COMMENT '商品ID',
    `evaluation_bad_ratio` DECIMAL(38,4) COMMENT '总差评率'
) COMMENT '商品差评率TopN'
ROW format delimited fields terminated BY '\t' 
stored AS orc tblproperties ('orc.compress' = 'SNAPPY');
