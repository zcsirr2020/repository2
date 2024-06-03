-- drop  table if exists mysql.yp_olap.rpt_goods_sale_topN;
create table mysql.yp_olap.rpt_goods_sale_topN(
    dt varchar COMMENT '统计日期',
    sku_id varchar COMMENT '商品ID',
    payment_num bigint COMMENT '销量'
) COMMENT '商品销量TopN';


-- drop  table if exists mysql.yp_olap.rpt_goods_favor_topN;
create table mysql.yp_olap.rpt_goods_favor_topN(
    dt varchar COMMENT '统计日期',
    sku_id varchar COMMENT '商品ID',
    favor_count bigint COMMENT '收藏量'
) COMMENT '商品收藏TopN';


-- drop  table if exists mysql.yp_olap.rpt_goods_cart_topN;
create table mysql.yp_olap.rpt_goods_cart_topN(
    dt varchar COMMENT '统计日期',
    sku_id varchar COMMENT '商品ID',
    cart_num bigint COMMENT '加入购物车数量'
) COMMENT '商品加入购物车TopN';


-- drop  table if exists mysql.yp_olap.rpt_goods_refund_topN;
create table mysql.yp_olap.rpt_goods_refund_topN(
    dt varchar COMMENT '统计日期',
    sku_id varchar COMMENT '商品ID',
    refund_ratio decimal(10,2) COMMENT '退款率'
) COMMENT '商品退款率TopN';