set hive.exec.mode.local.auto=True;
create database shangpin;
use shangpin;

-- 日期维度表：存储日期相关的维度信息，如年、月、日、星期、是否节假日等
CREATE TABLE IF NOT EXISTS ods_dim_date (
      year STRING COMMENT '年份',
      month STRING COMMENT '月份',
      day STRING COMMENT '日期',
      weekday STRING COMMENT '星期几',
      week STRING COMMENT '周次',
      is_holiday STRING COMMENT '是否为节假日',
      is_weekend STRING COMMENT '是否为周末',
      season STRING COMMENT '季节'
)
PARTITIONED BY (ds STRING COMMENT '分区日期，格式通常为yyyyMMdd')
LOCATION 'hdfs://cdh01:8020//warehouse/shangpin/ods/ods_dim_date'
TBLPROPERTIES (
'orc.compress' = 'SNAPPY',
'external.table.purge' = 'true'
);

-- 商品基础维度表：存储商品的基本信息
CREATE TABLE IF NOT EXISTS ods_dim_product_base (
      product_id STRING COMMENT '商品ID',
      product_name STRING COMMENT '商品名称',
      category STRING COMMENT '商品大类',
      sub_category STRING COMMENT '商品子类',
      brand STRING COMMENT '商品品牌',
      create_time STRING COMMENT '商品创建时间',
      status STRING COMMENT '商品状态',
      origin_price STRING COMMENT '商品原价',
      sales_volume STRING COMMENT '商品销量',
      online_days STRING COMMENT '商品在线天数'
)
PARTITIONED BY (ds STRING COMMENT '分区日期，格式通常为yyyyMMdd')
LOCATION 'hdfs://cdh01:8020//warehouse/shangpin/ods/ods_dim_product_base'
TBLPROPERTIES (
'orc.compress' = 'SNAPPY',
'external.table.purge' = 'true'
);

-- SKU维度表：存储商品库存单位(SKU)的详细信息
CREATE TABLE IF NOT EXISTS ods_dim_sku (
      sku_id STRING COMMENT 'SKU ID',
      product_id STRING COMMENT '所属商品ID',
      color STRING COMMENT '颜色',
      spec1 STRING COMMENT '规格1',
      spec2 STRING COMMENT '规格2',
      sku_price STRING COMMENT 'SKU单价',
      stock STRING COMMENT '库存数量',
      sku_status STRING COMMENT 'SKU状态'
)
PARTITIONED BY (ds STRING COMMENT '分区日期，格式通常为yyyyMMdd')
LOCATION 'hdfs://cdh01:8020//warehouse/shangpin/ods/ods_dim_sku'
TBLPROPERTIES (
'orc.compress' = 'SNAPPY',
'external.table.purge' = 'true'
);

-- 价格促销事实表：记录商品的价格变动和促销信息
CREATE TABLE IF NOT EXISTS ods_fact_price_promo (
      id STRING COMMENT '记录ID',
      product_id STRING COMMENT '商品ID',
      actual_price STRING COMMENT '实际售价',
      original_price STRING COMMENT '原价',
      price_band STRING COMMENT '价格带',
      is_promo STRING COMMENT '是否促销',
      promo_type STRING COMMENT '促销类型',
      discount STRING COMMENT '折扣力度',
      promo_start_date STRING COMMENT '促销开始日期',
      promo_end_date STRING COMMENT '促销结束日期'
)
PARTITIONED BY (ds STRING COMMENT '分区日期，格式通常为yyyyMMdd')
LOCATION 'hdfs://cdh01:8020//warehouse/shangpin/ods/ods_fact_price_promo'
TBLPROPERTIES (
'orc.compress' = 'SNAPPY',
'external.table.purge' = 'true'
);

-- 销售流量事实表：记录商品的销售和流量相关指标
CREATE TABLE IF NOT EXISTS ods_fact_sales_traffic (
      id STRING COMMENT '记录ID',
      product_id STRING COMMENT '商品ID',
      sku_id STRING COMMENT 'SKU ID',
      channel STRING COMMENT '销售渠道',
      pv STRING COMMENT '页面浏览量',
      uv STRING COMMENT '独立访客数',
      click_cnt STRING COMMENT '点击次数',
      add_cart_cnt STRING COMMENT '加购次数',
      collect_cnt STRING COMMENT '收藏次数',
      pay_cnt STRING COMMENT '支付次数',
      refund_cnt STRING COMMENT '退款次数',
      conversion_rate STRING COMMENT '转化率'
)
PARTITIONED BY (ds STRING COMMENT '分区日期，格式通常为yyyyMMdd')
LOCATION 'hdfs://cdh01:8020//warehouse/shangpin/ods/ods_fact_sales_traffic'
TBLPROPERTIES (
'orc.compress' = 'SNAPPY',
'external.table.purge' = 'true'
);

-- 用户评论事实表：记录用户对商品的评论和行为
CREATE TABLE IF NOT EXISTS ods_fact_user_review (
      id STRING COMMENT '评论ID',
      product_id STRING COMMENT '商品ID',
      sku_id STRING COMMENT 'SKU ID',
      user_id STRING COMMENT '用户ID',
      user_age STRING COMMENT '用户年龄',
      user_gender STRING COMMENT '用户性别',
      user_city STRING COMMENT '用户所在城市',
      user_behavior_type STRING COMMENT '用户行为类型',
      eval_score STRING COMMENT '评价分数',
      eval_content STRING COMMENT '评价内容',
      positive_tags STRING COMMENT '正面标签',
      negative_tags STRING COMMENT '负面标签',
      is_verified_purchase STRING COMMENT '是否为已验证购买'
)
PARTITIONED BY (ds STRING COMMENT '分区日期，格式通常为yyyyMMdd')
LOCATION 'hdfs://cdh01:8020//warehouse/shangpin/ods/ods_fact_user_review'
TBLPROPERTIES (
'orc.compress' = 'SNAPPY',
'external.table.purge' = 'true'
);
