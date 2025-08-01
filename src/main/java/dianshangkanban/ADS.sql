set hive.exec.mode.local.auto=True;
use ecommerce;

-- 1. 页面分析汇总表（ads_page_analysis）
CREATE TABLE ads_page_analysis (
      page_type STRING COMMENT '页面类型（店铺首页、商品详情页等）',
      platform STRING COMMENT '访问平台（移动APP、PC等）',
      referrer STRING COMMENT '流量来源（搜索引擎、朋友分享等）',
      stat_date STRING COMMENT '统计日期',
      visit_count BIGINT COMMENT '访问次数（非跳出行为数）',
      bounce_count BIGINT COMMENT '跳出次数',
      click_count BIGINT COMMENT '点击次数',
      add_cart_count BIGINT COMMENT '加购次数',
      purchase_count BIGINT COMMENT '购买次数',
      avg_stay_duration DECIMAL(10,2) COMMENT '平均停留时长（秒）',
      visitor_num BIGINT COMMENT '访客数（去重用户数）'
)COMMENT '页面分析汇总表'
PARTITIONED BY (ds STRING)
STORED AS ORC
LOCATION 'hdfs://cdh01:8020//warehouse/ecommerce/ads/ads_page_analysis';

-- 1. 页面分析汇总表数据插入
INSERT OVERWRITE TABLE ads_page_analysis PARTITION (ds = '20250731')
SELECT
    page_type,
    platform,
    referrer,
    '20250731' AS stat_date,  -- 固定统计日期
    COUNT(CASE WHEN behavior_type != '跳出' THEN 1 END) AS visit_count,
    COUNT(CASE WHEN behavior_type = '跳出' THEN 1 END) AS bounce_count,
    COUNT(CASE WHEN behavior_type = '点击' THEN 1 END) AS click_count,
    COUNT(CASE WHEN behavior_type = '加购' THEN 1 END) AS add_cart_count,
    COUNT(CASE WHEN behavior_type = '购买' THEN 1 END) AS purchase_count,
    AVG(cast(stay_duration AS INT)) AS avg_stay_duration,
    COUNT(DISTINCT user_id) AS visitor_num
FROM ods_behaviors
-- 筛选分区时间对应的行为数据
WHERE ds = '20250731'
GROUP BY page_type, platform, referrer;

select * from ads_page_analysis;

-- 2. 商品详情页访问表（ads_product_detail_page）
CREATE TABLE ads_product_detail_page (
      shop_id STRING COMMENT '店铺ID',
      product_id STRING COMMENT '商品ID',
      stat_date STRING COMMENT '统计日期',
      visit_count BIGINT COMMENT '访问次数',
      visitor_num BIGINT COMMENT '访客数',
      avg_stay_duration DECIMAL(10,2) COMMENT '平均停留时长',
      purchase_count BIGINT COMMENT '购买次数',
      purchase_amount DECIMAL(10,2) COMMENT '购买金额（含数量计算）'
)COMMENT '商品详情页访问表'
PARTITIONED BY (ds STRING)
STORED AS ORC
LOCATION 'hdfs://cdh01:8020//warehouse/ecommerce/ads/ads_product_detail_page';

-- 2. 商品详情页访问表数据插入
INSERT OVERWRITE TABLE ads_product_detail_page PARTITION (ds = '20250731')
SELECT
    b.shop_id,
    b.product_id,
    '20250731' AS stat_date,  -- 固定统计日期
    COUNT(CASE WHEN b.page_type = '商品详情页' AND b.behavior_type != '跳出' THEN 1 END) AS visit_count,
    COUNT(DISTINCT CASE WHEN b.page_type = '商品详情页' AND b.behavior_type != '跳出' THEN b.user_id END) AS visitor_num,
    AVG(CASE WHEN b.page_type = '商品详情页' THEN cast(b.stay_duration AS INT) END) AS avg_stay_duration,
    COUNT(CASE WHEN b.page_type = '商品详情页' AND b.behavior_type = '购买' THEN 1 END) AS purchase_count,
    SUM(CASE
            WHEN b.page_type = '商品详情页' AND b.behavior_type = '购买'
                THEN cast(p.price AS DECIMAL(10,2)) * cast(split(split(b.detail, '购买数量:')[1], ' ')[0] AS INT)
            ELSE 0
        END) AS purchase_amount
FROM ods_behaviors b
         LEFT JOIN ods_products p ON b.product_id = p.product_id
WHERE b.page_type = '商品详情页'
  -- 筛选分区时间对应的行为数据
  AND b.ds = '20250731'
GROUP BY b.shop_id, b.product_id;

select * from ads_product_detail_page;

-- 3. 页面点击分布表（ads_page_click_distribution）
CREATE TABLE ads_page_click_distribution (
      page_type STRING COMMENT '页面类型',
      detail STRING COMMENT '板块名称（如“首页推荐”）',
      stat_date STRING COMMENT '统计日期',
      click_count BIGINT COMMENT '板块点击次数',
      click_user_num BIGINT COMMENT '板块点击人数',
      purchase_amount DECIMAL(10,2) COMMENT '板块引导支付金额'
)COMMENT '页面点击分布表'
PARTITIONED BY (ds STRING)
STORED AS ORC
LOCATION 'hdfs://cdh01:8020//warehouse/ecommerce/ads/ads_page_click_distribution';

-- 3. 页面点击分布表数据插入
INSERT OVERWRITE TABLE ads_page_click_distribution PARTITION (ds = '20250731')
SELECT
    b.page_type,
    b.detail,
    '20250731' AS stat_date,  -- 固定统计日期
    COUNT(CASE WHEN b.behavior_type = '点击' THEN 1 END) AS click_count,
    COUNT(DISTINCT CASE WHEN b.behavior_type = '点击' THEN b.user_id END) AS click_user_num,
    SUM(CASE
            WHEN b.behavior_type = '购买'
                THEN cast(p.price AS DECIMAL(10,2)) * cast(split(split(b.detail, '购买数量:')[1], ' ')[0] AS INT)
            ELSE 0
        END) AS purchase_amount
FROM ods_behaviors b
         LEFT JOIN ods_products p ON b.product_id = p.product_id
WHERE b.behavior_type IN ('点击', '购买')
  -- 筛选分区时间对应的行为数据
  AND b.ds = '20250731'
GROUP BY b.page_type, b.detail;

select * from ads_page_click_distribution;

-- 4. 页面数据趋势表（ads_page_trend）
CREATE TABLE ads_page_trend (
      page_type STRING COMMENT '页面类型',
      stat_date STRING COMMENT '统计日期',
      visitor_num BIGINT COMMENT '访客数',
      click_user_num BIGINT COMMENT '点击人数',
      visit_count BIGINT COMMENT '访问次数',
      bounce_count BIGINT COMMENT '跳出次数',
      avg_stay_duration DECIMAL(10,2) COMMENT '平均停留时长'
)COMMENT '页面数据趋势表'
PARTITIONED BY (ds STRING)
STORED AS ORC
LOCATION 'hdfs://cdh01:8020//warehouse/ecommerce/ads/ads_page_trend';

-- 4. 页面数据趋势表数据插入
INSERT OVERWRITE TABLE ads_page_trend PARTITION (ds = '20250731')
SELECT
    page_type,
    to_date(from_unixtime(unix_timestamp(behavior_time, 'yyyy-MM-dd HH:mm:ss'))) AS stat_date,  -- 每日趋势
    COUNT(DISTINCT user_id) AS visitor_num,
    COUNT(DISTINCT CASE WHEN behavior_type = '点击' THEN user_id END) AS click_user_num,
    COUNT(CASE WHEN behavior_type != '跳出' THEN 1 END) AS visit_count,
    COUNT(CASE WHEN behavior_type = '跳出' THEN 1 END) AS bounce_count,
    AVG(cast(stay_duration AS INT)) AS avg_stay_duration
FROM ods_behaviors
WHERE ds= '20250731'
GROUP BY page_type, to_date(from_unixtime(unix_timestamp(behavior_time, 'yyyy-MM-dd HH:mm:ss')));

select * from ads_page_trend;