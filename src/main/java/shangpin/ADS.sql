set hive.exec.mode.local.auto=True;
use shangpin;

-- 1. 评价内容分析表（ads_review_content）
CREATE TABLE IF NOT EXISTS ads_review_content (
  product_id STRING COMMENT '商品ID',
  sku_id STRING COMMENT 'SKU ID',
  sku_spec STRING COMMENT 'SKU规格（颜色+尺寸）',
  -- 取消ARRAY<STRUCT>嵌套，改为行级字段存储单条评价信息
  review_id STRING COMMENT '评价ID',
  user_nick STRING COMMENT '用户昵称',
  content STRING COMMENT '评价内容',
  score INT COMMENT '评价分数',
  create_time STRING COMMENT '评价时间'
)
PARTITIONED BY (ds STRING COMMENT '分区日期 yyyyMMdd')
STORED AS ORC
LOCATION 'hdfs://cdh01:8020/warehouse/ads/ads_review_content'
TBLPROPERTIES ('orc.compress' = 'SNAPPY');

-- 1. 评价内容分析表（ads_review_content）插入逻辑（非嵌套格式）
INSERT OVERWRITE TABLE ads_review_content PARTITION (ds='20250808')
SELECT
    r.product_id,
    r.sku_id,
    CONCAT(s.color, '-', s.spec_comb) AS sku_spec,
    r.id AS review_id,         -- 直接提取字段，不使用STRUCT嵌套
    r.user_id AS user_nick,    -- 直接提取字段
    r.eval_content AS content, -- 直接提取字段
    r.eval_score AS score,     -- 直接提取字段
    CAST(r.create_time_dwd AS STRING) AS create_time -- 类型转换后直接提取
FROM dwd_fact_user_review_detail r
         LEFT JOIN dwd_dim_sku_info s
                   ON r.sku_id = s.sku_id
                       AND s.ds = '20250808'
WHERE r.ds = '20250808';

select * from ads_review_content;

-- 2. 评价指标趋势表（ads_review_trend）
CREATE TABLE IF NOT EXISTS ads_review_trend (
      product_id STRING COMMENT '商品ID',
      product_name STRING COMMENT '商品名称',
      dt STRING COMMENT '日期',  -- 原STRUCT中的dt字段
      positive_count INT COMMENT '正面评价数（≥4分）',  -- 原STRUCT中的positive_count
      negative_count INT COMMENT '负面评价数（≤2分）',  -- 原STRUCT中的negative_count
      active_count INT COMMENT '主动评价数（有内容的评价）'  -- 原STRUCT中的active_count
)
PARTITIONED BY (ds STRING COMMENT '分区日期 yyyyMMdd')
STORED AS ORC
LOCATION 'hdfs://cdh01:8020/warehouse/ads/ads_review_trend'
TBLPROPERTIES ('orc.compress' = 'SNAPPY');

-- 2. 评价指标趋势表（ads_review_trend）插入逻辑（非嵌套格式）
INSERT OVERWRITE TABLE ads_review_trend PARTITION (ds='20250808')
SELECT
    r.product_id,
    p.product_name,
    d.date_id AS dt,  -- 直接作为字段输出，不再放入STRUCT
    SUM(CASE WHEN r.eval_score >=4 THEN 1 ELSE 0 END) AS positive_count,  -- 直接作为字段
    SUM(CASE WHEN r.eval_score <=2 THEN 1 ELSE 0 END) AS negative_count,  -- 直接作为字段
    SUM(CASE WHEN r.eval_content != '' THEN 1 ELSE 0 END) AS active_count  -- 直接作为字段
FROM dwd_fact_user_review_detail r
         LEFT JOIN dwd_dim_product_info p
                   ON r.product_id = p.product_id
                       AND p.ds = '20250808'
         LEFT JOIN dwd_dim_date_info d
                   ON r.ds = d.ds
                       AND d.date_id BETWEEN '20250802' AND '20250808'  -- 近7天数据
WHERE r.ds BETWEEN '20250802' AND '20250808'  -- 调整过滤条件，确保获取近7天数据
GROUP BY r.product_id, p.product_name, d.date_id;  -- 按日期分组，每日一条记录

select * from ads_review_trend;

-- 3. 商品评分分析表（ads_score_analysis）
CREATE TABLE IF NOT EXISTS ads_score_analysis (
      product_id STRING COMMENT '商品ID',
      product_name STRING COMMENT '商品名称',
      total_score_1 INT COMMENT '整体-1分评价数',
      total_score_2 INT COMMENT '整体-2分评价数',
      total_score_3 INT COMMENT '整体-3分评价数',
      total_score_4 INT COMMENT '整体-4分评价数',
      total_score_5 INT COMMENT '整体-5分评价数',
      old_buyer_score_1 INT COMMENT '老买家-1分评价数',
      old_buyer_score_2 INT COMMENT '老买家-2分评价数',
      old_buyer_score_3 INT COMMENT '老买家-3分评价数',
      old_buyer_score_4 INT COMMENT '老买家-4分评价数',
      old_buyer_score_5 INT COMMENT '老买家-5分评价数'
)
PARTITIONED BY (ds STRING COMMENT '分区日期 yyyyMMdd')
STORED AS ORC
LOCATION 'hdfs://cdh01:8020/warehouse/ads/ads_score_analysis'
TBLPROPERTIES ('orc.compress' = 'SNAPPY');

-- 3. 插入逻辑（优化子查询，提前计算老买家用户）
INSERT OVERWRITE TABLE ads_score_analysis PARTITION (ds='20250808')
SELECT
    r.product_id,
    p.product_name,
    -- 整体人群各分数评价数
    SUM(CASE WHEN r.eval_score = 1 THEN 1 ELSE 0 END) AS total_score_1,
    SUM(CASE WHEN r.eval_score = 2 THEN 1 ELSE 0 END) AS total_score_2,
    SUM(CASE WHEN r.eval_score = 3 THEN 1 ELSE 0 END) AS total_score_3,
    SUM(CASE WHEN r.eval_score = 4 THEN 1 ELSE 0 END) AS total_score_4,
    SUM(CASE WHEN r.eval_score = 5 THEN 1 ELSE 0 END) AS total_score_5,
    -- 老买家人群各分数评价数（用 JOIN 替代 EXISTS）
    SUM(CASE
            WHEN r.eval_score = 1
                THEN 1 ELSE 0
        END) AS old_buyer_score_1,
    SUM(CASE
            WHEN r.eval_score = 2
                THEN 1 ELSE 0
        END) AS old_buyer_score_2,
    SUM(CASE
            WHEN r.eval_score = 3
                THEN 1 ELSE 0
        END) AS old_buyer_score_3,
    SUM(CASE
            WHEN r.eval_score = 4
                THEN 1 ELSE 0
        END) AS old_buyer_score_4,
    SUM(CASE
            WHEN r.eval_score = 5
                THEN 1 ELSE 0
        END) AS old_buyer_score_5
FROM dwd_fact_user_review_detail r
-- 左关联老买家标记（购买次数≥2）
         LEFT JOIN (
    SELECT user_id
    FROM dwd_fact_user_review_detail
    WHERE ds < '20250808'
    GROUP BY user_id
    HAVING COUNT(1) >= 2
) ob ON r.user_id = ob.user_id
-- 左关联历史购买记录（校验是否存在历史购买）
         LEFT JOIN (
    SELECT DISTINCT user_id  -- 去重，只要存在1条历史记录即可
    FROM dwd_fact_user_review_detail
    WHERE ds < '20250808'
) history ON r.user_id = history.user_id
-- 关联商品维度
         LEFT JOIN dwd_dim_product_info p
                   ON r.product_id = p.product_id
                       AND p.ds = '20250808'
WHERE r.ds = '20250808'  -- 仅统计当天评价
GROUP BY r.product_id, p.product_name;

select * from ads_score_analysis;

-- 4. 内容效果分析表（ads_content_effect）
CREATE TABLE IF NOT EXISTS ads_content_effect (
      product_id STRING COMMENT '商品ID',
      product_name STRING COMMENT '商品名称',
      pv INT COMMENT '浏览量',
      uv INT COMMENT '访客数',
      conversion DECIMAL(10,4) COMMENT '转化率',
      rank INT COMMENT '内容效果TOP排名'
)
PARTITIONED BY (ds STRING COMMENT '分区日期 yyyyMMdd')
STORED AS ORC
LOCATION 'hdfs://cdh01:8020/warehouse/ads/ads_content_effect'
TBLPROPERTIES ('orc.compress' = 'SNAPPY');

-- 4. 内容效果分析表（ads_content_effect）
INSERT OVERWRITE TABLE ads_content_effect PARTITION (ds='20250808')
SELECT
    c.product_id,
    p.product_name,
    c.pv,
    c.uv,
    c.conversion_rate AS conversion,
    -- 按商品分组，依据pv降序排，生成TOP排名
    ROW_NUMBER() OVER (PARTITION BY c.product_id ORDER BY c.pv DESC) AS rank
FROM dwd_fact_sales_traffic_detail c  -- 替换为实际存储内容数据的表
-- 关联商品维度信息
         LEFT JOIN dwd_dim_product_info p
                   ON c.product_id = p.product_id
                       AND p.ds = '20250808'
WHERE c.ds = '20250808';

select * from ads_content_effect;

-- 5. 搜索词效果分析表（ads_search_word）
CREATE TABLE IF NOT EXISTS ads_search_word (
      product_id STRING COMMENT '商品ID',
      product_name STRING COMMENT '商品名称',
      pv INT COMMENT '浏览量',
      conversion DECIMAL(10,4) COMMENT '转化率'
)
    PARTITIONED BY (ds STRING COMMENT '分区日期 yyyyMMdd')
    STORED AS ORC
    LOCATION 'hdfs://cdh01:8020/warehouse/ads/ads_search_word'
    TBLPROPERTIES ('orc.compress' = 'SNAPPY');

-- 5. 搜索词效果分析表（ads_search_word）
INSERT OVERWRITE TABLE ads_search_word PARTITION (ds='20250808')
SELECT
    s.product_id,
    p.product_name,
    s.pv,
    s.conversion_rate AS conversion
FROM dwd_fact_sales_traffic_detail s
         LEFT JOIN dwd_dim_product_info p
                   ON s.product_id = p.product_id
                       AND p.ds = '20250808'
WHERE s.ds = '20250808';

select * from ads_search_word;

-- 6. 价格分析表（ads_price_analysis）
CREATE TABLE IF NOT EXISTS ads_price_trend (
      product_id STRING COMMENT '商品ID',
      product_name STRING COMMENT '商品名称',
      sub_category STRING COMMENT '商品子类',
      dt STRING COMMENT '日期',
      price DECIMAL(10,2) COMMENT '当日价格',
      pv INT COMMENT '当日浏览量',
      pay_cnt INT COMMENT '当日成交数'
)
PARTITIONED BY (ds STRING COMMENT '分区日期 yyyyMMdd')
STORED AS ORC
LOCATION 'hdfs://cdh01:8020/warehouse/ads/ads_price_trend'
TBLPROPERTIES ('orc.compress' = 'SNAPPY');

-- 子类竞争排行榜（按子类维度）
CREATE TABLE IF NOT EXISTS ads_sub_category_rank (
      product_id STRING COMMENT '当前商品ID',
      product_name STRING COMMENT '当前商品名称',
      sub_category STRING COMMENT '商品子类',
      competitor_product_id STRING COMMENT '竞争商品ID',
      competitor_price DECIMAL(10,2) COMMENT '竞争商品价格',
      rank INT COMMENT '子类销量排名'
)
PARTITIONED BY (ds STRING COMMENT '分区日期 yyyyMMdd')
STORED AS ORC
LOCATION 'hdfs://cdh01:8020/warehouse/ads/ads_sub_category_rank'
TBLPROPERTIES ('orc.compress' = 'SNAPPY');


-- 2. 插入逻辑（拆分两个表分别处理）
-- 2.1 价格趋势数据插入
INSERT OVERWRITE TABLE ads_price_trend PARTITION (ds='20250808')
SELECT
    p.product_id,
    p.product_name,
    p.sub_category,
    d.date_id AS dt,
    pp.actual_price AS price,
    t.daily_pv AS pv,
    t.pay_cnt AS pay_cnt
FROM dwd_dim_product_info p
         LEFT JOIN dws_product_price_analysis pp
                   ON p.product_id = pp.product_id
                       AND pp.ds = '20250808'
         LEFT JOIN dws_product_core_summary t
                   ON p.product_id = t.product_id
                       AND t.ds = '20250808'
         LEFT JOIN dwd_dim_date_info d
                   ON p.ds = d.ds
                       AND d.date_id BETWEEN '20250802' AND '20250808'  -- 近7天趋势
WHERE p.ds = '20250808';

select * from ads_price_trend;

-- 2.2 子类竞争排名数据插入
INSERT OVERWRITE TABLE ads_sub_category_rank PARTITION (ds='20250808')
SELECT
    p.product_id,
    p.product_name,
    p.sub_category,
    sc.product_id AS competitor_product_id,
    sc.sales_volume AS competitor_sales,
    -- 按子类分区，按销量降序排名
    ROW_NUMBER() OVER (PARTITION BY p.sub_category ORDER BY sc.sales_volume DESC) AS rank
FROM dwd_dim_product_info p
         LEFT JOIN dws_product_core_summary sc
                   ON p.sub_category = sc.sub_category
                       AND sc.ds = '20250808'
WHERE p.ds = '20250808';

select * from ads_sub_category_rank;

-- 7. 流量渠道分析表（ads_traffic_channel）
CREATE TABLE IF NOT EXISTS ads_traffic_channel (
      product_id STRING COMMENT '商品ID',
      product_name STRING COMMENT '商品名称',
      channel STRING COMMENT '渠道（手淘/直通车/短视频）',
      uv INT COMMENT '访客数',
      uv_ratio DECIMAL(10,4) COMMENT '访客占比',
      conversion DECIMAL(10,4) COMMENT '转化率'
)
PARTITIONED BY (ds STRING COMMENT '分区日期 yyyyMMdd')
STORED AS ORC
LOCATION 'hdfs://cdh01:8020/warehouse/ads/ads_traffic_channel'
TBLPROPERTIES ('orc.compress' = 'SNAPPY');

-- 调整后的插入语句（直接获取字段）
INSERT OVERWRITE TABLE ads_traffic_channel PARTITION (ds='20250808')
SELECT
    t.product_id,
    p.product_name,
    t.channel,
    t.uv,
    t.uv / total.total_uv AS uv_ratio,  -- 访客占比=渠道UV/总UV
    t.conversion_rate AS conversion
FROM dwd_fact_sales_traffic_detail t
         LEFT JOIN dwd_dim_product_info p
                   ON t.product_id = p.product_id AND p.ds = '20250808'
         LEFT JOIN (
    SELECT product_id, SUM(uv) AS total_uv
    FROM dwd_fact_sales_traffic_detail
    WHERE ds = '20250808'
    GROUP BY product_id
) total
                   ON t.product_id = total.product_id
WHERE t.ds = '20250808';

select * from ads_traffic_channel;

-- 8. SKU 销售分析表（ads_sku_sales）
CREATE TABLE IF NOT EXISTS ads_sku_sales (
      product_id STRING COMMENT '商品ID',
      product_name STRING COMMENT '商品名称',
      sku_id STRING COMMENT 'SKU ID',
      color STRING COMMENT '颜色',
      spec STRING COMMENT '规格',
      sales INT COMMENT '销售量',
      hot_degree DECIMAL(10,4) COMMENT '热销程度',
      stock INT COMMENT '库存数量',
      stock_warning INT COMMENT '库存预警（1-预警，0-正常）'
)
PARTITIONED BY (ds STRING COMMENT '分区日期 yyyyMMdd')
STORED AS ORC
LOCATION 'hdfs://cdh01:8020/warehouse/ads/ads_sku_sales'
TBLPROPERTIES ('orc.compress' = 'SNAPPY');

-- 调整后的插入语句（直接获取字段）
INSERT OVERWRITE TABLE ads_sku_sales PARTITION (ds='20250808')
SELECT
    s.product_id,
    p.product_name,
    s.sku_id,
    s.color,
    s.spec_comb AS spec,
    s.sales_count AS sales,
    s.hot_degree,
    s.stock,
    s.stock_warning_flag AS stock_warning
FROM dws_sku_sales_detail s
         LEFT JOIN dwd_dim_product_info p
                   ON s.product_id = p.product_id AND p.ds = '20250808'
WHERE s.ds = '20250808';

select * from ads_sku_sales;