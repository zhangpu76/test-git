set hive.exec.mode.local.auto=True;
use shangpin;

-- 一、DWS 层核心宽表设计（按主题划分）
-- 1. 商品核心概况宽表（dws_product_core_summary）
-- 用途：支持单品核心数据趋势查询（浏览、点击、转化等）🔶1-14🔶
-- 建表语句
CREATE TABLE IF NOT EXISTS dws_product_core_summary (
      product_id STRING COMMENT '商品ID',
      product_name STRING COMMENT '商品名称',
      category STRING COMMENT '商品大类',
      sub_category STRING COMMENT '商品子类',
      brand STRING COMMENT '商品品牌',
      status STRING COMMENT '商品状态',
      total_pv INT COMMENT '累计浏览量',
      total_uv INT COMMENT '累计独立访客数',
      daily_pv INT COMMENT '当日浏览量',
      daily_uv INT COMMENT '当日独立访客数',
      click_cnt INT COMMENT '点击次数',
      click_rate DECIMAL(10,4) COMMENT '点击率',
      add_cart_cnt INT COMMENT '加购次数',
      pay_cnt INT COMMENT '支付次数',
      conversion_rate DECIMAL(10,4) COMMENT '转化率',
      sales_volume INT COMMENT '累计销量',
      create_time_dws TIMESTAMP COMMENT 'DWS层创建时间'
)
PARTITIONED BY (ds STRING)
STORED AS ORC
LOCATION 'hdfs://cdh01:8020/warehouse/dws/dws_product_core_summary'
TBLPROPERTIES (
    'orc.compress' = 'SNAPPY',
    'comment' = '商品核心概况宽表，支撑核心数据趋势分析'
    );

-- 插入数据（每日增量）
INSERT OVERWRITE TABLE dws_product_core_summary PARTITION (ds='20250808')
SELECT
    p.product_id,
    p.product_name,
    p.category,
    p.sub_category,
    p.brand,
    p.status,
    -- 累计指标（关联历史数据）
    COALESCE(hist.total_pv, 0) + COALESCE(today_pv.pv, 0) AS total_pv,
    COALESCE(hist.total_uv, 0) + COALESCE(today_uv.uv, 0) AS total_uv,
    -- 当日指标
    COALESCE(today_pv.pv, 0) AS daily_pv,
    COALESCE(today_uv.uv, 0) AS daily_uv,
    COALESCE(traffic.click_cnt, 0) AS click_cnt,
    COALESCE(traffic.click_rate, 0) AS click_rate,
    COALESCE(traffic.add_cart_cnt, 0) AS add_cart_cnt,
    COALESCE(traffic.pay_cnt, 0) AS pay_cnt,
    COALESCE(traffic.conversion_rate, 0) AS conversion_rate,
    p.sales_volume,
    current_timestamp() AS create_time_dws
FROM
    dwd_dim_product_info p
        LEFT JOIN (
        SELECT product_id, SUM(pv) AS pv FROM dwd_fact_sales_traffic_detail WHERE ds='20250808' GROUP BY product_id
    ) today_pv ON p.product_id = today_pv.product_id
        LEFT JOIN (
        SELECT product_id, SUM(uv) AS uv FROM dwd_fact_sales_traffic_detail WHERE ds='20250808' GROUP BY product_id
    ) today_uv ON p.product_id = today_uv.product_id
        LEFT JOIN (
        SELECT
            product_id,
            SUM(click_cnt) AS click_cnt,
            AVG(click_rate) AS click_rate,
            SUM(add_cart_cnt) AS add_cart_cnt,
            SUM(pay_cnt) AS pay_cnt,
            AVG(conversion_rate) AS conversion_rate
        FROM dwd_fact_sales_traffic_detail
        WHERE ds='20250808'
        GROUP BY product_id
    ) traffic ON p.product_id = traffic.product_id
        LEFT JOIN dws_product_core_summary hist
                  ON p.product_id = hist.product_id AND hist.ds = date_sub('20250808', 1)
WHERE p.ds = '20250808';

select * from dws_product_core_summary;

-- 2. SKU 销售详情宽表（dws_sku_sales_detail）
-- 用途：支持 SKU 热销程度分析及补货决策🔶1-21🔶
-- 建表语句
CREATE TABLE IF NOT EXISTS dws_sku_sales_detail (
      sku_id STRING COMMENT 'SKU ID',
      product_id STRING COMMENT '商品ID',
      product_name STRING COMMENT '商品名称',
      color STRING COMMENT '颜色',
      spec_comb STRING COMMENT '规格组合',
      sku_price DECIMAL(10,2) COMMENT 'SKU单价',
      stock INT COMMENT '当前库存',
      stock_warning_flag INT COMMENT '库存预警标识',
      sales_count INT COMMENT '当日销量',
      total_sales INT COMMENT '累计销量',
      hot_degree DECIMAL(10,4) COMMENT '热销指数（销量占比）',
      create_time_dws TIMESTAMP COMMENT 'DWS层创建时间'
)
PARTITIONED BY (ds STRING)
STORED AS ORC
LOCATION 'hdfs://cdh01:8020/warehouse/dws/dws_sku_sales_detail'
TBLPROPERTIES (
    'orc.compress' = 'SNAPPY',
    'comment' = 'SKU销售详情宽表，支撑SKU热销分析'
    );

-- 插入数据（每日增量）
INSERT OVERWRITE TABLE dws_sku_sales_detail PARTITION (ds='20250808')
SELECT
    s.sku_id,
    s.product_id,
    p.product_name,
    s.color,
    s.spec_comb,
    s.sku_price,
    s.stock,
    s.stock_warning_flag,
    COALESCE(traffic.pay_cnt, 0) AS sales_count,
    -- 累计销量=历史累计+当日销量
    COALESCE(hist.total_sales, 0) + COALESCE(traffic.pay_cnt, 0) AS total_sales,
    -- 热销指数=当日销量/商品总销量
    CASE WHEN COALESCE(prod_total.sales, 0) = 0 THEN 0
         ELSE COALESCE(traffic.pay_cnt, 0) / prod_total.sales
        END AS hot_degree,
    current_timestamp() AS create_time_dws
FROM
    dwd_dim_sku_info s
        LEFT JOIN dwd_dim_product_info p
                  ON s.product_id = p.product_id AND p.ds = '20250808'
        LEFT JOIN (
        SELECT sku_id, SUM(pay_cnt) AS pay_cnt
        FROM dwd_fact_sales_traffic_detail
        WHERE ds='20250808'
        GROUP BY sku_id
    ) traffic ON s.sku_id = traffic.sku_id
        LEFT JOIN (
        SELECT product_id, SUM(pay_cnt) AS sales
        FROM dwd_fact_sales_traffic_detail
        WHERE ds='20250808'
        GROUP BY product_id
    ) prod_total ON s.product_id = prod_total.product_id
        LEFT JOIN dws_sku_sales_detail hist
                  ON s.sku_id = hist.sku_id AND hist.ds = date_sub('20250808', 1)
WHERE s.ds = '20250808';

select * from dws_sku_sales_detail;

-- 3. 商品价格分析宽表（dws_product_price_analysis）
-- 用途：支撑价格趋势、价格带分布及定价决策🔶1-24🔶🔶1-33🔶
-- 建表语句
CREATE TABLE IF NOT EXISTS dws_product_price_analysis (
      product_id        STRING               COMMENT '商品ID',
      product_name      STRING               COMMENT '商品名称',
      category          STRING               COMMENT '商品大类',
      origin_price      DECIMAL(10,2)        COMMENT '原价',
      actual_price      DECIMAL(10,2)        COMMENT '实际售价',
      price_diff        DECIMAL(10,2)        COMMENT '价差，实际售价-原价',
      discount_rate     DECIMAL(10,4)        COMMENT '折扣率，实际售价/原价',
      price_band        STRING               COMMENT '价格带，如0-50元、50-100元等',
      price_strength    INT                  COMMENT '价格力星级，1-5星',
      is_promo          TINYINT              COMMENT '是否促销，0-否，1-是',
      promo_type        STRING               COMMENT '促销类型，如满减、直降、折扣等',
      price_trend       ARRAY<STRUCT<
          dt: STRING,                       -- 日期，使用dt避免与关键字冲突
          price: DECIMAL(10,2)              -- 当日价格
      >>                                    COMMENT '近7天价格趋势',
      create_time_dws   TIMESTAMP            COMMENT 'DWS层创建时间'
)
PARTITIONED BY (ds STRING COMMENT '分区字段，格式yyyyMMdd')
STORED AS ORC
LOCATION 'hdfs://cdh01:8020/warehouse/dws/dws_product_price_analysis'
TBLPROPERTIES (
    'orc.compress' = 'SNAPPY',
    'comment' = '商品价格分析宽表，支撑价格策略决策'
    );

-- 插入数据（每日增量）
INSERT OVERWRITE TABLE dws_product_price_analysis PARTITION (ds = '20250808')
SELECT
    p.product_id,
    p.product_name,
    p.category,
    p.origin_price,
    COALESCE(price.actual_price, p.origin_price) AS actual_price,
    p.origin_price - COALESCE(price.actual_price, p.origin_price) AS price_diff,
    COALESCE(price.discount_rate, 1.0) AS discount_rate,
    price.price_band,
    price.price_strength,
    COALESCE(price.is_promo, 0) AS is_promo,
    price.promo_type,
    -- 构建 price_trend：因无历史数据，近6天均用当日价格填充，或标记为无数据
    array(
            named_struct('dt', date_sub(to_date('20250808'), 6), 'price', CAST(COALESCE(price.actual_price, p.origin_price) AS DECIMAL(10,2))),
            named_struct('dt', date_sub(to_date('20250808'), 5), 'price', CAST(COALESCE(price.actual_price, p.origin_price) AS DECIMAL(10,2))),
            named_struct('dt', date_sub(to_date('20250808'), 4), 'price', CAST(COALESCE(price.actual_price, p.origin_price) AS DECIMAL(10,2))),
            named_struct('dt', date_sub(to_date('20250808'), 3), 'price', CAST(COALESCE(price.actual_price, p.origin_price) AS DECIMAL(10,2))),
            named_struct('dt', date_sub(to_date('20250808'), 2), 'price', CAST(COALESCE(price.actual_price, p.origin_price) AS DECIMAL(10,2))),
            named_struct('dt', date_sub(to_date('20250808'), 1), 'price', CAST(COALESCE(price.actual_price, p.origin_price) AS DECIMAL(10,2))),
            named_struct('dt', '20250808', 'price', CAST(COALESCE(price.actual_price, p.origin_price) AS DECIMAL(10,2)))
        ) AS price_trend,
    current_timestamp() AS create_time_dws
FROM
    dwd_dim_product_info p
        LEFT JOIN (
        SELECT
            product_id,
            AVG(actual_price) AS actual_price,
            AVG(discount_rate) AS discount_rate,
            MAX(price_band) AS price_band,
            MAX(3) AS price_strength,
            MAX(is_promo) AS is_promo,
            MAX(promo_type) AS promo_type
        FROM dwd_fact_price_promo_detail
        WHERE ds = '20250808'
        GROUP BY product_id
    ) price ON p.product_id = price.product_id
WHERE p.ds = '20250808';

select * from dws_product_price_analysis;

-- 4. 商品评价分析宽表（dws_product_evaluation_analysis）
-- 用途：支撑评价指标趋势及内容分析🔶1-62🔶
-- 建表语句
-- 商品评价分析宽表：整合商品维度、评价数据，支撑服务体验分析
CREATE TABLE IF NOT EXISTS dws_product_evaluation_analysis (
      product_id STRING COMMENT '商品ID，关联dwd_dim_product_info表中的product_id',
      product_name STRING COMMENT '商品名称，冗余dwd_dim_product_info表中的product_name',
      total_eval_count INT COMMENT '总评价数，统计该商品的所有评价数量',
      positive_eval_count INT COMMENT '正面评价数，评价分数大于等于4的评价数量',
      negative_eval_count INT COMMENT '负面评价数，评价分数小于等于2的评价数量',
      avg_eval_score DECIMAL(10,2) COMMENT '平均评分，所有评价分数的平均值',
      eval_trend ARRAY<STRUCT<dt:STRING, positive_count:INT, negative_count:INT>> COMMENT '近7天评价趋势，包含日期、当日正面评价数、当日负面评价数',
      top_positive_tags ARRAY<STRING> COMMENT 'TOP3正面标签，出现频率最高的3个正面标签',
      top_negative_tags ARRAY<STRING> COMMENT 'TOP3负面标签，出现频率最高的3个负面标签',
      create_time_dws TIMESTAMP COMMENT 'DWS层数据生成时间'
)
PARTITIONED BY (ds STRING COMMENT '分区日期，格式为yyyyMMdd，与ods、dwd层数据分区保持一致')
STORED AS ORC
LOCATION 'hdfs://cdh01:8020/warehouse/dws/dws_product_evaluation_analysis'
TBLPROPERTIES (
    'orc.compress' = 'SNAPPY',
    'comment' = '商品评价分析宽表，整合商品评价相关数据，用于分析商品评价情况'
    );


WITH
-- 1. 商品维度表（小表）
dim_product AS (
    SELECT product_id, product_name, ds
    FROM dwd_dim_product_info
    WHERE ds = '20250808'
),
-- 2. 用户评价事实表（大表过滤）
fact_review AS (
    SELECT product_id, id, eval_score, positive_tags, negative_tags
    FROM dwd_fact_user_review_detail
    WHERE ds = '20250808'
),
-- 3. 日期关联及评价趋势（提前聚合，修正 JOIN 条件）
review_trend AS (
    SELECT
        r.product_id,
        d.date_id AS dt,
        SUM(CASE WHEN r.eval_score >= 4 THEN 1 ELSE 0 END) AS positive_count,
        SUM(CASE WHEN r.eval_score <= 2 THEN 1 ELSE 0 END) AS negative_count
    FROM fact_review r
             JOIN dwd_dim_date_info d
                  ON r.id = d.date_id
                      AND d.date_id BETWEEN '20250802' AND '20250808'
    GROUP BY r.product_id, d.date_id
),
-- 4. 正面标签 TOP3（提前过滤，直接拿到符合条件的标签，无需再带 tag_rank 到主查询）
positive_tags AS (
    SELECT
        product_id, tag
    FROM (
             SELECT
                 product_id, tag,
                 ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY COUNT(1) DESC) AS tag_rank
             FROM fact_review r
                      LATERAL VIEW EXPLODE(r.positive_tags) exploded_tags AS tag
             GROUP BY product_id, tag
         ) sub
    WHERE tag_rank <= 3
),
-- 5. 负面标签 TOP3（提前过滤，直接拿到符合条件的标签，无需再带 tag_rank 到主查询）
negative_tags AS (
    SELECT
        product_id, tag
    FROM (
             SELECT
                 product_id, tag,
                 ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY COUNT(1) DESC) AS tag_rank
             FROM fact_review r
                      LATERAL VIEW EXPLODE(r.negative_tags) exploded_tags AS tag
             GROUP BY product_id, tag
         ) sub
    WHERE tag_rank <= 3
)

-- 最终查询：关联所有 CTE，简化 GROUP BY
INSERT OVERWRITE TABLE dws_product_evaluation_analysis
PARTITION (ds='20250808')
SELECT
    p.product_id,
    p.product_name,
    COUNT(r.id) AS total_eval_count,
    SUM(CASE WHEN r.eval_score >= 4 THEN 1 ELSE 0 END) AS positive_eval_count,
    SUM(CASE WHEN r.eval_score <= 2 THEN 1 ELSE 0 END) AS negative_eval_count,
    ROUND(AVG(r.eval_score), 2) AS avg_eval_score,
    COLLECT_LIST(
            NAMED_STRUCT(
                    'dt', t.dt,
                    'positive_count', CAST(t.positive_count AS INT),
                    'negative_count', CAST(t.negative_count AS INT)
                )
        ) AS eval_trend,
    COLLECT_LIST(pt.tag) AS top_positive_tags,  -- 直接用过滤好的标签
    COLLECT_LIST(nt.tag) AS top_negative_tags,  -- 直接用过滤好的标签
    CURRENT_TIMESTAMP() AS create_time_dws
FROM dim_product p
         LEFT JOIN fact_review r
                   ON p.product_id = r.product_id
         LEFT JOIN review_trend t
                   ON p.product_id = t.product_id
         LEFT JOIN positive_tags pt
                   ON p.product_id = pt.product_id
         LEFT JOIN negative_tags nt
                   ON p.product_id = nt.product_id
WHERE p.ds = '20250808'
GROUP BY
    p.product_id,
    p.product_name,
    t.dt;

select * from dws_product_evaluation_analysis;