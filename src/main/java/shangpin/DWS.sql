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
INSERT OVERWRITE TABLE dws_product_core_summary PARTITION (ds='${current_date}')
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
        SELECT product_id, SUM(pv) AS pv FROM dwd_fact_sales_traffic_detail WHERE ds='${current_date}' GROUP BY product_id
    ) today_pv ON p.product_id = today_pv.product_id
        LEFT JOIN (
        SELECT product_id, SUM(uv) AS uv FROM dwd_fact_sales_traffic_detail WHERE ds='${current_date}' GROUP BY product_id
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
        WHERE ds='${current_date}'
        GROUP BY product_id
    ) traffic ON p.product_id = traffic.product_id
        LEFT JOIN dws_product_core_summary hist
                  ON p.product_id = hist.product_id AND hist.ds = date_sub('${current_date}', 1)
WHERE p.ds = '${current_date}';

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
INSERT OVERWRITE TABLE dws_sku_sales_detail PARTITION (ds='${current_date}')
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
                  ON s.product_id = p.product_id AND p.ds = '${current_date}'
        LEFT JOIN (
        SELECT sku_id, SUM(pay_cnt) AS pay_cnt
        FROM dwd_fact_sales_traffic_detail
        WHERE ds='${current_date}'
        GROUP BY sku_id
    ) traffic ON s.sku_id = traffic.sku_id
        LEFT JOIN (
        SELECT product_id, SUM(pay_cnt) AS sales
        FROM dwd_fact_sales_traffic_detail
        WHERE ds='${current_date}'
        GROUP BY product_id
    ) prod_total ON s.product_id = prod_total.product_id
        LEFT JOIN dws_sku_sales_detail hist
                  ON s.sku_id = hist.sku_id AND hist.ds = date_sub('${current_date}', 1)
WHERE s.ds = '${current_date}';

select * from dws_sku_sales_detail;

-- 3. 商品价格分析宽表（dws_product_price_analysis）
-- 用途：支撑价格趋势、价格带分布及定价决策🔶1-24🔶🔶1-33🔶
-- 建表语句
CREATE TABLE IF NOT EXISTS dws_product_price_analysis (
      product_id STRING COMMENT '商品ID',
      product_name STRING COMMENT '商品名称',
      category STRING COMMENT '商品大类',
      origin_price DECIMAL(10,2) COMMENT '原价',
      actual_price DECIMAL(10,2) COMMENT '实际售价',
      price_diff DECIMAL(10,2) COMMENT '价差',
      discount_rate DECIMAL(10,4) COMMENT '折扣率',
      price_band STRING COMMENT '价格带',
      price_strength INT COMMENT '价格力星级',
      is_promo INT COMMENT '是否促销',
      promo_type STRING COMMENT '促销类型',
      price_trend ARRAY<STRUCT<date:STRING, price:DECIMAL(10,2)>> COMMENT '近7天价格趋势',
      create_time_dws TIMESTAMP COMMENT 'DWS层创建时间'
)
PARTITIONED BY (ds STRING)
STORED AS ORC
LOCATION 'hdfs://cdh01:8020/warehouse/dws/dws_product_price_analysis'
TBLPROPERTIES (
    'orc.compress' = 'SNAPPY',
    'comment' = '商品价格分析宽表，支撑价格策略决策'
    );

-- 插入数据（每日增量）
INSERT OVERWRITE TABLE dws_product_price_analysis PARTITION (ds='${current_date}')
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
    -- 构建近7天价格趋势（含当日）
    array(
            named_struct('date', date_sub('${current_date}', 6), 'price', hist6.price_trend),
            named_struct('date', date_sub('${current_date}', 5), 'price', hist5.price_trend),
            named_struct('date', date_sub('${current_date}', 4), 'price', hist4.price_trend),
            named_struct('date', date_sub('${current_date}', 3), 'price', hist3.price_trend),
            named_struct('date', date_sub('${current_date}', 2), 'price', hist2.price_trend),
            named_struct('date', date_sub('${current_date}', 1), 'price', hist1.price_trend),
            named_struct('date', '${current_date}', 'price', COALESCE(price.actual_price, p.origin_price))
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
            MAX(3) AS price_strength, -- 示例：固定为3星，实际应从价格力表获取
            MAX(is_promo) AS is_promo,
            MAX(promo_type) AS promo_type
        FROM dwd_fact_price_promo_detail
        WHERE ds='${current_date}'
        GROUP BY product_id
    ) price ON p.product_id = price.product_id
-- 关联近6天历史价格
        LEFT JOIN dws_product_price_analysis hist1
                  ON p.product_id = hist1.product_id AND hist1.ds = date_sub('${current_date}', 1)
        LEFT JOIN dws_product_price_analysis hist2
                  ON p.product_id = hist2.product_id AND hist2.ds = date_sub('${current_date}', 2)
        LEFT JOIN dws_product_price_analysis hist3
                  ON p.product_id = hist3.product_id AND hist3.ds = date_sub('${current_date}', 3)
        LEFT JOIN dws_product_price_analysis hist4
                  ON p.product_id = hist4.product_id AND hist4.ds = date_sub('${current_date}', 4)
        LEFT JOIN dws_product_price_analysis hist5
                  ON p.product_id = hist5.product_id AND hist5.ds = date_sub('${current_date}', 5)
        LEFT JOIN dws_product_price_analysis hist6
                  ON p.product_id = hist6.product_id AND hist6.ds = date_sub('${current_date}', 6)
WHERE p.ds = '${current_date}';

select * from dws_product_price_analysis;

-- 4. 商品评价分析宽表（dws_product_evaluation_analysis）
-- 用途：支撑评价指标趋势及内容分析🔶1-62🔶
-- 建表语句
CREATE TABLE IF NOT EXISTS dws_product_evaluation_analysis (
      product_id STRING COMMENT '商品ID',
      product_name STRING COMMENT '商品名称',
      total_eval_cnt INT COMMENT '总评价数',
      positive_eval_cnt INT COMMENT '正面评价数',
      negative_eval_cnt INT COMMENT '负面评价数',
      old_buyer_eval_cnt INT COMMENT '老买家评价数',
      avg_eval_score DECIMAL(10,2) COMMENT '平均评分',
      eval_trend ARRAY<STRUCT<date:STRING, positive:INT, negative:INT>> COMMENT '近7天评价趋势',
      top_positive_tags ARRAY<STRING> COMMENT 'TOP3正面标签',
      top_negative_tags ARRAY<STRING> COMMENT 'TOP3负面标签',
      create_time_dws TIMESTAMP COMMENT 'DWS层创建时间'
)
PARTITIONED BY (ds STRING)
STORED AS ORC
LOCATION 'hdfs://cdh01:8020/warehouse/dws/dws_product_evaluation_analysis'
TBLPROPERTIES (
    'orc.compress' = 'SNAPPY',
    'comment' = '商品评价分析宽表，支撑服务体验分析'
    );

-- 插入数据（每日增量）
INSERT OVERWRITE TABLE dws_product_evaluation_analysis PARTITION (ds='${current_date}')
SELECT
    p.product_id,
    p.product_name,
    -- 累计评价数=历史+当日
    COALESCE(hist.total_eval_cnt, 0) + COALESCE(eval.total, 0) AS total_eval_cnt,
    COALESCE(hist.positive_eval_cnt, 0) + COALESCE(eval.positive, 0) AS positive_eval_cnt,
    COALESCE(hist.negative_eval_cnt, 0) + COALESCE(eval.negative, 0) AS negative_eval_cnt,
    COALESCE(hist.old_buyer_eval_cnt, 0) + COALESCE(eval.old_buyer, 0) AS old_buyer_eval_cnt,
    -- 平均评分（历史评分*历史数量+当日评分*当日数量）/总数量
    CASE WHEN (COALESCE(hist.total_eval_cnt, 0) + COALESCE(eval.total, 0)) = 0 THEN 0
         ELSE (COALESCE(hist.avg_eval_score * hist.total_eval_cnt, 0) + COALESCE(eval.avg_score, 0) * COALESCE(eval.total, 0))
             / (COALESCE(hist.total_eval_cnt, 0) + COALESCE(eval.total, 0))
        END AS avg_eval_score,
    -- 近7天评价趋势
    array(
            named_struct('date', date_sub('${current_date}', 6), 'positive', hist6.eval_trend, 'negative', hist6.eval_trend),
            named_struct('date', date_sub('${current_date}', 5), 'positive', hist5.eval_trend, 'negative', hist5.eval_trend),
            named_struct('date', date_sub('${current_date}', 4), 'positive', hist4.eval_trend, 'negative', hist4.eval_trend),
            named_struct('date', date_sub('${current_date}', 3), 'positive', hist3.eval_trend, 'negative', hist3.eval_trend),
            named_struct('date', date_sub('${current_date}', 2), 'positive', hist2.eval_trend, 'negative', hist2.eval_trend),
            named_struct('date', date_sub('${current_date}', 1), 'positive', hist1.eval_trend, 'negative', hist1.eval_trend),
            named_struct('date', '${current_date}', 'positive', COALESCE(eval.positive, 0), 'negative', COALESCE(eval.negative, 0))
        ) AS eval_trend,
    -- 正面标签TOP3（示例：取出现次数最多的3个）
    array('质量好', '物流快', '性价比高') AS top_positive_tags,
    array('包装差', '尺寸不符', '发货慢') AS top_negative_tags,
    current_timestamp() AS create_time_dws
FROM
    dwd_dim_product_info p
        LEFT JOIN (
        SELECT
            product_id,
            COUNT(*) AS total,
            SUM(CASE WHEN eval_score >=4 THEN 1 ELSE 0 END) AS positive,
            SUM(CASE WHEN eval_score <=2 THEN 1 ELSE 0 END) AS negative,
            SUM(CASE WHEN user_id = '老买家' THEN 1 ELSE 0 END) AS old_buyer, -- 假设user_tag标识老买家
            AVG(eval_score) AS avg_score
        FROM dwd_fact_user_review_detail
        WHERE ds='${current_date}'
        GROUP BY product_id
    ) eval ON p.product_id = eval.product_id
-- 关联近6天历史评价
        LEFT JOIN dws_product_evaluation_analysis hist
                  ON p.product_id = hist.product_id AND hist.ds = date_sub('${current_date}', 1)
        LEFT JOIN dws_product_evaluation_analysis hist1
                  ON p.product_id = hist1.product_id AND hist1.ds = date_sub('${current_date}', 1)
        LEFT JOIN dws_product_evaluation_analysis hist2
                  ON p.product_id = hist2.product_id AND hist2.ds = date_sub('${current_date}', 2)
        LEFT JOIN dws_product_evaluation_analysis hist3
                  ON p.product_id = hist3.product_id AND hist3.ds = date_sub('${current_date}', 3)
        LEFT JOIN dws_product_evaluation_analysis hist4
                  ON p.product_id = hist4.product_id AND hist4.ds = date_sub('${current_date}', 4)
        LEFT JOIN dws_product_evaluation_analysis hist5
                  ON p.product_id = hist5.product_id AND hist5.ds = date_sub('${current_date}', 5)
        LEFT JOIN dws_product_evaluation_analysis hist6
                  ON p.product_id = hist6.product_id AND hist6.ds = date_sub('${current_date}', 6)
WHERE p.ds = '${current_date}';

select * from dws_product_evaluation_analysis;