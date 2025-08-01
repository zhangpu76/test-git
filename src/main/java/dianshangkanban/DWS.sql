set hive.exec.mode.local.auto=True;
use ecommerce;

-- 1. DWS层 - 页面分析宽表（dws_page_analysis_wide）
--------------------------------------------------------------------------------
-- 表结构创建
CREATE TABLE IF NOT EXISTS dws_page_analysis_wide (
      page_type         STRING COMMENT '页面类型（店铺页、商品详情页等）',
      page_module       STRING COMMENT '页面所属模块（关联dwd_user_behavior_detail）',
      platform          STRING COMMENT '平台（APP、H5等）',
      device_type       STRING COMMENT '设备类型（手机、平板等）',
      user_city_level   STRING COMMENT '用户城市级别（一线、二线等）',

      page_visit_cnt    BIGINT COMMENT '页面访问次数（去重行为数）',
      page_uv           BIGINT COMMENT '页面访问人数（去重用户数）',
      page_click_cnt    BIGINT COMMENT '页面点击次数（所有点击行为）',
      page_click_uv     BIGINT COMMENT '页面点击人数（去重点击用户数）',

      page_section_click_cnt  BIGINT COMMENT '页面板块点击次数',
      page_section_click_uv   BIGINT COMMENT '页面板块点击人数',
      page_section_pay_amt    DECIMAL(10,2) COMMENT '板块引导支付金额',

      page_order_cnt    BIGINT COMMENT '页面支付订单数',
      page_order_uv     BIGINT COMMENT '页面支付人数',
      page_pay_cnt      BIGINT COMMENT '支付订单数',
      page_pay_uv       BIGINT COMMENT '支付人数',
      page_pay_amt      DECIMAL(10,2) COMMENT '支付总金额',
      page_buy_rate     DECIMAL(5,2) COMMENT '购买转化率（支付人数 / 访问人数）',

      page_guide_sku_cnt  BIGINT COMMENT '页面引导商品数（去重商品ID数）',
      page_guide_pay_amt  DECIMAL(10,2) COMMENT '引导商品支付金额',
      page_guide_buy_uv   BIGINT COMMENT '引导商品购买人数',

      days_ago          INT COMMENT '统计天数（如0=当天，用于趋势）'
) COMMENT 'DWS层 - 页面分析宽表，聚合页面访问、点击、交易指标'
PARTITIONED BY (ds STRING)
STORED AS ORC
LOCATION 'hdfs://cdh01:8020//warehouse/ecommerce/dws/dws_page_analysis_wide'
TBLPROPERTIES ('orc.compress' = 'SNAPPY');

-- 数据插入（每日执行，ds为分区日期）
INSERT OVERWRITE TABLE dws_page_analysis_wide PARTITION (ds='20250731')
SELECT
    -- 维度字段（从dwd_user_behavior_detail获取）
    uv.page_type,
    uv.page_module,
    uv.platform,
    uv.device_type,
    uv.user_city_level,

    -- 页面基础流量指标
    COUNT(DISTINCT uv.behavior_id) AS page_visit_cnt,  -- 访问次数（去重行为）
    COUNT(DISTINCT uv.user_id)     AS page_uv,         -- 访问人数（去重用户）
    COUNT(DISTINCT CASE WHEN uv.behavior_type = '点击' THEN uv.behavior_id END) AS page_click_cnt, -- 点击次数
    COUNT(DISTINCT CASE WHEN uv.behavior_type = '点击' THEN uv.user_id END)     AS page_click_uv,  -- 点击人数

    -- 页面板块点击（假设dwd_user_behavior_detail有section_id字段标记板块，需实际存在）
    COUNT(DISTINCT CASE WHEN uv.behavior_type = '点击' THEN uv.behavior_id END) AS page_section_click_cnt,
    COUNT(DISTINCT CASE WHEN uv.behavior_type = '点击' THEN uv.user_id END)     AS page_section_click_uv,
    SUM(CASE WHEN od.is_valid THEN od.total_amount ELSE 0 END)                 AS page_section_pay_amt,

    -- 交易转化指标（关联订单、支付表）
    COUNT(DISTINCT od.order_id)  AS page_order_cnt,  -- 下单订单数
    COUNT(DISTINCT od.user_id)   AS page_order_uv,   -- 下单人数
    COUNT(DISTINCT pd.payment_id) AS page_pay_cnt,   -- 支付订单数
    COUNT(DISTINCT pd.user_id)    AS page_pay_uv,    -- 支付人数
    SUM(pd.payment_amount)        AS page_pay_amt,   -- 支付总金额
    -- 购买转化率：支付人数 / 访问人数（避免除数为0）
    CASE
        WHEN COUNT(DISTINCT uv.user_id) = 0 THEN 0
        ELSE ROUND(COUNT(DISTINCT pd.user_id) / COUNT(DISTINCT uv.user_id), 2)
        END AS page_buy_rate,

    -- 商品引导指标（关联商品ID）
    COUNT(DISTINCT uv.product_id) AS page_guide_sku_cnt,  -- 引导商品数
    SUM(CASE WHEN pd.is_valid THEN pd.payment_amount ELSE 0 END) AS page_guide_pay_amt,
    COUNT(DISTINCT CASE WHEN pd.is_valid THEN pd.user_id END)    AS page_guide_buy_uv,

    -- 时间趋势：固定为0（近30天趋势需单独处理，或用窗口函数扩展）
    0 AS days_ago
FROM dwd_user_behavior_detail uv
-- 关联订单表（需确保dwd_order_detail已生成）
         LEFT JOIN dwd_order_detail od
                   ON uv.user_id = od.user_id
                       AND uv.behavior_time >= od.order_time
                       AND uv.ds = od.ds
-- 关联支付表（需确保dwd_payment_detail已生成）
         LEFT JOIN dwd_payment_detail pd
                   ON od.order_id = pd.order_id
                       AND od.ds = pd.ds
WHERE uv.ds = '20250731'  -- 处理当日数据
  AND uv.behavior_type IN ('访问', '点击', '购买')  -- 筛选核心行为
GROUP BY
    uv.page_type, uv.page_module, uv.platform, uv.device_type, uv.user_city_level;



-- 2. DWS层 - 近30天页面趋势宽表（dws_page_trend_30d）
-- 表结构创建（按天存储近30天趋势）
CREATE TABLE IF NOT EXISTS dws_page_trend_30d (
      page_type         STRING COMMENT '页面类型（店铺页、商品详情页等）',
      days_ago          INT COMMENT '距离当前天数（0=当天，1=1天前...29=29天前）',
      page_uv           BIGINT COMMENT '当日页面访问人数',
      page_click_uv     BIGINT COMMENT '当日页面点击人数',
      page_pay_uv       BIGINT COMMENT '当日页面支付人数',
      page_pay_amt      DECIMAL(10,2) COMMENT '当日页面支付金额'
) COMMENT 'DWS层 - 页面近30天趋势宽表'
    PARTITIONED BY (ds STRING COMMENT '统计日期')  -- 分区字段单独定义，与表字段不重复
    STORED AS ORC
    LOCATION 'hdfs://cdh01:8020//warehouse/ecommerce/dws/dws_page_trend_30d'
    TBLPROPERTIES ('orc.compress' = 'SNAPPY');

-- 数据插入（近30天趋势，需遍历日期或用窗口函数，此处示例单日期）
-- 数据插入（近30天趋势，示例单日期，修正分区写入逻辑）
INSERT OVERWRITE TABLE dws_page_trend_30d PARTITION (ds='20250731')
SELECT
    uv.page_type,
    DATEDIFF('2025-07-31', uv.ds) AS days_ago,  -- 计算距离当前天数
    COUNT(DISTINCT uv.user_id) AS page_uv,
    COUNT(DISTINCT CASE WHEN uv.behavior_type = '点击' THEN uv.user_id END) AS page_click_uv,
    COUNT(DISTINCT CASE WHEN pd.is_valid THEN pd.user_id END) AS page_pay_uv,
    SUM(CASE WHEN pd.is_valid THEN pd.payment_amount ELSE 0 END) AS page_pay_amt
FROM dwd_user_behavior_detail uv
         LEFT JOIN dwd_payment_detail pd
                   ON uv.user_id = pd.user_id
                       AND uv.ds = pd.ds
WHERE uv.ds = '20250731'
  AND uv.behavior_type IN ('访问', '点击', '购买')
GROUP BY
    uv.page_type, uv.ds;


--------------------------------------------------------------------------------
-- 3. 执行验证（可选）
--------------------------------------------------------------------------------
-- 查看页面分析宽表数据
SELECT * FROM dws_page_analysis_wide WHERE ds='20250731' LIMIT 10;

-- 查看近30天趋势表数据
SELECT * FROM dws_page_trend_30d WHERE ds='20250731' LIMIT 10;