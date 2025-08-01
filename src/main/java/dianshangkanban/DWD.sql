set hive.exec.mode.local.auto=True;
use ecommerce;

-- 1. 用户行为明细事实表
CREATE TABLE IF NOT EXISTS dwd_user_behavior_detail (
      behavior_id STRING COMMENT '行为唯一ID',
      user_id STRING COMMENT '用户ID（关联dim_users）',
      product_id STRING COMMENT '商品ID（关联dim_products）',
      shop_id STRING COMMENT '店铺ID（关联dim_shops）',
      behavior_type STRING COMMENT '行为类型编码（关联dim_behavior_types）',
      behavior_name STRING COMMENT '行为类型名称（来自dim_behavior_types）',
      behavior_value INT COMMENT '行为价值权重（来自dim_behavior_types）',
      page_type STRING COMMENT '页面类型编码（关联dim_page_types）',
      page_module STRING COMMENT '页面所属模块（来自dim_page_types）',
      platform STRING COMMENT '平台编码（关联dim_platforms）',
      device_type STRING COMMENT '设备类型（来自dim_platforms）',
      user_city_level STRING COMMENT '用户城市级别（来自dim_users）',
      behavior_time TIMESTAMP COMMENT '行为发生时间',
      stay_duration INT COMMENT '页面停留时长（秒）',
      is_login BOOLEAN COMMENT '是否登录状态',
      behavior_extra STRING COMMENT '行为额外信息（JSON）',
      is_valid BOOLEAN COMMENT '是否有效行为'
)COMMENT 'DWD层-用户行为明细事实表'
PARTITIONED BY (ds STRING)
STORED AS ORC
LOCATION 'hdfs://cdh01:8020//warehouse/ecommerce/dwd/dwd_user_behavior_detail'
TBLPROPERTIES (
    'orc.compress' = 'SNAPPY',
    'transient_lastDdlTime' = '${current_timestamp}'
    );

-- 插入用户行为明细数据
INSERT OVERWRITE TABLE dwd_user_behavior_detail PARTITION (ds='20250731')
SELECT
    -- 行为唯一ID（用户ID + 时间 + 行为类型避免重复）
    CONCAT(ob.user_id, '_', ob.behavior_time, '_', ob.behavior_type) AS behavior_id,
    NVL(ob.user_id, 'unknown') AS user_id,
    -- 无商品关联的行为标记为'none'
    CASE
        WHEN ob.behavior_type IN ('浏览', '跳出', '点击首页') THEN 'none'
        ELSE NVL(ob.product_id, 'none')
        END AS product_id,
    -- 从商品维度表关联店铺ID（商品ID有效时）
    NVL(dp.shop_id, 'none') AS shop_id,
    ob.behavior_type AS behavior_type,
    -- 关联行为类型维度表补充名称、价值（无则用unknown/0）
    NVL(dbt.behavior_name, 'unknown') AS behavior_name,
    NVL(dbt.behavior_value, 0) AS behavior_value,
    ob.page_type AS page_type,
    -- 关联页面类型维度表补充模块（无则用unknown）
    NVL(dpt.page_module, 'unknown') AS page_module,
    ob.platform AS platform,
    -- 关联平台维度表补充设备类型（无则用unknown）
    NVL(dpl.device_type, 'unknown') AS device_type,
    -- 关联用户维度表补充城市级别（无则用unknown）
    NVL(du.city_level, 'unknown') AS user_city_level,
    -- 标准化行为时间格式（非法格式置为NULL）
    CASE
        WHEN ob.behavior_time RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}(:[0-9]{2})?$'
            THEN CAST(ob.behavior_time AS TIMESTAMP)
        ELSE NULL
        END AS behavior_time,
    -- 停留时长转换（字符串转INT，无效值置0）
    NVL(CAST(ob.stay_duration AS INT), 0) AS stay_duration,
    -- 原表无is_login，直接置为false（或根据业务逻辑调整）
    false AS is_login,
    -- 原表无behavior_extra，直接置为'unknown'（或根据业务逻辑调整）
    'unknown' AS behavior_extra,
    -- 过滤测试行为（无behavior_extra时默认有效）
    true AS is_valid
FROM ods_behaviors ob
-- 关联用户维度表（用户ID匹配，取当日维度）
         LEFT JOIN dim_users du
                   ON ob.user_id = du.user_id AND du.ds = '20250731'
-- 关联商品维度表（商品ID匹配，取当日维度）
         LEFT JOIN dim_products dp
                   ON ob.product_id = dp.product_id AND dp.ds = '20250731'
-- 关联行为类型维度表（行为类型匹配，取当日维度）
         LEFT JOIN dim_behavior_types dbt
                   ON ob.behavior_type = dbt.behavior_type AND dbt.ds = '20250731'
-- 关联页面类型维度表（页面类型匹配，取当日维度）
         LEFT JOIN dim_page_types dpt
                   ON ob.page_type = dpt.page_type AND dpt.ds = '20250731'
-- 关联平台维度表（平台编码匹配，取当日维度）
         LEFT JOIN dim_platforms dpl
                   ON ob.platform = dpl.platform AND dpl.ds = '20250731'
WHERE ob.ds = '20250731';

select * from dwd_user_behavior_detail;

-- 2. 订单明细事实表（基于实际数据重构）
CREATE TABLE IF NOT EXISTS dwd_order_detail (
      order_id          STRING COMMENT '订单唯一ID（behavior_id + user_id 拼接）',
      user_id           STRING COMMENT '下单用户ID',
      product_id        STRING COMMENT '商品ID',
      shop_id           STRING COMMENT '店铺ID（关联商品表）',
      order_time        TIMESTAMP COMMENT '下单时间（behavior_time 转换）',
      product_num       INT COMMENT '商品数量（默认 1，实际需业务补充）',
      product_price     DECIMAL(10,2) COMMENT '商品单价（关联 ods_products）',
      total_amount      DECIMAL(10,2) COMMENT '订单总金额（product_num * product_price）',
      order_status      STRING COMMENT '订单状态（默认已购买）',
      is_valid          BOOLEAN COMMENT '是否有效订单'
) COMMENT 'DWD层 - 订单明细事实表（基于购买行为 + 商品表关联）'
PARTITIONED BY (ds STRING)
STORED AS ORC
LOCATION 'hdfs://cdh01:8020//warehouse/ecommerce/dwd/dwd_order_detail'
TBLPROPERTIES ('orc.compress' = 'SNAPPY');

-- 插入订单明细数据（重构逻辑）
INSERT OVERWRITE TABLE dwd_order_detail PARTITION (ds='20250731')
SELECT
    -- 订单ID：行为ID + 用户ID 确保唯一
    CONCAT(ob.behavior_id, '_', ob.user_id) AS order_id,
    ob.user_id AS user_id,
    ob.product_id AS product_id,
    -- 从商品表关联店铺ID
    op.shop_id AS shop_id,
    -- 转换下单时间（行为时间）
    CASE
        WHEN ob.behavior_time RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}(:[0-9]{2})?$'
            THEN CAST(ob.behavior_time AS TIMESTAMP)
        ELSE NULL
        END AS order_time,
    -- 商品数量：默认 1（无 detail 解析，需业务确认）
    1 AS product_num,
    -- 从商品表获取单价
    op.price AS product_price,
    -- 计算总金额（数量 * 单价）
    1 * op.price AS total_amount,
    -- 订单状态：标记为已购买
    '已购买' AS order_status,
    -- 有效性：过滤测试行为（若 behavior_extra 含 test 则无效，无该字段则默认有效）
    CASE WHEN ob.detail LIKE '%test%' THEN false ELSE true END AS is_valid
FROM ods_behaviors ob
-- 关联商品表，补充店铺ID、单价
         LEFT JOIN ods_products op
                   ON ob.product_id = op.product_id
WHERE ob.ds = '20250731'
  AND ob.behavior_type = '购买';  -- 用 '购买' 识别订单行为

select * from dwd_order_detail;

-- 3. 支付明细事实表（需先创建支付渠道维度表 dim_payment_channels）
CREATE TABLE IF NOT EXISTS dwd_payment_detail (
      payment_id        STRING COMMENT '支付唯一ID（behavior_id + order_id 拼接）',
      order_id          STRING COMMENT '关联订单ID',
      user_id           STRING COMMENT '支付用户ID',
      payment_time      TIMESTAMP COMMENT '支付时间（购买行为时间）',
      payment_amount    DECIMAL(10,2) COMMENT '支付金额（订单总金额）',
      payment_status    STRING COMMENT '支付状态（购买即成功）',
      is_valid          BOOLEAN COMMENT '是否有效支付'
) COMMENT 'DWD层 - 支付明细事实表（基于购买行为，无独立支付类型）'
PARTITIONED BY (ds STRING)
STORED AS ORC
LOCATION 'hdfs://cdh01:8020//warehouse/ecommerce/dwd/dwd_payment_detail'
TBLPROPERTIES ('orc.compress' = 'SNAPPY');

INSERT OVERWRITE TABLE dwd_payment_detail PARTITION (ds='20250731')
SELECT
    -- 支付ID：行为ID + 订单ID（订单ID = behavior_id + user_id）
    CONCAT(ob.behavior_id, '_', CONCAT(ob.behavior_id, '_', ob.user_id)) AS payment_id,
    CONCAT(ob.behavior_id, '_', ob.user_id) AS order_id,  -- 关联订单ID（与订单表规则一致）
    ob.user_id AS user_id,
    -- 转换支付时间（购买行为时间）
    CASE
        WHEN ob.behavior_time RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}(:[0-9]{2})?$'
            THEN CAST(ob.behavior_time AS TIMESTAMP)
        ELSE NULL
        END AS payment_time,
    -- 支付金额：关联订单表的总金额
    od.total_amount AS payment_amount,
    -- 支付状态：购买即成功
    '成功' AS payment_status,
    -- 有效性：过滤测试行为
    CASE WHEN ob.detail LIKE '%test%' THEN false ELSE true END AS is_valid
FROM ods_behaviors ob
-- 关联订单明细（需先运行 dwd_order_detail 插入）
         LEFT JOIN dwd_order_detail od
                   ON CONCAT(ob.behavior_id, '_', ob.user_id) = od.order_id  -- 订单ID拼接规则一致
WHERE ob.ds = '20250731'
  AND ob.behavior_type = '购买';  -- 用 '购买' 识别支付行为

select * from dwd_payment_detail;