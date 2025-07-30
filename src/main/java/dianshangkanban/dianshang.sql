set hive.exec.mode.local.auto=True;
create database aa;
use aa;

-- 用户表
drop table if exists users;
create external table if not exists users (
      user_id STRING COMMENT '用户ID',
      username STRING COMMENT '用户名',
      gender STRING COMMENT '性别',
      age INT COMMENT '年龄',
      city STRING COMMENT '城市',
      register_time STRING COMMENT '注册时间',
      credit_score INT COMMENT '信用评分',
      activity_level STRING COMMENT '活跃度'
)row format delimited fields terminated by ','
stored as textfile
tblproperties ('orc.compress'='SNAPPY');
load data local inpath '/opt/module/ecommerce_data/users.csv' into table users;

select * from users;

-- 店铺表
drop table if exists shops;
create external table if not exists shops (
      shop_id STRING COMMENT '店铺ID',
      shop_name STRING COMMENT '店铺名称',
      shop_type STRING COMMENT '店铺类型',
      city STRING COMMENT '城市',
      open_time STRING COMMENT '开业时间',
      level STRING COMMENT '店铺等级',
      overall_score DECIMAL(3,1) COMMENT '综合评分',
      product_score DECIMAL(3,1) COMMENT '商品评分',
      service_score DECIMAL(3,1) COMMENT '服务评分',
      logistics_score DECIMAL(3,1) COMMENT '物流评分',
      followers BIGINT COMMENT '粉丝数',
      monthly_sales BIGINT COMMENT '月销量',
      is_premium BOOLEAN COMMENT '是否高级店铺',
      product_count INT COMMENT '商品数量',
      promotion_frequency INT COMMENT '促销频率（月）'
)row format delimited fields terminated by ','
stored as textfile
tblproperties ('orc.compress'='SNAPPY');
load data local inpath '/opt/module/ecommerce_data/shops.csv' into table shops;

select * from shops;

-- 商品表
drop table if exists products;
create external table if not exists products (
      product_id STRING COMMENT '商品ID',
      product_name STRING COMMENT '商品名称',
      category_id STRING COMMENT '分类ID',
      shop_id STRING COMMENT '店铺ID',
      price DECIMAL(10,2) COMMENT '价格',
      inventory INT COMMENT '库存',
      sales_volume BIGINT COMMENT '销量',
      create_time STRING COMMENT '创建时间',
      brand STRING COMMENT '品牌',
      is_hot BOOLEAN COMMENT '是否热门',
      is_new BOOLEAN COMMENT '是否新品',
      discount_rate DECIMAL(3,2) COMMENT '折扣率',
      rating DECIMAL(3,1) COMMENT '用户评分',
      category STRING COMMENT '一级分类',
      sub_category STRING COMMENT '二级分类'
)row format delimited fields terminated by ','
stored as textfile
tblproperties ('orc.compress'='SNAPPY');
load data local inpath '/opt/module/ecommerce_data/products.csv' into table products;

select * from products;

-- 行为日志表
drop table if exists behaviors;
create external table if not exists behaviors (
      behavior_id STRING COMMENT '行为ID',
      user_id STRING COMMENT '用户ID',
      shop_id STRING COMMENT '店铺ID',
      product_id STRING COMMENT '商品ID',
      page_type STRING COMMENT '页面类型',
      behavior_type STRING COMMENT '行为类型',
      behavior_time STRING COMMENT '行为时间',
      stay_duration INT COMMENT '停留时长（秒）',
      platform STRING COMMENT '平台',
      referrer STRING COMMENT '来源渠道',
      detail STRING COMMENT '行为详情'
)row format delimited fields terminated by ','
stored as textfile
tblproperties ('orc.compress'='SNAPPY');
load data local inpath '/opt/module/ecommerce_data/behaviors.csv' into table behaviors;

select page_type from behaviors group by page_type;


-- 1. 店铺核心页面访问量（PV）对比
-- 指标定义：统计周期内，店铺核心页面（店铺首页、店铺分类页、店铺活动页）的访问总次数，用于对比不同入口页面的流量规模。
-- 业务价值：判断哪个页面是店铺的主要流量入口，为资源分配（如活动页推广）提供依据。
SELECT
    page_type,
    COUNT(*) AS page_views  -- 页面访问量
FROM
    behaviors
WHERE
    behavior_time BETWEEN '2023-10-01 00:00:00' AND '2023-10-31 23:59:59'  -- 时间范围
  AND page_type IN ('店铺首页', '店铺分类页', '店铺活动页')  -- 店铺核心入口页面
  AND referrer NOT IN ('直播间', '短视频', '图文', '微详情')  -- 排除指定渠道
GROUP BY
    page_type
ORDER BY
    page_views DESC;  -- 按访问量降序排列
-- 2. 店铺核心页面到商品详情页的点击率（CTR）
-- 指标定义：统计周期内，用户从店铺核心页面（店铺首页、店铺分类页、店铺活动页）点击进入商品详情页的比例（点击次数 ÷ 访问量）。
-- 业务价值：衡量店铺入口页面引导用户查看商品的效率，点击率低的页面需优化商品推荐布局。
-- 步骤1：统计店铺核心页面的总访问量
WITH core_page_views AS (
    SELECT
        page_type,
        COUNT(*) AS total_visits
    FROM
        behaviors
    WHERE
        behavior_time BETWEEN '2023-10-01 00:00:00' AND '2023-10-31 23:59:59'
      AND page_type IN ('店铺首页', '店铺分类页', '店铺活动页')
      AND referrer NOT IN ('直播间', '短视频', '图文', '微详情')
    GROUP BY
        page_type
),
-- 找出从核心页面点击后紧接着访问商品详情页的记录
     click_to_product AS (
         SELECT
             b1.page_type,
             COUNT(*) AS click_count
         FROM
             behaviors b1  -- 当前行为（核心页面的点击）
                 JOIN
             behaviors b2  -- 下一个行为（商品详情页的访问）
             ON
                         b1.user_id = b2.user_id  -- 同一用户
                     AND b2.behavior_time > b1.behavior_time  -- 后续行为
                     AND DATEDIFF(b2.behavior_time, b1.behavior_time) <= 1  -- 限定时间窗口（1天内）
         WHERE
             b1.behavior_time BETWEEN '2023-10-01 00:00:00' AND '2023-10-31 23:59:59'
           AND b1.page_type IN ('店铺首页', '店铺分类页', '店铺活动页')
           AND b1.behavior_type = '点击'
           AND b1.referrer NOT IN ('直播间', '短视频', '图文', '微详情')
           AND b2.page_type = '商品详情页'  -- 目标页面
         GROUP BY
             b1.page_type
     )
SELECT
    c.page_type,
    c.total_visits,
    COALESCE(p.click_count, 0) AS click_count,
    ROUND(COALESCE(p.click_count * 100.0 / c.total_visits, 0), 2) AS ctr_percentage
FROM
    core_page_views c
        LEFT JOIN
    click_to_product p ON c.page_type = p.page_type
ORDER BY
    ctr_percentage DESC;
-- 3. 店铺核心页面跳失率
-- 指标定义：统计周期内，用户仅访问店铺核心页面（未浏览其他页面）就离开的比例（跳失用户数 ÷ 总访问用户数）。
-- 业务价值：跳失率高说明页面未满足用户预期（如内容无关、引导不足），需优化页面内容或交互。
-- 步骤1：统计访问核心页面的所有用户（去重）
-- WITH core_page_users AS (
--     SELECT DISTINCT
--         user_id,
--         page_type
--     FROM
--         behaviors
--     WHERE
--         behavior_time BETWEEN '2023-10-01 00:00:00' AND '2023-10-31 23:59:59'
--       AND page_type IN ('店铺首页', '店铺分类页', '店铺活动页')
--       AND referrer NOT IN ('直播间', '短视频', '图文', '微详情')
-- ),
-- -- 步骤2：统计跳失用户（仅访问核心页面，无其他页面行为）
--      bounce_users AS (
--          SELECT
--              b.user_id,
--              b.page_type
--          FROM
--              behaviors b
--          WHERE
--              b.behavior_time BETWEEN '2023-10-01 00:00:00' AND '2023-10-31 23:59:59'
--            AND b.page_type IN ('店铺首页', '店铺分类页', '店铺活动页')
--            AND referrer NOT IN ('直播间', '短视频', '图文', '微详情')
--          GROUP BY
--              b.user_id,
--              b.page_type
--          HAVING
--             -- 该用户在统计周期内仅访问过当前核心页面，无其他页面行为
--                  COUNT(DISTINCT b.page_type) = 1
--             AND NOT EXISTS (
--              SELECT 1
--              FROM behaviors b2
--              WHERE b2.user_id = b.user_id
--                AND b2.behavior_time BETWEEN '2023-10-01 00:00:00' AND '2023-10-31 23:59:59'
--                AND b2.page_type NOT IN ('店铺首页', '店铺分类页', '店铺活动页')  -- 其他页面
--          )
--      )
-- -- 步骤3：计算跳失率（保留2位小数）
-- SELECT
--     c.page_type,
--     COUNT(DISTINCT c.user_id) AS total_users,  -- 总访问用户数
--     COUNT(DISTINCT b.user_id) AS bounce_users,  -- 跳失用户数
--     ROUND(COUNT(DISTINCT b.user_id) * 100.0 / COUNT(DISTINCT c.user_id), 2) AS bounce_rate_percentage
-- FROM
--     core_page_users c
--         LEFT JOIN
--     bounce_users b ON c.user_id = b.user_id AND c.page_type = b.page_type
-- GROUP BY
--     c.page_type
-- ORDER BY
--     bounce_rate_percentage DESC;
-- 优化方案
-- 使用 窗口函数 和 CTE 预聚合 减少表扫描次数，避免子查询中的全表关联：

-- 预计算用户的页面访问统计信息
WITH user_page_stats AS (
    SELECT
        user_id,
        COUNT(DISTINCT page_type) AS visited_page_types,
        MAX(CASE WHEN page_type IN ('店铺首页', '店铺分类页', '店铺活动页') THEN 1 ELSE 0 END) AS visited_core_pages,
        MAX(CASE WHEN page_type NOT IN ('店铺首页', '店铺分类页', '店铺活动页') THEN 1 ELSE 0 END) AS visited_other_pages
    FROM
        behaviors
    WHERE
        behavior_time BETWEEN '2023-10-01 00:00:00' AND '2023-10-31 23:59:59'
      AND referrer NOT IN ('直播间', '短视频', '图文', '微详情')
    GROUP BY
        user_id
),
-- 标记跳失用户（仅访问核心页面且未访问其他页面）
     bounce_users AS (
         SELECT
             user_id
         FROM
             user_page_stats
         WHERE
                 visited_core_pages = 1      -- 访问过核心页面
           AND visited_other_pages = 0 -- 未访问过其他页面
           AND visited_page_types = 1  -- 仅访问了1种页面类型
     ),
-- 统计各页面的访问用户（去重）
     page_visitors AS (
         SELECT
             page_type,
             user_id
         FROM
             behaviors
         WHERE
             behavior_time BETWEEN '2023-10-01 00:00:00' AND '2023-10-31 23:59:59'
           AND page_type IN ('店铺首页', '店铺分类页', '店铺活动页')
           AND referrer NOT IN ('直播间', '短视频', '图文', '微详情')
         GROUP BY
             page_type, user_id  -- 确保每个用户在每个页面只计一次
     )
-- 计算跳失率
SELECT
    p.page_type,
    COUNT(p.user_id) AS total_users,
    COUNT(CASE WHEN b.user_id IS NOT NULL THEN 1 END) AS bounce_users,
    ROUND(COUNT(CASE WHEN b.user_id IS NOT NULL THEN 1 END) * 100.0 / COUNT(p.user_id), 2) AS bounce_rate_percentage
FROM
    page_visitors p
        LEFT JOIN
    bounce_users b ON p.user_id = b.user_id
GROUP BY
    p.page_type
ORDER BY
    bounce_rate_percentage DESC;