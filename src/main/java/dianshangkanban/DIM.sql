set hive.exec.mode.local.auto=True;
use ecommerce;

-- 1. 商品维度表（dim_products）
CREATE TABLE IF NOT EXISTS dim_products (
      product_id STRING COMMENT '商品ID',
      product_name STRING COMMENT '商品名称',
      category_id STRING COMMENT '分类ID',
      category_name STRING COMMENT '分类名称',  -- 对应ods的category
      sub_category_name STRING COMMENT '子分类名称',  -- 对应ods的sub_category
      shop_id STRING COMMENT '所属店铺ID',
      price DECIMAL(10,2) COMMENT '商品价格',
      inventory INT COMMENT '库存数量',
      sales_volume INT COMMENT '销售数量',
      create_time TIMESTAMP COMMENT '创建时间',
      brand STRING COMMENT '品牌',
      is_hot BOOLEAN COMMENT '是否热门（1=是，0=否）',
      is_new BOOLEAN COMMENT '是否新品（1=是，0=否）',
      discount_rate DECIMAL(5,2) COMMENT '折扣率',
      rating DECIMAL(3,2) COMMENT '商品评分',
      is_valid BOOLEAN COMMENT '是否有效（默认有效）',
      update_time TIMESTAMP COMMENT '维度更新时间'
)COMMENT '商品维度表（精简版）'
partitioned by (`ds` string comment '统计日期')
STORED AS ORC
LOCATION 'hdfs://cdh01:8020//warehouse/ecommerce/dim/dim_products'
TBLPROPERTIES (
    'orc.compress' = 'SNAPPY',
    'transient_lastDdlTime' = '${current_timestamp}'
    );

-- 1. 商品维度表插入
INSERT OVERWRITE TABLE dim_products partition (ds='20250731')
SELECT
    product_id,
    product_name,
    nvl(category_id, 'unknown') as category_id,
    nvl(category, 'unknown') as category_name,
    nvl(sub_category, 'unknown') as sub_category_name,
    nvl(shop_id, 'unknown') as shop_id,
    -- 价格转换（放宽正则，允许整数或小数）
    case
        when price rlike '^[0-9]+(\\.[0-9]{1,2})?$'
            then cast(price as decimal(10,2))
        else 0.00
        end as price,
    -- 库存转换
    case
        when inventory rlike '^[0-9]+$'
            then cast(inventory as int)
        else 0
        end as inventory,
    -- 销量转换
    case
        when sales_volume rlike '^[0-9]+$'
            then cast(sales_volume as int)
        else 0
        end as sales_volume,
    -- 时间转换（放宽格式匹配，允许无秒数）
    case
        when create_time rlike '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}(:[0-9]{2})?$'
            then cast(create_time as timestamp)
        else null
        end as create_time,
    nvl(brand, 'unknown') as brand,
    case
        when is_hot = '1' then true
        when is_hot = '0' then false
        else false
        end as is_hot,
    case
        when is_new = '1' then true
        when is_new = '0' then false
        else false
        end as is_new,
    case
        when discount_rate rlike '^[0-9]+(\\.[0-9]{1,2})?$'
            then cast(discount_rate as decimal(5,2))
        else 1.00
        end as discount_rate,
    case
        when rating rlike '^[0-9]+(\\.[0-9]{1,2})?$'
            then cast(rating as decimal(3,2))
        else 0.00
        end as rating,
    true as is_valid,
    -- 转换为年月日格式（yyyy-MM-dd）
    to_date(current_timestamp()) as update_time  -- 仅保留年月日
FROM ods_products
WHERE ds = '20250731';

select * from dim_products;

-- 2. 店铺维度表（dim_shops）
CREATE TABLE IF NOT EXISTS dim_shops (
      shop_id STRING COMMENT '店铺ID',
      shop_name STRING COMMENT '店铺名称',
      shop_type STRING COMMENT '店铺类型',
      city STRING COMMENT '所在城市',
      city_level STRING COMMENT '城市级别（一线/新一线/其他）',
      open_time string COMMENT '开店时间',
      `level` string COMMENT '店铺等级',
      overall_score DECIMAL(3,2) COMMENT '综合评分',
      followers INT COMMENT '粉丝数量',
      monthly_sales INT COMMENT '月销量',
      is_premium BOOLEAN COMMENT '是否旗舰店（1=是，0=否）',
      product_count INT COMMENT '商品总数',
      is_valid BOOLEAN COMMENT '是否有效（默认有效）',
      update_time string COMMENT '维度更新时间'
)COMMENT '店铺维度表（精简版）'
partitioned by (`ds` string comment '统计日期')
STORED AS ORC
LOCATION 'hdfs://cdh01:8020//warehouse/ecommerce/dim/dim_shops'
TBLPROPERTIES (
    'orc.compress' = 'SNAPPY',
    'transient_lastDdlTime' = '${current_timestamp}'
    );

-- 2. 店铺维度表插入
INSERT OVERWRITE TABLE dim_shops
    PARTITION (ds='20250801')
SELECT
    shop_id,
    shop_name,
    shop_type,
    city,
    -- 城市级别逻辑不变
    CASE
        WHEN city IN ('北京','上海','广州','深圳') THEN '一线城市'
        WHEN city IN ('杭州','南京','成都','武汉','重庆','西安') THEN '新一线城市'
        ELSE '其他城市'
        END AS city_level,
    -- 1. open_time：转 timestamp 后格式化字符串
    date_format(cast(open_time as string), 'yyyy-MM-dd HH:mm:ss') AS open_time,
    -- 2. level：直接引用原字段（若原表是 level 小写，去掉``反引号更稳妥）
    cast(level as string) AS level,
    cast(overall_score as decimal(3,2)) AS overall_score,
    cast(followers as int) AS followers,
    cast(monthly_sales as int) AS monthly_sales,
    case when is_premium = '1' then true else false end AS is_premium,
    cast(product_count as int) AS product_count,
    true AS is_valid,
    -- 3. update_time：当前时间转字符串格式
    date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') AS update_time
FROM ods_shops
WHERE ds = '20250801';

select * from dim_shops;

-- 3. 用户维度表（dim_users）
CREATE TABLE IF NOT EXISTS dim_users (
      user_id STRING COMMENT '用户ID',
      user_name STRING COMMENT '用户名',
      gender STRING COMMENT '性别',
      age INT COMMENT '年龄',
      age_group STRING COMMENT '年龄分组（如18-25岁）',
      city STRING COMMENT '所在城市',
      city_level STRING COMMENT '城市级别（一线/新一线/其他）',
      create_time string COMMENT '注册时间',
      user_age_days INT COMMENT '用户账龄（天）',
      score DECIMAL(6,2) COMMENT '用户积分',
      is_active BOOLEAN COMMENT '是否活跃（近30天有行为）',
      is_valid BOOLEAN COMMENT '是否有效（默认有效）',
      update_time string COMMENT '维度更新时间'
)COMMENT '用户维度表（精简版）'
partitioned by (`ds` string comment '统计日期')
STORED AS ORC
LOCATION 'hdfs://cdh01:8020//warehouse/ecommerce/dim/dim_users'
TBLPROPERTIES (
    'orc.compress' = 'SNAPPY',
    'transient_lastDdlTime' = '${current_timestamp}'
    );

-- 3. 用户维度表插入
INSERT OVERWRITE TABLE dim_users partition (ds='20250731')
SELECT
    u.user_id,
    u.username,
    u.gender,
    cast(u.age as int) as age,
    -- 补充年龄分组
    case
        when cast(u.age as int) < 18 then '未成年人'
        when cast(u.age as int) between 18 and 25 then '18-25岁'
        when cast(u.age as int) between 26 and 35 then '26-35岁'
        when cast(u.age as int) between 36 and 45 then '36-45岁'
        else '46岁以上'
        end as age_group,
    u.city,
    -- 补充城市级别
    case
        when u.city in ('北京','上海','广州','深圳') then '一线城市'
        else '其他城市'
        end as city_level,
    date_format(cast(u.register_time as string), 'yyyy-MM-dd HH:mm:ss') AS create_time,
    -- 计算用户账龄（天）- 修复括号缺失问题
    datediff(current_date(), to_date(cast(u.register_time as string))) as user_age_days,
    cast(u.credit_score as decimal(6,2)) as score,
    -- 判断近30天是否活跃（改用左连接+聚合判断）
    case when max(b.user_id) is not null then true else false end as is_active,
    true as is_valid,  -- 默认有效
    date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') AS update_time
FROM ods_users u
-- 左连接行为表，筛选近30天的活跃记录
         left join (
    select distinct user_id
    from ods_behaviors
    where to_date(cast(behavior_time as timestamp)) >= date_add(current_date(), -30)
      and ds = '20250731'
) b on u.user_id = b.user_id
WHERE u.ds = '20250731'
group by u.user_id, u.username, u.gender, u.age, u.city, u.register_time, u.credit_score;

select * from dim_users;

-- 4. 行为类型维度表（dim_behavior_types）
CREATE TABLE IF NOT EXISTS dim_behavior_types (
      behavior_type STRING COMMENT '行为类型编码',
      behavior_name STRING COMMENT '行为类型名称',
      behavior_desc STRING COMMENT '行为描述',
      behavior_value INT COMMENT '行为价值权重（如点击=1，购买=5）'
)COMMENT '用户行为类型维度表'
partitioned by (`ds` string comment '统计日期')
STORED AS ORC
LOCATION 'hdfs://cdh01:8020//warehouse/ecommerce/dim/dim_behavior_types'
TBLPROPERTIES (
    'orc.compress' = 'SNAPPY'
    );

-- 4. 行为类型维度表插入
INSERT OVERWRITE TABLE dim_behavior_types partition (ds='20250731')
SELECT
    behavior_type,
    -- 行为名称映射（原表已是中文，直接复用或微调）
    behavior_type as behavior_name,  -- 如果原表名称就是业务想要的，直接引用
    -- 行为描述（根据原表中文值匹配）
    case behavior_type
        when '点击' then '用户点击商品/页面'
        when '收藏' then '用户收藏商品'
        when '加购' then '用户将商品加入购物车'
        when '购买' then '用户购买商品'
        when '跳出' then '用户离开页面/流程'  -- 补充原表存在的“跳出”
        when '浏览' then '用户浏览商品/页面内容'  -- 补充原表存在的“浏览”
        when '评论' then '用户发表评论'  -- 若原表有“评论”可保留
        else '其他行为'
        end as behavior_desc,
    -- 行为价值权重（根据原表中文值匹配）
    case behavior_type
        when '点击' then 1
        when '收藏' then 2
        when '加购' then 3
        when '购买' then 5
        when '评论' then 2
        when '浏览' then 1  -- 补充“浏览”的权重，可根据业务调整
        when '跳出' then 0  -- 补充“跳出”的权重，可根据业务调整
        else 0
        end as behavior_value
FROM (
         select distinct behavior_type
         from ods_behaviors
         where ds = '20250731'
     ) t;

select * from dim_behavior_types;

-- 5. 页面类型维度表（dim_page_types）
CREATE TABLE IF NOT EXISTS dim_page_types (
      page_type STRING COMMENT '页面类型编码',
      page_name STRING COMMENT '页面类型名称',
      page_module STRING COMMENT '所属模块（如首页/商品详情页）'
)COMMENT '页面类型维度表'
partitioned by (`ds` string comment '统计日期')
STORED AS ORC
LOCATION 'hdfs://cdh01:8020//warehouse/ecommerce/dim/dim_page_types'
TBLPROPERTIES (
    'orc.compress' = 'SNAPPY'
    );

-- 5. 页面类型维度表插入
INSERT OVERWRITE TABLE dim_page_types partition (ds = '20250731')
SELECT
    page_type,
    -- 页面名称：原表已是中文，直接复用或微调（若需简化可自定义）
    CASE page_type
        WHEN '店铺首页' THEN '店铺首页'       -- 或简化为“首页”，根据需求定
        WHEN '商品详情页' THEN '商品详情页'
        WHEN '店铺分类页' THEN '分类页'       -- 原表“店铺分类页”对应“分类页”
        WHEN '购物车页面' THEN '购物车'
        WHEN '结算页面' THEN '结算页'
        WHEN '店铺活动页' THEN '活动页'       -- 补充原表“店铺活动页”
        WHEN '搜索结果页' THEN '搜索页'       -- 补充原表“搜索结果页”
        ELSE page_type
        END AS page_name,
    -- 所属模块：按原表中文分类归属
    CASE page_type
        WHEN '店铺首页' THEN '首页模块'
        WHEN '商品详情页' THEN '商品模块'
        WHEN '店铺分类页' THEN '分类模块'
        WHEN '购物车页面' THEN '交易模块'
        WHEN '结算页面' THEN '交易模块'
        WHEN '店铺活动页' THEN '营销模块'     -- 活动页归为营销模块
        WHEN '搜索结果页' THEN '搜索模块'     -- 搜索页归为搜索模块
        ELSE '其他模块'
        END AS page_module
FROM (
         SELECT DISTINCT page_type
         FROM ods_behaviors
         WHERE ds = '20250731'
     ) t;

select * from dim_page_types;

-- 6. 平台维度表（dim_platforms）
CREATE TABLE IF NOT EXISTS dim_platforms (
      platform STRING COMMENT '平台编码',
      platform_name STRING COMMENT '平台名称',
      device_type STRING COMMENT '设备类型（如安卓/苹果/PC）'
)COMMENT '平台维度表'
partitioned by (`ds` string comment '统计日期')
STORED AS ORC
    LOCATION 'hdfs://cdh01:8020//warehouse/ecommerce/dim/dim_platforms'
TBLPROPERTIES (
    'orc.compress' = 'SNAPPY'
    );

-- 6. 平台维度表插入
INSERT OVERWRITE TABLE dim_platforms partition (ds='20250731')
SELECT
    platform,
    -- 平台名称：直接复用原表中文，或简化（根据需求调整）
    CASE platform
        WHEN '移动网页' THEN '移动网页端'
        WHEN 'PC' THEN '电脑端'
        WHEN '平板' THEN '平板端'
        WHEN '移动APP' THEN '移动应用端'
        ELSE platform
        END AS platform_name,
    -- 设备类型：按中文平台归属
    CASE platform
        WHEN '移动网页' THEN '移动设备（网页）'
        WHEN 'PC' THEN '电脑设备'
        WHEN '平板' THEN '平板设备'
        WHEN '移动APP' THEN '移动设备（APP）'
        ELSE '未知设备'
        END AS device_type
FROM (
         SELECT DISTINCT platform
         FROM ods_behaviors
         WHERE ds = '20250731'
     ) t;

select * from dim_platforms;

