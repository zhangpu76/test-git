set hive.exec.mode.local.auto=True;
use shangpin;
-- 设置Hive执行参数
set hive.exec.mode.local.auto=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.auto.convert.join=true;

-- 1. 日期维度明细表
CREATE TABLE IF NOT EXISTS dwd_dim_date_info (
      date_id STRING COMMENT '日期ID,格式yyyyMMdd',
      year INT COMMENT '年份',
      month INT COMMENT '月份',
      day INT COMMENT '日期',
      weekday INT COMMENT '星期几(1-7)',
      week INT COMMENT '周次',
      is_holiday INT COMMENT '是否为节假日(1是0否)',
      is_weekend INT COMMENT '是否为周末(1是0否)',
      season string COMMENT '季节',
      quarter INT COMMENT '季度(1-4)',
      month_begin_flag INT COMMENT '是否月初(1是0否)',
      month_end_flag INT COMMENT '是否月末(1是0否)',
      create_time TIMESTAMP COMMENT '记录创建时间'
)
PARTITIONED BY (ds STRING COMMENT '分区日期，格式yyyyMMdd')
STORED AS ORC
LOCATION 'hdfs://cdh01:8020//warehouse/shangpin/dwd/dwd_dim_date_info'
TBLPROPERTIES (
    'orc.compress' = 'SNAPPY',
    'external.table.purge' = 'true'
    );

-- 插入数据到日期维度明细表
INSERT OVERWRITE TABLE dwd_dim_date_info PARTITION (ds='20250808')
SELECT
    concat(year,
           lpad(month, 2, '0'),
           lpad(day, 2, '0')) as date_id,
    cast(year as INT) as year,
    cast(month as INT) as month,
    cast(day as INT) as day,
    cast(weekday as INT) as weekday,
    cast(week as INT) as week,
    -- is_holiday 转换为 1/0
    case when is_holiday = '是' then 1 else 0 end as is_holiday,
    -- is_weekend 转换为 1/0
    case when is_weekend = '是' then 1 else 0 end as is_weekend,
    -- season 直接保留字符串，或按需转编码（这里演示保留原文，也可转数字编码）
    cast(season as string) as season,
    -- 季度计算
    case
        when cast(month as INT) between 1 and 3 then 1
        when cast(month as INT) between 4 and 6 then 2
        when cast(month as INT) between 7 and 9 then 3
        else 4
        end as quarter,
    -- 是否月初
    case when cast(day as INT) = 1 then 1 else 0 end as month_begin_flag,
    -- 是否月末
    case
        when (cast(month as INT) in (1,3,5,7,8,10,12) and cast(day as INT) = 31)
            or (cast(month as INT) in (4,6,9,11) and cast(day as INT) = 30)
            or (cast(month as INT) = 2 and cast(day as INT) in (28,29))
            then 1 else 0
        end as month_end_flag,
    current_timestamp() as create_time
FROM ods_dim_date
WHERE ds = '20250808';

select * from dwd_dim_date_info;

-- 2. 商品维度明细表
CREATE TABLE IF NOT EXISTS dwd_dim_product_info (
      product_id STRING COMMENT '商品ID',
      product_name STRING COMMENT '商品名称',
      category STRING COMMENT '商品大类',
      sub_category STRING COMMENT '商品子类',
      brand STRING COMMENT '商品品牌',
      create_time TIMESTAMP COMMENT '商品创建时间',
      status STRING COMMENT '商品状态',
      status_code INT COMMENT '商品状态编码',
      origin_price DECIMAL(10,2) COMMENT '商品原价',
      sales_volume INT COMMENT '商品销量',
      online_days INT COMMENT '商品在线天数',
      category_level1_code STRING COMMENT '一级分类编码',
      category_level2_code STRING COMMENT '二级分类编码',
      brand_code STRING COMMENT '品牌编码',
      is_delete INT COMMENT '是否删除(1是0否)',
      update_time TIMESTAMP COMMENT '记录更新时间',
      create_time_dwd TIMESTAMP COMMENT 'DWD层记录创建时间'
)
PARTITIONED BY (ds STRING COMMENT '分区日期，格式yyyyMMdd')
STORED AS ORC
LOCATION 'hdfs://cdh01:8020//warehouse/shangpin/dwd/dwd_dim_product_info'
TBLPROPERTIES (
    'orc.compress' = 'SNAPPY',
    'external.table.purge' = 'true'
    );

-- 插入数据到商品维度明细表
INSERT OVERWRITE TABLE dwd_dim_product_info PARTITION (ds='20250808')
SELECT
    product_id,
    trim(product_name) as product_name,
    category,
    sub_category,
    brand,
    -- create_time 处理：提取年月日
    case
        when create_time = '' then null
        else date_format(create_time, 'yyyy-MM-dd')
        end as create_time,
    status,
    case
        when status = '上架' then 1
        when status = '下架' then 0
        when status = '库存不足' then 2
        else 99
        end as status_code,
    cast(origin_price as DECIMAL(10,2)) as origin_price,
    cast(nvl(sales_volume, 0) as INT) as sales_volume,
    cast(nvl(online_days, 0) as INT) as online_days,
    -- 一级分类编码：用hash生成（示例，可根据需求调整）
    category as category_level1_code,
    -- 直接使用 sub_category 的值作为二级分类编码（中文）
    sub_category as category_level2_code,
    brand as brand_code,
    0 as is_delete,
    current_timestamp() as update_time,
    current_timestamp() as create_time_dwd
FROM ods_dim_product_base
WHERE ds = '20250808';

select * from dwd_dim_product_info;

-- 3. SKU维度明细表
CREATE TABLE IF NOT EXISTS dwd_dim_sku_info (
      sku_id STRING COMMENT 'SKU ID',
      product_id STRING COMMENT '所属商品ID',
      color STRING COMMENT '颜色',
      color_code STRING COMMENT '颜色编码',
      spec1 STRING COMMENT '规格1',
      spec2 STRING COMMENT '规格2',
      spec_comb STRING COMMENT '规格组合',
      sku_price DECIMAL(10,2) COMMENT 'SKU单价',
      stock INT COMMENT '库存数量',
      sku_status STRING COMMENT 'SKU状态',
      sku_status_code INT COMMENT 'SKU状态编码',
      stock_warning_flag INT COMMENT '库存预警标识(1是0否)',
      is_delete INT COMMENT '是否删除(1是0否)',
      update_time TIMESTAMP COMMENT '记录更新时间',
      create_time_dwd TIMESTAMP COMMENT 'DWD层记录创建时间'
)
PARTITIONED BY (ds STRING COMMENT '分区日期，格式yyyyMMdd')
STORED AS ORC
LOCATION 'hdfs://cdh01:8020//warehouse/shangpin/dwd/dwd_dim_sku_info'
TBLPROPERTIES (
    'orc.compress' = 'SNAPPY',
    'external.table.purge' = 'true'
    );

-- 插入数据到SKU维度明细表
INSERT OVERWRITE TABLE dwd_dim_sku_info PARTITION (ds='20250808')
SELECT
    sku_id,
    product_id,
    color,
    -- 颜色编码映射
    case color
        when '红色' then 'RED'
        when '蓝色' then 'BLUE'
        when '黑色' then 'BLACK'
        when '白色' then 'WHITE'
        else upper(substr(color, 1, 3))
        end as color_code,
    spec1,
    spec2,
    concat_ws(';', spec1, spec2) as spec_comb,
    cast(sku_price as DECIMAL(10,2)) as sku_price,
    cast(nvl(stock, 0) as INT) as stock,
    sku_status,
    case
        when sku_status = '正常' then 1
        when sku_status = '缺货' then 0
        when sku_status = '停售' then 2
        else 99
        end as sku_status_code,
    -- 库存低于10则预警
    case when cast(nvl(stock, 0) as INT) < 10 then 1 else 0 end as stock_warning_flag,
    0 as is_delete,
    current_timestamp() as update_time,
    current_timestamp() as create_time_dwd
FROM ods_dim_sku
WHERE ds = '20250808';

select * from dwd_dim_sku_info;

-- 4. 商品价格促销事实明细表
CREATE TABLE IF NOT EXISTS dwd_fact_price_promo_detail (
      id STRING COMMENT '记录ID',
      product_id STRING COMMENT '商品ID',
      actual_price DECIMAL(10,2) COMMENT '实际售价',
      original_price DECIMAL(10,2) COMMENT '原价',
      price_diff DECIMAL(10,2) COMMENT '价差',
      discount_rate DECIMAL(5,2) COMMENT '折扣率',
      price_band STRING COMMENT '价格带',
      price_band_code INT COMMENT '价格带编码',
      is_promo INT COMMENT '是否促销(1是0否)',
      promo_type STRING COMMENT '促销类型',
      promo_type_code INT COMMENT '促销类型编码',
      discount DECIMAL(5,2) COMMENT '折扣力度',
      promo_start_date TIMESTAMP COMMENT '促销开始日期',
      promo_end_date TIMESTAMP COMMENT '促销结束日期',
      promo_days INT COMMENT '促销天数',
      is_promo_valid INT COMMENT '当前是否在促销期(1是0否)',
      create_time_dwd TIMESTAMP COMMENT 'DWD层记录创建时间'
)
PARTITIONED BY (ds STRING COMMENT '分区日期，格式yyyyMMdd')
STORED AS ORC
LOCATION 'hdfs://cdh01:8020//warehouse/shangpin/dwd/dwd_fact_price_promo_detail'
TBLPROPERTIES (
    'orc.compress' = 'SNAPPY',
    'external.table.purge' = 'true'
    );

-- 插入数据到商品价格促销事实明细表
INSERT OVERWRITE TABLE dwd_fact_price_promo_detail PARTITION (ds='20250808')
SELECT
    id,
    product_id,
    cast(actual_price as DECIMAL(10,2)) as actual_price,
    cast(original_price as DECIMAL(10,2)) as original_price,
    -- 计算价差
    cast(original_price as DECIMAL(10,2)) - cast(actual_price as DECIMAL(10,2)) as price_diff,
    -- 计算折扣率
    case when cast(original_price as DECIMAL(10,2)) = 0 then 0
         else cast(actual_price as DECIMAL(10,2)) / cast(original_price as DECIMAL(10,2))
        end as discount_rate,
    price_band,
    -- 价格带编码
    case
        when price_band = '0-100元' then 1
        when price_band = '101-300元' then 2
        when price_band = '301-500元' then 3
        when price_band = '501-1000元' then 4
        when price_band = '1000元以上' then 5
        else 0
        end as price_band_code,
    case when is_promo = '是' then 1 else 0 end as is_promo,
    promo_type,
    -- 促销类型编码
    case
        when promo_type = '满减' then 1
        when promo_type = '折扣' then 2
        when promo_type = '买赠' then 3
        when promo_type = '秒杀' then 4
        when promo_type = '买一送一' then 5
        when promo_type = '优惠券' then 6
        when promo_type = '会员专享' then 7
        when promo_type = '拼团优惠' then 8
        when promo_type = '新品特惠' then 9
        when promo_type = '无' then 10
        when promo_type = '清仓特惠' then 11
        when promo_type = '满赠' then 12
        when promo_type = '秒杀活动' then 13
        when promo_type = '积分兑换' then 14
        when promo_type = '第二件半价' then 15
        when promo_type = '组合优惠' then 16
        when promo_type = '跨店满减' then 17
        when promo_type = '限时折扣' then 18
        when promo_type is null then 0
        else 0
        end as promo_type_code,
    cast(discount as DECIMAL(5,2)) as discount,
    case
        when promo_start_date = '' then null
        else date_format(promo_start_date, 'yyyy-MM-dd')
        end as promo_start_date,
    case
        when promo_end_date = '' then null
        else date_format(promo_end_date, 'yyyy-MM-dd')
        end as promo_end_date,
    -- 计算促销天数
    datediff(to_date(promo_end_date), to_date(promo_start_date)) + 1 as promo_days,
    -- 判断当前是否在促销期
    case when current_date() between to_date(promo_start_date) and to_date(promo_end_date)
             then 1 else 0 end as is_promo_valid,
    current_timestamp() as create_time_dwd
FROM ods_fact_price_promo
WHERE ds = '20250808';

select * from dwd_fact_price_promo_detail;

-- 5. 销售流量事实明细表
CREATE TABLE IF NOT EXISTS dwd_fact_sales_traffic_detail (
      id STRING COMMENT '记录ID',
      product_id STRING COMMENT '商品ID',
      sku_id STRING COMMENT 'SKU ID',
      channel STRING COMMENT '销售渠道',
      channel_code INT COMMENT '渠道编码',
      pv INT COMMENT '页面浏览量',
      uv INT COMMENT '独立访客数',
      click_cnt INT COMMENT '点击次数',
      click_rate DECIMAL(10,4) COMMENT '点击率',
      add_cart_cnt INT COMMENT '加购次数',
      add_cart_rate DECIMAL(10,4) COMMENT '加购率',
      collect_cnt INT COMMENT '收藏次数',
      collect_rate DECIMAL(10,4) COMMENT '收藏率',
      pay_cnt INT COMMENT '支付次数',
      pay_rate DECIMAL(10,4) COMMENT '支付转化率',
      refund_cnt INT COMMENT '退款次数',
      refund_rate DECIMAL(10,4) COMMENT '退款率',
      conversion_rate DECIMAL(10,4) COMMENT '整体转化率',
      create_time_dwd TIMESTAMP COMMENT 'DWD层记录创建时间'
)
PARTITIONED BY (ds STRING COMMENT '分区日期，格式yyyyMMdd')
STORED AS ORC
LOCATION 'hdfs://cdh01:8020//warehouse/shangpin/dwd/dwd_fact_sales_traffic_detail'
TBLPROPERTIES (
    'orc.compress' = 'SNAPPY',
    'external.table.purge' = 'true'
    );

-- 插入数据到销售流量事实明细表
INSERT OVERWRITE TABLE dwd_fact_sales_traffic_detail PARTITION (ds='20250808')
SELECT
    id,
    product_id,
    sku_id,
    channel,
    -- 渠道编码
    case
        when channel = '京东自营' then 1
        when channel = '品牌官网' then 2
        when channel = '天猫旗舰店' then 3
        when channel = '快手小店' then 4
        when channel = '抖音直播' then 5
        when channel = '拼多多' then 6
        when channel = '线下门店' then 7
        when channel is null then 0
        else 0
        end as channel_code,
    cast(nvl(pv, 0) as INT) as pv,
    cast(nvl(uv, 0) as INT) as uv,
    cast(nvl(click_cnt, 0) as INT) as click_cnt,
    -- 点击率 = 点击次数/浏览量
    case when cast(nvl(pv, 0) as INT) = 0 then 0
         else cast(nvl(click_cnt, 0) as DECIMAL(10,4)) / cast(nvl(pv, 0) as DECIMAL(10,4))
        end as click_rate,
    cast(nvl(add_cart_cnt, 0) as INT) as add_cart_cnt,
    -- 加购率 = 加购次数/点击次数
    case when cast(nvl(click_cnt, 0) as INT) = 0 then 0
         else cast(nvl(add_cart_cnt, 0) as DECIMAL(10,4)) / cast(nvl(click_cnt, 0) as DECIMAL(10,4))
        end as add_cart_rate,
    cast(nvl(collect_cnt, 0) as INT) as collect_cnt,
    -- 收藏率 = 收藏次数/点击次数
    case when cast(nvl(click_cnt, 0) as INT) = 0 then 0
         else cast(nvl(collect_cnt, 0) as DECIMAL(10,4)) / cast(nvl(click_cnt, 0) as DECIMAL(10,4))
        end as collect_rate,
    cast(nvl(pay_cnt, 0) as INT) as pay_cnt,
    -- 支付转化率 = 支付次数/加购次数
    case when cast(nvl(add_cart_cnt, 0) as INT) = 0 then 0
         else cast(nvl(pay_cnt, 0) as DECIMAL(10,4)) / cast(nvl(add_cart_cnt, 0) as DECIMAL(10,4))
        end as pay_rate,
    cast(nvl(refund_cnt, 0) as INT) as refund_cnt,
    -- 退款率 = 退款次数/支付次数
    case when cast(nvl(pay_cnt, 0) as INT) = 0 then 0
         else cast(nvl(refund_cnt, 0) as DECIMAL(10,4)) / cast(nvl(pay_cnt, 0) as DECIMAL(10,4))
        end as refund_rate,
    cast(conversion_rate as DECIMAL(10,4)) as conversion_rate,
    current_timestamp() as create_time_dwd
FROM ods_fact_sales_traffic
WHERE ds = '20250808';

select * from dwd_fact_sales_traffic_detail;

-- 6. 用户评论事实明细表
CREATE TABLE IF NOT EXISTS dwd_fact_user_review_detail (
      id STRING COMMENT '评论ID',
      product_id STRING COMMENT '商品ID',
      sku_id STRING COMMENT 'SKU ID',
      user_id STRING COMMENT '用户ID',
      user_age INT COMMENT '用户年龄',
      age_group STRING COMMENT '年龄组',
      user_gender STRING COMMENT '用户性别',
      user_gender_code INT COMMENT '性别编码',
      user_city STRING COMMENT '用户所在城市',
      city_level INT COMMENT '城市等级',
      user_behavior_type STRING COMMENT '用户行为类型',
      behavior_type_code INT COMMENT '行为类型编码',
      eval_score INT COMMENT '评价分数',
      eval_level STRING COMMENT '评价等级',
      eval_content STRING COMMENT '评价内容',
      content_length INT COMMENT '评价内容长度',
      positive_tags ARRAY<STRING> COMMENT '正面标签',
      negative_tags ARRAY<STRING> COMMENT '负面标签',
      positive_count INT COMMENT '正面标签数量',
      negative_count INT COMMENT '负面标签数量',
      is_verified_purchase INT COMMENT '是否为已验证购买(1是0否)',
      create_time_dwd TIMESTAMP COMMENT 'DWD层记录创建时间'
)
PARTITIONED BY (ds STRING COMMENT '分区日期，格式yyyyMMdd')
STORED AS ORC
LOCATION 'hdfs://cdh01:8020//warehouse/shangpin/dwd/dwd_fact_user_review_detail'
TBLPROPERTIES (
    'orc.compress' = 'SNAPPY',
    'external.table.purge' = 'true'
    );

-- 插入数据到用户评论事实明细表
INSERT OVERWRITE TABLE dwd_fact_user_review_detail PARTITION (ds='20250808')
SELECT
    id,
    product_id,
    sku_id,
    user_id,
    cast(nvl(user_age, 0) as INT) as user_age,
    -- 年龄分组
    case
        when cast(nvl(user_age, 0) as INT) between 0 and 17 then '未成年'
        when cast(nvl(user_age, 0) as INT) between 18 and 24 then '18-24岁'
        when cast(nvl(user_age, 0) as INT) between 25 and 30 then '25-30岁'
        when cast(nvl(user_age, 0) as INT) between 31 and 40 then '31-40岁'
        when cast(nvl(user_age, 0) as INT) between 41 and 50 then '41-50岁'
        when cast(nvl(user_age, 0) as INT) > 50 then '50岁以上'
        else '未知'
        end as age_group,
    user_gender,
    case
        when user_gender = '男' then 1
        when user_gender = '女' then 2
        else 0
        end as user_gender_code,
    user_city,
    -- 城市等级划分(示例)
    case
        when user_city in ('北京', '上海', '广州', '深圳') then 1
        when length(user_city) > 0 and user_city not in ('北京', '上海', '广州', '深圳') then 2
        else 0
        end as city_level,
    user_behavior_type,
    case
        when user_behavior_type = '加入购物车' then 1
        when user_behavior_type = '搜索' then 2
        when user_behavior_type = '收藏' then 3
        when user_behavior_type = '浏览' then 4
        when user_behavior_type = '评价' then 5
        when user_behavior_type = '购买' then 6
        when user_behavior_type = '退货' then 7
        when user_behavior_type is null then 0  -- 空值处理
        else 0
        end as behavior_type_code,
    cast(eval_score as INT) as eval_score,
    -- 评价等级
    case
        when cast(eval_score as INT) = 7 then '非常好'
        when cast(eval_score as INT) = 6 then '很好'
        when cast(eval_score as INT) = 5 then '好'
        when cast(eval_score as INT) = 4 then '一般'
        when cast(eval_score as INT) = 3 then '非常差'
        when cast(eval_score as INT) = 2 then '很差'
        when cast(eval_score as INT) = 1 then '非常差'
        else '无评分'
        end as eval_level,
    trim(eval_content) as eval_content,
    length(trim(eval_content)) as content_length,
    -- 将正面标签字符串转换为数组
    split(regexp_replace(positive_tags, '[\\[\\]]', ''), ',') as positive_tags,
    -- 将负面标签字符串转换为数组
    split(regexp_replace(negative_tags, '[\\[\\]]', ''), ',') as negative_tags,
    size(split(regexp_replace(positive_tags, '[\\[\\]]', ''), ',')) as positive_count,
    size(split(regexp_replace(negative_tags, '[\\[\\]]', ''), ',')) as negative_count,
    case when is_verified_purchase = '是' then 1 else 0 end as is_verified_purchase,
    current_timestamp() as create_time_dwd
FROM ods_fact_user_review
WHERE ds = '20250808';

select eval_content from dwd_fact_user_review_detail group by eval_content;