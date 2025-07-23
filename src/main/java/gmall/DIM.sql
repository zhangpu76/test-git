set hive.exec.mode.local.auto=true;
use gmall;

drop table if exists dim_sku;
create external table dim_sku
(
    `id`                   string COMMENT 'SKU_ID',
    `price`                decimal(16, 2) COMMENT '商品价格',
    `sku_name`             string COMMENT '商品名称',
    `sku_desc`             string COMMENT '商品描述',
    `weight`               decimal(16, 2) COMMENT '重量',
    `is_sale`              boolean COMMENT '是否在售',
    `spu_id`               string COMMENT 'SPU编号',
    `spu_name`             string COMMENT 'SPU名称',
    `category3_id`         string COMMENT '三级品类ID',
    `category3_name`       string COMMENT '三级品类名称',
    `category2_id`         string COMMENT '二级品类id',
    `category2_name`       string COMMENT '二级品类名称',
    `category1_id`         string COMMENT '一级品类ID',
    `category1_name`       string COMMENT '一级品类名称',
    `tm_id`                  string COMMENT '品牌ID',
    `tm_name`               string COMMENT '品牌名称',
    `sku_attr_values`      array<struct<attr_id :string,
                                        value_id :string,
                                        attr_name :string,
                                        value_name:string>> COMMENT '平台属性',
    `sku_sale_attr_values` array<struct<sale_attr_id :string,
                                        sale_attr_value_id :string,
                                        sale_attr_name :string,
                                        sale_attr_value_name:string>> COMMENT '销售属性',
    `create_time`          string COMMENT '创建时间'
) COMMENT '商品维度表'
partitioned by (`ds` string)
stored as orc
location 'hdfs://cdh01:8020/bigdata_warehouse/gmall/dim/dim_sku/'
tblproperties (
    'orc.compress' = 'SNAPPY',
    'external.table.purge' = 'true'
    );

--跟商品有关系
--主维表：sku
--相关维表：spu c3 c2 c1 tm
--加载数据
with
    sku as
        (
            select
                id,
                price,
                sku_name,
                sku_desc,
                weight,
                is_sale,
                spu_id,
                category3_id,
                tm_id,
                create_time
            from ods_sku_info
            where ds='20250629'
        ),
    spu as
        (
            select
                id,
                spu_name
            from ods_spu_info
            where ds='20250629'
        ),
    c3 as
        (
            select
                id,
                name,
                category2_id
            from ods_base_category3
            where ds='20250629'
        ),
    c2 as
        (
            select
                id,
                name,
                category1_id
            from ods_base_category2
            where ds='20250629'
        ),
    c1 as
        (
            select
                id,
                name
            from ods_base_category1
            where ds='20250629'
        ),
    tm as
        (
            select
                id,
                tm_name
            from ods_base_trademark
            where ds='20250629'
        ),
    attr as
        (
            select
                sku_id,
                collect_set(named_struct('attr_id',cast(attr_id as string),'value_id',cast(value_id as string),'attr_name',attr_name,'value_name',value_name)) attrs
            from ods_sku_attr_value
            where ds='20250629'
            group by sku_id
        ),
    sale_attr as
        (
            select
                sku_id,
                collect_set(named_struct('sale_attr_id',cast(sale_attr_id as string),'sale_attr_value_id',cast(sale_attr_value_id as string),'sale_attr_name',sale_attr_name,'sale_attr_value_name',sale_attr_value_name)) sale_attrs
            from ods_sku_sale_attr_value
            where ds='20250629'
            group by sku_id
        )
insert overwrite table dim_sku partition(ds='20250629')
select
    sku.id,
    sku.price,
    sku.sku_name,
    sku.sku_desc,
    sku.weight,
    sku.is_sale,
    sku.spu_id,
    spu.spu_name,
    sku.category3_id,
    c3.name,
    c3.category2_id,
    c2.name,
    c2.category1_id,
    c1.name,
    sku.tm_id,
    tm.tm_name,
    attr.attrs,
    sale_attr.sale_attrs,
    sku.create_time
from sku
         left join spu on sku.spu_id=spu.id
         left join c3 on sku.category3_id=c3.id
         left join c2 on c3.category2_id=c2.id
         left join c1 on c2.category1_id=c1.id
         left join tm on sku.tm_id=tm.id
         left join attr on sku.id=attr.sku_id
         left join sale_attr on sku.id=sale_attr.sku_id;
select * from dim_sku;

--- 优惠劵
-- 主维表:coupon_info
-- 相关维表:coupon_range,base_dic
-- 建表语句

drop table if exists dim_coupon;
create external table dim_coupon
(
    `id`                  string COMMENT '优惠券编号',
    `coupon_name`       string COMMENT '优惠券名称',
    `coupon_type_code` string COMMENT '优惠券类型编码',
    `coupon_type_name` string COMMENT '优惠券类型名称',
    `condition_amount` decimal(16, 2) COMMENT '满额数',
    `condition_num`     bigint COMMENT '满件数',
    `activity_id`       string COMMENT '活动编号',
    `benefit_amount`   decimal(16, 2) COMMENT '减免金额',
    `benefit_discount` decimal(16, 2) COMMENT '折扣',
    `benefit_rule`     string COMMENT '优惠规则:满元*减*元，满*件打*折',
    `create_time`       string COMMENT '创建时间',
    `range_type_code`  string COMMENT '优惠范围类型编码',
    `range_type_name`  string COMMENT '优惠范围类型名称',
    `limit_num`         bigint COMMENT '最多领取次数',
    `taken_count`       bigint COMMENT '已领取次数',
    `start_time`        string COMMENT '可以领取的开始时间',
    `end_time`          string COMMENT '可以领取的结束时间',
    `operate_time`      string COMMENT '修改时间',
    `expire_time`       string COMMENT '过期时间'
) COMMENT '优惠券维度表'
partitioned by (`ds` string)
stored as orc
location 'hdfs://cdh01:8020/bigdata_warehouse/gmall/dim/dim_coupon/'
tblproperties (
'orc.compress' = 'SNAPPY',
'external.table.purge' = 'true'
);

--加载数据
insert overwrite table dim_coupon partition(ds='20250629')
select
    `id`               ,
    `coupon_name`      ,
    `coupon_type_code` ,
    base_dic.dic_name coupon_type_name ,
    `condition_amount` ,
    `condition_num`    ,
    `activity_id`      ,
    `benefit_amount`   ,
    `benefit_discount` ,
    `benefit_rule`     ,
    `create_time`      ,
    `range_type_code`  ,
    base_dic.dic_name `range_type_name`  ,
    `limit_num`        ,
    `taken_count`      ,
    `start_time`       ,
    `end_time`         ,
    `operate_time`     ,
    `expire_time`
from
    (
        select
            `id`               ,
            `coupon_name`      ,
            coupon_type `coupon_type_code` ,
            `condition_amount` ,
            `condition_num`    ,
            `activity_id`      ,
            `benefit_amount`   ,
            `benefit_discount` ,
            case coupon_type
                when '3201' then concat('满',condition_amount,'元减',benefit_amount,'元')
                when '3202' then concat('满',condition_num,'件打', benefit_discount,' 折')
                when '3203' then concat('减',benefit_amount,'元')
                end benefit_rule,
            `create_time`      ,
            range_type `range_type_code`  ,
            `limit_num`        ,
            `taken_count`      ,
            `start_time`       ,
            `end_time`         ,
            `operate_time`     ,
            `expire_time`
        from ods_coupon_info where ds = "20250629"
    ) coupon_info
        left join
    (
        select
            dic_code,
            dic_name
        from ods_base_dic where ds = "20250629" and parent_code='32'
    ) base_dic
    on  coupon_info.coupon_type_code  = base_dic.dic_code
        left join
    (
        select
            dic_code,
            dic_name
        from ods_base_dic where ds = "20250629"  and parent_code='33'
    ) base_dic2
    on  coupon_info.range_type_code  = base_dic2.dic_code;

select * from dim_coupon;

-- 活动表
-- 主维表： activity_rule
-- 相关维表：  activity_info  activity_sku  base_dic
-- 建表语句
drop table if exists dim_activity;
create external table dim_activity
(
    `activity_rule_id`   string COMMENT '活动规则ID',
    `activity_id`         string COMMENT '活动ID',
    `activity_name`       string COMMENT '活动名称',
    `activity_type_code` string COMMENT '活动类型编码',
    `activity_type_name` string COMMENT '活动类型名称',
    `activity_desc`       string COMMENT '活动描述',
    `start_time`           string COMMENT '开始时间',
    `end_time`             string COMMENT '结束时间',
    `create_time`          string COMMENT '创建时间',
    `condition_amount`    decimal(16, 2) COMMENT '满减金额',
    `condition_num`       bigint COMMENT '满减件数',
    `benefit_amount`      decimal(16, 2) COMMENT '优惠金额',
    `benefit_discount`   decimal(16, 2) COMMENT '优惠折扣',
    `benefit_rule`        string COMMENT '优惠规则',
    `benefit_level`       string COMMENT '优惠级别'
) COMMENT '活动维度表'
    partitioned by (`ds` string)
    stored as orc
    location 'hdfs://cdh01:8020/bigdata_warehouse/gmall/dim/dim_activity/'
    tblproperties (
    'orc.compress' = 'SNAPPY',
    'external.table.purge' = 'true'
    );


-- 加载数据
insert overwrite table dim_activity partition(ds='20250629')
select
    rule.id,
    info.id,
    activity_name,
    rule.activity_type,
    dic.dic_name,
    activity_desc,
    start_time,
    end_time,
    create_time,
    condition_amount,
    condition_num,
    benefit_amount,
    benefit_discount,
    case rule.activity_type
        when '3101' then concat('满',condition_amount,'元减',benefit_amount,'元')
        when '3102' then concat('满',condition_num,'件打', benefit_discount,' 折')
        when '3103' then concat('打', benefit_discount,'折')
        end benefit_rule,
    benefit_level
from
    (
        select
            id,
            activity_id,
            activity_type,
            condition_amount,
            condition_num,
            benefit_amount,
            benefit_discount,
            benefit_level
        from ods_activity_rule
        where ds='20250629'
    )rule
        left join
    (
        select
            id,
            activity_name,
            activity_type,
            activity_desc,
            start_time,
            end_time,
            create_time
        from ods_activity_info
        where ds='20250629'
    )info
    on rule.activity_id=info.id
        left join
    (
        select
            dic_code,
            dic_name
        from ods_base_dic
        where ds='20250629'
          and parent_code='31'
    )dic
    on rule.activity_type=dic.dic_code;
select * from dim_activity;

---地区维度表
drop table if exists dim_province;
create external table dim_province
(
    `id`              string COMMENT '省份ID',
    `province_name` string COMMENT '省份名称',
    `area_code`     string COMMENT '地区编码',
    `iso_code`      string COMMENT '旧版国际标准地区编码，供可视化使用',
    `iso_3166_2`    string COMMENT '新版国际标准地区编码，供可视化使用',
    `region_id`     string COMMENT '地区ID',
    `region_name`   string COMMENT '地区名称'
) COMMENT '地区维度表'
    partitioned by (`ds` string)
    stored as orc
    location 'hdfs://cdh01:8020/bigdata_warehouse/gmall/dim/dim_province/'
    tblproperties (
    'orc.compress' = 'SNAPPY',
    'external.table.purge' = 'true'
    );

-- 加载数据
insert overwrite table dim_province partition(ds='20250629')
select
    province.id,
    province.name,
    province.area_code,
    province.iso_code,
    province.iso_3166_2,
    region_id,
    region_name
from
    (
        select
            id,
            name,
            region_id,
            area_code,
            iso_code,
            iso_3166_2
        from ods_base_province
        where ds='20250629'
    )province
        left join
    (
        select
            id,
            region_name
        from ods_base_region
        where ds='20250629'
    )region
    on province.region_id=region.id;
select * from dim_province;
-- 营销坑位
-- 主维表：dim_promotion_pos
drop table if exists dim_promotion_pos;
create external table dim_promotion_pos
(
    `id`                 string COMMENT '营销坑位ID',
    `pos_location`     string COMMENT '营销坑位位置',
    `pos_type`          string COMMENT '营销坑位类型 ',
    `promotion_type`   string COMMENT '营销类型',
    `create_time`       string COMMENT '创建时间',
    `operate_time`      string COMMENT '修改时间'
) COMMENT '营销坑位维度表'
    partitioned by (`ds` string)
    stored as orc
    location 'hdfs://cdh01:8020/bigdata_warehouse/gmall/dim/dim_promotion_pos/'
    tblproperties (
    'orc.compress' = 'SNAPPY',
    'external.table.purge' = 'true'
    );

--加载数据
insert overwrite table dim_promotion_pos partition(ds='20250629')
select
    `id`,
    `pos_location`,
    `pos_type`,
    `promotion_type`,
    `create_time`,
    `operate_time`
from ods_promotion_pos
where ds='20250629';
select * from ods_promotion_pos;

--营销渠道
drop table if exists dim_promotion_refer;
create external table dim_promotion_refer
(
    `id`                    string COMMENT '营销渠道ID',
    `refer_name`          string COMMENT '营销渠道名称',
    `create_time`         string COMMENT '创建时间',
    `operate_time`        string COMMENT '修改时间'
) COMMENT '营销渠道维度表'
    partitioned by (`ds` string)
    stored as orc
    location 'hdfs://cdh01:8020/bigdata_warehouse/gmall/dim/dim_promotion_refer/'
    tblproperties (
    'orc.compress' = 'SNAPPY',
    'external.table.purge' = 'true'
    );
--加载数据
insert overwrite table dim_promotion_refer partition(ds='20250629')
select
    `id`,
    `refer_name`,
    `create_time`,
    `operate_time`
from ods_promotion_refer
where ds='20250629';
select * from dim_promotion_refer;
--日期表
drop table if exists dim_date;
create external table dim_date
(
    `date_id`    string COMMENT '日期ID',
    `week_id`    string COMMENT '周ID,一年中的第几周',
    `week_day`   string COMMENT '周几',
    `day`         string COMMENT '每月的第几天',
    `month`       string COMMENT '一年中的第几月',
    `quarter`    string COMMENT '一年中的第几季度',
    `year`        string COMMENT '年份',
    `is_workday` string COMMENT '是否是工作日',
    `holiday_id` string COMMENT '节假日'
) COMMENT '日期维度表'
    stored as orc
    location 'hdfs://cdh01:8020/bigdata_warehouse/gmall/dim/dim_date/'
    tblproperties (
    'orc.compress' = 'SNAPPY',
    'external.table.purge' = 'true'
    );
-- 先创建临时表
drop table if exists tmp_dim_date_info;
create external table tmp_dim_date_info (
                                            `date_id`       string COMMENT '日',
                                            `week_id`       string COMMENT '周ID',
                                            `week_day`      string COMMENT '周几',
                                            `day`            string COMMENT '每月的第几天',
                                            `month`          string COMMENT '第几月',
                                            `quarter`       string COMMENT '第几季度',
                                            `year`           string COMMENT '年',
                                            `is_workday`    string COMMENT '是否是工作日',
                                            `holiday_id`    string COMMENT '节假日'
) COMMENT '时间维度表'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
location 'hdfs://cdh01:8020/bigdata_warehouse/gmall/tmp/tmp_dim_date_info/';
load data  inpath '/bigdata_warehouse/gmall/tmp/tmp_dim_date_info/date_info.txt' into table tmp_dim_date_info;


-- 通过查询插入的方式在做
insert overwrite table dim_date select * from tmp_dim_date_info;

-- 用户表
-- 特点：变化缓慢，量大
-- 做全量表数据冗余，咱们用拉链表
-- 拉链表对于采集先全量一次，后面没一天都是增量和更新数据
create external table dim_user_zip
(
    `id`           string COMMENT '用户ID',
    `name`         string COMMENT '用户姓名',
    `phone_num`    string COMMENT '手机号码',
    `email`        string COMMENT '邮箱',
    `user_level`   string COMMENT '用户等级',
    `birthday`     string COMMENT '生日',
    `gender`       string COMMENT '性别',
    `create_time`  string COMMENT '创建时间',
    `operate_time` string COMMENT '操作时间',
    `start_date`   string COMMENT '开始日期',
    `end_date`     string COMMENT '结束日期'
) COMMENT '用户维度表'
    partitioned by (`ds` string)
    stored as orc
    location 'hdfs://cdh01:8020/bigdata_warehouse/gmall/dim/dim_user_zip/'
    tblproperties (
    'orc.compress' = 'SNAPPY',
    'external.table.purge' = 'true'
    );

--第一次数据装载
insert overwrite table dim_user_zip partition (ds = '20250629')
select
    id,
    concat(substr(name, 1, 1), '*')                name,
    if(phone_num regexp '^(13[0-9]|14[01456879]|15[0-35-9]|16[2567]|17[0-8]|18[0-9]|19[0-35-9])\\d{8}$',
       concat(substr(phone_num, 1, 3), '*'), null) phone_num,
    if(email regexp '^[a-zA-Z0-9_-]+@[a-zA-Z0-9_-]+(\\.[a-zA-Z0-9_-]+)+$',
       concat('*@', split(email, '@')[1]), null)   email,
    user_level,
    birthday,
    gender,
    create_time,
    operate_time,
    '20250629'                                        start_date,
    '9999-12-31'                                        end_date
from ods_user_info
where ds = '20250629';

select * from dim_user_zip;
--每日做拉链
--9999分区数据
-- 开启动态分区
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;

-- 插入每日变更数据
INSERT OVERWRITE TABLE dim_user_zip PARTITION (ds)
SELECT
    id,
    name,
    phone_num,
    email,
    user_level,
    birthday,
    gender,
    create_time,
    operate_time,
    start_date,
    end_date,
    CASE
        WHEN rn = 1 THEN '9999-12-31'
        ELSE '20250629'
        END AS dt
FROM (
         SELECT
             id,
             -- 脱敏处理
             CASE WHEN name IS NOT NULL THEN CONCAT(SUBSTRING(name, 1, 1), '*') ELSE NULL END AS name,
             CASE
                 WHEN phone_num RLIKE '^1[3-9][0-9]{9}$' THEN CONCAT(SUBSTRING(phone_num, 1, 3), '****', SUBSTRING(phone_num, 8))
                 ELSE NULL
                 END AS phone_num,
             CASE
                 WHEN email RLIKE '^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\\.[a-zA-Z0-9-.]+$' THEN CONCAT('***@', SPLIT(email, '@')[1])
                 ELSE NULL
                 END AS email,
             user_level,
             birthday,
             gender,
             create_time,
             operate_time,
             '20250629' AS start_date,
             CASE
                 WHEN rn = 1 THEN '9999-12-31'
                 ELSE '20250629'
                 END AS end_date,
             rn
         FROM (
                  SELECT *,
                         ROW_NUMBER() OVER (PARTITION BY id ORDER BY start_date DESC) AS rn
                  FROM (
                           -- 合并变更数据 + 历史快照
                           SELECT
                               CAST(id AS STRING) AS id,
                               CAST(name AS STRING) AS name,
                               CAST(phone_num AS STRING) AS phone_num,
                               CAST(email AS STRING) AS email,
                               CAST(user_level AS STRING) AS user_level,
                               CAST(birthday AS STRING) AS birthday,  -- ⭐ 这里关键
                               CAST(gender AS STRING) AS gender,
                               CAST(create_time AS STRING) AS create_time,
                               CAST(operate_time AS STRING) AS operate_time,
                               '20250629' AS start_date,
                               '9999-12-31' AS end_date
                           FROM ods_user_info
                           WHERE ds = '20250629'

                           UNION ALL

                           SELECT
                               id,
                               name,
                               phone_num,
                               email,
                               user_level,
                               birthday,
                               gender,
                               create_time,
                               operate_time,
                               start_date,
                               end_date
                           FROM dim_user_zip
                           WHERE ds = '9999-12-31'
                       ) merged_data
              ) deduped_data
     ) final_data;

select * from dim_user_zip;