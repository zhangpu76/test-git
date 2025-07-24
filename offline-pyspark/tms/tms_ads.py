# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
import sys
def get_spark_session ():
    """获取配置好的 SparkSession"""
    spark = SparkSession.builder \
        .appName("TMSAdsETL") \
        .config("hive.metastore.uris", "thrift://cdh01:9083") \
        .config("spark.sql.hive.convertMetastoreOrc", "true") \
        .config("spark.hadoop.hive.exec.dynamic.partition", "true") \
        .config("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict") \
        .enableHiveSupport() \
        .getOrCreate ()
    spark.sparkContext.setLogLevel ("WARN")
    spark.sql ("USE tms") # 切换到目标数据库
    return spark
def insert_to_hive (df, table_name, partition_date):
    """将 DataFrame 写入 Hive 表，兼容低版本 Python"""
    if df.rdd.isEmpty ():
        print ("[WARN] No data found for {0} on {1}, skipping write.".format (table_name, partition_date))
        return
    #写入 Hive 表（已包含 ds 分区字段时直接写入）
    df.write.mode("overwrite").insertInto("tms.{0}".format(table_name))
def etl_ads_city_stats (spark, ds):
    """城市分析表"""
    #创建表
    create_sql = """
    create external table ads_city_stats(
    ds string COMMENT ' 统计日期 ',
    recent_days bigint COMMENT ' 最近天数，1: 最近 1 天，7: 最近 7 天，30: 最近 30 天 ',
    city_id bigint COMMENT ' 城市 ID',
    city_name string COMMENT ' 城市名称 ',
    order_count bigint COMMENT ' 下单数 ',
    order_amount decimal COMMENT ' 下单金额 ',
    trans_finish_count bigint COMMENT ' 完成运输次数 ',
    trans_finish_distance decimal (16,2) COMMENT ' 完成运输里程 ',
    trans_finish_dur_sec bigint COMMENT ' 完成运输时长，单位：秒 ',
    avg_trans_finish_distance decimal (16,2) COMMENT ' 平均每次运输里程 ',
    avg_trans_finish_dur_sec bigint COMMENT ' 平均每次运输时长，单位：秒 '
    ) comment ' 城市分析 '
    row format delimited fields terminated by '\t'
    location '/warehouse/tms/ads/ads_city_stats'
    """
    spark.sql ("drop table if exists ads_city_stats")
    spark.sql (create_sql)
    #插入数据
    insert_sql = """
    select ds,
    recent_days,
    city_id,
    city_name,
    order_count,
    order_amount,
    trans_finish_count,
    trans_finish_distance,
    trans_finish_dur_sec,
    avg_trans_finish_distance,
    avg_trans_finish_dur_sec
    from ads_city_stats
    union
    select
    nvl(city_order_1d.ds, city_trans_1d.ds) as ds,
    nvl(city_order_1d.recent_days, city_trans_1d.recent_days) as recent_days,
    nvl(city_order_1d.city_id, city_trans_1d.city_id) as city_id,
    nvl(city_order_1d.city_name, city_trans_1d.city_name) as city_name,
    order_count,
    order_amount,
    trans_finish_count,
    trans_finish_distance,
    nvl(trans_finish_dur_sec, 0) as trans_finish_dur_sec,
    nvl(avg_trans_finish_distance, 0) as avg_trans_finish_distance,
    nvl(avg_trans_finish_dur_sec, 0) as avg_trans_finish_dur_sec
    from (
    select
    '{0}' as ds,
    1 as recent_days,
    city_id,
    city_name,
    sum(order_count) as order_count,
    sum(order_amount) as order_amount
    from dws_trade_org_cargo_type_order_1d
    where ds = '{0}'
    group by city_id, city_name
    ) city_order_1d
    full outer join (
    select
    '{0}' as ds,
    1 as recent_days,
    city_id,
    city_name,
    sum(trans_finish_count) as trans_finish_count,
    sum(trans_finish_distance) as trans_finish_distance,
    sum(coalesce(trans_finish_dur_sec, 0)) as trans_finish_dur_sec,
    case
    when sum(trans_finish_count) > 0 then sum(trans_finish_distance) / sum(trans_finish_count)
    else 0
    end as avg_trans_finish_distance,
    case
    when sum(trans_finish_count) > 0 then sum(coalesce(trans_finish_dur_sec, 0)) / sum(trans_finish_count)
    else 0
    end as avg_trans_finish_dur_sec
    from (
    select
    if(org_level = 1, city_for_level1.id, city_for_level2.id) as city_id,
    if(org_level = 1, city_for_level1.name, city_for_level2.name) as city_name,
    trans_origin.trans_finish_count,
    trans_origin.trans_finish_distance,
    trans_origin.trans_finish_dur_sec
    from (
    select
    org_id,
    trans_finish_count,
    trans_finish_distance,
    trans_finish_dur_sec
    from dws_trans_org_truck_model_type_trans_finish_1d
    where ds = '{0}'
    ) trans_origin
    left join (
    select
    id,
    org_level,
    region_id
    from dim_organ_full
    where ds = '{0}'
    ) organ on trans_origin.org_id = organ.id
    left join (
    select
    id,
    name,
    parent_id
    from dim_region_full
    where ds = '{0}'
    ) city_for_level1 on organ.region_id = city_for_level1.id
    left join (
    select
    id,
    name
    from dim_region_full
    where ds = '{0}'
    ) city_for_level2 on city_for_level1.parent_id = city_for_level2.id
    ) trans_1d
    group by city_id, city_name
    ) city_trans_1d
    on city_order_1d.ds = city_trans_1d.ds
    and city_order_1d.recent_days = city_trans_1d.recent_days
    and city_order_1d.city_id = city_trans_1d.city_id
    and city_order_1d.city_name = city_trans_1d.city_name
    union
    select
    nvl(city_order_nd.ds, city_trans_nd.ds) as ds,
    nvl(city_order_nd.recent_days, city_trans_nd.recent_days) as recent_days,
    nvl(city_order_nd.city_id, city_trans_nd.city_id) as city_id,
    nvl(city_order_nd.city_name, city_trans_nd.city_name) as city_name,
    order_count,
    order_amount,
    trans_finish_count,
    trans_finish_distance,
    nvl(trans_finish_dur_sec, 0) as trans_finish_dur_sec,
    nvl(avg_trans_finish_distance, 0) as avg_trans_finish_distance,
    nvl(avg_trans_finish_dur_sec, 0) as avg_trans_finish_dur_sec
    from (
    select
    '{0}' as ds,
    recent_days,
    city_id,
    city_name,
    sum(order_count) as order_count,
    sum(order_amount) as order_amount
    from dws_trade_org_cargo_type_order_nd
    where ds = '{0}'
    group by city_id, city_name, recent_days
    ) city_order_nd
    full outer join (
    select
    '{0}' as ds,
    recent_days,
    city_id,
    city_name,
    sum(trans_finish_count) as trans_finish_count,
    sum(trans_finish_distance) as trans_finish_distance,
    sum(coalesce(trans_finish_dur_sec, 0)) as trans_finish_dur_sec,
    case
    when sum(trans_finish_count) > 0 then sum(trans_finish_distance) / sum(trans_finish_count)
    else 0
    end as avg_trans_finish_distance,
    case
    when sum(trans_finish_count) > 0 then sum(coalesce(trans_finish_dur_sec, 0)) / sum(trans_finish_count)
    else 0
    end as avg_trans_finish_dur_sec
    from dws_trans_shift_trans_finish_nd
    where ds = '{0}'
    group by city_id, city_name, recent_days
    ) city_trans_nd
    on city_order_nd.ds = city_trans_nd.ds
    and city_order_nd.recent_days = city_trans_nd.recent_days
    and city_order_nd.city_id = city_trans_nd.city_id
    and city_order_nd.city_name = city_trans_nd.city_name
    """.format(ds)
    df = spark.sql(insert_sql)
    insert_to_hive(df, "ads_city_stats", ds)
def etl_ads_driver_stats (spark, ds):
    """司机分析表"""
    #创建表
    create_sql = """
    create external table ads_driver_stats(
    ds string COMMENT ' 统计日期 ',
    recent_days tinyint COMMENT ' 最近天数，7: 最近 7 天，30: 最近 30 天 ',
    driver_emp_id bigint comment ' 第一司机员工 ID',
    driver_name string comment ' 第一司机姓名 ',
    trans_finish_count bigint COMMENT ' 完成运输次数 ',
    trans_finish_distance decimal (16,2) COMMENT ' 完成运输里程 ',
    trans_finish_dur_sec bigint COMMENT ' 完成运输时长，单位：秒 ',
    avg_trans_finish_distance decimal (16,2) COMMENT ' 平均每次运输里程 ',
    avg_trans_finish_dur_sec bigint COMMENT ' 平均每次运输时长，单位：秒 ',
    trans_finish_late_count bigint COMMENT ' 逾期次数 '
    ) comment ' 司机分析 '
    row format delimited fields terminated by '\t'
    location '/warehouse/tms/ads/ads_driver_stats'
    """
    spark.sql ("drop table if exists ads_driver_stats")
    spark.sql (create_sql)
    #插入数据
    insert_sql = """
    select ds,
    recent_days,
    driver_emp_id,
    driver_name,
    trans_finish_count,
    trans_finish_distance,
    trans_finish_dur_sec,
    avg_trans_finish_distance,
    avg_trans_finish_dur_sec,
    trans_finish_late_count
    from ads_driver_stats
    where 1=0
    union all
    select
    '{0}' as ds,
    recent_days,
    driver_id,
    driver_name,
    sum(trans_finish_count) as trans_finish_count,
    sum(trans_finish_distance) as trans_finish_distance,
    sum(trans_finish_dur_sec) as trans_finish_dur_sec,
    case when sum(trans_finish_count) = 0 then 0
    else round(sum(trans_finish_distance) / sum(trans_finish_count), 2)
    end as avg_trans_finish_distance,
    case when sum(trans_finish_count) = 0 then 0
    else round(sum(trans_finish_dur_sec) / sum(trans_finish_count))
    end as avg_trans_finish_dur_sec,
    sum(trans_finish_delay_count) as trans_finish_late_count
    from (
    select
    recent_days,
    driver1_emp_id as driver_id,
    driver1_name as driver_name,
    trans_finish_count,
    trans_finish_distance,
    trans_finish_dur_sec,
    trans_finish_delay_count
    from dws_trans_shift_trans_finish_nd
    where ds = '{0}'
    and driver2_emp_id is null
    and recent_days in (7, 30)
    union all
    select
    recent_days,
    cast(coalesce(driver_info[0], 0) as bigint) as driver_id,
    driver_info[1] as driver_name,
    trans_finish_count,
    trans_finish_distance / 2 as trans_finish_distance,
    trans_finish_dur_sec / 2 as trans_finish_dur_sec,
    trans_finish_delay_count
    from (
    select
    recent_days,
    array(
    array(coalesce(driver1_emp_id, 0), driver1_name),
    array(coalesce(driver2_emp_id, 0), driver2_name)
    ) as driver_arr,
    trans_finish_count,
    trans_finish_distance,
    trans_finish_dur_sec,
    trans_finish_delay_count
    from dws_trans_shift_trans_finish_nd
    where ds = '{0}'
    and driver2_emp_id is not null
    and recent_days in (7, 30)
    ) t1
    lateral view explode(driver_arr) tmp as driver_info
    ) t2
    where driver_id != 0
    group by recent_days, driver_id, driver_name
    """.format(ds)
    df = spark.sql(insert_sql)
    insert_to_hive(df, "ads_driver_stats", ds)
def etl_ads_express_city_stats (spark, ds):
    """各城市快递统计表"""
    #创建表
    create_sql = """
    create external table ads_express_city_stats(
    ds string COMMENT ' 统计日期 ',
    recent_days tinyint COMMENT ' 最近天数，1: 最近 1 天，7: 最近 7 天，30: 最近 30 天 ',
    city_id bigint COMMENT ' 城市 ID',
    city_name string COMMENT ' 城市名称 ',
    receive_order_count bigint COMMENT ' 揽收次数 ',
    receive_order_amount decimal (16,2) COMMENT ' 揽收金额 ',
    deliver_suc_count bigint COMMENT ' 派送成功次数 ',
    sort_count bigint COMMENT ' 分拣次数 '
    ) comment ' 各城市快递统计 '
    row format delimited fields terminated by '\t'
    location '/warehouse/tms/ads/ads_express_city_stats'
    """
    spark.sql ("drop table if exists ads_express_city_stats")
    spark.sql (create_sql)
    #插入数据
    insert_sql = """
    select ds,
    recent_days,
    city_id,
    city_name,
    receive_order_count,
    receive_order_amount,
    deliver_suc_count,
    sort_count
    from ads_express_city_stats
    union
    select nvl(nvl(city_deliver_1d.ds, city_sort_1d.ds), city_receive_1d.ds) ds,
    nvl(nvl(city_deliver_1d.recent_days, city_sort_1d.recent_days),
    city_receive_1d.recent_days) recent_days,
    nvl(nvl(city_deliver_1d.city_id, city_sort_1d.city_id),
    city_receive_1d.city_id) city_id,
    nvl(nvl(city_deliver_1d.city_name, city_sort_1d.city_name),
    city_receive_1d.city_name) city_name,
    receive_order_count,
    receive_order_amount,
    deliver_suc_count,
    sort_count
    from (select '{0}' ds,
    1 recent_days,
    city_id,
    city_name,
    sum(order_count) deliver_suc_count
    from dws_trans_org_deliver_suc_1d
    where ds = '{0}'
    group by city_id, city_name) city_deliver_1d
    full outer join
    (select '{0}' ds,
    1 recent_days,
    city_id,
    city_name,
    sum(sort_count) sort_count
    from dws_trans_org_sort_1d
    where ds = '{0}'
    group by city_id, city_name) city_sort_1d
    on city_deliver_1d.ds = city_sort_1d.ds
    and city_deliver_1d.recent_days = city_sort_1d.recent_days
    and city_deliver_1d.city_id = city_sort_1d.city_id
    and city_deliver_1d.city_name = city_sort_1d.city_name
    full outer join
    (select '{0}' ds,
    1 recent_days,
    city_id,
    city_name,
    sum(order_count) receive_order_count,
    sum(order_amount) receive_order_amount
    from dws_trans_org_receive_1d
    where ds = '{0}'
    group by city_id, city_name) city_receive_1d
    on city_deliver_1d.ds = city_receive_1d.ds
    and city_deliver_1d.recent_days = city_receive_1d.recent_days
    and city_deliver_1d.city_id = city_receive_1d.city_id
    and city_deliver_1d.city_name = city_receive_1d.city_name
    union
    select nvl(nvl(city_deliver_nd.ds, city_sort_nd.ds), city_receive_nd.ds) ds,
    nvl(nvl(city_deliver_nd.recent_days, city_sort_nd.recent_days),
    city_receive_nd.recent_days) recent_days,
    nvl(nvl(city_deliver_nd.city_id, city_sort_nd.city_id),
    city_receive_nd.city_id) city_id,
    nvl(nvl(city_deliver_nd.city_name, city_sort_nd.city_name),
    city_receive_nd.city_name) city_name,
    receive_order_count,
    receive_order_amount,
    deliver_suc_count,
    sort_count
    from (select '{0}' ds,
    recent_days,
    city_id,
    city_name,
    sum(order_count) deliver_suc_count
    from dws_trans_org_deliver_suc_nd
    where ds = '{0}'
    group by recent_days, city_id, city_name) city_deliver_nd
    full outer join
    (select '{0}' ds,
    recent_days,
    city_id,
    city_name,
    sum(sort_count) sort_count
    from dws_trans_org_sort_nd
    where ds = '{0}'
    group by recent_days, city_id, city_name) city_sort_nd
    on city_deliver_nd.ds = city_sort_nd.ds
    and city_deliver_nd.recent_days = city_sort_nd.recent_days
    and city_deliver_nd.city_id = city_sort_nd.city_id
    and city_deliver_nd.city_name = city_sort_nd.city_name
    full outer join
    (select '{0}' ds,
    recent_days,
    city_id,
    city_name,
    sum(order_count) receive_order_count,
    sum(order_amount) receive_order_amount
    from dws_trans_org_receive_nd
    where ds = '{0}'
    group by recent_days, city_id, city_name) city_receive_nd
    on city_deliver_nd.ds = city_receive_nd.ds
    and city_deliver_nd.recent_days = city_receive_nd.recent_days
    and city_deliver_nd.city_id = city_receive_nd.city_id
    and city_deliver_nd.city_name = city_receive_nd.city_name
    """.format(ds)
    df = spark.sql(insert_sql)
    insert_to_hive(df, "ads_express_city_stats", ds)
def etl_ads_express_org_stats (spark, ds):
    """各机构快递统计表"""
    #创建表
    create_sql = """
    create external table ads_express_org_stats(
    ds string COMMENT ' 统计日期 ',
    recent_days tinyint COMMENT ' 最近天数，1: 最近 1 天，7: 最近 7 天，30: 最近 30 天 ',
    org_id bigint COMMENT ' 机构 ID',
    org_name string COMMENT ' 机构名称 ',
    receive_order_count bigint COMMENT ' 揽收次数 ',
    receive_order_amount decimal (16,2) COMMENT ' 揽收金额 ',
    deliver_suc_count bigint COMMENT ' 派送成功次数 ',
    sort_count bigint COMMENT ' 分拣次数 '
    ) comment ' 各机构快递统计 '
    row format delimited fields terminated by '\t'
    location '/warehouse/tms/ads/ads_express_org_stats'
    """
    spark.sql ("drop table if exists ads_express_org_stats")
    spark.sql (create_sql)
    #插入数据
    insert_sql = """
    select ds,
    recent_days,
    org_id,
    org_name,
    receive_order_count,
    receive_order_amount,
    deliver_suc_count,
    sort_count
    from ads_express_org_stats
    union
    select nvl(nvl(org_deliver_1d.ds, org_sort_1d.ds), org_receive_1d.ds) ds,
    nvl(nvl(org_deliver_1d.recent_days, org_sort_1d.recent_days),
    org_receive_1d.recent_days) recent_days,
    nvl(nvl(org_deliver_1d.org_id, org_sort_1d.org_id),
    org_receive_1d.org_id) org_id,
    nvl(nvl(org_deliver_1d.org_name, org_sort_1d.org_name),
    org_receive_1d.org_name) org_name,
    receive_order_count,
    receive_order_amount,
    deliver_suc_count,
    sort_count
    from (select '{0}' ds,
    1 recent_days,
    org_id,
    org_name,
    sum(order_count) deliver_suc_count
    from dws_trans_org_deliver_suc_1d
    where ds = '{0}'
    group by org_id, org_name) org_deliver_1d
    full outer join
    (select '{0}' ds,
    1 recent_days,
    org_id,
    org_name,
    sum(sort_count) sort_count
    from dws_trans_org_sort_1d
    where ds = '{0}'
    group by org_id, org_name) org_sort_1d
    on org_deliver_1d.ds = org_sort_1d.ds
    and org_deliver_1d.recent_days = org_sort_1d.recent_days
    and org_deliver_1d.org_id = org_sort_1d.org_id
    and org_deliver_1d.org_name = org_sort_1d.org_name
    full outer join
    (select '{0}' ds,
    1 recent_days,
    org_id,
    org_name,
    sum(order_count) receive_order_count,
    sum(order_amount) receive_order_amount
    from dws_trans_org_receive_1d
    where ds = '{0}'
    group by org_id, org_name) org_receive_1d
    on org_deliver_1d.ds = org_receive_1d.ds
    and org_deliver_1d.recent_days = org_receive_1d.recent_days
    and org_deliver_1d.org_id = org_receive_1d.org_id
    and org_deliver_1d.org_name = org_receive_1d.org_name
    union
    select nvl(nvl(org_deliver_nd.ds, org_sort_nd.ds), org_receive_nd.ds) ds,
    nvl(nvl(org_deliver_nd.recent_days, org_sort_nd.recent_days),
    org_receive_nd.recent_days) recent_days,
    nvl(nvl(org_deliver_nd.org_id, org_sort_nd.org_id),
    org_receive_nd.org_id) org_id,
    nvl(nvl(org_deliver_nd.org_name, org_sort_nd.org_name),
    org_receive_nd.org_name) org_name,
    receive_order_count,
    receive_order_amount,
    deliver_suc_count,
    sort_count
    from (select '{0}' ds,
    recent_days,
    org_id,
    org_name,
    sum(order_count) deliver_suc_count
    from dws_trans_org_deliver_suc_nd
    where ds = '{0}'
    group by recent_days, org_id, org_name) org_deliver_nd
    full outer join
    (select '{0}' ds,
    recent_days,
    org_id,
    org_name,
    sum(sort_count) sort_count
    from dws_trans_org_sort_nd
    where ds = '{0}'
    group by recent_days, org_id, org_name) org_sort_nd
    on org_deliver_nd.ds = org_sort_nd.ds
    and org_deliver_nd.recent_days = org_sort_nd.recent_days
    and org_deliver_nd.org_id = org_sort_nd.org_id
    and org_deliver_nd.org_name = org_sort_nd.org_name
    full outer join
    (select '{0}' ds,
    recent_days,
    org_id,
    org_name,
    sum(order_count) receive_order_count,
    sum(order_amount) receive_order_amount
    from dws_trans_org_receive_nd
    where ds = '{0}'
    group by recent_days, org_id, org_name) org_receive_nd
    on org_deliver_nd.ds = org_receive_nd.ds
    and org_deliver_nd.recent_days = org_receive_nd.recent_days
    and org_deliver_nd.org_id = org_receive_nd.org_id
    and org_deliver_nd.org_name = org_receive_nd.org_name
    """.format(ds)
    df = spark.sql(insert_sql)
    insert_to_hive(df, "ads_express_org_stats", ds)
def etl_ads_express_province_stats (spark, ds):
    """各省份快递统计表"""
    #创建表
    create_sql = """
    create external table ads_express_province_stats(
    ds string COMMENT ' 统计日期 ',
    recent_days tinyint COMMENT ' 最近天数，1: 最近 1 天，7: 最近 7 天，30: 最近 30 天 ',
    province_id bigint COMMENT ' 省份 ID',
    province_name string COMMENT ' 省份名称 ',
    receive_order_count bigint COMMENT ' 揽收次数 ',
    receive_order_amount decimal (16,2) COMMENT ' 揽收金额 ',
    deliver_suc_count bigint COMMENT ' 派送成功次数 ',
    sort_count bigint COMMENT ' 分拣次数 '
    ) comment ' 各省份快递统计 '
    row format delimited fields terminated by '\t'
    location '/warehouse/tms/ads/ads_express_province_stats'
    """
    spark.sql ("drop table if exists ads_express_province_stats")
    spark.sql (create_sql)
    #插入数据
    insert_sql = """
    select ds,
    recent_days,
    province_id,
    province_name,
    receive_order_count,
    receive_order_amount,
    deliver_suc_count,
    sort_count
    from ads_express_province_stats
    union
    select nvl(nvl(province_deliver_1d.ds, province_sort_1d.ds), province_receive_1d.ds) ds,
    nvl(nvl(province_deliver_1d.recent_days, province_sort_1d.recent_days),
    province_receive_1d.recent_days) recent_days,
    nvl(nvl(province_deliver_1d.province_id, province_sort_1d.province_id),
    province_receive_1d.province_id) province_id,
    nvl(nvl(province_deliver_1d.province_name, province_sort_1d.province_name),
    province_receive_1d.province_name) province_name,
    receive_order_count,
    receive_order_amount,
    deliver_suc_count,
    sort_count
    from (select '{0}' ds,
    1 recent_days,
    province_id,
    province_name,
    sum(order_count) deliver_suc_count
    from dws_trans_org_deliver_suc_1d
    where ds = '{0}'
    group by province_id, province_name) province_deliver_1d
    full outer join
    (select '{0}' ds,
    1 recent_days,
    province_id,
    province_name,
    sum(sort_count) sort_count
    from dws_trans_org_sort_1d
    where ds = '{0}'
    group by province_id, province_name) province_sort_1d
    on province_deliver_1d.ds = province_sort_1d.ds
    and province_deliver_1d.recent_days = province_sort_1d.recent_days
    and province_deliver_1d.province_id = province_sort_1d.province_id
    and province_deliver_1d.province_name = province_sort_1d.province_name
    full outer join
    (select '{0}' ds,
    1 recent_days,
    province_id,
    province_name,
    sum(order_count) receive_order_count,
    sum(order_amount) receive_order_amount
    from dws_trans_org_receive_1d
    where ds = '{0}'
    group by province_id, province_name) province_receive_1d
    on province_deliver_1d.ds = province_receive_1d.ds
    and province_deliver_1d.recent_days = province_receive_1d.recent_days
    and province_deliver_1d.province_id = province_receive_1d.province_id
    and province_deliver_1d.province_name = province_receive_1d.province_name
    union
    select nvl(nvl(province_deliver_nd.ds, province_sort_nd.ds), province_receive_nd.ds) ds,
    nvl(nvl(province_deliver_nd.recent_days, province_sort_nd.recent_days),
    province_receive_nd.recent_days) recent_days,
    nvl(nvl(province_deliver_nd.province_id, province_sort_nd.province_id),
    province_receive_nd.province_id) province_id,
    nvl(nvl(province_deliver_nd.province_name, province_sort_nd.province_name),
    province_receive_nd.province_name) province_name,
    receive_order_count,
    receive_order_amount,
    deliver_suc_count,
    sort_count
    from (select '{0}' ds,
    recent_days,
    province_id,
    province_name,
    sum(order_count) deliver_suc_count
    from dws_trans_org_deliver_suc_nd
    where ds = '{0}'
    group by recent_days, province_id, province_name) province_deliver_nd
    full outer join
    (select '{0}' ds,
    recent_days,
    province_id,
    province_name,
    sum(sort_count) sort_count
    from dws_trans_org_sort_nd
    where ds = '{0}'
    group by recent_days, province_id, province_name) province_sort_nd
    on province_deliver_nd.ds = province_sort_nd.ds
    and province_deliver_nd.recent_days = province_sort_nd.recent_days
    and province_deliver_nd.province_id = province_sort_nd.province_id
    and province_deliver_nd.province_name = province_sort_nd.province_name
    full outer join
    (select '{0}' ds,
    recent_days,
    province_id,
    province_name,
    sum(order_count) receive_order_count,
    sum(order_amount) receive_order_amount
    from dws_trans_org_receive_nd
    where ds = '{0}'
    group by recent_days, province_id, province_name) province_receive_nd
    on province_deliver_nd.ds = province_receive_nd.ds
    and province_deliver_nd.recent_days = province_receive_nd.recent_days
    and province_deliver_nd.province_id = province_receive_nd.province_id
    and province_deliver_nd.province_name = province_receive_nd.province_name
    """.format(ds)
    df = spark.sql(insert_sql)
    insert_to_hive(df, "ads_express_province_stats", ds)
def etl_ads_express_stats (spark, ds):
    """快递综合统计表"""
    #创建表
    create_sql = """
    create external table ads_express_stats(
    ds string COMMENT ' 统计日期 ',
    recent_days tinyint COMMENT ' 最近天数，1: 最近 1 天，7: 最近 7 天，30: 最近 30 天 ',
    deliver_suc_count bigint COMMENT ' 派送成功次数（订单数）',
    sort_count bigint COMMENT ' 分拣次数 '
    ) comment ' 快递综合统计 '
    row format delimited fields terminated by '\t'
    location '/warehouse/tms/ads/ads_express_stats'
    """
    spark.sql ("drop table if exists ads_express_stats")
    spark.sql (create_sql)
    #插入数据
    insert_sql = """
    select
    ds,
    recent_days,
    deliver_suc_count,
    sort_count
    from ads_express_stats
    where 1=0
    union all
    select
    nvl(deliver_1d.ds, sort_1d.ds) as ds,
    nvl(deliver_1d.recent_days, sort_1d.recent_days) as recent_days,
    nvl(deliver_1d.deliver_suc_count, 0) as deliver_suc_count,
    nvl(sort_1d.sort_count, 0) as sort_count
    from (
    select
    '{0}' as ds,
    1 as recent_days,
    org_id,
    sum(order_count) as deliver_suc_count
    from dws_trans_org_deliver_suc_1d
    where ds = '{0}'
    group by org_id
    ) deliver_1d
    full outer join (
    select
    '{0}' as ds,
    1 as recent_days,
    org_id,
    sum(sort_count) as sort_count
    from dws_trans_org_sort_1d
    where ds = '{0}'
    group by org_id
    ) sort_1d
    on deliver_1d.org_id = sort_1d.org_id
    and deliver_1d.ds = sort_1d.ds
    and deliver_1d.recent_days = sort_1d.recent_days
    union all
    select
    nvl(deliver_nd.ds, sort_nd.ds) as ds,
    nvl(deliver_nd.recent_days, sort_nd.recent_days) as recent_days,
    nvl(deliver_nd.deliver_suc_count, 0) as deliver_suc_count,
    nvl(sort_nd.sort_count, 0) as sort_count
    from (
    select
    '{0}' as ds,
    recent_days,
    org_id,
    sum(order_count) as deliver_suc_count
    from dws_trans_org_deliver_suc_nd
    where ds = '{0}'
    group by recent_days, org_id
    ) deliver_nd
    full outer join (
    select
    '{0}' as ds,
    recent_days,
    org_id,
    sum(sort_count) as sort_count
    from dws_trans_org_sort_nd
    where ds = '{0}'
    group by recent_days, org_id
    ) sort_nd
    on deliver_nd.org_id = sort_nd.org_id
    and deliver_nd.ds = sort_nd.ds
    and deliver_nd.recent_days = sort_nd.recent_days
    """.format(ds)
    df = spark.sql(insert_sql)
    insert_to_hive(df, "ads_express_stats", ds)
def etl_ads_line_stats (spark, ds):
    """线路分析表"""
    #创建表
    create_sql = """
    create external table ads_line_stats(
    ds string COMMENT ' 统计日期 ',
    recent_days tinyint COMMENT ' 最近天数，7: 最近 7 天，30: 最近 30 天 ',
    line_id bigint COMMENT ' 线路 ID',
    line_name string COMMENT ' 线路名称 ',
    trans_finish_count bigint COMMENT ' 完成运输次数 ',
    trans_finish_distance decimal (16,2) COMMENT ' 完成运输里程 ',
    trans_finish_dur_sec bigint COMMENT ' 完成运输时长，单位：秒 ',
    trans_finish_order_count bigint COMMENT ' 运输完成运单数 '
    ) comment ' 线路分析 '
    row format delimited fields terminated by '\t'
    location '/warehouse/tms/ads/ads_line_stats'
    """
    spark.sql ("drop table if exists ads_line_stats")
    spark.sql (create_sql)
    #插入数据
    insert_sql = """
    select ds,
    recent_days,
    line_id,
    line_name,
    trans_finish_count,
    trans_finish_distance,
    trans_finish_dur_sec,
    trans_finish_order_count
    from ads_line_stats
    union
    select '{0}' ds,
    recent_days,
    line_id,
    line_name,
    sum(trans_finish_count) trans_finish_count,
    sum(trans_finish_distance) trans_finish_distance,
    sum(trans_finish_dur_sec) trans_finish_dur_sec,
    sum(trans_finish_order_count) trans_finish_order_count
    from dws_trans_shift_trans_finish_nd
    where ds = '{0}'
    group by line_id, line_name, recent_days
    """.format(ds)
    df = spark.sql(insert_sql)
    insert_to_hive(df, "ads_line_stats", ds)
def etl_ads_order_cargo_type_stats (spark, ds):
    """各类型货物运单统计表"""
    #创建表
    create_sql = """
    create external table ads_order_cargo_type_stats(
    ds string COMMENT ' 统计日期 ',
    recent_days tinyint COMMENT ' 最近天数，1: 最近 1 天，7: 最近 7 天，30: 最近 30 天 ',
    cargo_type string COMMENT ' 货物类型 ',
    cargo_type_name string COMMENT ' 货物类型名称 ',
    order_count bigint COMMENT ' 下单数 ',
    order_amount decimal (16,2) COMMENT ' 下单金额 '
    ) comment ' 各类型货物运单统计 '
    row format delimited fields terminated by '\t'
    location '/warehouse/tms/ads/ads_order_cargo_type_stats'
    """
    spark.sql ("drop table if exists ads_order_cargo_type_stats")
    spark.sql (create_sql)
    #插入数据
    insert_sql = """
    select ds,
    recent_days,
    cargo_type,
    cargo_type_name,
    order_count,
    order_amount
    from ads_order_cargo_type_stats
    union
    select '{0}' ds,
    1 recent_days,
    cargo_type,
    cargo_type_name,
    sum(order_count) order_count,
    sum(order_amount) order_amount
    from dws_trade_org_cargo_type_order_1d
    where ds = '{0}'
    group by cargo_type, cargo_type_name
    union
    select '{0}' ds,
    recent_days,
    cargo_type,
    cargo_type_name,
    sum(order_count) order_count,
    sum(order_amount) order_amount
    from dws_trade_org_cargo_type_order_nd
    where ds = '{0}'
    group by cargo_type, cargo_type_name, recent_days
    """.format(ds)
    df = spark.sql(insert_sql)
    insert_to_hive(df, "ads_order_cargo_type_stats", ds)
def etl_ads_order_stats (spark, ds):
    """运单综合统计表"""
    #创建表
    create_sql = """
    create external table ads_order_stats(
    ds string COMMENT ' 统计日期 ',
    recent_days tinyint COMMENT ' 最近天数，1: 最近 1 天，7: 最近 7 天，30: 最近 30 天 ',
    order_count bigint COMMENT ' 下单数 ',
    order_amount decimal (16,2) COMMENT ' 下单金额 '
    ) comment ' 运单综合统计 '
    row format delimited fields terminated by '\t'
    location '/warehouse/tms/ads/ads_order_stats'
    """
    spark.sql ("drop table if exists ads_order_stats")
    spark.sql (create_sql)
    #插入数据
    insert_sql = """
    select ds,
    recent_days,
    order_count,
    order_amount
    from ads_order_stats
    union
    select '{0}' ds,
    1 recent_days,
    sum(order_count) order_count,
    sum(order_amount) order_amount
    from dws_trade_org_cargo_type_order_1d
    where ds = '{0}'
    union
    select '{0}' ds,
    recent_days,
    sum(order_count) order_count,
    sum(order_amount) order_amount
    from dws_trade_org_cargo_type_order_nd
    where ds = '{0}'
    group by recent_days
    """.format(ds)
    df = spark.sql(insert_sql)
    insert_to_hive(df, "ads_order_stats", ds)
def etl_ads_org_stats (spark, ds):
    """机构分析表"""
    #创建表
    create_sql = """
    create external table ads_org_stats(
    ds string COMMENT ' 统计日期 ',
    recent_days tinyint COMMENT ' 最近天数，1: 最近 1 天，7: 最近 7 天，30: 最近 30 天 ',
    org_id bigint COMMENT ' 机构 ID',
    org_name string COMMENT ' 机构名称 ',
    order_count bigint COMMENT ' 下单数 ',
    order_amount decimal COMMENT ' 下单金额 ',
    trans_finish_count bigint COMMENT ' 完成运输次数 ',
    trans_finish_distance decimal (16,2) COMMENT ' 完成运输里程 ',
    trans_finish_dur_sec bigint COMMENT ' 完成运输时长，单位：秒 ',
    avg_trans_finish_distance decimal (16,2) COMMENT ' 平均每次运输里程 ',
    avg_trans_finish_dur_sec bigint COMMENT ' 平均每次运输时长，单位：秒 '
    ) comment ' 机构分析 '
    row format delimited fields terminated by '\t'
    location '/warehouse/tms/ads/ads_org_stats'
    """
    spark.sql ("drop table if exists ads_org_stats")
    spark.sql (create_sql)
    #插入数据
    insert_sql = """
    select ds,
    recent_days,
    org_id,
    org_name,
    order_count,
    order_amount,
    trans_finish_count,
    trans_finish_distance,
    trans_finish_dur_sec,
    avg_trans_finish_distance,
    avg_trans_finish_dur_sec
    from ads_org_stats
    union
    select nvl(org_order_1d.ds, org_trans_1d.ds) ds,
    nvl(org_order_1d.recent_days, org_trans_1d.recent_days) recent_days,
    nvl(org_order_1d.org_id, org_trans_1d.org_id) org_id,
    nvl(org_order_1d.org_name, org_trans_1d.org_name) org_name,
    order_count,
    order_amount,
    trans_finish_count,
    trans_finish_distance,
    trans_finish_dur_sec,
    avg_trans_finish_distance,
    avg_trans_finish_dur_sec
    from (select '{0}' ds,
    1 recent_days,
    org_id,
    org_name,
    sum(order_count) order_count,
    sum(order_amount) order_amount
    from dws_trade_org_cargo_type_order_1d
    where ds = '{0}'
    group by org_id, org_name) org_order_1d
    full outer join
    (select '{0}' ds,
    org_id,
    org_name,
    1 recent_days,
    sum(trans_finish_count) trans_finish_count,
    sum(trans_finish_distance) trans_finish_distance,
    sum(trans_finish_dur_sec) trans_finish_dur_sec,
    case when sum(trans_finish_count) = 0 then 0
    else sum(trans_finish_distance) / sum(trans_finish_count)
    end avg_trans_finish_distance,
    case when sum(trans_finish_count) = 0 then 0
    else sum(trans_finish_dur_sec) / sum(trans_finish_count)
    end avg_trans_finish_dur_sec
    from dws_trans_org_truck_model_type_trans_finish_1d
    where ds = '{0}'
    group by org_id, org_name
    ) org_trans_1d
    on org_order_1d.ds = org_trans_1d.ds
    and org_order_1d.recent_days = org_trans_1d.recent_days
    and org_order_1d.org_id = org_trans_1d.org_id
    and org_order_1d.org_name = org_trans_1d.org_name
    union
    select org_order_nd.ds,
    org_order_nd.recent_days,
    org_order_nd.org_id,
    org_order_nd.org_name,
    order_count,
    order_amount,
    trans_finish_count,
    trans_finish_distance,
    trans_finish_dur_sec,
    avg_trans_finish_distance,
    avg_trans_finish_dur_sec
    from (select '{0}' ds,
    recent_days,
    org_id,
    org_name,
    sum(order_count) order_count,
    sum(order_amount) order_amount
    from dws_trade_org_cargo_type_order_nd
    where ds = '{0}'
    group by org_id, org_name, recent_days) org_order_nd
    join
    (select '{0}' ds,
    recent_days,
    org_id,
    org_name,
    sum(trans_finish_count) trans_finish_count,
    sum(trans_finish_distance) trans_finish_distance,
    sum(trans_finish_dur_sec) trans_finish_dur_sec,
    case when sum(trans_finish_count) = 0 then 0
    else sum(trans_finish_distance) / sum(trans_finish_count)
    end avg_trans_finish_distance,
    case when sum(trans_finish_count) = 0 then 0
    else sum(trans_finish_dur_sec) / sum(trans_finish_count)
    end avg_trans_finish_dur_sec
    from dws_trans_shift_trans_finish_nd
    where ds = '{0}'
    group by org_id, org_name, recent_days
    ) org_trans_nd
    on org_order_nd.ds = org_trans_nd.ds
    and org_order_nd.recent_days = org_trans_nd.recent_days
    and org_order_nd.org_id = org_trans_nd.org_id
    and org_order_nd.org_name = org_trans_nd.org_name
    """.format(ds)
    df = spark.sql(insert_sql)
    insert_to_hive(df, "ads_org_stats", ds)
def etl_ads_shift_stats (spark, ds):
    """班次分析表"""
    #创建表
    create_sql = """
    create external table ads_shift_stats(
    ds string COMMENT ' 统计日期 ',
    recent_days tinyint COMMENT ' 最近天数，7: 最近 7 天，30: 最近 30 天 ',
    shift_id bigint COMMENT ' 班次 ID',
    trans_finish_count bigint COMMENT ' 完成运输次数 ',
    trans_finish_distance decimal (16,2) COMMENT ' 完成运输里程 ',
    trans_finish_dur_sec bigint COMMENT ' 完成运输时长，单位：秒 ',
    trans_finish_order_count bigint COMMENT ' 运输完成运单数 '
    ) comment ' 班次分析 '
    row format delimited fields terminated by '\t'
    location '/warehouse/tms/ads/ads_shift_stats'
    """
    spark.sql ("drop table if exists ads_shift_stats")
    spark.sql (create_sql)
    #插入数据
    insert_sql = """
    select ds,
    recent_days,
    shift_id,
    trans_finish_count,
    trans_finish_distance,
    trans_finish_dur_sec,
    trans_finish_order_count
    from ads_shift_stats
    union
    select '{0}' ds,
    recent_days,
    shift_id,
    trans_finish_count,
    trans_finish_distance,
    trans_finish_dur_sec,
    trans_finish_order_count
    from dws_trans_shift_trans_finish_nd
    where ds = '{0}'
    """.format(ds)
    df = spark.sql(insert_sql)
    insert_to_hive(df, "ads_shift_stats", ds)
def etl_ads_trans_order_stats (spark, ds):
    """运单相关统计表"""
    #创建表
    create_sql = """
    create external table ads_trans_order_stats(
    ds string COMMENT ' 统计日期 ',
    recent_days tinyint COMMENT ' 最近天数，1: 最近 1 天，7: 最近 7 天，30: 最近 30 天 ',
    receive_order_count bigint COMMENT ' 接单总数 ',
    receive_order_amount decimal (16,2) COMMENT ' 接单金额 ',
    dispatch_order_count bigint COMMENT ' 发单总数 ',
    dispatch_order_amount decimal (16,2) COMMENT ' 发单金额 '
    ) comment ' 运单相关统计 '
    row format delimited fields terminated by '\t'
    location '/warehouse/tms/ads/ads_trans_order_stats'
    """
    spark.sql ("drop table if exists ads_trans_order_stats")
    spark.sql (create_sql)
    #插入数据
    insert_sql = """
    select ds,
    recent_days,
    receive_order_count,
    receive_order_amount,
    dispatch_order_count,
    dispatch_order_amount
    from ads_trans_order_stats
    union
    select '{0}' ds,
    nvl(receive_1d.recent_days, dispatch_1d.recent_days) recent_days,
    receive_order_count,
    receive_order_amount,
    dispatch_order_count,
    dispatch_order_amount
    from (select 1 recent_days,
    sum(order_count) receive_order_count,
    sum(order_amount) receive_order_amount
    from dws_trans_org_receive_1d
    where ds = '{0}') receive_1d
    full outer join
    (select 1 recent_days,
    order_count dispatch_order_count,
    order_amount dispatch_order_amount
    from dws_trans_dispatch_1d
    where ds = '{0}') dispatch_1d
    on receive_1d.recent_days = dispatch_1d.recent_days
    union
    select '{0}' ds,
    nvl(receive_nd.recent_days, dispatch_nd.recent_days) recent_days,
    receive_order_count,
    receive_order_amount,
    dispatch_order_count,
    dispatch_order_amount
    from (select recent_days,
    sum(order_count) receive_order_count,
    sum(order_amount) receive_order_amount
    from dws_trans_org_receive_nd
    where ds = '{0}'
    group by recent_days) receive_nd
    full outer join
    (select recent_days,
    order_count dispatch_order_count,
    order_amount dispatch_order_amount
    from dws_trans_dispatch_nd
    where ds = '{0}') dispatch_nd
    on receive_nd.recent_days = dispatch_nd.recent_days
    """.format(ds)
    df = spark.sql(insert_sql)
    insert_to_hive(df, "ads_trans_order_stats", ds)
def etl_ads_trans_order_stats_td (spark, ds):
    """历史至今运单统计表"""
    #创建表
    create_sql = """
    create external table ads_trans_order_stats_td(
    ds string COMMENT ' 统计日期 ',
    bounding_order_count bigint COMMENT ' 运输中运单总数 ',
    bounding_order_amount decimal (16,2) COMMENT ' 运输中运单金额 '
    ) comment ' 历史至今运单统计 '
    row format delimited fields terminated by '\t'
    location '/warehouse/tms/ads/ads_trans_order_stats_td'
    """
    spark.sql ("drop table if exists ads_trans_order_stats_td")
    spark.sql (create_sql)
    #插入数据
    insert_sql = """
    select ds,
    bounding_order_count,
    bounding_order_amount
    from ads_trans_order_stats_td
    where ds != '{0}'
    union all
    select ds,
    sum(order_count) as bounding_order_count,
    sum(order_amount) as bounding_order_amount
    from (
    select ds,
    order_count,
    order_amount
    from dws_trans_dispatch_td
    where ds = '{0}'
    union all
    select ds,
    order_count * (-1) as order_count,
    order_amount * (-1) as order_amount
    from dws_trans_bound_finish_td
    where ds = '{0}'
    ) new
    group by ds
    """.format(ds)
    df = spark.sql(insert_sql)
    insert_to_hive(df, "ads_trans_order_stats_td", ds)
def etl_ads_trans_stats (spark, ds):
    """运输综合统计表"""
    #创建表
    create_sql = """
    create external table ads_trans_stats(
    ds string COMMENT ' 统计日期 ',
    recent_days tinyint COMMENT ' 最近天数，1: 最近 1 天，7: 最近 7 天，30: 最近 30 天 ',
    trans_finish_count bigint COMMENT ' 完成运输次数 ',
    trans_finish_distance decimal (16,2) COMMENT ' 完成运输里程 ',
    trans_finish_dur_sec bigint COMMENT ' 完成运输时长，单位：秒 '
    ) comment ' 运输综合统计 '
    row format delimited fields terminated by '\t'
    location '/warehouse/tms/ads/ads_trans_stats'
    """
    spark.sql ("drop table if exists ads_trans_stats")
    spark.sql (create_sql)
    #插入数据
    insert_sql = """
    select ds,
    recent_days,
    trans_finish_count,
    trans_finish_distance,
    trans_finish_dur_sec
    from ads_trans_stats
    union
    select '{0}' ds,
    1 recent_days,
    sum(trans_finish_count) trans_finish_count,
    sum(trans_finish_distance) trans_finish_distance,
    sum(trans_finish_dur_sec) trans_finish_dur_sec
    from dws_trans_org_truck_model_type_trans_finish_1d
    where ds = '{0}'
    union
    select '{0}' ds,
    recent_days,
    sum(trans_finish_count) trans_finish_count,
    sum(trans_finish_distance) trans_finish_distance,
    sum(trans_finish_dur_sec) trans_finish_dur_sec
    from dws_trans_shift_trans_finish_nd
    where ds = '{0}'
    group by recent_days
    """.format(ds)
    df = spark.sql(insert_sql)
    insert_to_hive(df, "ads_trans_stats", ds)
def etl_ads_truck_stats (spark, ds):
    """卡车分析表"""
    #创建表
    create_sql = """
    create external table ads_truck_stats(
    ds string COMMENT ' 统计日期 ',
    recent_days tinyint COMMENT ' 最近天数，1: 最近 1 天，7: 最近 7 天，30: 最近 30 天 ',
    truck_model_type string COMMENT ' 卡车类别编码 ',
    truck_model_type_name string COMMENT ' 卡车类别名称 ',
    trans_finish_count bigint COMMENT ' 完成运输次数 ',
    trans_finish_distance decimal (16,2) COMMENT ' 完成运输里程 ',
    trans_finish_dur_sec bigint COMMENT ' 完成运输时长，单位：秒 ',
    avg_trans_finish_distance decimal (16,2) COMMENT ' 平均每次运输里程 ',
    avg_trans_finish_dur_sec bigint COMMENT ' 平均每次运输时长，单位：秒 '
    ) comment ' 卡车分析 '
    row format delimited fields terminated by '\t'
    location '/warehouse/tms/ads/ads_truck_stats'
    """
    spark.sql ("drop table if exists ads_truck_stats")
    spark.sql (create_sql)
    #插入数据
    insert_sql = """
    select ds,
    recent_days,
    truck_model_type,
    truck_model_type_name,
    trans_finish_count,
    trans_finish_distance,
    trans_finish_dur_sec,
    avg_trans_finish_distance,
    avg_trans_finish_dur_sec
    from ads_truck_stats
    union all
    select '{0}' ds,
    recent_days,
    truck_model_type,
    truck_model_type_name,
    sum(trans_finish_count) trans_finish_count,
    sum(trans_finish_distance) trans_finish_distance,
    sum(trans_finish_dur_sec) trans_finish_dur_sec,
    case when sum(trans_finish_count) = 0 then 0
    else sum(trans_finish_distance) / sum(trans_finish_count)
    end avg_trans_finish_distance,
    case when sum(trans_finish_count) = 0 then 0
    else sum(trans_finish_dur_sec) / sum(trans_finish_count)
    end avg_trans_finish_dur_sec
    from dws_trans_shift_trans_finish_nd
    where ds = '{0}'
    group by truck_model_type, truck_model_type_name, recent_days
    """.format(ds)
    df = spark.sql(insert_sql)
    insert_to_hive(df, "ads_truck_stats", ds)
def main (partition_date):
    """主函数：执行所有 ADS 层表的 ETL"""
    spark = get_spark_session ()
    #执行各 ADS 表的 ETL
    etl_ads_city_stats(spark, partition_date)
    etl_ads_driver_stats(spark, partition_date)
    etl_ads_express_city_stats(spark, partition_date)
    etl_ads_express_org_stats(spark, partition_date)
    etl_ads_express_province_stats(spark, partition_date)
    etl_ads_express_stats(spark, partition_date)
    etl_ads_line_stats(spark, partition_date)
    etl_ads_order_cargo_type_stats(spark, partition_date)
    etl_ads_order_stats(spark, partition_date)
    etl_ads_org_stats(spark, partition_date)
    etl_ads_shift_stats(spark, partition_date)
    etl_ads_trans_order_stats(spark, partition_date)
    etl_ads_trans_order_stats_td(spark, partition_date)
    etl_ads_trans_stats(spark, partition_date)
    etl_ads_truck_stats(spark, partition_date)
    print("[SUCCESS] All TMS ADS tables ETL completed for ds={0}".format(partition_date))
    spark.stop()
if __name__ == "main":
    if len(sys.argv) != 2:
        print("Usage: spark-submit tms_ads.py <partition_date>")
sys.exit(1)
ds = sys.argv[1]
main(ds)