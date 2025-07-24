# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
import sys


def get_spark_session():
    """获取配置好的SparkSession"""
    spark = SparkSession.builder \
        .appName("TMSDwsETL") \
        .config("hive.metastore.uris", "thrift://cdh01:9083") \
        .config("spark.sql.hive.convertMetastoreOrc", "true") \
        .config("spark.hadoop.hive.exec.dynamic.partition", "true") \
        .config("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict") \
        .enableHiveSupport() \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.sql("USE tms")  # 切换到目标数据库
    return spark


def insert_to_hive(df, table_name, partition_date):
    """将DataFrame写入Hive表"""
    if df.rdd.isEmpty():
        print("[WARN] No data found for {0} on {1}, skipping write.".format(table_name, partition_date))
        return
    # 添加分区字段
    df_with_partition = df.withColumn("ds", lit(partition_date))
    # 写入Hive表
    df_with_partition.write.mode("overwrite").insertInto("tms.{0}".format(table_name))


def etl_dws_trade_org_cargo_type_order_1d(spark, ds):
    """交易域机构货物类型粒度下单1日汇总表"""
    sql = """
    SELECT org_id,
           org_name,
           city_id,
           region.name city_name,
           cargo_type,
           cargo_type_name,
           order_count,
           order_amount
    FROM (
        SELECT org_id,
               org_name,
               sender_city_id  city_id,
               cargo_type,
               cargo_type_name,
               count(order_id) order_count,
               sum(amount)     order_amount
        FROM (
            SELECT order_id,
                   cargo_type,
                   cargo_type_name,
                   sender_district_id,
                   sender_city_id,
                   max(amount) amount,
                   ds  -- 确保从子查询中保留ds列
            FROM (
                SELECT order_id,
                       cargo_type,
                       cargo_type_name,
                       sender_district_id,
                       sender_city_id,
                       amount,
                       ds
                FROM dwd_trade_order_detail_inc
                WHERE ds = '{0}'
            ) detail
            GROUP BY order_id, cargo_type, cargo_type_name, sender_district_id, sender_city_id, ds
        ) distinct_detail
        LEFT JOIN (
            SELECT id org_id,
                   org_name,
                   region_id,
                   ds  -- 从dim_organ_full表中选择ds列
            FROM dim_organ_full
            WHERE ds = '{0}'
        ) org ON distinct_detail.sender_district_id = org.region_id
        WHERE org.ds = '{0}'  -- 添加过滤条件确保ds匹配
        GROUP BY org_id, org_name, cargo_type, cargo_type_name, sender_city_id
    ) agg
    LEFT JOIN (
        SELECT id, name, ds  -- 从dim_region_full表中选择ds列
        FROM dim_region_full
        WHERE ds = '{0}'
    ) region ON agg.city_id = region.id
    WHERE region.ds = '{0}'  -- 添加过滤条件确保ds匹配
    """.format(ds)
    df = spark.sql(sql)
    insert_to_hive(df, "dws_trade_org_cargo_type_order_1d", ds)


def etl_dws_trans_dispatch_1d(spark, ds):
    """物流域发单1日汇总表"""
    sql = """
    SELECT count(order_id)      order_count,
           sum(distinct_amount) order_amount
    FROM (
        SELECT order_id,
               ds,
               max(amount) distinct_amount
        FROM dwd_trans_dispatch_detail_inc
        WHERE ds = '{0}'
        GROUP BY order_id, ds
    ) distinct_info
    """.format(ds)
    df = spark.sql(sql)
    insert_to_hive(df, "dws_trans_dispatch_1d", ds)


def etl_dws_trans_org_deliver_suc_1d(spark, ds):
    """物流域转运站粒度派送成功1日汇总表"""
    sql = """
    SELECT org_id,
           org_name,
           city_id,
           city.name       city_name,
           province_id,
           province.name   province_name,
           count(order_id) order_count
    FROM (
        SELECT order_id,
               receiver_district_id,
               ds
        FROM dwd_trans_deliver_suc_detail_inc
        WHERE ds = '{0}'
        GROUP BY order_id, receiver_district_id, ds
    ) detail
    LEFT JOIN (
        SELECT id        org_id,
               org_name,
               region_id district_id
        FROM dim_organ_full
        WHERE ds = '{0}'
    ) organ ON detail.receiver_district_id = organ.district_id
    LEFT JOIN (
        SELECT id,
               parent_id city_id
        FROM dim_region_full
        WHERE ds = '{0}'
    ) district ON organ.district_id = district.id
    LEFT JOIN (
        SELECT id,
               name,
               parent_id province_id
        FROM dim_region_full
        WHERE ds = '{0}'
    ) city ON district.city_id = city.id
    LEFT JOIN (
        SELECT id,
               name
        FROM dim_region_full
        WHERE ds = '{0}'
    ) province ON city.province_id = province.id
    GROUP BY org_id, org_name, city_id, city.name, province_id, province.name, ds
    """.format(ds)
    df = spark.sql(sql)
    insert_to_hive(df, "dws_trans_org_deliver_suc_1d", ds)


def etl_dws_trans_org_receive_1d(spark, ds):
    """物流域转运站粒度揽收1日汇总表"""
    sql = """
    SELECT org_id,
           org_name,
           city_id,
           city_name,
           province_id,
           province_name,
           count(order_id)      order_count,
           sum(distinct_amount) order_amount
    FROM (
        SELECT order_id,
               org_id,
               org_name,
               city_id,
               city_name,
               province_id,
               province_name,
               max(amount) distinct_amount,
               ds  -- 保留ds字段并传递到外层
        FROM (
            SELECT order_id,
                   amount,
                   sender_district_id,
                   ds  -- 最内层子查询的ds
            FROM dwd_trans_receive_detail_inc
            WHERE ds = '{0}'
        ) detail
        LEFT JOIN (
            SELECT id org_id,
                   org_name,
                   region_id
            FROM dim_organ_full
            WHERE ds = '{0}'
        ) organ ON detail.sender_district_id = organ.region_id
        LEFT JOIN (
            SELECT id,
                   parent_id
            FROM dim_region_full
            WHERE ds = '{0}'
        ) district ON organ.region_id = district.id
        LEFT JOIN (
            SELECT id   city_id,
                   name city_name,
                   parent_id
            FROM dim_region_full
            WHERE ds = '{0}'
        ) city ON district.parent_id = city.city_id  -- 用重命名后的city_id关联
        LEFT JOIN (
            SELECT id   province_id,
                   name province_name
            FROM dim_region_full
            WHERE ds = '{0}'
        ) province ON city.parent_id = province.province_id  -- 用重命名后的province_id关联
        GROUP BY order_id, org_id, org_name, city_id, city_name, province_id, province_name, ds  -- 包含ds作为分组依据
    ) distinct_tb
    GROUP BY org_id, org_name, city_id, city_name, province_id, province_name, ds  -- 外层聚合包含ds
    """.format(ds)
    df = spark.sql(sql)
    insert_to_hive(df, "dws_trans_org_receive_1d", ds)


def etl_dws_trans_org_sort_1d(spark, ds):
    """物流域机构粒度分拣1日汇总表"""
    sql = """
    SELECT org_id,
           org_name,
           if(org_level = 1, city_for_level1.id, province_for_level1.id)         city_id,
           if(org_level = 1, city_for_level1.name, province_for_level1.name)     city_name,
           if(org_level = 1, province_for_level1.id, province_for_level2.id)     province_id,
           if(org_level = 1, province_for_level1.name, province_for_level2.name) province_name,
           sort_count
    FROM (
        SELECT org_id,
               count(*) sort_count,
               ds
        FROM dwd_bound_sort_inc
        WHERE ds = '{0}'
        GROUP BY org_id, ds
    ) agg
    LEFT JOIN (
        SELECT id,
               org_name,
               org_level,
               region_id
        FROM dim_organ_full
        WHERE ds = '{0}'
    ) org ON agg.org_id = org.id
    LEFT JOIN (
        SELECT id,
               name,
               parent_id
        FROM dim_region_full
        WHERE ds = '{0}'
    ) city_for_level1 ON org.region_id = city_for_level1.id
    LEFT JOIN (
        SELECT id,
               name,
               parent_id
        FROM dim_region_full
        WHERE ds = '{0}'
    ) province_for_level1 ON city_for_level1.parent_id = province_for_level1.id
    LEFT JOIN (
        SELECT id,
               name
        FROM dim_region_full
        WHERE ds = '{0}'
    ) province_for_level2 ON province_for_level1.parent_id = province_for_level2.id
    """.format(ds)
    df = spark.sql(sql)
    insert_to_hive(df, "dws_trans_org_sort_1d", ds)


def etl_dws_trans_org_truck_model_type_trans_finish_1d(spark, ds):
    """物流域机构卡车类别粒度运输1日汇总表"""
    sql = """
    SELECT org_id,
           org_name,
           truck_model_type,
           truck_model_type_name,
           count(trans_finish.id) truck_finish_count,
           sum(actual_distance)   trans_finish_distance,
           sum(finish_dur_sec)    trans_finish_dur_sec
    FROM (
        SELECT id,
               start_org_id   org_id,
               start_org_name org_name,
               truck_id,
               actual_distance,
               finish_dur_sec,
               ds
        FROM dwd_trans_trans_finish_inc
        WHERE ds = '{0}'
    ) trans_finish
    LEFT JOIN (
        SELECT id,
               truck_model_type,
               truck_model_type_name
        FROM dim_truck_full
        WHERE ds = '{0}'
    ) truck_info ON trans_finish.truck_id = truck_info.id
    GROUP BY org_id, org_name, truck_model_type, truck_model_type_name, ds
    """.format(ds)
    df = spark.sql(sql)
    insert_to_hive(df, "dws_trans_org_truck_model_type_trans_finish_1d", ds)


def etl_dws_trade_org_cargo_type_order_nd(spark, ds):
    """交易域机构货物类型粒度下单n日汇总表"""
    sql = """
    SELECT org_id,
           org_name,
           city_id,
           city_name,
           cargo_type,
           cargo_type_name,
           recent_days,
           sum(order_count) as order_count,
           sum(order_amount) as order_amount
    FROM dws_trade_org_cargo_type_order_1d
    LATERAL VIEW explode(array(7, 30)) tmp as recent_days
    WHERE ds >= regexp_replace(
            date_add(regexp_replace('{0}', '(\d{{4}})(\d{{2}})(\d{{2}})', '$1-$2-$3'), -recent_days + 1),
            '-', ''
        )
      AND ds <= '{0}'
    GROUP BY org_id, org_name, city_id, city_name, cargo_type, cargo_type_name, recent_days
    """.format(ds)
    df = spark.sql(sql)
    insert_to_hive(df, "dws_trade_org_cargo_type_order_nd", ds)


def etl_dws_trans_bound_finish_td(spark, ds):
    """物流域转运完成历史至今汇总表"""
    sql = """
    SELECT count(order_id)   order_count,
           sum(order_amount) order_amount
    FROM (
        SELECT order_id,
               max(amount) order_amount
        FROM dwd_trans_bound_finish_detail_inc
        WHERE ds <= '{0}'
        GROUP BY order_id
    ) distinct_info
    """.format(ds)
    df = spark.sql(sql)
    insert_to_hive(df, "dws_trans_bound_finish_td", ds)


def etl_dws_trans_dispatch_nd(spark, ds):
    """物流域发单n日汇总表"""
    sql = """
    SELECT recent_days,
           sum(order_count) as order_count,
           sum(order_amount) as order_amount
    FROM dws_trans_dispatch_1d
    LATERAL VIEW explode(array(7, 30)) tmp as recent_days
    WHERE ds >= regexp_replace(
            date_add(regexp_replace('{0}', '(\d{{4}})(\d{{2}})(\d{{2}})', '$1-$2-$3'), -recent_days + 1),
            '-', ''
        )
      AND ds <= '{0}'
    GROUP BY recent_days
    """.format(ds)
    df = spark.sql(sql)
    insert_to_hive(df, "dws_trans_dispatch_nd", ds)


def etl_dws_trans_dispatch_td(spark, ds):
    """物流域发单历史至今汇总表"""
    sql = """
    SELECT sum(order_count)  order_count,
           sum(order_amount) order_amount
    FROM dws_trans_dispatch_1d
    WHERE ds <= '{0}'
    """.format(ds)
    df = spark.sql(sql)
    insert_to_hive(df, "dws_trans_dispatch_td", ds)


def etl_dws_trans_org_deliver_suc_nd(spark, ds):
    """物流域转运站粒度派送成功n日汇总表"""
    sql = """
    SELECT org_id,
           org_name,
           city_id,
           city_name,
           province_id,
           province_name,
           recent_days,
           sum(order_count) as order_count
    FROM dws_trans_org_deliver_suc_1d
    LATERAL VIEW explode(array(7, 30)) tmp as recent_days
    WHERE ds >= regexp_replace(
            date_add(regexp_replace('{0}', '(\d{{4}})(\d{{2}})(\d{{2}})', '$1-$2-$3'), -recent_days + 1),
            '-', ''
        )
      AND ds <= '{0}'
    GROUP BY org_id, org_name, city_id, city_name, province_id, province_name, recent_days
    """.format(ds)
    df = spark.sql(sql)
    insert_to_hive(df, "dws_trans_org_deliver_suc_nd", ds)


def etl_dws_trans_org_receive_nd(spark, ds):
    """物流域转运站粒度揽收n日汇总表"""
    # 计算7天和30天的起始日期
    start_7d = spark.sql(
        "select regexp_replace(date_add(regexp_replace('{0}', '(\d{{4}})(\d{{2}})(\d{{2}})', '$1-$2-$3'), -6), '-', '') as start".format(
            ds)).collect()[0][0]
    start_30d = spark.sql(
        "select regexp_replace(date_add(regexp_replace('{0}', '(\d{{4}})(\d{{2}})(\d{{2}})', '$1-$2-$3'), -29), '-', '') as start".format(
            ds)).collect()[0][0]

    sql = """
    -- 7天汇总
    SELECT org_id, org_name, city_id, city_name, province_id, province_name,
           7 as recent_days,
           sum(order_count) as order_count,
           sum(order_amount) as order_amount
    FROM dws_trans_org_receive_1d
    WHERE ds between '{0}' and '{1}'
    GROUP BY org_id, org_name, city_id, city_name, province_id, province_name

    UNION ALL

    -- 30天汇总
    SELECT org_id, org_name, city_id, city_name, province_id, province_name,
           30 as recent_days,
           sum(order_count) as order_count,
           sum(order_amount) as order_amount
    FROM dws_trans_org_receive_1d
    WHERE ds between '{2}' and '{1}'
    GROUP BY org_id, org_name, city_id, city_name, province_id, province_name
    """.format(start_7d, ds, start_30d)
    df = spark.sql(sql)
    insert_to_hive(df, "dws_trans_org_receive_nd", ds)


def etl_dws_trans_org_sort_nd(spark, ds):
    """物流域机构粒度分拣n日汇总表"""
    sql = """
    SELECT org_id,
           org_name,
           city_id,
           city_name,
           province_id,
           province_name,
           recent_days,
           sum(sort_count) as sort_count
    FROM dws_trans_org_sort_1d
    LATERAL VIEW explode(array(7, 30)) tmp as recent_days
    WHERE ds >= regexp_replace(
            date_add(regexp_replace('{0}', '(\d{{4}})(\d{{2}})(\d{{2}})', '$1-$2-$3'), -recent_days + 1),
            '-', ''
        )
      AND ds <= '{0}'
    GROUP BY org_id, org_name, city_id, city_name, province_id, province_name, recent_days
    """.format(ds)
    df = spark.sql(sql)
    insert_to_hive(df, "dws_trans_org_sort_nd", ds)


def etl_dws_trans_shift_trans_finish_nd(spark, ds):
    """物流域班次粒度转运完成n日汇总表"""
    # 计算30天前的日期
    start_30d = spark.sql(
        "select regexp_replace(date_add(regexp_replace('{0}', '(\d{{4}})(\d{{2}})(\d{{2}})', '$1-$2-$3'), -29), '-', '') as start".format(
            ds)).collect()[0][0]

    sql = """
    SELECT aggregated.shift_id,
           if(first.org_level = 1, first.region_id, city.id) as city_id,
           if(first.org_level = 1, first.region_name, city.name) as city_name,
           aggregated.org_id,
           aggregated.org_name,
           aggregated.line_id,
           for_line_name.line_name,
           aggregated.driver1_emp_id,
           aggregated.driver1_name,
           aggregated.driver2_emp_id,
           aggregated.driver2_name,
           truck_info.truck_model_type,
           truck_info.truck_model_type_name,
           30 as recent_days,
           sum(aggregated.trans_finish_count) as trans_finish_count,
           sum(aggregated.trans_finish_distance) as trans_finish_distance,
           sum(aggregated.trans_finish_dur_sec) as trans_finish_dur_sec,
           sum(aggregated.trans_finish_order_count) as trans_finish_order_count,
           sum(aggregated.trans_finish_delay_count) as trans_finish_delay_count
    FROM (
        SELECT shift_id,
               line_id,
               truck_id,
               start_org_id as org_id,
               start_org_name as org_name,
               driver1_emp_id,
               driver1_name,
               driver2_emp_id,
               driver2_name,
               count(id) as trans_finish_count,
               sum(actual_distance) as trans_finish_distance,
               sum(finish_dur_sec) as trans_finish_dur_sec,
               sum(order_num) as trans_finish_order_count,
               sum(if(actual_end_time > estimate_end_time, 1, 0)) as trans_finish_delay_count
        FROM dwd_trans_trans_finish_inc
        WHERE ds between '{0}' and '{1}'
        GROUP BY shift_id, line_id, truck_id, start_org_id, start_org_name,
                 driver1_emp_id, driver1_name, driver2_emp_id, driver2_name
    ) aggregated
    LEFT JOIN dim_organ_full first 
           ON aggregated.org_id = first.id 
          AND first.ds = '{1}'
    LEFT JOIN dim_region_full parent 
           ON first.region_id = parent.id 
          AND parent.ds = '{1}'
    LEFT JOIN dim_region_full city 
           ON parent.parent_id = city.id 
          AND city.ds = '{1}'
    LEFT JOIN dim_shift_full for_line_name 
           ON aggregated.shift_id = for_line_name.id 
          AND for_line_name.ds = '{1}'
    LEFT JOIN dim_truck_full truck_info 
           ON aggregated.truck_id = truck_info.id 
          AND truck_info.ds = '{1}'
    GROUP BY aggregated.shift_id,
             if(first.org_level = 1, first.region_id, city.id),
             if(first.org_level = 1, first.region_name, city.name),
             aggregated.org_id,
             aggregated.org_name,
             aggregated.line_id,
             for_line_name.line_name,
             aggregated.driver1_emp_id,
             aggregated.driver1_name,
             aggregated.driver2_emp_id,
             aggregated.driver2_name,
             truck_info.truck_model_type,
             truck_info.truck_model_type_name
    """.format(start_30d, ds)
    df = spark.sql(sql)
    insert_to_hive(df, "dws_trans_shift_trans_finish_nd", ds)


def main(partition_date):
    """主函数：执行所有DWS层表的ETL"""
    spark = get_spark_session()

    # 1日汇总表（先执行，作为后续汇总的基础）
    etl_dws_trade_org_cargo_type_order_1d(spark, partition_date)
    etl_dws_trans_dispatch_1d(spark, partition_date)
    etl_dws_trans_org_deliver_suc_1d(spark, partition_date)
    etl_dws_trans_org_receive_1d(spark, partition_date)
    etl_dws_trans_org_sort_1d(spark, partition_date)
    etl_dws_trans_org_truck_model_type_trans_finish_1d(spark, partition_date)

    # n日汇总表（依赖1日表）
    etl_dws_trade_org_cargo_type_order_nd(spark, partition_date)
    etl_dws_trans_dispatch_nd(spark, partition_date)
    etl_dws_trans_org_deliver_suc_nd(spark, partition_date)
    etl_dws_trans_org_receive_nd(spark, partition_date)
    etl_dws_trans_org_sort_nd(spark, partition_date)
    etl_dws_trans_shift_trans_finish_nd(spark, partition_date)

    # 历史至今汇总表
    etl_dws_trans_bound_finish_td(spark, partition_date)
    etl_dws_trans_dispatch_td(spark, partition_date)

    print("[SUCCESS] All TMS DWS tables ETL completed for ds={0}".format(partition_date))
    spark.stop()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: spark-submit tms_dws.py <partition_date>")
        sys.exit(1)
    ds = sys.argv[1]
    main(ds)