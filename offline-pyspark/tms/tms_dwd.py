# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql import functions as F  # 统一导入functions模块并别名F
import sys

from pyspark.sql.types import StringType


# 获取SparkSession
def get_spark_session():
    spark = SparkSession.builder \
        .appName("TmsDwdEtlFull") \
        .config("hive.metastore.uris", "thrift://cdh01:9083") \
        .config("spark.sql.hive.convertMetastoreOrc", "true") \
        .config("spark.hadoop.hive.exec.dynamic.partition", "true") \
        .config("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict") \
        .enableHiveSupport() \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.sql("USE tms")
    return spark


# 创建外部表通用函数（修正版本：分开执行DROP和CREATE）
def create_external_table(spark, table_name, schema, location, partition_cols):
    """
    创建外部表
    :param spark: SparkSession
    :param table_name: 表名
    :param schema: 字段列表，格式：[(字段名, 类型, 注释), ...]
    :param location: 存储路径
    :param partition_cols: 分区字段列表，格式：[(字段名, 类型, 注释), ...]
    """
    # 1. 先执行删除表语句（单独执行）
    drop_sql = "DROP TABLE IF EXISTS {}".format(table_name)
    spark.sql(drop_sql)

    # 构建字段定义
    fields_def = ", ".join([
        "`{0}` {1} COMMENT '{2}'".format(col_name, data_type, comment)
        for col_name, data_type, comment in schema
    ])
    # 构建分区字段定义
    partition_def = ", ".join([
        "`{0}` {1} COMMENT '{2}'".format(col_name, data_type, comment)
        for col_name, data_type, comment in partition_cols
    ]) if partition_cols else ""
    # 表注释（从表名提取）
    table_comment = "COMMENT '{0}事务事实表'".format(schema[0][2].split('事务')[0]) if len(schema) > 0 else ""

    # 2. 构建并执行创建表语句（单独执行）
    create_sql = """
    CREATE EXTERNAL TABLE {0}(
        {1}
    )
    {2}
    PARTITIONED BY ({3})
    STORED AS ORC
    LOCATION '{4}'
    TBLPROPERTIES('orc.compress' = 'snappy')
    """.format(table_name, fields_def, table_comment, partition_def, location)
    spark.sql(create_sql)


# 插入分区表通用函数（优化：确保列顺序与目标表一致，兼容Python 2.x）
def insert_overwrite_partition(df, table_name, partition_col, partition_val, spark):
    """覆盖写入分区表，确保列顺序与目标表一致"""
    # 添加分区列
    df = df.withColumn(partition_col, F.lit(partition_val))
    # 获取目标表的列顺序（使用format格式化，兼容Python 2.x）
    target_cols = spark.sql("SELECT * FROM {0} LIMIT 0".format(table_name)).columns
    # 按目标表列顺序重新排序
    df = df.select(target_cols)
    # 写入数据
    df.write.mode("overwrite").insertInto(table_name)


# ------------------------------ dwd_trade_order_detail_inc ------------------------------
def etl_dwd_trade_order_detail_inc(spark, ds):
    # 读取源表数据
    cargo_df = spark.sql("""
        SELECT 
            after.id,
            after.order_id,
            after.cargo_type,
            after.volume_length,
            after.volume_width,
            after.volume_height,
            after.weight,
            concat(substring(after.create_time, 1, 10), ' ', substring(after.create_time, 12, 8)) AS order_time,
            unix_timestamp(now()) AS ts  -- 生成时间戳（秒级）作为ts源
        FROM ods_order_cargo AS after
        WHERE after.is_deleted = '0'
    """)

    info_df = spark.sql("""
        SELECT 
            after.id,
            after.order_no,
            after.status,
            after.collect_type,
            after.user_id,
            after.receiver_complex_id,
            after.receiver_province_id,
            after.receiver_city_id,
            after.receiver_district_id,
            concat(substring(after.receiver_name, 1, 1), '*') AS receiver_name,
            after.sender_complex_id,
            after.sender_province_id,
            after.sender_city_id,
            after.sender_district_id,
            concat(substring(after.sender_name, 1, 1), '*') AS sender_name,
            after.cargo_num,
            after.amount,
            date_format(
                from_utc_timestamp(
                    to_timestamp(CAST(after.estimate_arrive_time AS BIGINT) / 1000),
                    'UTC'
                ),
                'yyyy-MM-dd HH:mm:ss'
            ) AS estimate_arrive_time,
            after.distance
        FROM ods_order_info AS after
        WHERE after.is_deleted = '0'
    """)

    # 维度表数据
    base_dic = spark.sql("SELECT id, name FROM ods_base_dic WHERE is_deleted = '0'")

    # 关联数据，补充ts字段
    result_df = cargo_df.alias("cargo") \
        .join(info_df.alias("info"), F.col("cargo.order_id") == F.col("info.id"), "inner") \
        .join(
        base_dic.alias("cargo_type_dic"),
        F.col("cargo.cargo_type") == F.col("cargo_type_dic.id").cast(StringType()),
        "left"
    ) \
        .join(
        base_dic.alias("status_dic"),
        F.col("info.status") == F.col("status_dic.id").cast(StringType()),
        "left"
    ) \
        .join(
        base_dic.alias("collect_type_dic"),
        F.col("info.collect_type") == F.col("collect_type_dic.id").cast(StringType()),
        "left"
    ) \
        .select(
        F.col("cargo.id"),
        F.col("cargo.order_id"),
        F.col("cargo.cargo_type"),
        F.col("cargo_type_dic.name").alias("cargo_type_name"),
        F.col("cargo.volume_length"),
        F.col("cargo.volume_width"),
        F.col("cargo.volume_height"),
        F.col("cargo.weight"),
        F.col("cargo.order_time"),
        F.col("info.order_no"),
        F.col("info.status"),
        F.col("status_dic.name").alias("status_name"),
        F.col("info.collect_type"),
        F.col("collect_type_dic.name").alias("collect_type_name"),
        F.col("info.user_id"),
        F.col("info.receiver_complex_id"),
        F.col("info.receiver_province_id"),
        F.col("info.receiver_city_id"),
        F.col("info.receiver_district_id"),
        F.col("info.receiver_name"),
        F.col("info.sender_complex_id"),
        F.col("info.sender_province_id"),
        F.col("info.sender_city_id"),
        F.col("info.sender_district_id"),
        F.col("info.sender_name"),
        F.col("info.cargo_num"),
        F.col("info.amount"),
        F.col("info.estimate_arrive_time"),
        F.col("info.distance"),
        F.col("cargo.ts").cast("bigint").alias("ts"),  # 补充ts字段（匹配表结构）
        F.date_format(F.col("cargo.order_time"), 'yyyy-MM-dd').alias("ds")  # 分区字段ds
    )

    insert_overwrite_partition(result_df, "dwd_trade_order_detail_inc", "ds", ds, spark)


# ------------------------------ dwd_trade_pay_suc_detail_inc ------------------------------
def etl_dwd_trade_pay_suc_detail_inc(spark, ds):
    cargo_df = spark.sql("""
        SELECT 
            after.id,
            after.order_id,
            after.cargo_type,
            after.volume_length,
            after.volume_width,
            after.volume_height,
            after.weight,
            unix_timestamp(now()) AS ts  -- 时间戳字段
        FROM ods_order_cargo AS after
        WHERE after.is_deleted = '0'
    """)

    info_df = spark.sql("""
        SELECT 
            after.id,
            after.order_no,
            after.status,
            after.collect_type,
            after.user_id,
            after.receiver_complex_id,
            after.receiver_province_id,
            after.receiver_city_id,
            after.receiver_district_id,
            concat(substring(after.receiver_name, 1, 1), '*') AS receiver_name,
            after.sender_complex_id,
            after.sender_province_id,
            after.sender_city_id,
            after.sender_district_id,
            concat(substring(after.sender_name, 1, 1), '*') AS sender_name,
            after.payment_type,
            after.cargo_num,
            after.amount,
            date_format(
                from_utc_timestamp(
                    to_timestamp(CAST(after.estimate_arrive_time AS BIGINT) / 1000),
                    'UTC'
                ),
                'yyyy-MM-dd HH:mm:ss'
            ) AS estimate_arrive_time,
            after.distance,
            concat(substring(after.update_time, 1, 10), ' ', substring(after.update_time, 12, 8)) AS payment_time
        FROM ods_order_info AS after
    """)

    base_dic = spark.sql("SELECT id, name FROM ods_base_dic WHERE is_deleted = '0'")

    result_df = cargo_df.alias("cargo") \
        .join(info_df.alias("info"), F.col("cargo.order_id") == F.col("info.id"), "inner") \
        .join(
        base_dic.alias("cargo_type_dic"),
        F.col("cargo.cargo_type") == F.col("cargo_type_dic.id").cast(StringType()),
        "left"
    ) \
        .join(
        base_dic.alias("status_dic"),
        F.col("info.status") == F.col("status_dic.id").cast(StringType()),
        "left"
    ) \
        .join(
        base_dic.alias("collect_type_dic"),
        F.col("info.collect_type") == F.col("collect_type_dic.id").cast(StringType()),
        "left"
    ) \
        .join(
        base_dic.alias("payment_type_dic"),
        F.col("info.payment_type") == F.col("payment_type_dic.id").cast(StringType()),
        "left"
    ) \
        .select(
        F.col("cargo.id"),
        F.col("cargo.order_id"),
        F.col("cargo.cargo_type"),
        F.col("cargo_type_dic.name").alias("cargo_type_name"),
        F.col("cargo.volume_length"),
        F.col("cargo.volume_width"),
        F.col("cargo.volume_height"),
        F.col("cargo.weight"),
        F.col("info.payment_time"),
        F.col("info.order_no"),
        F.col("info.status"),
        F.col("status_dic.name").alias("status_name"),
        F.col("info.collect_type"),
        F.col("collect_type_dic.name").alias("collect_type_name"),
        F.col("info.user_id"),
        F.col("info.receiver_complex_id"),
        F.col("info.receiver_province_id"),
        F.col("info.receiver_city_id"),
        F.col("info.receiver_district_id"),
        F.col("info.receiver_name"),
        F.col("info.sender_complex_id"),
        F.col("info.sender_province_id"),
        F.col("info.sender_city_id"),
        F.col("info.sender_district_id"),
        F.col("info.sender_name"),
        F.col("info.payment_type"),
        F.col("payment_type_dic.name").alias("payment_type_name"),
        F.col("info.cargo_num"),
        F.col("info.amount"),
        F.col("info.estimate_arrive_time"),
        F.col("info.distance"),
        F.col("cargo.ts").cast("bigint").alias("ts"),  # 补充ts字段
        F.date_format(F.col("info.payment_time"), 'yyyy-MM-dd').alias("ds")
    )

    insert_overwrite_partition(result_df, "dwd_trade_pay_suc_detail_inc", "ds", ds, spark)


# ------------------------------ dwd_trans_sign_detail_inc ------------------------------
def etl_dwd_trans_sign_detail_inc(spark, ds):
    cargo_df = spark.sql("""
        SELECT 
            after.id,
            after.order_id,
            after.cargo_type,
            after.volume_length,
            after.volume_width,
            after.volume_height,
            after.weight,
            concat(substring(after.create_time, 1, 10), ' ', substring(after.create_time, 12, 8)) AS order_time,
            unix_timestamp(now()) AS ts  -- 时间戳字段
        FROM ods_order_cargo AS after
        WHERE after.is_deleted = '0'
    """)

    info_df = spark.sql("""
        SELECT 
            after.id,
            after.order_no,
            after.status,
            after.collect_type,
            after.user_id,
            after.receiver_complex_id,
            after.receiver_province_id,
            after.receiver_city_id,
            after.receiver_district_id,
            concat(substring(after.receiver_name, 1, 1), '*') AS receiver_name,
            after.sender_complex_id,
            after.sender_province_id,
            after.sender_city_id,
            after.sender_district_id,
            concat(substring(after.sender_name, 1, 1), '*') AS sender_name,
            after.payment_type,
            after.cargo_num,
            after.amount,
            date_format(
                from_utc_timestamp(
                    to_timestamp(CAST(after.estimate_arrive_time AS BIGINT) / 1000),
                    'UTC'
                ),
                'yyyy-MM-dd HH:mm:ss'
            ) AS estimate_arrive_time,
            after.distance,
            concat(substring(after.update_time, 1, 10), ' ', substring(after.update_time, 12, 8)) AS sign_time,
            -- 新增end_date逻辑（参考其他表）
            CASE 
                WHEN after.status IN ('60080', '60999') THEN concat(substring(after.update_time, 1, 10))
                ELSE '9999-12-31' 
            END AS end_date
        FROM ods_order_info AS after
    """)

    base_dic = spark.sql("SELECT id, name FROM ods_base_dic WHERE is_deleted = '0'")

    result_df = cargo_df.alias("cargo") \
        .join(info_df.alias("info"), F.col("cargo.order_id") == F.col("info.id"), "inner") \
        .join(
        base_dic.alias("cargo_type_dic"),
        F.col("cargo.cargo_type") == F.col("cargo_type_dic.id").cast(StringType()),
        "left"
    ) \
        .join(
        base_dic.alias("status_dic"),
        F.col("info.status") == F.col("status_dic.id").cast(StringType()),
        "left"
    ) \
        .join(
        base_dic.alias("collect_type_dic"),
        F.col("info.collect_type") == F.col("collect_type_dic.id").cast(StringType()),
        "left"
    ) \
        .join(
        base_dic.alias("payment_type_dic"),
        F.col("info.payment_type") == F.col("payment_type_dic.id").cast(StringType()),
        "left"
    ) \
        .select(
        F.col("cargo.id"),
        F.col("cargo.order_id"),
        F.col("cargo.cargo_type"),
        F.col("cargo_type_dic.name").alias("cargo_type_name"),
        F.col("cargo.volume_length"),
        F.col("cargo.volume_width"),
        F.col("cargo.volume_height"),
        F.col("cargo.weight"),
        F.col("info.sign_time"),
        F.col("info.order_no"),
        F.col("info.status"),
        F.col("status_dic.name").alias("status_name"),
        F.col("info.collect_type"),
        F.col("collect_type_dic.name").alias("collect_type_name"),
        F.col("info.user_id"),
        F.col("info.receiver_complex_id"),
        F.col("info.receiver_province_id"),
        F.col("info.receiver_city_id"),
        F.col("info.receiver_district_id"),
        F.col("info.receiver_name"),
        F.col("info.sender_complex_id"),
        F.col("info.sender_province_id"),
        F.col("info.sender_city_id"),
        F.col("info.sender_district_id"),
        F.col("info.sender_name"),
        F.col("info.payment_type"),
        F.col("payment_type_dic.name").alias("payment_type_name"),
        F.col("info.cargo_num"),
        F.col("info.amount"),
        F.col("info.estimate_arrive_time"),
        F.col("info.distance"),
        F.col("cargo.ts").cast("bigint").alias("ts"),
        F.date_format(F.col("cargo.order_time"), 'yyyy-MM-dd').alias("start_date"),  # 取自下单时间
        F.col("info.end_date"),  # 取自info_df的end_date
        F.date_format(F.col("info.sign_time"), 'yyyy-MM-dd').alias("ds")
    )

    insert_overwrite_partition(result_df, "dwd_trans_sign_detail_inc", "ds", ds, spark)


# ------------------------------ dwd_trade_order_process_inc ------------------------------
def etl_dwd_trade_order_process_inc(spark, ds):
    cargo_df = spark.sql("""
        SELECT 
            after.id,
            after.order_id,
            after.cargo_type,
            after.volume_length,
            after.volume_width,
            after.volume_height,
            after.weight,
            concat(substring(after.create_time, 1, 10), ' ', substring(after.create_time, 12, 8)) AS order_time,
            unix_timestamp(now()) AS ts  -- 时间戳字段
        FROM ods_order_cargo AS after
        WHERE after.is_deleted = '0'
    """)

    info_df = spark.sql("""
        SELECT 
            after.id,
            after.order_no,
            after.status,
            after.collect_type,
            after.user_id,
            after.receiver_complex_id,
            after.receiver_province_id,
            after.receiver_city_id,
            after.receiver_district_id,
            concat(substring(after.receiver_name, 1, 1), '*') AS receiver_name,
            after.sender_complex_id,
            after.sender_province_id,
            after.sender_city_id,
            after.sender_district_id,
            concat(substring(after.sender_name, 1, 1), '*') AS sender_name,
            after.payment_type,
            after.cargo_num,
            after.amount,
            date_format(
                from_utc_timestamp(
                    to_timestamp(CAST(after.estimate_arrive_time AS BIGINT) / 1000),
                    'UTC'
                ),
                'yyyy-MM-dd HH:mm:ss'
            ) AS estimate_arrive_time,
            after.distance,
            CASE 
                WHEN after.status IN ('60080', '60999') THEN concat(substring(after.update_time, 1, 10))
                ELSE '9999-12-31' 
            END AS end_date
        FROM ods_order_info AS after
        WHERE after.is_deleted = '0'
    """)

    base_dic = spark.sql("SELECT id, name FROM ods_base_dic WHERE is_deleted = '0'")

    result_df = cargo_df.alias("cargo") \
        .join(info_df.alias("info"), F.col("cargo.order_id") == F.col("info.id"), "inner") \
        .join(
        base_dic.alias("cargo_type_dic"),
        F.col("cargo.cargo_type") == F.col("cargo_type_dic.id").cast(StringType()),
        "left"
    ) \
        .join(
        base_dic.alias("status_dic"),
        F.col("info.status") == F.col("status_dic.id").cast(StringType()),
        "left"
    ) \
        .join(
        base_dic.alias("collect_type_dic"),
        F.col("info.collect_type") == F.col("collect_type_dic.id").cast(StringType()),
        "left"
    ) \
        .join(
        base_dic.alias("payment_type_dic"),
        F.col("info.payment_type") == F.col("payment_type_dic.id").cast(StringType()),
        "left"
    ) \
        .select(
        F.col("cargo.id"),
        F.col("cargo.order_id"),
        F.col("cargo.cargo_type"),
        F.col("cargo_type_dic.name").alias("cargo_type_name"),
        F.col("cargo.volume_length"),
        F.col("cargo.volume_width"),
        F.col("cargo.volume_height"),
        F.col("cargo.weight"),
        F.col("cargo.order_time"),
        F.col("info.order_no"),
        F.col("info.status"),
        F.col("status_dic.name").alias("status_name"),
        F.col("info.collect_type"),
        F.col("collect_type_dic.name").alias("collect_type_name"),
        F.col("info.user_id"),
        F.col("info.receiver_complex_id"),
        F.col("info.receiver_province_id"),
        F.col("info.receiver_city_id"),
        F.col("info.receiver_district_id"),
        F.col("info.receiver_name"),
        F.col("info.sender_complex_id"),
        F.col("info.sender_province_id"),
        F.col("info.sender_city_id"),
        F.col("info.sender_district_id"),
        F.col("info.sender_name"),
        F.col("info.payment_type"),
        F.col("payment_type_dic.name").alias("payment_type_name"),
        F.col("info.cargo_num"),
        F.col("info.amount"),
        F.col("info.estimate_arrive_time"),
        F.col("info.distance"),
        F.col("cargo.ts").cast("bigint").alias("ts"),  # 补充ts字段
        F.date_format(F.col("cargo.order_time"), 'yyyy-MM-dd').alias("start_date"),
        F.col("info.end_date"),
        F.col("info.end_date").alias("ds")
    )

    # 移除partitionBy("ds")，仅保留insertInto
    result_df.write.mode("overwrite").insertInto("dwd_trade_order_process_inc")


# ------------------------------ dwd_trans_trans_finish_inc ------------------------------
def etl_dwd_trans_trans_finish_inc(spark, ds):
    # 读取源表数据
    info_df = spark.sql("""
        SELECT 
            after.id,
            after.shift_id,
            after.line_id,
            after.start_org_id,
            after.start_org_name,
            after.end_org_id,
            after.end_org_name,
            after.order_num,
            after.driver1_emp_id,
            concat(substring(after.driver1_name, 1, 1), '*') AS driver1_name,
            after.driver2_emp_id,
            concat(substring(after.driver2_name, 1, 1), '*') AS driver2_name,
            after.truck_id,
            md5(after.truck_no) AS truck_no,
            date_format(
                from_utc_timestamp(
                    to_timestamp(CAST(after.actual_start_time AS BIGINT) / 1000),
                    'UTC'
                ),
                'yyyy-MM-dd HH:mm:ss'
            ) AS actual_start_time,
            date_format(
                from_utc_timestamp(
                    to_timestamp(CAST(after.actual_end_time AS BIGINT) / 1000),
                    'UTC'
                ),
                'yyyy-MM-dd HH:mm:ss'
            ) AS actual_end_time,
            after.actual_distance,
            (CAST(after.actual_end_time AS BIGINT) - CAST(after.actual_start_time AS BIGINT)) / 1000 AS finish_dur_sec,
            date_format(
                from_utc_timestamp(
                    to_timestamp(CAST(after.actual_end_time AS BIGINT) / 1000),
                    'UTC'
                ),
                'yyyy-MM-dd'
            ) AS ds,
            unix_timestamp(now()) AS ts  -- 时间戳字段
        FROM ods_transport_task AS after
    """)

    # 关联维度表
    dim_shift = spark.sql("SELECT id, estimated_time FROM dim_shift_full")

    result_df = info_df.alias("info") \
        .join(dim_shift.alias("dim_tb"), F.col("info.shift_id") == F.col("dim_tb.id"), "left") \
        .select(
        F.col("info.id"),
        F.col("info.shift_id"),
        F.col("info.line_id"),
        F.col("info.start_org_id"),
        F.col("info.start_org_name"),
        F.col("info.end_org_id"),
        F.col("info.end_org_name"),
        F.col("info.order_num"),
        F.col("info.driver1_emp_id"),
        F.col("info.driver1_name"),
        F.col("info.driver2_emp_id"),
        F.col("info.driver2_name"),
        F.col("info.truck_id"),
        F.col("info.truck_no"),
        F.col("info.actual_start_time"),
        F.col("info.actual_end_time"),
        F.col("dim_tb.estimated_time").alias("estimate_end_time"),
        F.col("info.actual_distance"),
        F.col("info.finish_dur_sec"),
        F.col("info.ts").cast("bigint").alias("ts"),  # 补充ts字段
        F.col("info.ds")
    ) \
        .limit(200)

    insert_overwrite_partition(result_df, "dwd_trans_trans_finish_inc", "ds", ds, spark)


# ------------------------------ dwd_bound_inbound_inc ------------------------------
def etl_dwd_bound_inbound_inc(spark, ds):
    # 读取源表数据
    df = spark.sql("""
        SELECT 
            after.id,
            after.order_id,
            after.org_id,
            date_format(
                from_utc_timestamp(
                    to_timestamp(CAST(after.inbound_time AS BIGINT) / 1000),
                    'UTC'
                ),
                'yyyy-MM-dd HH:mm:ss'
            ) AS inbound_time,
            after.inbound_emp_id,
            unix_timestamp(now()) AS ts  -- 时间戳字段
        FROM ods_order_org_bound AS after
        LIMIT 250
    """)

    insert_overwrite_partition(df, "dwd_bound_inbound_inc", "ds", ds, spark)


# ------------------------------ dwd_bound_sort_inc ------------------------------
def etl_dwd_bound_sort_inc(spark, ds):
    # 读取源表数据
    df = spark.sql("""
        SELECT 
            after.id,
            after.order_id,
            after.org_id,
            date_format(
                from_utc_timestamp(
                    to_timestamp(CAST(after.sort_time AS BIGINT) / 1000),
                    'UTC'
                ),
                'yyyy-MM-dd HH:mm:ss'
            ) AS sort_time,
            after.sorter_emp_id,
            unix_timestamp(now()) AS ts  -- 时间戳字段
        FROM ods_order_org_bound AS after
        WHERE after.sort_time IS NOT NULL
        LIMIT 250
    """)

    insert_overwrite_partition(df, "dwd_bound_sort_inc", "ds", ds, spark)


# ------------------------------ dwd_bound_outbound_inc ------------------------------
def etl_dwd_bound_outbound_inc(spark, ds):
    # 读取源表数据
    df = spark.sql("""
        SELECT 
            after.id,
            after.order_id,
            after.org_id,
            date_format(
                from_utc_timestamp(
                    to_timestamp(CAST(after.outbound_time AS BIGINT) / 1000),
                    'UTC'
                ),
                'yyyy-MM-dd HH:mm:ss'
            ) AS outbound_time,
            after.outbound_emp_id,
            unix_timestamp(now()) AS ts  -- 时间戳字段
        FROM ods_order_org_bound AS after
        WHERE after.outbound_time IS NOT NULL
    """)

    insert_overwrite_partition(df, "dwd_bound_outbound_inc", "ds", ds, spark)


# 未实现的ETL函数
def etl_dwd_trade_order_cancel_detail_inc(spark, ds):
    pass


def etl_dwd_trans_receive_detail_inc(spark, ds):
    pass


def etl_dwd_trans_dispatch_detail_inc(spark, ds):
    pass


def etl_dwd_trans_bound_finish_detail_inc(spark, ds):
    pass


def etl_dwd_trans_deliver_suc_detail_inc(spark, ds):
    pass


# 主函数
def main(ds):
    spark = get_spark_session()

    # 1. dwd_trade_order_detail_inc
    schema = [
        ("id", "bigint", "运单明细ID"),
        ("order_id", "string", "运单ID"),
        ("cargo_type", "string", "货物类型ID"),
        ("cargo_type_name", "string", "货物类型名称"),
        ("volume_length", "bigint", "长cm"),
        ("volume_width", "bigint", "宽cm"),
        ("volume_height", "bigint", "高cm"),
        ("weight", "decimal(16,2)", "重量 kg"),
        ("order_time", "string", "下单时间"),
        ("order_no", "string", "运单号"),
        ("status", "string", "运单状态"),
        ("status_name", "string", "运单状态名称"),
        ("collect_type", "string", "取件类型"),
        ("collect_type_name", "string", "取件类型名称"),
        ("user_id", "bigint", "用户ID"),
        ("receiver_complex_id", "bigint", "收件人小区id"),
        ("receiver_province_id", "string", "收件人省份id"),
        ("receiver_city_id", "string", "收件人城市id"),
        ("receiver_district_id", "string", "收件人区县id"),
        ("receiver_name", "string", "收件人姓名"),
        ("sender_complex_id", "bigint", "发件人小区id"),
        ("sender_province_id", "string", "发件人省份id"),
        ("sender_city_id", "string", "发件人城市id"),
        ("sender_district_id", "string", "发件人区县id"),
        ("sender_name", "string", "发件人姓名"),
        ("cargo_num", "bigint", "货物个数"),
        ("amount", "decimal(16,2)", "金额"),
        ("estimate_arrive_time", "string", "预计到达时间"),
        ("distance", "decimal(16,2)", "距离"),
        ("ts", "bigint", "时间戳")
    ]
    partition_cols = [("ds", "string", "统计日期")]
    create_external_table(
        spark,
        "dwd_trade_order_detail_inc",
        schema,
        "/warehouse/tms/dwd/dwd_trade_order_detail_inc",
        partition_cols
    )
    etl_dwd_trade_order_detail_inc(spark, ds)

    # 2. dwd_trade_pay_suc_detail_inc
    schema = [
        ("id", "bigint", "运单明细ID"),
        ("order_id", "string", "运单ID"),
        ("cargo_type", "string", "货物类型ID"),
        ("cargo_type_name", "string", "货物类型名称"),
        ("volume_length", "bigint", "长cm"),
        ("volume_width", "bigint", "宽cm"),
        ("volume_height", "bigint", "高cm"),
        ("weight", "decimal(16,2)", "重量 kg"),
        ("payment_time", "string", "支付时间"),
        ("order_no", "string", "运单号"),
        ("status", "string", "运单状态"),
        ("status_name", "string", "运单状态名称"),
        ("collect_type", "string", "取件类型"),
        ("collect_type_name", "string", "取件类型名称"),
        ("user_id", "bigint", "用户ID"),
        ("receiver_complex_id", "bigint", "收件人小区id"),
        ("receiver_province_id", "string", "收件人省份id"),
        ("receiver_city_id", "string", "收件人城市id"),
        ("receiver_district_id", "string", "收件人区县id"),
        ("receiver_name", "string", "收件人姓名"),
        ("sender_complex_id", "bigint", "发件人小区id"),
        ("sender_province_id", "string", "发件人省份id"),
        ("sender_city_id", "string", "发件人城市id"),
        ("sender_district_id", "string", "发件人区县id"),
        ("sender_name", "string", "发件人姓名"),
        ("payment_type", "string", "支付方式"),
        ("payment_type_name", "string", "支付方式名称"),
        ("cargo_num", "bigint", "货物个数"),
        ("amount", "decimal(16,2)", "金额"),
        ("estimate_arrive_time", "string", "预计到达时间"),
        ("distance", "decimal(16,2)", "距离"),
        ("ts", "bigint", "时间戳")
    ]
    partition_cols = [("ds", "string", "统计日期")]
    create_external_table(
        spark,
        "dwd_trade_pay_suc_detail_inc",
        schema,
        "/warehouse/tms/dwd/dwd_trade_pay_suc_detail_inc",
        partition_cols
    )
    etl_dwd_trade_pay_suc_detail_inc(spark, ds)

    # 3. dwd_trade_order_cancel_detail_inc
    schema = [
        ("id", "bigint", "运单明细ID"),
        ("order_id", "string", "运单ID"),
        ("cargo_type", "string", "货物类型ID"),
        ("cargo_type_name", "string", "货物类型名称"),
        ("volume_length", "bigint", "长cm"),
        ("volume_width", "bigint", "宽cm"),
        ("volume_height", "bigint", "高cm"),
        ("weight", "decimal(16,2)", "重量 kg"),
        ("cancel_time", "string", "取消时间"),
        ("order_no", "string", "运单号"),
        ("status", "string", "运单状态"),
        ("status_name", "string", "运单状态名称"),
        ("collect_type", "string", "取件类型"),
        ("collect_type_name", "string", "取件类型名称"),
        ("user_id", "bigint", "用户ID"),
        ("receiver_complex_id", "bigint", "收件人小区id"),
        ("receiver_province_id", "string", "收件人省份id"),
        ("receiver_city_id", "string", "收件人城市id"),
        ("receiver_district_id", "string", "收件人区县id"),
        ("receiver_name", "string", "收件人姓名"),
        ("sender_complex_id", "bigint", "发件人小区id"),
        ("sender_province_id", "string", "发件人省份id"),
        ("sender_city_id", "string", "发件人城市id"),
        ("sender_district_id", "string", "发件人区县id"),
        ("sender_name", "string", "发件人姓名"),
        ("cargo_num", "bigint", "货物个数"),
        ("amount", "decimal(16,2)", "金额"),
        ("estimate_arrive_time", "string", "预计到达时间"),
        ("distance", "decimal(16,2)", "距离"),
        ("ts", "bigint", "时间戳")
    ]
    partition_cols = [("ds", "string", "统计日期")]
    create_external_table(
        spark,
        "dwd_trade_order_cancel_detail_inc",
        schema,
        "/warehouse/tms/dwd/dwd_trade_order_cancel_detail_inc",
        partition_cols
    )
    etl_dwd_trade_order_cancel_detail_inc(spark, ds)

    # 4. dwd_trans_receive_detail_inc
    schema = [
        ("id", "bigint", "运单明细ID"),
        ("order_id", "string", "运单ID"),
        ("cargo_type", "string", "货物类型ID"),
        ("cargo_type_name", "string", "货物类型名称"),
        ("volume_length", "bigint", "长cm"),
        ("volume_width", "bigint", "宽cm"),
        ("volume_height", "bigint", "高cm"),
        ("weight", "decimal(16,2)", "重量 kg"),
        ("receive_time", "string", "揽收时间"),
        ("order_no", "string", "运单号"),
        ("status", "string", "运单状态"),
        ("status_name", "string", "运单状态名称"),
        ("collect_type", "string", "取件类型"),
        ("collect_type_name", "string", "取件类型名称"),
        ("user_id", "bigint", "用户ID"),
        ("receiver_complex_id", "bigint", "收件人小区id"),
        ("receiver_province_id", "string", "收件人省份id"),
        ("receiver_city_id", "string", "收件人城市id"),
        ("receiver_district_id", "string", "收件人区县id"),
        ("receiver_name", "string", "收件人姓名"),
        ("sender_complex_id", "bigint", "发件人小区id"),
        ("sender_province_id", "string", "发件人省份id"),
        ("sender_city_id", "string", "发件人城市id"),
        ("sender_district_id", "string", "发件人区县id"),
        ("sender_name", "string", "发件人姓名"),
        ("payment_type", "string", "支付方式"),
        ("payment_type_name", "string", "支付方式名称"),
        ("cargo_num", "bigint", "货物个数"),
        ("amount", "decimal(16,2)", "金额"),
        ("estimate_arrive_time", "string", "预计到达时间"),
        ("distance", "decimal(16,2)", "距离"),
        ("ts", "bigint", "时间戳")
    ]
    partition_cols = [("ds", "string", "统计日期")]
    create_external_table(
        spark,
        "dwd_trans_receive_detail_inc",
        schema,
        "/warehouse/tms/dwd/dwd_trans_receive_detail_inc",
        partition_cols
    )
    etl_dwd_trans_receive_detail_inc(spark, ds)

    # 5. dwd_trans_dispatch_detail_inc
    schema = [
        ("id", "bigint", "运单明细ID"),
        ("order_id", "string", "运单ID"),
        ("cargo_type", "string", "货物类型ID"),
        ("cargo_type_name", "string", "货物类型名称"),
        ("volume_length", "bigint", "长cm"),
        ("volume_width", "bigint", "宽cm"),
        ("volume_height", "bigint", "高cm"),
        ("weight", "decimal(16,2)", "重量 kg"),
        ("dispatch_time", "string", "发单时间"),
        ("order_no", "string", "运单号"),
        ("status", "string", "运单状态"),
        ("status_name", "string", "运单状态名称"),
        ("collect_type", "string", "取件类型"),
        ("collect_type_name", "string", "取件类型名称"),
        ("user_id", "bigint", "用户ID"),
        ("receiver_complex_id", "bigint", "收件人小区id"),
        ("receiver_province_id", "string", "收件人省份id"),
        ("receiver_city_id", "string", "收件人城市id"),
        ("receiver_district_id", "string", "收件人区县id"),
        ("receiver_name", "string", "收件人姓名"),
        ("sender_complex_id", "bigint", "发件人小区id"),
        ("sender_province_id", "string", "发件人省份id"),
        ("sender_city_id", "string", "发件人城市id"),
        ("sender_district_id", "string", "发件人区县id"),
        ("sender_name", "string", "发件人姓名"),
        ("payment_type", "string", "支付方式"),
        ("payment_type_name", "string", "支付方式名称"),
        ("cargo_num", "bigint", "货物个数"),
        ("amount", "decimal(16,2)", "金额"),
        ("estimate_arrive_time", "string", "预计到达时间"),
        ("distance", "decimal(16,2)", "距离"),
        ("ts", "bigint", "时间戳")
    ]
    partition_cols = [("ds", "string", "统计日期")]
    create_external_table(
        spark,
        "dwd_trans_dispatch_detail_inc",
        schema,
        "/warehouse/tms/dwd/dwd_trans_dispatch_detail_inc",
        partition_cols
    )
    etl_dwd_trans_dispatch_detail_inc(spark, ds)

    # 6. dwd_trans_bound_finish_detail_inc
    schema = [
        ("id", "bigint", "运单明细ID"),
        ("order_id", "string", "运单ID"),
        ("cargo_type", "string", "货物类型ID"),
        ("cargo_type_name", "string", "货物类型名称"),
        ("volume_length", "bigint", "长cm"),
        ("volume_width", "bigint", "宽cm"),
        ("volume_height", "bigint", "高cm"),
        ("weight", "decimal(16,2)", "重量 kg"),
        ("bound_finish_time", "string", "转运完成时间"),
        ("order_no", "string", "运单号"),
        ("status", "string", "运单状态"),
        ("status_name", "string", "运单状态名称"),
        ("collect_type", "string", "取件类型"),
        ("collect_type_name", "string", "取件类型名称"),
        ("user_id", "bigint", "用户ID"),
        ("receiver_complex_id", "bigint", "收件人小区id"),
        ("receiver_province_id", "string", "收件人省份id"),
        ("receiver_city_id", "string", "收件人城市id"),
        ("receiver_district_id", "string", "收件人区县id"),
        ("receiver_name", "string", "收件人姓名"),
        ("sender_complex_id", "bigint", "发件人小区id"),
        ("sender_province_id", "string", "发件人省份id"),
        ("sender_city_id", "string", "发件人城市id"),
        ("sender_district_id", "string", "发件人区县id"),
        ("sender_name", "string", "发件人姓名"),
        ("payment_type", "string", "支付方式"),
        ("payment_type_name", "string", "支付方式名称"),
        ("cargo_num", "bigint", "货物个数"),
        ("amount", "decimal(16,2)", "金额"),
        ("estimate_arrive_time", "string", "预计到达时间"),
        ("distance", "decimal(16,2)", "距离"),
        ("ts", "bigint", "时间戳")
    ]
    partition_cols = [("ds", "string", "统计日期")]
    create_external_table(
        spark,
        "dwd_trans_bound_finish_detail_inc",
        schema,
        "/warehouse/tms/dwd/dwd_trans_bound_finish_detail_inc",
        partition_cols
    )
    etl_dwd_trans_bound_finish_detail_inc(spark, ds)

    # 7. dwd_trans_deliver_suc_detail_inc
    schema = [
        ("id", "bigint", "运单明细ID"),
        ("order_id", "string", "运单ID"),
        ("cargo_type", "string", "货物类型ID"),
        ("cargo_type_name", "string", "货物类型名称"),
        ("volume_length", "bigint", "长cm"),
        ("volume_width", "bigint", "宽cm"),
        ("volume_height", "bigint", "高cm"),
        ("weight", "decimal(16,2)", "重量 kg"),
        ("deliver_suc_time", "string", "派送成功时间"),
        ("order_no", "string", "运单号"),
        ("status", "string", "运单状态"),
        ("status_name", "string", "运单状态名称"),
        ("collect_type", "string", "取件类型"),
        ("collect_type_name", "string", "取件类型名称"),
        ("user_id", "bigint", "用户ID"),
        ("receiver_complex_id", "bigint", "收件人小区id"),
        ("receiver_province_id", "string", "收件人省份id"),
        ("receiver_city_id", "string", "收件人城市id"),
        ("receiver_district_id", "string", "收件人区县id"),
        ("receiver_name", "string", "收件人姓名"),
        ("sender_complex_id", "bigint", "发件人小区id"),
        ("sender_province_id", "string", "发件人省份id"),
        ("sender_city_id", "string", "发件人城市id"),
        ("sender_district_id", "string", "发件人区县id"),
        ("sender_name", "string", "发件人姓名"),
        ("payment_type", "string", "支付方式"),
        ("payment_type_name", "string", "支付方式名称"),
        ("cargo_num", "bigint", "货物个数"),
        ("amount", "decimal(16,2)", "金额"),
        ("estimate_arrive_time", "string", "预计到达时间"),
        ("distance", "decimal(16,2)", "距离"),
        ("ts", "bigint", "时间戳")
    ]
    partition_cols = [("ds", "string", "统计日期")]
    create_external_table(
        spark,
        "dwd_trans_deliver_suc_detail_inc",
        schema,
        "/warehouse/tms/dwd/dwd_trans_deliver_suc_detail_inc",
        partition_cols
    )
    etl_dwd_trans_deliver_suc_detail_inc(spark, ds)

    # 8. dwd_trans_sign_detail_inc
    schema = [
        ("id", "bigint", "运单明细ID"),
        ("order_id", "string", "运单ID"),
        ("cargo_type", "string", "货物类型ID"),
        ("cargo_type_name", "string", "货物类型名称"),
        ("volume_length", "bigint", "长cm"),
        ("volume_width", "bigint", "宽cm"),
        ("volume_height", "bigint", "高cm"),
        ("weight", "decimal(16,2)", "重量 kg"),
        ("sign_time", "string", "签收时间"),
        ("order_no", "string", "运单号"),
        ("status", "string", "运单状态"),
        ("status_name", "string", "运单状态名称"),
        ("collect_type", "string", "取件类型"),
        ("collect_type_name", "string", "取件类型名称"),
        ("user_id", "bigint", "用户ID"),
        ("receiver_complex_id", "bigint", "收件人小区id"),
        ("receiver_province_id", "string", "收件人省份id"),
        ("receiver_city_id", "string", "收件人城市id"),
        ("receiver_district_id", "string", "收件人区县id"),
        ("receiver_name", "string", "收件人姓名"),
        ("sender_complex_id", "bigint", "发件人小区id"),
        ("sender_province_id", "string", "发件人省份id"),
        ("sender_city_id", "string", "发件人城市id"),
        ("sender_district_id", "string", "发件人区县id"),
        ("sender_name", "string", "发件人姓名"),
        ("payment_type", "string", "支付方式"),
        ("payment_type_name", "string", "支付方式名称"),
        ("cargo_num", "bigint", "货物个数"),
        ("amount", "decimal(16,2)", "金额"),
        ("estimate_arrive_time", "string", "预计到达时间"),
        ("distance", "decimal(16,2)", "距离"),
        ("ts", "bigint", "时间戳"),
        ("start_date", "string", "开始日期"),
        ("end_date", "string", "结束日期")
    ]
    partition_cols = [("ds", "string", "统计日期")]
    create_external_table(
        spark,
        "dwd_trans_sign_detail_inc",
        schema,
        "/warehouse/tms/dwd/dwd_trans_sign_detail_inc",
        partition_cols
    )
    etl_dwd_trans_sign_detail_inc(spark, ds)

    # 9. dwd_trade_order_process_inc
    schema = [
        ("id", "bigint", "运单明细ID"),
        ("order_id", "string", "运单ID"),
        ("cargo_type", "string", "货物类型ID"),
        ("cargo_type_name", "string", "货物类型名称"),
        ("volume_length", "bigint", "长cm"),
        ("volume_width", "bigint", "宽cm"),
        ("volume_height", "bigint", "高cm"),
        ("weight", "decimal(16,2)", "重量 kg"),
        ("order_time", "string", "下单时间"),
        ("order_no", "string", "运单号"),
        ("status", "string", "运单状态"),
        ("status_name", "string", "运单状态名称"),
        ("collect_type", "string", "取件类型"),
        ("collect_type_name", "string", "取件类型名称"),
        ("user_id", "bigint", "用户ID"),
        ("receiver_complex_id", "bigint", "收件人小区id"),
        ("receiver_province_id", "string", "收件人省份id"),
        ("receiver_city_id", "string", "收件人城市id"),
        ("receiver_district_id", "string", "收件人区县id"),
        ("receiver_name", "string", "收件人姓名"),
        ("sender_complex_id", "bigint", "发件人小区id"),
        ("sender_province_id", "string", "发件人省份id"),
        ("sender_city_id", "string", "发件人城市id"),
        ("sender_district_id", "string", "发件人区县id"),
        ("sender_name", "string", "发件人姓名"),
        ("payment_type", "string", "支付方式"),
        ("payment_type_name", "string", "支付方式名称"),
        ("cargo_num", "bigint", "货物个数"),
        ("amount", "decimal(16,2)", "金额"),
        ("estimate_arrive_time", "string", "预计到达时间"),
        ("distance", "decimal(16,2)", "距离"),
        ("ts", "bigint", "时间戳"),
        ("start_date", "string", "开始日期"),
        ("end_date", "string", "结束日期")
    ]
    partition_cols = [("ds", "string", "统计日期")]
    create_external_table(
        spark,
        "dwd_trade_order_process_inc",
        schema,
        "/warehouse/tms/dwd/dwd_order_process",
        partition_cols
    )
    etl_dwd_trade_order_process_inc(spark, ds)

    # 10. dwd_trans_trans_finish_inc
    schema = [
        ("id", "bigint", "运输任务ID"),
        ("shift_id", "bigint", "车次ID"),
        ("line_id", "bigint", "路线ID"),
        ("start_org_id", "bigint", "起始机构ID"),
        ("start_org_name", "string", "起始机构名称"),
        ("end_org_id", "bigint", "目的机构ID"),
        ("end_org_name", "string", "目的机构名称"),
        ("order_num", "bigint", "运单个数"),
        ("driver1_emp_id", "bigint", "司机1ID"),
        ("driver1_name", "string", "司机1名称"),
        ("driver2_emp_id", "bigint", "司机2ID"),
        ("driver2_name", "string", "司机2名称"),
        ("truck_id", "bigint", "卡车ID"),
        ("truck_no", "string", "卡车号牌"),
        ("actual_start_time", "string", "实际启动时间"),
        ("actual_end_time", "string", "实际到达时间"),
        ("estimate_end_time", "string", "预估到达时间"),
        ("actual_distance", "decimal(16,2)", "实际行驶距离"),
        ("finish_dur_sec", "bigint", "运输时长(秒)"),
        ("ts", "bigint", "时间戳")
    ]
    partition_cols = [("ds", "string", "统计日期")]
    create_external_table(
        spark,
        "dwd_trans_trans_finish_inc",
        schema,
        "/warehouse/tms/dwd/dwd_trans_trans_finish_inc",
        partition_cols
    )
    etl_dwd_trans_trans_finish_inc(spark, ds)

    # 11. dwd_bound_inbound_inc
    schema = [
        ("id", "bigint", "中转记录ID"),
        ("order_id", "bigint", "运单ID"),
        ("org_id", "bigint", "机构ID"),
        ("inbound_time", "string", "入库时间"),
        ("inbound_emp_id", "bigint", "入库人员"),
        ("ts", "bigint", "时间戳")
    ]
    partition_cols = [("ds", "string", "统计日期")]
    create_external_table(
        spark,
        "dwd_bound_inbound_inc",
        schema,
        "/warehouse/tms/dwd/dwd_bound_inbound_inc",
        partition_cols
    )
    etl_dwd_bound_inbound_inc(spark, ds)

    # 12. dwd_bound_sort_inc
    schema = [
        ("id", "bigint", "中转记录ID"),
        ("order_id", "bigint", "订单ID"),
        ("org_id", "bigint", "机构ID"),
        ("sort_time", "string", "分拣时间"),
        ("sorter_emp_id", "bigint", "分拣人员"),
        ("ts", "bigint", "时间戳")
    ]
    partition_cols = [("ds", "string", "统计日期")]
    create_external_table(
        spark,
        "dwd_bound_sort_inc",
        schema,
        "/warehouse/tms/dwd/dwd_bound_sort_inc",
        partition_cols
    )
    etl_dwd_bound_sort_inc(spark, ds)

    # 13. dwd_bound_outbound_inc
    schema = [
        ("id", "bigint", "中转记录ID"),
        ("order_id", "bigint", "订单ID"),
        ("org_id", "bigint", "机构ID"),
        ("outbound_time", "string", "出库时间"),
        ("outbound_emp_id", "bigint", "出库人员"),
        ("ts", "bigint", "时间戳")
    ]
    partition_cols = [("ds", "string", "统计日期")]
    create_external_table(
        spark,
        "dwd_bound_outbound_inc",
        schema,
        "/warehouse/tms/dwd/dwd_bound_outbound_inc",
        partition_cols
    )
    etl_dwd_bound_outbound_inc(spark, ds)

    print("[SUCCESS] All DWD tables ETL completed for ds={0}".format(ds))


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: spark-submit tms_dwd_etl.py <partition_date>")
        sys.exit(1)
    ds = sys.argv[1]
    main(ds)