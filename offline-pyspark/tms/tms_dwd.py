# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
import sys


def get_spark_session():
    """获取配置好的SparkSession"""
    spark = SparkSession.builder \
        .appName("TMSDwdETL") \
        .config("hive.metastore.uris", "thrift://cdh01:9083") \
        .config("spark.sql.hive.convertMetastoreOrc", "true") \
        .config("spark.hadoop.hive.exec.dynamic.partition", "true") \
        .config("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict") \
        .enableHiveSupport() \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.sql("USE tms")  # 切换到目标数据库
    return spark


def insert_to_hive(df, table_name, partition_date=None):
    """将DataFrame写入Hive表，兼容动态分区"""
    if df.rdd.isEmpty():
        print("[WARN] No data found for {0}, skipping write.".format(table_name))
        return
    # 若需固定分区则添加，动态分区由SQL内部处理
    if partition_date:
        df = df.withColumn("ds", lit(partition_date))
    df.write.mode("overwrite").insertInto("tms.{}".format(table_name))


def etl_dwd_trade_order_detail_inc(spark, ds):
    """交易域订单明细事务事实表"""
    sql = """
    SELECT
        cargo.id,
        order_id,
        cargo_type,
        dic_for_cargo_type.name               cargo_type_name,
        volume_length,
        volume_width,
        volume_height,
        weight,
        order_time,
        order_no,
        status,
        dic_for_status.name                   status_name,
        collect_type,
        dic_for_collect_type.name             collect_type_name,
        user_id,
        receiver_complex_id,
        receiver_province_id,
        receiver_city_id,
        receiver_district_id,
        receiver_name,
        sender_complex_id,
        sender_province_id,
        sender_city_id,
        sender_district_id,
        sender_name,
        cargo_num,
        amount,
        estimate_arrive_time,
        distance,
        unix_timestamp(order_time, 'yyyy-MM-dd HH:mm:ss') * 1000  ts,
        date_format(order_time, 'yyyy-MM-dd') ds
    FROM (
        SELECT 
            after.id,
            after.order_id,
            after.cargo_type,
            after.volume_length,
            after.volume_width,
            after.volume_height,
            after.weight,
            concat(substr(after.create_time, 1, 10), ' ', substr(after.create_time, 12, 8)) order_time,
            ds
        FROM ods_order_cargo as after
        WHERE is_deleted = '0' AND ds = '{0}'
    ) cargo
    JOIN (
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
            concat(substr(after.receiver_name, 1, 1), '*') receiver_name,
            after.sender_complex_id,
            after.sender_province_id,
            after.sender_city_id,
            after.sender_district_id,
            concat(substr(after.sender_name, 1, 1), '*')   sender_name,
            after.cargo_num,
            after.amount,
            case
                when after.estimate_arrive_time is null or trim(after.estimate_arrive_time) = '' then null
                when after.estimate_arrive_time rlike '^\\d+$' then date_format(
                        from_utc_timestamp(
                            to_timestamp(
                                case when length(after.estimate_arrive_time) = 10 
                                     then cast(after.estimate_arrive_time as bigint) 
                                     else cast(after.estimate_arrive_time as bigint)/1000 
                                end
                            ), 
                            'UTC'
                        ),
                        'yyyy-MM-dd HH:mm:ss'
                    )
                else date_format(from_utc_timestamp(to_timestamp(after.estimate_arrive_time), 'UTC'), 'yyyy-MM-dd HH:mm:ss')
                end as estimate_arrive_time,
            after.distance
        FROM ods_order_info as after
        WHERE is_deleted = '0' AND ds = '{0}'
    ) info ON cargo.order_id = info.id
    LEFT JOIN (
        SELECT id, name FROM ods_base_dic WHERE is_deleted = '0' AND ds = '{0}'
    ) dic_for_cargo_type ON cargo.cargo_type = cast(dic_for_cargo_type.id as string)
    LEFT JOIN (
        SELECT id, name FROM ods_base_dic WHERE is_deleted = '0' AND ds = '{0}'
    ) dic_for_status ON info.status = cast(dic_for_status.id as string)
    -- 修复：collect_type应关联dic_for_collect_type自身的id，而非dic_for_cargo_type的id
    LEFT JOIN (
        SELECT id, name FROM ods_base_dic WHERE is_deleted = '0' AND ds = '{0}'
    ) dic_for_collect_type ON info.collect_type = cast(dic_for_collect_type.id as string)
    """.format(ds)
    df = spark.sql(sql)
    insert_to_hive(df, "dwd_trade_order_detail_inc")


def etl_dwd_trade_pay_suc_detail_inc(spark, ds):
    """交易域支付成功事务事实表"""
    sql = """
    SELECT
        cargo.id,
        order_id,
        cargo_type,
        dic_for_cargo_type.name                 cargo_type_name,
        volume_length,
        volume_width,
        volume_height,
        weight,
        payment_time,
        order_no,
        status,
        dic_for_status.name                     status_name,
        collect_type,
        dic_for_collect_type.name               collect_type_name,
        user_id,
        receiver_complex_id,
        receiver_province_id,
        receiver_city_id,
        receiver_district_id,
        receiver_name,
        sender_complex_id,
        sender_province_id,
        sender_city_id,
        sender_district_id,
        sender_name,
        payment_type,
        dic_for_payment_type.name               payment_type_name,
        cargo_num,
        amount,
        case
            when estimate_arrive_time is null or trim(estimate_arrive_time) = '' then null
            when estimate_arrive_time rlike '^\\d+$' then date_format(
                    from_utc_timestamp(
                        to_timestamp(
                            case when length(estimate_arrive_time) = 10 
                                 then cast(estimate_arrive_time as bigint) 
                                 else cast(estimate_arrive_time as bigint)/1000 
                            end
                        ), 
                        'UTC'
                    ),
                    'yyyy-MM-dd HH:mm:ss'
                )
            else estimate_arrive_time
            end as estimate_arrive_time,
        distance,
        unix_timestamp(payment_time, 'yyyy-MM-dd HH:mm:ss') * 1000  ts,
        date_format(payment_time, 'yyyy-MM-dd') ds
    FROM (
        SELECT 
            after.id,
            after.order_id,
            after.cargo_type,
            after.volume_length,
            after.volume_width,
            after.volume_height,
            after.weight,
            ds
        FROM ods_order_cargo as after
        WHERE is_deleted = '0' AND ds = '{0}'
    ) cargo
    JOIN (
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
            concat(substr(after.receiver_name, 1, 1), '*') receiver_name,
            after.sender_complex_id,
            after.sender_province_id,
            after.sender_city_id,
            after.sender_district_id,
            concat(substr(after.sender_name, 1, 1), '*') sender_name,
            after.payment_type,
            after.cargo_num,
            after.amount,
            after.estimate_arrive_time,
            after.distance,
            concat(substr(after.update_time, 1, 10), ' ', substr(after.update_time, 12, 8)) payment_time
        FROM ods_order_info as after
        WHERE is_deleted = '0' AND ds = '{0}'
    ) info ON cargo.order_id = info.id
    LEFT JOIN (
        SELECT id, name FROM ods_base_dic WHERE is_deleted = '0' AND ds = '{0}'
    ) dic_for_cargo_type ON cargo.cargo_type = cast(dic_for_cargo_type.id as string)
    LEFT JOIN (
        SELECT id, name FROM ods_base_dic WHERE is_deleted = '0' AND ds = '{0}'
    ) dic_for_status ON info.status = cast(dic_for_status.id as string)
    -- 修复：collect_type关联dic_for_collect_type自身id
    LEFT JOIN (
        SELECT id, name FROM ods_base_dic WHERE is_deleted = '0' AND ds = '{0}'
    ) dic_for_collect_type ON info.collect_type = cast(dic_for_collect_type.id as string)
    LEFT JOIN (
        SELECT id, name FROM ods_base_dic WHERE is_deleted = '0' AND ds = '{0}'
    ) dic_for_payment_type ON info.payment_type = cast(dic_for_payment_type.id as string)
    """.format(ds)
    df = spark.sql(sql)
    insert_to_hive(df, "dwd_trade_pay_suc_detail_inc")


def etl_dwd_trade_order_cancel_detail_inc(spark, ds):
    """交易域取消运单事务事实表"""
    sql = """
    SELECT
        cargo.id,
        order_id,
        cargo_type,
        dic_for_cargo_type.name                 cargo_type_name,
        volume_length,
        volume_width,
        volume_height,
        weight,
        cancel_time,
        order_no,
        status,
        dic_for_status.name                     status_name,
        collect_type,
        dic_for_collect_type.name               collect_type_name,
        user_id,
        receiver_complex_id,
        receiver_province_id,
        receiver_city_id,
        receiver_district_id,
        receiver_name,
        sender_complex_id,
        sender_province_id,
        sender_city_id,
        sender_district_id,
        sender_name,
        cargo_num,
        amount,
        case
            when info.estimate_arrive_time is null or trim(info.estimate_arrive_time) = '' then null
            when info.estimate_arrive_time rlike '^\\d+$' then date_format(
                    from_utc_timestamp(
                        to_timestamp(
                            case when length(info.estimate_arrive_time) = 10 
                                 then cast(info.estimate_arrive_time as bigint) 
                                 else cast(info.estimate_arrive_time as bigint)/1000 
                            end
                        ), 
                        'UTC'
                    ),
                    'yyyy-MM-dd HH:mm:ss'
                )
            else info.estimate_arrive_time
            end as estimate_arrive_time,
        distance,
        unix_timestamp(cancel_time, 'yyyy-MM-dd HH:mm:ss') * 1000  ts,
        date_format(cancel_time, 'yyyy-MM-dd') ds
    FROM (
        SELECT 
            after.id,
            after.order_id,
            after.cargo_type,
            after.volume_length,
            after.volume_width,
            after.volume_height,
            after.weight,
            ds
        FROM ods_order_cargo as after
        WHERE is_deleted = '0' AND ds = '{0}'
    ) cargo
    JOIN (
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
            concat(substr(after.receiver_name, 1, 1), '*') receiver_name,
            after.sender_complex_id,
            after.sender_province_id,
            after.sender_city_id,
            after.sender_district_id,
            concat(substr(after.sender_name, 1, 1), '*') sender_name,
            after.cargo_num,
            after.amount,
            after.estimate_arrive_time,
            after.distance,
            concat(substr(after.update_time, 1, 10), ' ', substr(after.update_time, 12, 8)) cancel_time
        FROM ods_order_info as after
        WHERE is_deleted = '0' AND ds = '{0}'
    ) info ON cargo.order_id = info.id
    LEFT JOIN (
        SELECT id, name FROM ods_base_dic WHERE is_deleted = '0' AND ds = '{0}'
    ) dic_for_cargo_type ON cargo.cargo_type = cast(dic_for_cargo_type.id as string)
    LEFT JOIN (
        SELECT id, name FROM ods_base_dic WHERE is_deleted = '0' AND ds = '{0}'
    ) dic_for_status ON info.status = cast(dic_for_status.id as string)
    -- 修复：collect_type关联dic_for_collect_type自身id
    LEFT JOIN (
        SELECT id, name FROM ods_base_dic WHERE is_deleted = '0' AND ds = '{0}'
    ) dic_for_collect_type ON info.collect_type = cast(dic_for_collect_type.id as string)
    """.format(ds)
    df = spark.sql(sql)
    insert_to_hive(df, "dwd_trade_order_cancel_detail_inc")


def etl_dwd_trans_receive_detail_inc(spark, ds):
    """物流域揽收事务事实表"""
    sql = """
    SELECT
        cargo.id,
        order_id,
        cargo_type,
        dic_for_cargo_type.name                 cargo_type_name,
        volume_length,
        volume_width,
        volume_height,
        weight,
        receive_time,
        order_no,
        status,
        dic_for_status.name                     status_name,
        collect_type,
        dic_for_collect_type.name               collect_type_name,
        user_id,
        receiver_complex_id,
        receiver_province_id,
        receiver_city_id,
        receiver_district_id,
        receiver_name,
        sender_complex_id,
        sender_province_id,
        sender_city_id,
        sender_district_id,
        sender_name,
        payment_type,
        dic_for_payment_type.name               payment_type_name,
        cargo_num,
        amount,
        case
            when info.estimate_arrive_time is null or trim(info.estimate_arrive_time) = '' then null
            when info.estimate_arrive_time rlike '^\\d+$' then date_format(
                    from_utc_timestamp(
                        to_timestamp(
                            case when length(info.estimate_arrive_time) = 10 
                                 then cast(info.estimate_arrive_time as bigint) 
                                 else cast(info.estimate_arrive_time as bigint)/1000 
                            end
                        ), 
                        'UTC'
                    ),
                    'yyyy-MM-dd HH:mm:ss'
                )
            else info.estimate_arrive_time
            end as estimate_arrive_time,
        distance,
        unix_timestamp(receive_time, 'yyyy-MM-dd HH:mm:ss') * 1000  ts,
        date_format(receive_time, 'yyyy-MM-dd') ds
    FROM (
        SELECT 
            after.id,
            after.order_id,
            after.cargo_type,
            after.volume_length,
            after.volume_width,
            after.volume_height,
            after.weight,
            ds
        FROM ods_order_cargo as after
        WHERE is_deleted = '0' AND ds = '{0}'
    ) cargo
    JOIN (
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
            concat(substr(after.receiver_name, 1, 1), '*') receiver_name,
            after.sender_complex_id,
            after.sender_province_id,
            after.sender_city_id,
            after.sender_district_id,
            concat(substr(after.sender_name, 1, 1), '*') sender_name,
            after.payment_type,
            after.cargo_num,
            after.amount,
            after.estimate_arrive_time,
            after.distance,
            concat(substr(after.update_time, 1, 10), ' ', substr(after.update_time, 12, 8)) receive_time
        FROM ods_order_info as after
        WHERE is_deleted = '0' AND ds = '{0}'
    ) info ON cargo.order_id = info.id
    LEFT JOIN (
        SELECT id, name FROM ods_base_dic WHERE is_deleted = '0' AND ds = '{0}'
    ) dic_for_cargo_type ON cargo.cargo_type = cast(dic_for_cargo_type.id as string)
    LEFT JOIN (
        SELECT id, name FROM ods_base_dic WHERE is_deleted = '0' AND ds = '{0}'
    ) dic_for_status ON info.status = cast(dic_for_status.id as string)
    -- 修复：collect_type关联dic_for_collect_type自身id
    LEFT JOIN (
        SELECT id, name FROM ods_base_dic WHERE is_deleted = '0' AND ds = '{0}'
    ) dic_for_collect_type ON info.collect_type = cast(dic_for_collect_type.id as string)
    LEFT JOIN (
        SELECT id, name FROM ods_base_dic WHERE is_deleted = '0' AND ds = '{0}'
    ) dic_for_payment_type ON info.payment_type = cast(dic_for_payment_type.id as string)
    """.format(ds)
    df = spark.sql(sql)
    insert_to_hive(df, "dwd_trans_receive_detail_inc")


def etl_dwd_trans_dispatch_detail_inc(spark, ds):
    """物流域发单事务事实表"""
    sql = """
    SELECT
        cargo.id,
        order_id,
        cargo_type,
        dic_for_cargo_type.name                 cargo_type_name,
        volume_length,
        volume_width,
        volume_height,
        weight,
        dispatch_time,
        order_no,
        status,
        dic_for_status.name                     status_name,
        collect_type,
        dic_for_collect_type.name               collect_type_name,
        user_id,
        receiver_complex_id,
        receiver_province_id,
        receiver_city_id,
        receiver_district_id,
        receiver_name,
        sender_complex_id,
        sender_province_id,
        sender_city_id,
        sender_district_id,
        sender_name,
        payment_type,
        dic_for_payment_type.name               payment_type_name,
        cargo_num,
        amount,
        estimate_arrive_time,
        distance,
        unix_timestamp(dispatch_time, 'yyyy-MM-dd HH:mm:ss') * 1000  ts,
        date_format(dispatch_time, 'yyyy-MM-dd') ds
    FROM (
        SELECT 
            after.id,
            after.order_id,
            after.cargo_type,
            after.volume_length,
            after.volume_width,
            after.volume_height,
            after.weight,
            ds
        FROM ods_order_cargo as after
        WHERE is_deleted = '0' AND ds = '{0}'
    ) cargo
    JOIN (
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
            concat(substr(after.receiver_name, 1, 1), '*') receiver_name,
            after.sender_complex_id,
            after.sender_province_id,
            after.sender_city_id,
            after.sender_district_id,
            concat(substr(after.sender_name, 1, 1), '*') sender_name,
            after.payment_type,
            after.cargo_num,
            after.amount,
            case
                when after.estimate_arrive_time is null or trim(after.estimate_arrive_time) = '' then null
                when after.estimate_arrive_time rlike '^\\d+$' then date_format(
                        from_utc_timestamp(
                            to_timestamp(
                                case when length(after.estimate_arrive_time) = 10 
                                     then cast(after.estimate_arrive_time as bigint) 
                                     else cast(after.estimate_arrive_time as bigint)/1000 
                                end
                            ), 
                            'UTC'
                        ),
                        'yyyy-MM-dd HH:mm:ss'
                    )
                else date_format(from_utc_timestamp(to_timestamp(after.estimate_arrive_time), 'UTC'), 'yyyy-MM-dd HH:mm:ss')
                end as estimate_arrive_time,
            after.distance,
            concat(substr(after.update_time, 1, 10), ' ', substr(after.update_time, 12, 8)) dispatch_time
        FROM ods_order_info as after
        WHERE is_deleted = '0' AND ds = '{0}'
    ) info ON cargo.order_id = info.id
    LEFT JOIN (
        SELECT id, name FROM ods_base_dic WHERE is_deleted = '0' AND ds = '{0}'
    ) dic_for_cargo_type ON cargo.cargo_type = cast(dic_for_cargo_type.id as string)
    LEFT JOIN (
        SELECT id, name FROM ods_base_dic WHERE is_deleted = '0' AND ds = '{0}'
    ) dic_for_status ON info.status = cast(dic_for_status.id as string)
    -- 修复：collect_type关联dic_for_collect_type自身id
    LEFT JOIN (
        SELECT id, name FROM ods_base_dic WHERE is_deleted = '0' AND ds = '{0}'
    ) dic_for_collect_type ON info.collect_type = cast(dic_for_collect_type.id as string)
    LEFT JOIN (
        SELECT id, name FROM ods_base_dic WHERE is_deleted = '0' AND ds = '{0}'
    ) dic_for_payment_type ON info.payment_type = cast(dic_for_payment_type.id as string)
    """.format(ds)
    df = spark.sql(sql)
    insert_to_hive(df, "dwd_trans_dispatch_detail_inc")


def etl_dwd_trans_bound_finish_detail_inc(spark, ds):
    """物流域转运完成事务事实表"""
    sql = """
    SELECT
        cargo.id,
        order_id,
        cargo_type,
        dic_for_cargo_type.name                 cargo_type_name,
        volume_length,
        volume_width,
        volume_height,
        weight,
        bound_finish_time,
        order_no,
        status,
        dic_for_status.name                     status_name,
        collect_type,
        dic_for_collect_type.name               collect_type_name,
        user_id,
        receiver_complex_id,
        receiver_province_id,
        receiver_city_id,
        receiver_district_id,
        receiver_name,
        sender_complex_id,
        sender_province_id,
        sender_city_id,
        sender_district_id,
        sender_name,
        payment_type,
        dic_for_payment_type.name               payment_type_name,
        cargo_num,
        amount,
        estimate_arrive_time,
        distance,
        unix_timestamp(bound_finish_time, 'yyyy-MM-dd HH:mm:ss') * 1000  ts,
        date_format(bound_finish_time, 'yyyy-MM-dd') ds
    FROM (
        SELECT 
            after.id,
            after.order_id,
            after.cargo_type,
            after.volume_length,
            after.volume_width,
            after.volume_height,
            after.weight,
            ds
        FROM ods_order_cargo as after
        WHERE is_deleted = '0' AND ds = '{0}'
    ) cargo
    JOIN (
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
            concat(substr(after.receiver_name, 1, 1), '*') receiver_name,
            after.sender_complex_id,
            after.sender_province_id,
            after.sender_city_id,
            after.sender_district_id,
            concat(substr(after.sender_name, 1, 1), '*') sender_name,
            after.payment_type,
            after.cargo_num,
            after.amount,
            case
                when after.estimate_arrive_time is null or trim(after.estimate_arrive_time) = '' then null
                when after.estimate_arrive_time rlike '^\\d+$' then date_format(
                        from_utc_timestamp(
                            to_timestamp(
                                case when length(after.estimate_arrive_time) = 10 
                                     then cast(after.estimate_arrive_time as bigint) 
                                     else cast(after.estimate_arrive_time as bigint)/1000 
                                end
                            ), 
                            'UTC'
                        ),
                        'yyyy-MM-dd HH:mm:ss'
                    )
                else date_format(from_utc_timestamp(to_timestamp(after.estimate_arrive_time), 'UTC'), 'yyyy-MM-dd HH:mm:ss')
                end as estimate_arrive_time,
            after.distance,
            concat(substr(after.update_time, 1, 10), ' ', substr(after.update_time, 12, 8)) bound_finish_time
        FROM ods_order_info as after
        WHERE is_deleted = '0' AND ds = '{0}'
    ) info ON cargo.order_id = info.id
    LEFT JOIN (
        SELECT id, name FROM ods_base_dic WHERE is_deleted = '0' AND ds = '{0}'
    ) dic_for_cargo_type ON cargo.cargo_type = cast(dic_for_cargo_type.id as string)
    LEFT JOIN (
        SELECT id, name FROM ods_base_dic WHERE is_deleted = '0' AND ds = '{0}'
    ) dic_for_status ON info.status = cast(dic_for_status.id as string)
    -- 修复：collect_type关联dic_for_collect_type自身id
    LEFT JOIN (
        SELECT id, name FROM ods_base_dic WHERE is_deleted = '0' AND ds = '{0}'
    ) dic_for_collect_type ON info.collect_type = cast(dic_for_collect_type.id as string)
    LEFT JOIN (
        SELECT id, name FROM ods_base_dic WHERE is_deleted = '0' AND ds = '{0}'
    ) dic_for_payment_type ON info.payment_type = cast(dic_for_payment_type.id as string)
    """.format(ds)
    df = spark.sql(sql)
    insert_to_hive(df, "dwd_trans_bound_finish_detail_inc")


def etl_dwd_trans_deliver_suc_detail_inc(spark, ds):
    """物流域派送成功事务事实表"""
    sql = """
    SELECT
        cargo.id,
        order_id,
        cargo_type,
        dic_for_cargo_type.name                 cargo_type_name,
        volume_length,
        volume_width,
        volume_height,
        weight,
        deliver_suc_time,
        order_no,
        status,
        dic_for_status.name                     status_name,
        collect_type,
        dic_for_collect_type.name               collect_type_name,
        user_id,
        receiver_complex_id,
        receiver_province_id,
        receiver_city_id,
        receiver_district_id,
        receiver_name,
        sender_complex_id,
        sender_province_id,
        sender_city_id,
        sender_district_id,
        sender_name,
        payment_type,
        dic_for_payment_type.name               payment_type_name,
        cargo_num,
        amount,
        estimate_arrive_time,
        distance,
        unix_timestamp(deliver_suc_time, 'yyyy-MM-dd HH:mm:ss') * 1000  ts,
        date_format(deliver_suc_time, 'yyyy-MM-dd') ds
    FROM (
        SELECT 
            after.id,
            after.order_id,
            after.cargo_type,
            after.volume_length,
            after.volume_width,
            after.volume_height,
            after.weight,
            ds
        FROM ods_order_cargo as after
        WHERE is_deleted = '0' AND ds = '{0}'
    ) cargo
    JOIN (
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
            concat(substr(after.receiver_name, 1, 1), '*') receiver_name,
            after.sender_complex_id,
            after.sender_province_id,
            after.sender_city_id,
            after.sender_district_id,
            concat(substr(after.sender_name, 1, 1), '*') sender_name,
            after.payment_type,
            after.cargo_num,
            after.amount,
            case
                when after.estimate_arrive_time is null or trim(after.estimate_arrive_time) = '' then null
                when after.estimate_arrive_time rlike '^\\d+$' then date_format(
                        from_utc_timestamp(
                            to_timestamp(
                                case when length(after.estimate_arrive_time) = 10 
                                     then cast(after.estimate_arrive_time as bigint) 
                                     else cast(after.estimate_arrive_time as bigint)/1000 
                                end
                            ), 
                            'UTC'
                        ),
                        'yyyy-MM-dd HH:mm:ss'
                    )
                else date_format(from_utc_timestamp(to_timestamp(after.estimate_arrive_time), 'UTC'), 'yyyy-MM-dd HH:mm:ss')
                end as estimate_arrive_time,
            after.distance,
            concat(substr(after.update_time, 1, 10), ' ', substr(after.update_time, 12, 8)) deliver_suc_time
        FROM ods_order_info as after
        WHERE is_deleted = '0' AND ds = '{0}'
    ) info ON cargo.order_id = info.id
    LEFT JOIN (
        SELECT id, name FROM ods_base_dic WHERE is_deleted = '0' AND ds = '{0}'
    ) dic_for_cargo_type ON cargo.cargo_type = cast(dic_for_cargo_type.id as string)
    LEFT JOIN (
        SELECT id, name FROM ods_base_dic WHERE is_deleted = '0' AND ds = '{0}'
    ) dic_for_status ON info.status = cast(dic_for_status.id as string)
    -- 修复：collect_type关联dic_for_collect_type自身id
    LEFT JOIN (
        SELECT id, name FROM ods_base_dic WHERE is_deleted = '0' AND ds = '{0}'
    ) dic_for_collect_type ON info.collect_type = cast(dic_for_collect_type.id as string)
    LEFT JOIN (
        SELECT id, name FROM ods_base_dic WHERE is_deleted = '0' AND ds = '{0}'
    ) dic_for_payment_type ON info.payment_type = cast(dic_for_payment_type.id as string)
    """.format(ds)
    df = spark.sql(sql)
    insert_to_hive(df, "dwd_trans_deliver_suc_detail_inc")


def etl_dwd_trans_sign_detail_inc(spark, ds):
    """物流域签收事务事实表"""
    sql = """
    SELECT
        cargo.id,
        order_id,
        cargo_type,
        dic_for_cargo_type.name                 cargo_type_name,
        volume_length,
        volume_width,
        volume_height,
        weight,
        sign_time,
        order_no,
        status,
        dic_for_status.name                     status_name,
        collect_type,
        dic_for_collect_type.name               collect_type_name,
        user_id,
        receiver_complex_id,
        receiver_province_id,
        receiver_city_id,
        receiver_district_id,
        receiver_name,
        sender_complex_id,
        sender_province_id,
        sender_city_id,
        sender_district_id,
        sender_name,
        payment_type,
        dic_for_payment_type.name               payment_type_name,
        cargo_num,
        amount,
        case
            when info.estimate_arrive_time is null or trim(info.estimate_arrive_time) = '' then null
            when info.estimate_arrive_time rlike '^\\d+$' then date_format(
                    from_utc_timestamp(
                        to_timestamp(
                            case when length(info.estimate_arrive_time) = 10 
                                 then cast(info.estimate_arrive_time as bigint) 
                                 else cast(info.estimate_arrive_time as bigint)/1000 
                            end
                        ), 
                        'UTC'
                    ),
                    'yyyy-MM-dd HH:mm:ss'
                )
            else info.estimate_arrive_time
            end as estimate_arrive_time,
        distance,
        unix_timestamp(sign_time, 'yyyy-MM-dd HH:mm:ss') * 1000  ts,
        date_format(sign_time, 'yyyy-MM-dd') ds
    FROM (
        SELECT 
            after.id,
            after.order_id,
            after.cargo_type,
            after.volume_length,
            after.volume_width,
            after.volume_height,
            after.weight,
            ds
        FROM ods_order_cargo as after
        WHERE is_deleted = '0' AND ds = '{0}'
    ) cargo
    JOIN (
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
            concat(substr(after.receiver_name, 1, 1), '*') receiver_name,
            after.sender_complex_id,
            after.sender_province_id,
            after.sender_city_id,
            after.sender_district_id,
            concat(substr(after.sender_name, 1, 1), '*') sender_name,
            after.payment_type,
            after.cargo_num,
            after.amount,
            after.estimate_arrive_time,
            after.distance,
            concat(substr(after.update_time, 1, 10), ' ', substr(after.update_time, 12, 8)) sign_time
        FROM ods_order_info as after
        WHERE is_deleted = '0' AND ds = '{0}'
    ) info ON cargo.order_id = info.id
    LEFT JOIN (
        SELECT id, name FROM ods_base_dic WHERE is_deleted = '0' AND ds = '{0}'
    ) dic_for_cargo_type ON cargo.cargo_type = cast(dic_for_cargo_type.id as string)
    LEFT JOIN (
        SELECT id, name FROM ods_base_dic WHERE is_deleted = '0' AND ds = '{0}'
    ) dic_for_status ON info.status = cast(dic_for_status.id as string)
    -- 修复：collect_type关联dic_for_collect_type自身id
    LEFT JOIN (
        SELECT id, name FROM ods_base_dic WHERE is_deleted = '0' AND ds = '{0}'
    ) dic_for_collect_type ON info.collect_type = cast(dic_for_collect_type.id as string)
    LEFT JOIN (
        SELECT id, name FROM ods_base_dic WHERE is_deleted = '0' AND ds = '{0}'
    ) dic_for_payment_type ON info.payment_type = cast(dic_for_payment_type.id as string)
    """.format(ds)
    df = spark.sql(sql)
    insert_to_hive(df, "dwd_trans_sign_detail_inc")


def etl_dwd_trade_order_process_inc(spark, ds):
    """交易域运单累积快照事实表"""
    sql = """
    SELECT
        cargo.id,
        order_id,
        cargo_type,
        dic_for_cargo_type.name               cargo_type_name,
        volume_length,
        volume_width,
        volume_height,
        weight,
        order_time,
        order_no,
        status,
        dic_for_status.name                   status_name,
        collect_type,
        dic_for_collect_type.name             collect_type_name,
        user_id,
        receiver_complex_id,
        receiver_province_id,
        receiver_city_id,
        receiver_district_id,
        receiver_name,
        sender_complex_id,
        sender_province_id,
        sender_city_id,
        sender_district_id,
        sender_name,
        payment_type,
        dic_for_payment_type.name             payment_type_name,
        cargo_num,
        amount,
        estimate_arrive_time,
        distance,
        unix_timestamp(order_time, 'yyyy-MM-dd HH:mm:ss') * 1000  ts,
        date_format(order_time, 'yyyy-MM-dd') start_date,
        end_date,
        end_date                              ds
    FROM (
        SELECT 
            after.id,
            after.order_id,
            after.cargo_type,
            after.volume_length,
            after.volume_width,
            after.volume_height,
            after.weight,
            concat(substr(after.create_time, 1, 10), ' ', substr(after.create_time, 12, 8)) order_time,
            ds
        FROM ods_order_cargo as after
        WHERE is_deleted = '0' AND ds = '{0}'
    ) cargo
    JOIN (
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
            concat(substr(after.receiver_name, 1, 1), '*') receiver_name,
            after.sender_complex_id,
            after.sender_province_id,
            after.sender_city_id,
            after.sender_district_id,
            concat(substr(after.sender_name, 1, 1), '*') sender_name,
            after.payment_type,
            after.cargo_num,
            after.amount,
            case
                when after.estimate_arrive_time is null or trim(after.estimate_arrive_time) = '' then null
                when after.estimate_arrive_time rlike '^\\d+$' then date_format(
                        from_utc_timestamp(
                            to_timestamp(
                                case when length(after.estimate_arrive_time) = 10 
                                     then cast(after.estimate_arrive_time as bigint) 
                                     else cast(after.estimate_arrive_time as bigint)/1000 
                                end
                            ), 
                            'UTC'
                        ),
                        'yyyy-MM-dd HH:mm:ss'
                    )
                else date_format(from_utc_timestamp(to_timestamp(after.estimate_arrive_time), 'UTC'), 'yyyy-MM-dd HH:mm:ss')
                end as estimate_arrive_time,
            after.distance,
            if(after.status = '60080' or after.status = '60999',
               concat(substr(after.update_time, 1, 10)),
               '9999-12-31')                               end_date
        FROM ods_order_info as after
        WHERE is_deleted = '0' AND ds = '{0}'
    ) info ON cargo.order_id = info.id
    LEFT JOIN (
        SELECT id, name FROM ods_base_dic WHERE is_deleted = '0' AND ds = '{0}'
    ) dic_for_cargo_type ON cargo.cargo_type = cast(dic_for_cargo_type.id as string)
    LEFT JOIN (
        SELECT id, name FROM ods_base_dic WHERE is_deleted = '0' AND ds = '{0}'
    ) dic_for_status ON info.status = cast(dic_for_status.id as string)
    -- 修复：collect_type关联dic_for_collect_type自身id
    LEFT JOIN (
        SELECT id, name FROM ods_base_dic WHERE is_deleted = '0' AND ds = '{0}'
    ) dic_for_collect_type ON info.collect_type = cast(dic_for_collect_type.id as string)
    LEFT JOIN (
        SELECT id, name FROM ods_base_dic WHERE is_deleted = '0' AND ds = '{0}'
    ) dic_for_payment_type ON info.payment_type = cast(dic_for_payment_type.id as string)
    """.format(ds)
    df = spark.sql(sql)
    insert_to_hive(df, "dwd_trade_order_process_inc")


def etl_dwd_trans_trans_finish_inc(spark, ds):
    """物流域运输事务事实表"""
    sql = """
    SELECT
        info.id,
        shift_id,
        line_id,
        start_org_id,
        start_org_name,
        end_org_id,
        end_org_name,
        order_num,
        driver1_emp_id,
        driver1_name,
        driver2_emp_id,
        driver2_name,
        truck_id,
        truck_no,
        case
            when info.actual_start_time_str is null or trim(info.actual_start_time_str) = '' then null
            when info.actual_start_time_str rlike '^\\d+$' then date_format(
                    from_utc_timestamp(
                        to_timestamp(
                            case when length(info.actual_start_time_str) = 10 
                                 then cast(info.actual_start_time_str as bigint) 
                                 else cast(info.actual_start_time_str as bigint)/1000 
                            end
                        ), 
                        'UTC'
                    ),
                    'yyyy-MM-dd HH:mm:ss'
                )
            else info.actual_start_time_str
            end as actual_start_time,
        case
            when info.actual_end_time_str is null or trim(info.actual_end_time_str) = '' then null
            when info.actual_end_time_str rlike '^\\d+$' then date_format(
                    from_utc_timestamp(
                        to_timestamp(
                            case when length(info.actual_end_time_str) = 10 
                                 then cast(info.actual_end_time_str as bigint) 
                                 else cast(info.actual_end_time_str as bigint)/1000 
                            end
                        ), 
                        'UTC'
                    ),
                    'yyyy-MM-dd HH:mm:ss'
                )
            else info.actual_end_time_str
            end as actual_end_time,
        dim_tb.estimated_time as estimate_end_time,
        actual_distance,
        case
            when info.actual_start_time_str is not null and info.actual_end_time_str is not null
                then unix_timestamp(info.actual_end_time_str, 'yyyy-MM-dd HH:mm:ss')
                - unix_timestamp(info.actual_start_time_str, 'yyyy-MM-dd HH:mm:ss')
            else null
            end as finish_dur_sec,
        case
            when info.actual_end_time_str rlike '^\\d+$' then cast(info.actual_end_time_str as bigint)
            else unix_timestamp(info.actual_end_time_str, 'yyyy-MM-dd HH:mm:ss') * 1000
            end as ts,
        info.ds  -- 分区字段ds
    FROM (
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
            concat(substr(after.driver1_name, 1, 1), '*') as driver1_name,
            after.driver2_emp_id,
            concat(substr(after.driver2_name, 1, 1), '*') as driver2_name,
            after.truck_id,
            md5(after.truck_no) as truck_no,
            after.actual_start_time as actual_start_time_str,
            after.actual_end_time as actual_end_time_str,
            after.actual_distance,
            case
                when after.actual_end_time rlike '^\\d+$' then date_format(
                        from_utc_timestamp(
                            to_timestamp(
                                case when length(after.actual_end_time) = 10 
                                     then cast(after.actual_end_time as bigint) 
                                     else cast(after.actual_end_time as bigint)/1000 
                                end
                            ), 
                            'UTC'
                        ),
                        'yyyy-MM-dd'
                    )
                else date_format(from_utc_timestamp(to_timestamp(after.actual_end_time), 'UTC'), 'yyyy-MM-dd')
                end as ds
        FROM ods_transport_task after
        WHERE is_deleted = '0' AND ds = '{0}'
    ) info
    LEFT JOIN (
        SELECT id, estimated_time FROM dim_shift_full WHERE ds = '{0}'
    ) dim_tb ON info.shift_id = dim_tb.id
    LIMIT 200
    """.format(ds)
    df = spark.sql(sql)
    insert_to_hive(df, "dwd_trans_trans_finish_inc")


def etl_dwd_bound_inbound_inc(spark, ds):
    """中转域入库事务事实表"""
    sql = """
    SELECT
        after.id,
        after.order_id,
        after.org_id,
        case
            when after.inbound_time is null or trim(after.inbound_time) = '' then null
            when after.inbound_time rlike '^\\d+$' then date_format(
                    from_utc_timestamp(
                        to_timestamp(
                            case when length(after.inbound_time) = 10 
                                 then cast(after.inbound_time as bigint) 
                                 else cast(after.inbound_time as bigint)/1000 
                            end
                        ), 
                        'UTC'
                    ),
                    'yyyy-MM-dd HH:mm:ss'
                )
            else date_format(from_utc_timestamp(to_timestamp(after.inbound_time), 'UTC'), 'yyyy-MM-dd HH:mm:ss')
            end as inbound_time,
        after.inbound_emp_id,
        '{0}' ds
    FROM ods_order_org_bound after
    WHERE is_deleted = '0' AND ds = '{0}'
    LIMIT 250
    """.format(ds)
    df = spark.sql(sql)
    insert_to_hive(df, "dwd_bound_inbound_inc")


def etl_dwd_bound_sort_inc(spark, ds):
    """中转域分拣事务事实表"""
    sql = """
    SELECT
        after.id,
        after.order_id,
        after.org_id,
        case
            when after.sort_time is null or trim(after.sort_time) = '' then null
            when after.sort_time rlike '^\\d+$' then date_format(
                    from_utc_timestamp(
                        to_timestamp(
                            case when length(after.sort_time) = 10 
                                 then cast(after.sort_time as bigint) 
                                 else cast(after.sort_time as bigint)/1000 
                            end
                        ), 
                        'UTC'
                    ),
                    'yyyy-MM-dd HH:mm:ss'
                )
            else after.sort_time
            end as sort_time,
        after.sorter_emp_id,
        '{0}' ds
    FROM ods_order_org_bound as after
    WHERE is_deleted = '0' AND ds = '{0}' AND after.sort_time is not null
    LIMIT 250
    """.format(ds)
    df = spark.sql(sql)
    insert_to_hive(df, "dwd_bound_sort_inc")


def etl_dwd_bound_outbound_inc(spark, ds):
    """中转域出库事务事实表"""
    sql = """
    SELECT
        after.id,
        after.order_id,
        after.org_id,
        case
            when after.outbound_time is null or trim(after.outbound_time) = '' then null
            when after.outbound_time rlike '^\\d+$' then date_format(
                    from_utc_timestamp(
                        to_timestamp(
                            case when length(after.outbound_time) = 10 
                                 then cast(after.outbound_time as bigint) 
                                 else cast(after.outbound_time as bigint)/1000 
                            end
                        ), 
                        'UTC'
                    ),
                    'yyyy-MM-dd HH:mm:ss'
                )
            else after.outbound_time
            end as outbound_time,
        after.outbound_emp_id,
        '{0}' ds
    FROM ods_order_org_bound as after
    WHERE is_deleted = '0' AND ds = '{0}' AND after.outbound_time is not null
    """.format(ds)
    df = spark.sql(sql)
    insert_to_hive(df, "dwd_bound_outbound_inc")


def main(partition_date):
    """主函数：执行所有DWD表的ETL"""
    spark = get_spark_session()
    # 按业务域顺序执行
    # 交易域
    etl_dwd_trade_order_detail_inc(spark, partition_date)
    etl_dwd_trade_pay_suc_detail_inc(spark, partition_date)
    etl_dwd_trade_order_cancel_detail_inc(spark, partition_date)
    etl_dwd_trade_order_process_inc(spark, partition_date)

    # 物流域
    etl_dwd_trans_receive_detail_inc(spark, partition_date)
    etl_dwd_trans_dispatch_detail_inc(spark, partition_date)
    etl_dwd_trans_bound_finish_detail_inc(spark, partition_date)
    etl_dwd_trans_deliver_suc_detail_inc(spark, partition_date)
    etl_dwd_trans_sign_detail_inc(spark, partition_date)
    etl_dwd_trans_trans_finish_inc(spark, partition_date)

    # 中转域
    etl_dwd_bound_inbound_inc(spark, partition_date)
    etl_dwd_bound_sort_inc(spark, partition_date)
    etl_dwd_bound_outbound_inc(spark, partition_date)

    print("[SUCCESS] All TMS DWD tables ETL completed for ds={0}".format(partition_date))
    spark.stop()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: spark-submit tms_dwd.py <partition_date>")
        sys.exit(1)
    ds = sys.argv[1]
    main(ds)