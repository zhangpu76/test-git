# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, date_format, coalesce
import sys

def get_spark_session():
    spark = SparkSession.builder \
        .appName("HiveDwdETL") \
        .config("hive.metastore.uris", "thrift://cdh01:9083") \
        .config("spark.sql.hive.convertMetastoreOrc", "true") \
        .config("spark.hadoop.hive.exec.dynamic.partition", "true") \
        .config("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict") \
        .enableHiveSupport() \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.sql("USE gmall")
    return spark

def insert_to_hive(df, table_name, partition_date):
    if df.rdd.isEmpty():
        # print(f"[WARN] No data found for {table_name} on {partition_date}, skipping write.")
        print("[WARN] No data found for {table_name} on {partition_date}, skipping write.".format(
            table_name=table_name,
            partition_date=partition_date
        ))
        return
    df_with_partition = df.withColumn("ds", lit(partition_date))
    # df_with_partition.write.mode("overwrite").insertInto(f"gmall.{table_name}")
    df_with_partition.write.mode("overwrite").insertInto("gmall.{}".format(table_name))
def etl_dwd_trade_cart_add_inc(spark, ds):
    sql = """
    select
        id,
        user_id,
        sku_id,
        date_format(create_time, 'yyyy-MM-dd') as date_id,
        create_time,
        sku_num
    from ods_cart_info
    where ds = '{0}'
    """.format(ds)
    df = spark.sql(sql)
    insert_to_hive(df, "dwd_trade_cart_add_inc", ds)

def etl_dwd_trade_order_detail_inc(spark, ds):
    sql = """
    select
        od.id,
        od.order_id,
        oi.user_id,
        od.sku_id,
        oi.province_id,
        act.activity_id,
        act.activity_rule_id,
        cou.coupon_id,
        date_format(od.create_time, 'yyyy-MM-dd') as date_id,
        null as create_time,
        od.sku_num,
        od.split_original_amount,
        coalesce(od.split_activity_amount, 0.0) as split_activity_amount,
        coalesce(od.split_coupon_amount, 0.0) as split_coupon_amount,
        od.split_total_amount
    from
    (
        select
            id,
            order_id,
            sku_id,
            create_time,
            sku_num,
            sku_num * order_price as split_original_amount,
            split_total_amount,
            split_activity_amount,
            split_coupon_amount
        from ods_order_detail
        where ds = '{0}'
    ) od
    left join
    (
        select id, user_id, province_id
        from ods_order_info
        where ds = '{0}'
    ) oi on od.order_id = oi.id
    left join
    (
        select order_detail_id, activity_id, activity_rule_id
        from ods_order_detail_activity
        where ds = '{0}'
    ) act on od.id = act.order_detail_id
    left join
    (
        select order_detail_id, coupon_id
        from ods_order_detail_coupon
        where ds = '{0}'
    ) cou on od.id = cou.order_detail_id
    """.format(ds)
    df = spark.sql(sql)
    insert_to_hive(df, "dwd_trade_order_detail_inc", ds)

def etl_dwd_trade_pay_detail_suc_inc(spark, ds):
    sql = """
    select
        od.id,
        od.order_id,
        pi.user_id,
        od.sku_id,
        oi.province_id,
        act.activity_id,
        act.activity_rule_id,
        cou.coupon_id,
        pi.payment_type,
        pay_dic.dic_name as payment_type_name,
        date_format(pi.callback_time, 'yyyy-MM-dd') as date_id,
        pi.callback_time,
        od.sku_num,
        od.split_original_amount,
        coalesce(od.split_activity_amount, 0.0) as split_activity_amount,
        coalesce(od.split_coupon_amount, 0.0) as split_coupon_amount,
        od.split_total_amount as split_payment_amount
    from
    (
        select
            id,
            order_id,
            sku_id,
            sku_num,
            sku_num * order_price as split_original_amount,
            split_total_amount,
            split_activity_amount,
            split_coupon_amount
        from ods_order_detail
        where ds = '{0}'
    ) od
    join
    (
        select user_id, order_id, payment_type, callback_time
        from ods_payment_info
        where ds = '{0}'
    ) pi on od.order_id = pi.order_id
    left join
    (
        select id, province_id
        from ods_order_info
        where ds = '{0}'
    ) oi on od.order_id = oi.id
    left join
    (
        select order_detail_id, activity_id, activity_rule_id
        from ods_order_detail_activity
        where ds = '{0}'
    ) act on od.id = act.order_detail_id
    left join
    (
        select order_detail_id, coupon_id
        from ods_order_detail_coupon
        where ds = '{0}'
    ) cou on od.id = cou.order_detail_id
    left join
    (
        select dic_code, dic_name
        from ods_base_dic
        where ds = '{0}'
        and parent_code = '11'
    ) pay_dic on pi.payment_type = pay_dic.dic_code
    """.format(ds)
    df = spark.sql(sql)
    insert_to_hive(df, "dwd_trade_pay_detail_suc_inc", ds)

def etl_dwd_trade_cart_full(spark, ds):
    sql = """
    select
        id,
        user_id,
        sku_id,
        sku_name,
        sku_num
    from ods_cart_info
    where ds = '{0}'
      and is_ordered = '0'
    """.format(ds)
    df = spark.sql(sql)
    insert_to_hive(df, "dwd_trade_cart_full", ds)

def etl_dwd_trade_trade_flow_acc(spark, ds):
    sql = """
    select
        oi.id as order_id,
        oi.user_id,
        oi.province_id,
        date_format(oi.create_time,'yyyy-MM-dd') as order_date_id,
        oi.create_time as order_time,
        date_format(pi.callback_time,'yyyy-MM-dd') as payment_date_id,
        pi.callback_time as payment_time,
        date_format(log.finish_time,'yyyy-MM-dd') as finish_date_id,
        log.finish_time as finish_time,
        oi.original_total_amount as order_original_amount,
        oi.activity_reduce_amount as order_activity_amount,
        oi.coupon_reduce_amount as order_coupon_amount,
        oi.total_amount as order_total_amount,
        coalesce(pi.payment_amount, 0.0) as payment_amount
    from
    (
        select id, user_id, province_id, create_time, original_total_amount,
               activity_reduce_amount, coupon_reduce_amount, total_amount
        from ods_order_info
        where ds = '{0}'
    ) oi
    left join
    (
        select order_id, callback_time, total_amount as payment_amount
        from ods_payment_info
        where ds = '{0}'
    ) pi on oi.id = pi.order_id
    left join
    (
        select order_id, operate_time as finish_time
        from ods_order_status_log
        where ds = '{0}'
          and order_status = '1004'
    ) log on oi.id = log.order_id
    """.format(ds)
    df = spark.sql(sql)
    insert_to_hive(df, "dwd_trade_trade_flow_acc", ds)

def etl_dwd_tool_coupon_used_inc(spark, ds):
    sql = """
    select
        id,
        coupon_id,
        user_id,
        order_id,
        date_format(get_time, 'yyyy-MM-dd') as date_id,
        used_time as payment_time
    from ods_coupon_use
    where ds = '{0}'
    """.format(ds)
    df = spark.sql(sql)
    insert_to_hive(df, "dwd_tool_coupon_used_inc", ds)

def etl_dwd_interaction_favor_add_inc(spark, ds):
    sql = """
    select
        id,
        user_id,
        sku_id,
        date_format(create_time, 'yyyy-MM-dd') as date_id,
        create_time
    from ods_favor_info
    where ds = '{0}'
    """.format(ds)
    df = spark.sql(sql)
    insert_to_hive(df, "dwd_interaction_favor_add_inc", ds)

# dwd_traffic_page_view_inc 没有插入语句，只有表结构，暂时不写ETL

def etl_dwd_user_register_inc(spark, ds):
    sql = """
    select
        user_id,
        date_id,
        create_time,
        channel,
        province_id,
        version_code,
        mid_id,
        brand,
        model,
        operate_system
    from dwd_user_register_inc
    where ds = '{0}'
    """.format(ds)
    df = spark.sql(sql)
    insert_to_hive(df, "dwd_user_register_inc", ds)

def etl_dwd_user_login_inc(spark, ds):
    sql = """
    select
        user_id,
        date_id,
        login_time,
        channel,
        province_id,
        version_code,
        mid_id,
        brand,
        model,
        operate_system
    from dwd_user_login_inc
    where ds = '{0}'
    """.format(ds)
    df = spark.sql(sql)
    insert_to_hive(df, "dwd_user_login_inc", ds)

def main(partition_date):
    spark = get_spark_session()
    etl_dwd_trade_cart_add_inc(spark, partition_date)
    etl_dwd_trade_order_detail_inc(spark, partition_date)
    etl_dwd_trade_pay_detail_suc_inc(spark, partition_date)
    etl_dwd_trade_cart_full(spark, partition_date)
    etl_dwd_trade_trade_flow_acc(spark, partition_date)
    etl_dwd_tool_coupon_used_inc(spark, partition_date)
    etl_dwd_interaction_favor_add_inc(spark, partition_date)
    # dwd_traffic_page_view_inc 没有ETL逻辑，保持建表即可
    etl_dwd_user_register_inc(spark, partition_date)
    etl_dwd_user_login_inc(spark, partition_date)

    # print(f"[SUCCESS] All DWD tables ETL completed for ds={partition_date}")
    print("[SUCCESS] All DWD tables ETL completed for ds={}".format(partition_date))
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: spark-submit this_script.py <partition_date>")
        sys.exit(1)
    ds = sys.argv[1]
    main(ds)
