# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, sum as _sum, count, countDistinct, coalesce, when, min as _min, max as _max, expr, col

import sys

def get_spark_session():
    spark = SparkSession.builder \
        .appName("DWS_ETL") \
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
        print("[WARN] No data for {} on {}, skipping write.".format(table_name, partition_date))
        return
    df = df.withColumn("ds", lit(partition_date))
    df.write.mode("overwrite").insertInto("gmall." + table_name)
    print("[INFO] Written {} for ds={}".format(table_name, partition_date))

def etl_dws_trade_user_sku_order_1d(spark, ds):
    dwd = spark.table("dwd_trade_order_detail_inc").filter("ds='{}'".format(ds))
    sku = spark.table("dim_sku").filter("ds='{}'".format(ds)).selectExpr(
        "id", "sku_name", "category1_id", "category1_name", "category2_id", "category2_name",
        "category3_id", "category3_name", "tm_id", "tm_name"
    )
    od = dwd.groupBy("ds", "user_id", "sku_id").agg(
        count("*").alias("order_count_1d"),
        _sum("sku_num").alias("order_num_1d"),
        _sum("split_original_amount").alias("order_original_amount_1d"),
        _sum(coalesce("split_activity_amount", lit(0.0))).alias("activity_reduce_amount_1d"),
        _sum(coalesce("split_coupon_amount", lit(0.0))).alias("coupon_reduce_amount_1d"),
        _sum("split_total_amount").alias("order_total_amount_1d")
    )

    df = od.join(sku, od.sku_id == sku.id, "left") \
        .select(
            "user_id",
            sku.id.alias("sku_id"),
            "sku_name",
            "category1_id",
            "category1_name",
            "category2_id",
            "category2_name",
            "category3_id",
            "category3_name",
            "tm_id",
            "tm_name",
            "order_count_1d",
            "order_num_1d",
            "order_original_amount_1d",
            "activity_reduce_amount_1d",
            "coupon_reduce_amount_1d",
            "order_total_amount_1d",
            od.ds.alias("ds")
        )
    insert_to_hive(df, "dws_trade_user_sku_order_1d", ds)

def etl_dws_trade_user_order_1d(spark, ds):
    dwd = spark.table("dwd_trade_order_detail_inc").filter("ds='{}'".format(ds))
    df = dwd.groupBy("user_id", "ds").agg(
        countDistinct("order_id").alias("order_count_1d"),
        _sum("sku_num").alias("order_num_1d"),
        _sum("split_original_amount").alias("order_original_amount_1d"),
        _sum(coalesce("split_activity_amount", lit(0.0))).alias("activity_reduce_amount_1d"),
        _sum(coalesce("split_coupon_amount", lit(0.0))).alias("coupon_reduce_amount_1d"),
        _sum("split_total_amount").alias("order_total_amount_1d")
    ).selectExpr(
        "user_id",
        "order_count_1d",
        "order_num_1d",
        "order_original_amount_1d",
        "activity_reduce_amount_1d",
        "coupon_reduce_amount_1d",
        "order_total_amount_1d",
        "ds"
    )
    insert_to_hive(df, "dws_trade_user_order_1d", ds)

def etl_dws_trade_user_cart_add_1d(spark, ds):
    dwd = spark.table("dwd_trade_cart_add_inc").filter("ds='{}'".format(ds))
    df = dwd.groupBy("user_id", "ds").agg(
        count("*").alias("cart_add_count_1d"),
        _sum("sku_num").alias("cart_add_num_1d")
    ).selectExpr("user_id", "cart_add_count_1d", "cart_add_num_1d", "ds")
    insert_to_hive(df, "dws_trade_user_cart_add_1d", ds)

def etl_dws_trade_user_payment_1d(spark, ds):
    dwd = spark.table("dwd_trade_pay_detail_suc_inc").filter("ds='{}'".format(ds))
    df = dwd.groupBy("user_id", "ds").agg(
        countDistinct("order_id").alias("payment_count_1d"),
        _sum("sku_num").alias("payment_num_1d"),
        _sum("split_payment_amount").alias("payment_amount_1d")
    ).selectExpr("user_id", "payment_count_1d", "payment_num_1d", "payment_amount_1d", "ds")
    insert_to_hive(df, "dws_trade_user_payment_1d", ds)

def etl_dws_trade_province_order_1d(spark, ds):
    dwd = spark.table("dwd_trade_order_detail_inc").filter("ds='{}'".format(ds))
    prov = spark.table("dim_province").filter("ds='{}'".format(ds)).selectExpr(
        "id", "province_name", "area_code", "iso_code", "iso_3166_2"
    )
    od = dwd.groupBy("province_id", "ds").agg(
        countDistinct("order_id").alias("order_count_1d"),
        _sum("split_original_amount").alias("order_original_amount_1d"),
        _sum(coalesce("split_activity_amount", lit(0.0))).alias("activity_reduce_amount_1d"),
        _sum(coalesce("split_coupon_amount", lit(0.0))).alias("coupon_reduce_amount_1d"),
        _sum("split_total_amount").alias("order_total_amount_1d")
    )
    df = od.join(prov, od.province_id == prov.id, "left").select(
        od.province_id,
        "province_name",
        "area_code",
        "iso_code",
        "iso_3166_2",
        "order_count_1d",
        "order_original_amount_1d",
        "activity_reduce_amount_1d",
        "coupon_reduce_amount_1d",
        "order_total_amount_1d",
        od.ds.alias("ds")
    )
    insert_to_hive(df, "dws_trade_province_order_1d", ds)

def etl_dws_tool_user_coupon_coupon_used_1d(spark, ds):
    coupon_used = spark.table("dwd_tool_coupon_used_inc").filter("ds='{}'".format(ds))
    coupon_dim = spark.table("dim_coupon").filter("ds='{}'".format(ds)).selectExpr(
        "id", "coupon_name", "coupon_type_code", "coupon_type_name", "benefit_rule"
    )
    used_agg = coupon_used.groupBy("ds", "user_id", "coupon_id").agg(count("*").alias("used_count"))
    df = used_agg.join(coupon_dim, used_agg.coupon_id == coupon_dim.id, "left").select(
        "user_id",
        "coupon_id",
        "coupon_name",
        "coupon_type_code",
        "coupon_type_name",
        "benefit_rule",
        "used_count",
        used_agg.ds.alias("ds")
    )
    insert_to_hive(df, "dws_tool_user_coupon_coupon_used_1d", ds)

def etl_dws_interaction_sku_favor_add_1d(spark, ds):
    favor = spark.table("dwd_interaction_favor_add_inc").filter("ds='{}'".format(ds))
    sku = spark.table("dim_sku").filter("ds='{}'".format(ds)).selectExpr(
        "id", "sku_name", "category1_id", "category1_name", "category2_id", "category2_name",
        "category3_id", "category3_name", "tm_id", "tm_name"
    )
    favor_agg = favor.groupBy("ds", "sku_id").agg(count("*").alias("favor_add_count_1d"))
    df = favor_agg.join(sku, favor_agg.sku_id == sku.id, "left").select(
        favor_agg.sku_id.alias("sku_id"),
        "sku_name",
        "category1_id",
        "category1_name",
        "category2_id",
        "category2_name",
        "category3_id",
        "category3_name",
        "tm_id",
        "tm_name",
        "favor_add_count_1d",
        favor_agg.ds.alias("ds")
    )
    insert_to_hive(df, "dws_interaction_sku_favor_add_1d", ds)

def etl_dws_traffic_session_page_view_1d(spark, ds):
    traffic = spark.table("dwd_traffic_page_view_inc").filter("ds='{}'".format(ds))
    df = traffic.groupBy("session_id", "mid_id", "brand", "model", "operate_system", "version_code", "channel").agg(
        _sum("during_time").alias("during_time_1d"),
        count("*").alias("page_count_1d")
    ).withColumn("ds", lit(ds))
    insert_to_hive(df, "dws_traffic_session_page_view_1d", ds)

def etl_dws_traffic_page_visitor_page_view_1d(spark, ds):
    traffic = spark.table("dwd_traffic_page_view_inc").filter("ds='{}'".format(ds))
    df = traffic.groupBy("mid_id", "brand", "model", "operate_system", "page_id").agg(
        _sum("during_time").alias("during_time_1d"),
        count("*").alias("view_count_1d")
    ).withColumn("ds", lit(ds))
    insert_to_hive(df, "dws_traffic_page_visitor_page_view_1d", ds)

# 7天30天等多日汇总表示范(例：dws_trade_user_sku_order_nd)
def etl_dws_trade_user_sku_order_nd(spark, ds):
    base = spark.table("dws_trade_user_sku_order_1d").filter("ds <= '{}'".format(ds))
    df = base.groupBy("user_id", "sku_id", "sku_name", "category1_id", "category1_name",
                      "category2_id", "category2_name", "category3_id", "category3_name",
                      "tm_id", "tm_name").agg(
        _sum(when(expr("ds >= date_sub('{}', 6)".format(ds)), col("order_count_1d")).otherwise(0)).alias("order_count_7d"),
        _sum(when(expr("ds >= date_sub('{}', 6)".format(ds)), col("order_num_1d")).otherwise(0)).alias("order_num_7d"),
        _sum(when(expr("ds >= date_sub('{}', 6)".format(ds)), col("order_original_amount_1d")).otherwise(0)).alias("order_original_amount_7d"),
        _sum(when(expr("ds >= date_sub('{}', 6)".format(ds)), col("activity_reduce_amount_1d")).otherwise(0)).alias("activity_reduce_amount_7d"),
        _sum(when(expr("ds >= date_sub('{}', 6)".format(ds)), col("coupon_reduce_amount_1d")).otherwise(0)).alias("coupon_reduce_amount_7d"),
        _sum(when(expr("ds >= date_sub('{}', 6)".format(ds)), col("order_total_amount_1d")).otherwise(0)).alias("order_total_amount_7d"),
        _sum("order_count_1d").alias("order_count_30d"),
        _sum("order_num_1d").alias("order_num_30d"),
        _sum("order_original_amount_1d").alias("order_original_amount_30d"),
        _sum("activity_reduce_amount_1d").alias("activity_reduce_amount_30d"),
        _sum("coupon_reduce_amount_1d").alias("coupon_reduce_amount_30d"),
        _sum("order_total_amount_1d").alias("order_total_amount_30d")
    )
    insert_to_hive(df, "dws_trade_user_sku_order_nd", ds)

def main(ds):
    spark = get_spark_session()
    etl_dws_trade_user_sku_order_1d(spark, ds)
    etl_dws_trade_user_order_1d(spark, ds)
    etl_dws_trade_user_cart_add_1d(spark, ds)
    etl_dws_trade_user_payment_1d(spark, ds)
    etl_dws_trade_province_order_1d(spark, ds)
    etl_dws_tool_user_coupon_coupon_used_1d(spark, ds)
    etl_dws_interaction_sku_favor_add_1d(spark, ds)
    etl_dws_traffic_session_page_view_1d(spark, ds)
    etl_dws_traffic_page_visitor_page_view_1d(spark, ds)
    etl_dws_trade_user_sku_order_nd(spark, ds)
    # 这里继续调用其他多日汇总、历史累计等ETL函数
    print("[SUCCESS] All DWS ETL completed for ds={}".format(ds))

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: spark-submit script.py <partition_date>")
        sys.exit(1)
    main(sys.argv[1])
