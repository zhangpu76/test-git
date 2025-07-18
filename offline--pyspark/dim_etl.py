# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
import sys

def get_spark_session():
    spark = SparkSession.builder \
        .appName("HiveDimETL") \
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
        print("[WARN] No data found for {} on {}, skipping write.".format(table_name, partition_date))
        return
    df_with_partition = df.withColumn("ds", lit(partition_date))
    df_with_partition.write.mode("overwrite").insertInto("gmall.{}".format(table_name))

def etl_dim_sku(spark, ds):
    sql = """
    SELECT
      sku.id,
      sku.price,
      sku.sku_name,
      sku.sku_desc,
      sku.weight,
      sku.is_sale,
      sku.spu_id,
      spu.spu_name,
      sku.category3_id,
      c3.name AS category3_name,
      c2.id AS category2_id,
      c2.name AS category2_name,
      c1.id AS category1_id,
      c1.name AS category1_name,
      sku.tm_id,
      tm.tm_name,
      attr.attrs AS sku_attr_values,
      sale_attr.sale_attrs AS sku_sale_attr_values,
      sku.create_time
    FROM ods_sku_info sku
    LEFT JOIN ods_spu_info spu ON sku.spu_id = spu.id AND spu.ds = '{0}'
    LEFT JOIN ods_base_category3 c3 ON sku.category3_id = c3.id AND c3.ds = '{0}'
    LEFT JOIN ods_base_category2 c2 ON c3.category2_id = c2.id AND c2.ds = '{0}'
    LEFT JOIN ods_base_category1 c1 ON c2.category1_id = c1.id AND c1.ds = '{0}'
    LEFT JOIN ods_base_trademark tm ON sku.tm_id = tm.id AND tm.ds = '{0}'
    LEFT JOIN (
      SELECT sku_id,
             collect_set(named_struct(
               'attr_id', cast(attr_id as string),
               'value_id', cast(value_id as string),
               'attr_name', attr_name,
               'value_name', value_name)) AS attrs
      FROM ods_sku_attr_value
      WHERE ds = '{0}'
      GROUP BY sku_id
    ) attr ON sku.id = attr.sku_id
    LEFT JOIN (
      SELECT sku_id,
             collect_set(named_struct(
               'sale_attr_id', cast(sale_attr_id as string),
               'sale_attr_value_id', cast(sale_attr_value_id as string),
               'sale_attr_name', sale_attr_name,
               'sale_attr_value_name', sale_attr_value_name)) AS sale_attrs
      FROM ods_sku_sale_attr_value
      WHERE ds = '{0}'
      GROUP BY sku_id
    ) sale_attr ON sku.id = sale_attr.sku_id
    WHERE sku.ds = '{0}'
    """.format(ds)
    df = spark.sql(sql)
    insert_to_hive(df, "dim_sku", ds)

def etl_dim_coupon(spark, ds):
    sql = """
    SELECT
      ci.id,
      ci.coupon_name,
      CAST(ci.coupon_type AS STRING) AS coupon_type_code,
      coupon_dic.dic_name AS coupon_type_name,
      ci.condition_amount,
      ci.condition_num,
      ci.activity_id,
      ci.benefit_amount,
      ci.benefit_discount,
      CASE CAST(ci.coupon_type AS STRING)
          WHEN '3201' THEN CONCAT('满', ci.condition_amount, '元减', ci.benefit_amount, '元')
          WHEN '3202' THEN CONCAT('满', ci.condition_num, '件打', ci.benefit_discount, ' 折')
          WHEN '3203' THEN CONCAT('减', ci.benefit_amount, '元')
      END AS benefit_rule,
      ci.create_time,
      CAST(ci.range_type AS STRING) AS range_type_code,
      range_dic.dic_name AS range_type_name,
      ci.limit_num,
      ci.taken_count,
      ci.start_time,
      ci.end_time,
      ci.operate_time,
      ci.expire_time
    FROM (
      SELECT * FROM ods_coupon_info WHERE ds = '{0}'
    ) ci
    LEFT JOIN (
      SELECT dic_code, dic_name FROM ods_base_dic WHERE ds = '{0}' AND parent_code='32'
    ) coupon_dic ON ci.coupon_type = coupon_dic.dic_code
    LEFT JOIN (
      SELECT dic_code, dic_name FROM ods_base_dic WHERE ds = '{0}' AND parent_code='33'
    ) range_dic ON ci.range_type = range_dic.dic_code
    """.format(ds)
    df = spark.sql(sql)
    insert_to_hive(df, "dim_coupon", ds)

def etl_dim_activity(spark, ds):
    sql = """
    SELECT
        rule.id AS activity_rule_id,
        info.id AS activity_id,
        activity_name,
        rule.activity_type AS activity_type_code,
        dic.dic_name AS activity_type_name,
        activity_desc,
        start_time,
        end_time,
        rule.create_time,
        condition_amount,
        condition_num,
        benefit_amount,
        benefit_discount,
        CASE rule.activity_type
            WHEN '3101' THEN CONCAT('满', condition_amount, '元减', benefit_amount, '元')
            WHEN '3102' THEN CONCAT('满', condition_num, '件打', benefit_discount, ' 折')
            WHEN '3103' THEN CONCAT('打', benefit_discount, '折')
        END AS benefit_rule,
        benefit_level
    FROM (
        SELECT * FROM ods_activity_rule WHERE ds = '{0}'
    ) rule
    LEFT JOIN (
        SELECT * FROM ods_activity_info WHERE ds = '{0}'
    ) info ON rule.activity_id = info.id
    LEFT JOIN (
        SELECT dic_code, dic_name FROM ods_base_dic WHERE ds = '{0}' AND parent_code='31'
    ) dic ON rule.activity_type = dic.dic_code
    """.format(ds)
    df = spark.sql(sql)
    insert_to_hive(df, "dim_activity", ds)

def etl_dim_province(spark, ds):
    sql = """
    SELECT
      province.id,
      province.name AS province_name,
      province.area_code,
      province.iso_code,
      province.iso_3166_2,
      region.id AS region_id,
      region.region_name
    FROM (
      SELECT * FROM ods_base_province WHERE ds = '{0}'
    ) province
    LEFT JOIN (
      SELECT * FROM ods_base_region WHERE ds = '{0}'
    ) region ON province.region_id = region.id
    """.format(ds)
    df = spark.sql(sql)
    insert_to_hive(df, "dim_province", ds)

def etl_dim_promotion_pos(spark, ds):
    sql = """
    SELECT id, pos_location, pos_type, promotion_type, create_time, operate_time
    FROM ods_promotion_pos
    WHERE ds = '{0}'
    """.format(ds)
    df = spark.sql(sql)
    insert_to_hive(df, "dim_promotion_pos", ds)

def etl_dim_promotion_refer(spark, ds):
    sql = """
    SELECT id, refer_name, create_time, operate_time
    FROM ods_promotion_refer
    WHERE ds = '{0}'
    """.format(ds)
    df = spark.sql(sql)
    insert_to_hive(df, "dim_promotion_refer", ds)

def etl_dim_date(spark):
    sql = "SELECT * FROM tmp_dim_date_info"
    df = spark.sql(sql)
    df.write.mode("overwrite").insertInto("gmall.dim_date")

def etl_dim_user_zip(spark, ds):
    sql = """
    SELECT
      id,
      CONCAT(SUBSTR(name, 1, 1), '*') AS name,
      CASE WHEN phone_num RLIKE '^1[3-9][0-9]{9}$' THEN CONCAT(SUBSTR(phone_num, 1, 3), '****', SUBSTR(phone_num, 8)) ELSE NULL END AS phone_num,
      CASE WHEN email RLIKE '^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\\.[a-zA-Z0-9-.]+$' THEN CONCAT('***@', SPLIT(email, '@')[1]) ELSE NULL END AS email,
      user_level,
      birthday,
      gender,
      create_time,
      operate_time,
      '{0}' AS start_date,
      '9999-12-31' AS end_date
    FROM ods_user_info
    WHERE ds = '{0}'
    """.replace("{", "{{").replace("}", "}}").replace("{{0}}", "{0}").format(ds)
    df = spark.sql(sql)
    insert_to_hive(df, "dim_user_zip", ds)


def main(partition_date):
    spark = get_spark_session()

    etl_dim_sku(spark, partition_date)
    etl_dim_coupon(spark, partition_date)
    etl_dim_activity(spark, partition_date)
    etl_dim_province(spark, partition_date)
    etl_dim_promotion_pos(spark, partition_date)
    etl_dim_promotion_refer(spark, partition_date)
    etl_dim_date(spark)
    etl_dim_user_zip(spark, partition_date)

    print("[SUCCESS] All dimension tables ETL completed for ds={}".format(partition_date))

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: spark-submit this_script.py <partition_date>")
        sys.exit(1)
    ds = sys.argv[1]
    main(ds)
