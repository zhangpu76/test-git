# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, collect_set, md5, col
import sys


def get_spark_session():
    """获取配置好的SparkSession"""
    spark = SparkSession.builder \
        .appName("TMSDimETL") \
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
    """将DataFrame写入Hive表，兼容低版本Python"""
    if df.rdd.isEmpty():
        # 替换f-string为format格式，兼容Python3.5及以下
        print("[WARN] No data found for {0} on {1}, skipping write.".format(table_name, partition_date))
        return
    # 添加分区字段
    df_with_partition = df.withColumn("ds", lit(partition_date))
    # 修复插入表名的语法，使用format格式
    df_with_partition.write.mode("overwrite").insertInto("tms.{0}".format(table_name))


def etl_dim_complex_full(spark, ds):
    """维度表：小区全量表"""
    sql = """
    SELECT
        complex_info.id,
        complex_name,
        courier_emp_ids,
        province_id,
        dic_for_prov.name province_name,
        city_id,
        dic_for_city.name city_name,
        district_id,
        district_name
    FROM (
        SELECT id, complex_name, province_id, city_id, district_id, district_name
        FROM ods_base_complex
        WHERE ds = '{0}' AND is_deleted = '0'
    ) complex_info
    JOIN (
        SELECT id, name
        FROM ods_base_region_info
        WHERE ds = '{0}' AND is_deleted = '0'
    ) dic_for_prov ON complex_info.province_id = dic_for_prov.id
    JOIN (
        SELECT id, name
        FROM ods_base_region_info
        WHERE ds = '{0}' AND is_deleted = '0'
    ) dic_for_city ON complex_info.city_id = dic_for_city.id
    LEFT JOIN (
        SELECT
            collect_set(cast(courier_emp_id as string)) courier_emp_ids,
            complex_id
        FROM ods_express_courier_complex
        WHERE ds = '{0}' AND is_deleted = '0'
        GROUP BY complex_id
    ) complex_courier ON complex_info.id = complex_courier.complex_id
    """.format(ds)
    df = spark.sql(sql)
    insert_to_hive(df, "dim_complex_full", ds)


def etl_dim_organ_full(spark, ds):
    """维度表：机构全量表"""
    sql = """
    SELECT
        organ_info.id,
        organ_info.org_name,
        org_level,
        region_id,
        region_info.name region_name,
        region_info.dict_code region_code,
        org_parent_id,
        org_for_parent.org_name org_parent_name
    FROM (
        SELECT id, org_name, org_level, region_id, org_parent_id
        FROM ods_base_organ
        WHERE ds = '{0}' AND is_deleted = '0'
    ) organ_info
    LEFT JOIN (
        SELECT id, name, dict_code
        FROM ods_base_region_info
        WHERE ds = '{0}' AND is_deleted = '0'
    ) region_info ON organ_info.region_id = region_info.id
    LEFT JOIN (
        SELECT id, org_name
        FROM ods_base_organ
        WHERE ds = '{0}' AND is_deleted = '0'
    ) org_for_parent ON organ_info.org_parent_id = org_for_parent.id
    """.format(ds)
    df = spark.sql(sql)
    insert_to_hive(df, "dim_organ_full", ds)


def etl_dim_region_full(spark, ds):
    """维度表：地区全量表"""
    sql = """
    SELECT
        id,
        parent_id,
        name,
        dict_code,
        short_name
    FROM ods_base_region_info
    WHERE ds = '{0}' AND is_deleted = '0'
    """.format(ds)
    df = spark.sql(sql)
    insert_to_hive(df, "dim_region_full", ds)


def etl_dim_express_courier_full(spark, ds):
    """维度表：快递员全量表"""
    sql = """
    SELECT
        express_cor_info.id,
        emp_id,
        org_id,
        org_name,
        md5(working_phone) working_phone,
        express_type,
        dic_info.name express_type_name
    FROM (
        SELECT id, emp_id, org_id, working_phone, express_type
        FROM ods_express_courier
        WHERE ds = '{0}' AND is_deleted = '0'
    ) express_cor_info
    JOIN (
        SELECT id, org_name
        FROM ods_base_organ
        WHERE ds = '{0}' AND is_deleted = '0'
    ) organ_info ON express_cor_info.org_id = organ_info.id
    JOIN (
        SELECT id, name
        FROM ods_base_dic
        WHERE ds = '{0}' AND is_deleted = '0'
    ) dic_info ON express_type = dic_info.id
    """.format(ds)
    df = spark.sql(sql)
    insert_to_hive(df, "dim_express_courier_full", ds)


def etl_dim_shift_full(spark, ds):
    """维度表：班次全量表"""
    sql = """
    SELECT
        shift_info.id,
        line_id,
        line_info.name line_name,
        line_no,
        line_level,
        org_id,
        transport_line_type_id,
        dic_info.name transport_line_type_name,
        start_org_id,
        start_org_name,
        end_org_id,
        end_org_name,
        pair_line_id,
        distance,
        cost,
        estimated_time,
        start_time,
        driver1_emp_id,
        driver2_emp_id,
        truck_id,
        pair_shift_id
    FROM (
        SELECT id, line_id, start_time, driver1_emp_id, driver2_emp_id, truck_id, pair_shift_id
        FROM ods_line_base_shift
        WHERE ds = '{0}' AND is_deleted = '0'
    ) shift_info
    JOIN (
        SELECT id, name, line_no, line_level, org_id, transport_line_type_id,
               start_org_id, start_org_name, end_org_id, end_org_name,
               pair_line_id, distance, cost, estimated_time
        FROM ods_line_base_info
        WHERE ds = '{0}' AND is_deleted = '0'
    ) line_info ON shift_info.line_id = line_info.id
    JOIN (
        SELECT id, name
        FROM ods_base_dic
        WHERE ds = '{0}' AND is_deleted = '0'
    ) dic_info ON line_info.transport_line_type_id = dic_info.id
    """.format(ds)
    df = spark.sql(sql)
    insert_to_hive(df, "dim_shift_full", ds)


def etl_dim_truck_driver_full(spark, ds):
    """维度表：货车司机全量表"""
    sql = """
    SELECT
        driver_info.id,
        emp_id,
        org_id,
        organ_info.org_name,
        team_id,
        team_info.name team_name,
        license_type,
        init_license_date,
        expire_date,
        license_no,
        is_enabled
    FROM (
        SELECT id, emp_id, org_id, team_id, license_type, init_license_date,
               expire_date, license_no, is_enabled
        FROM ods_truck_driver
        WHERE ds = '{0}' AND is_deleted = '0'
    ) driver_info
    JOIN (
        SELECT id, org_name
        FROM ods_base_organ
        WHERE ds = '{0}' AND is_deleted = '0'
    ) organ_info ON driver_info.org_id = organ_info.id
    JOIN (
        SELECT id, name
        FROM ods_truck_team
        WHERE ds = '{0}' AND is_deleted = '0'
    ) team_info ON driver_info.team_id = team_info.id
    """.format(ds)
    df = spark.sql(sql)
    insert_to_hive(df, "dim_truck_driver_full", ds)


def etl_dim_truck_full(spark, ds):
    """维度表：货车全量表"""
    sql = """
    SELECT
        truck_info.id,
        team_id,
        team_info.name team_name,
        team_no,
        org_id,
        org_name,
        manager_emp_id,
        md5(truck_no) truck_no,
        truck_model_id,
        model_name truck_model_name,
        model_type truck_model_type,
        dic_for_type.name truck_model_type_name,
        model_no truck_model_no,
        brand truck_brand,
        dic_for_brand.name truck_brand_name,
        truck_weight,
        load_weight,
        total_weight,
        eev,
        boxcar_len,
        boxcar_wd,
        boxcar_hg,
        max_speed,
        oil_vol,
        device_gps_id,
        engine_no,
        license_registration_date,
        license_last_check_date,
        license_expire_date,
        is_enabled
    FROM (
        SELECT id, team_id, truck_no, truck_model_id, device_gps_id, engine_no,
               license_registration_date, license_last_check_date, 
               license_expire_date, is_enabled
        FROM ods_truck_info
        WHERE ds = '{0}' AND is_deleted = '0'  -- 补充ds过滤条件
    ) truck_info
    JOIN (
        SELECT id, name, team_no, org_id, manager_emp_id
        FROM ods_truck_team
        WHERE ds = '{0}' AND is_deleted = '0'  -- 补充ds过滤条件
    ) team_info ON truck_info.team_id = team_info.id
    JOIN (
        SELECT id, org_name
        FROM ods_base_organ
        WHERE ds = '{0}' AND is_deleted = '0'
    ) organ_info ON team_info.org_id = organ_info.id
    JOIN (
        SELECT id, model_name, model_type, model_no, brand, 
               truck_weight, load_weight, total_weight, eev,
               boxcar_len, boxcar_wd, boxcar_hg, max_speed, oil_vol
        FROM ods_truck_model
        WHERE ds = '{0}' AND is_deleted = '0'  -- 补充ds过滤条件
    ) model_info ON truck_info.truck_model_id = model_info.id
    JOIN (
        SELECT id, name
        FROM ods_base_dic
        WHERE ds = '{0}' AND is_deleted = '0'
    ) dic_for_type ON model_info.model_type = dic_for_type.id
    JOIN (
        SELECT id, name
        FROM ods_base_dic
        WHERE ds = '{0}' AND is_deleted = '0'
    ) dic_for_brand ON model_info.brand = dic_for_brand.id
    """.format(ds)
    df = spark.sql(sql)
    insert_to_hive(df, "dim_truck_full", ds)


def etl_dim_user_zip(spark, ds):
    """维度表：用户拉链表"""
    sql = """
    SELECT
        id,
        login_name,
        nick_name,
        md5(passwd) passwd,
        md5(real_name) real_name,
        md5(CASE
            WHEN phone_num RLIKE '^(13[0-9]|14[01456879]|15[0-35-9]|16[2567]|17[0-8]|18[0-9]|19[0-35-9])\\d{{8}}$'
            THEN phone_num ELSE NULL END
        ) phone_num,
        md5(CASE 
            WHEN email RLIKE '^[a-zA-Z0-9_-]+@[a-zA-Z0-9_-]+(\\.[a-zA-Z0-9_-]+)+$'
            THEN email ELSE NULL END
        ) email,
        user_level,
        date_add('1970-01-01', cast(birthday as int)) birthday,
        gender,
        -- 修改这里：先将字符串解析为timestamp，再转换时区
        date_format(from_utc_timestamp(to_timestamp(create_time, 'yyyy-MM-dd HH:mm:ss'), 'UTC'), 'yyyy-MM-dd') start_date,
        '9999-12-31' end_date
    FROM ods_user_info
    WHERE ds = '{0}' AND is_deleted = '0'
    """.format(ds)
    df = spark.sql(sql)
    insert_to_hive(df, "dim_user_zip", ds)


def etl_dim_user_address_zip(spark, ds):
    """维度表：用户地址拉链表"""
    sql = """
    SELECT
        id,
        user_id,
        md5(CASE
            WHEN phone RLIKE '^(13[0-9]|14[01456879]|15[0-35-9]|16[2567]|17[0-8]|18[0-9]|19[0-35-9])\\d{{8}}$'
            THEN phone ELSE NULL END
        ) phone,
        province_id,
        city_id,
        district_id,
        complex_id,
        address,
        is_default,
        concat(substr(create_time, 1, 10), ' ', substr(create_time, 12, 8)) start_date,
        '9999-12-31' end_date
    FROM ods_user_address
    WHERE ds = '{0}' AND is_deleted = '0'
    """.format(ds)
    df = spark.sql(sql)
    insert_to_hive(df, "dim_user_address_zip", ds)


def main(partition_date):
    """主函数：执行所有维度表的ETL"""
    spark = get_spark_session()
    # 按依赖顺序执行ETL
    etl_dim_region_full(spark, partition_date)  # 地区表：作为其他表的基础
    etl_dim_organ_full(spark, partition_date)  # 机构表：依赖地区表
    etl_dim_complex_full(spark, partition_date)  # 小区表：依赖地区表和机构表
    etl_dim_express_courier_full(spark, partition_date)  # 快递员表：依赖机构表
    etl_dim_shift_full(spark, partition_date)  # 班次表：依赖线路和机构表
    etl_dim_truck_driver_full(spark, partition_date)  # 司机表：依赖机构和团队表
    etl_dim_truck_full(spark, partition_date)  # 货车表：依赖团队和车型表
    etl_dim_user_zip(spark, partition_date)  # 用户表
    etl_dim_user_address_zip(spark, partition_date)  # 用户地址表：依赖用户表

    # 修复成功提示的格式
    print("[SUCCESS] All TMS dimension tables ETL completed for ds={0}".format(partition_date))


# 修复入口函数判断条件（__main__）
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: spark-submit tms_dim.py <partition_date>")  # 明确脚本名称
        sys.exit(1)
    ds = sys.argv[1]
    main(ds)