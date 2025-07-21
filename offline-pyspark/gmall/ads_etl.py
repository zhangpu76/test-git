# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
import sys


def get_spark_session():
    spark = SparkSession.builder \
        .appName("ADS_ETL") \
        .config("hive.metastore.uris", "thrift://cdh01:9083") \
        .config("spark.sql.hive.convertMetastoreOrc", "true") \
        .config("spark.hadoop.hive.exec.dynamic.partition", "true") \
        .config("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict") \
        .enableHiveSupport() \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.sql("USE gmall")  # 确保使用正确的数据库
    return spark


def insert_to_hive(df, table_name, partition_date):
    if df.rdd.isEmpty():
        print("[WARN] No data for {} on {}, skipping write.".format(table_name, partition_date))
        return
    df = df.withColumn("dt", lit(partition_date))  # 添加 dt 列
    try:
        df.write.mode("overwrite").insertInto("gmall." + table_name)
        print("[INFO] Written {} for dt={}".format(table_name, partition_date))
    except Exception as e:
        print("[ERROR] Failed to insert into {} for dt={}: {}".format(table_name, partition_date, e))


def etl_ads_traffic_stats_by_channel(spark, dt):
    sql = """
    select
        '{0}' dt,
        recent_days,
        channel,
        cast(count(distinct(mid_id)) as bigint) uv_count,
        cast(avg(during_time_1d)/1000 as bigint) avg_duration_sec,
        cast(avg(page_count_1d) as bigint) avg_page_count,
        cast(count(*) as bigint) sv_count,
        cast(sum(if(page_count_1d=1,1,0))/count(*) as decimal(16,2)) bounce_rate
    from dws_traffic_session_page_view_1d lateral view explode(array(1,7,30)) tmp as recent_days
    where ds >= date_add('{0}', -recent_days + 1)  -- 改为 ds
    group by recent_days, channel
    """.format(dt)
    df = spark.sql(sql)
    insert_to_hive(df, "ads_traffic_stats_by_channel", dt)


def etl_ads_page_path(spark, dt):
    sql = """
    select
        '{0}' dt,
        source,
        nvl(target,'null') target,
        count(*) path_count
    from
    (
        select
            concat('step-', rn, ':', page_id) source,
            concat('step-', rn + 1, ':', next_page_id) target
        from
        (
            select
                page_id,
                lead(page_id,1,null) over(partition by session_id order by view_time) next_page_id,
                row_number() over (partition by session_id order by view_time) rn
            from dwd_traffic_page_view_inc
            where ds='{0}'  -- 改为 ds
        ) t1
    ) t2
    group by source, target
    """.format(dt)
    df = spark.sql(sql)
    insert_to_hive(df, "ads_page_path", dt)


def etl_ads_user_change(spark, dt):
    sql = """
    select
        churn.dt,
        user_churn_count,
        user_back_count
    from
    (
        select
            '{0}' dt,
            count(*) user_churn_count
        from dws_user_user_login_td
        where ds='{0}'  -- 改为 ds
          and login_date_last = date_add('{0}', -7)
    ) churn
    join
    (
        select
            '{0}' dt,
            count(*) user_back_count
        from
        (
            select
                user_id,
                login_date_last
            from dws_user_user_login_td
            where ds='{0}'  -- 改为 ds
              and login_date_last = '{0}'
        ) t1
        join
        (
            select
                user_id,
                login_date_last login_date_previous
            from dws_user_user_login_td
            where ds = date_add('{0}', -1)
        ) t2
        on t1.user_id = t2.user_id
        where datediff(login_date_last, login_date_previous) >= 8
    ) back
    on churn.dt = back.dt
    """.format(dt)
    df = spark.sql(sql)
    insert_to_hive(df, "ads_user_change", dt)


def etl_ads_user_stats(spark, dt):
    sql = """
    select
        '{0}' dt,
        recent_days,
        sum(if(login_date_first >= date_add('{0}', -recent_days + 1), 1, 0)) new_user_count,
        count(*) active_user_count
    from dws_user_user_login_td lateral view explode(array(1,7,30)) tmp as recent_days
    where ds = '{0}'  -- 改为 ds
      and login_date_last >= date_add('{0}', -recent_days + 1)
    group by recent_days
    """.format(dt)
    df = spark.sql(sql)
    insert_to_hive(df, "ads_user_stats", dt)


def etl_ads_user_action(spark, dt):
    sql = """
    select
        '{0}' dt,
        home_count,
        good_detail_count,
        cart_count,
        order_count,
        payment_count
    from
    (
        select
            1 recent_days,
            sum(if(page_id='home',1,0)) home_count,
            sum(if(page_id='good_detail',1,0)) good_detail_count
        from dws_traffic_page_visitor_page_view_1d
        where ds='{0}'  -- 改为 ds
          and page_id in ('home','good_detail')
    ) page
    join
    (
        select
            1 recent_days,
            count(*) cart_count
        from dws_trade_user_cart_add_1d
        where ds='{0}'  -- 改为 ds
    ) cart
    on page.recent_days = cart.recent_days
    join
    (
        select
            1 recent_days,
            count(*) order_count
        from dws_trade_user_order_1d
        where ds='{0}'  -- 改为 ds
    ) ord
    on page.recent_days = ord.recent_days
    join
    (
        select
            1 recent_days,
            count(*) payment_count
        from dws_trade_user_payment_1d
        where ds='{0}'  -- 改为 ds
    ) pay
    on page.recent_days = pay.recent_days
    """.format(dt)
    df = spark.sql(sql)
    insert_to_hive(df, "ads_user_action", dt)


def etl_ads_order_continuously_user_count(spark, dt):
    sql = """
    select
        '{0}' dt,
        7 recent_days,
        count(distinct(user_id)) order_continuously_user_count
    from
    (
        select
            user_id,
            datediff(lead(ds,2,'{0}') over(partition by user_id order by ds), ds) diff
        from dws_trade_user_order_1d
        where ds >= date_add('{0}', -6)
    ) t1
    where diff = 2
    """.format(dt)
    df = spark.sql(sql)
    insert_to_hive(df, "ads_order_continuously_user_count", dt)


def etl_ads_repeat_purchase_by_tm(spark, dt):
    sql = """
    select
        '{0}' dt,
        30 recent_days,
        tm_id,
        tm_name,
        cast(sum(if(order_count>=2,1,0))/sum(if(order_count>=1,1,0)) as decimal(16,2)) order_repeat_rate
    from
    (
        select
            user_id,
            tm_id,
            tm_name,
            sum(order_count_30d) order_count
        from dws_trade_user_sku_order_nd
        where ds='{0}'  -- 改为 ds
        group by user_id, tm_id, tm_name
    ) t1
    group by tm_id, tm_name
    """.format(dt)
    df = spark.sql(sql)
    insert_to_hive(df, "ads_repeat_purchase_by_tm", dt)


def etl_ads_order_stats_by_tm(spark, dt):
    sql = """
    select
        '{0}' dt,
        recent_days,
        tm_id,
        tm_name,
        sum(order_count_1d) order_count,
        count(distinct user_id) order_user_count
    from dws_trade_user_sku_order_1d
    lateral view explode(array(7, 30)) tmp as recent_days
    where ds='{0}'  -- 改为 ds
    group by recent_days, tm_id, tm_name
    union all
    select
        '{0}' dt,
        recent_days,
        tm_id,
        tm_name,
        sum(order_count) order_count,
        count(distinct if(order_count > 0, user_id, null)) order_user_count
    from
    (
        select
            recent_days,
            tm_id,
            tm_name,
            case recent_days
                when 7 then order_count_7d
                when 30 then order_count_30d
            end order_count,
            user_id  -- 确保 user_id 被选入
        from dws_trade_user_sku_order_nd lateral view explode(array(7, 30)) tmp as recent_days
        where ds='{0}'  -- 改为 ds
    ) t1
    group by recent_days, tm_id, tm_name
    """.format(dt)
    df = spark.sql(sql)
    insert_to_hive(df, "ads_order_stats_by_tm", dt)


def etl_ads_order_stats_by_cate(spark, dt):
    sql = """
    select
        '{0}' dt,
        recent_days,
        category1_id,
        category1_name,
        category2_id,
        category2_name,
        category3_id,
        category3_name,
        sum(order_count_1d) order_count,
        count(distinct user_id) order_user_count
    from
    dws_trade_user_sku_order_1d
    lateral view explode(array(7, 30)) tmp as recent_days
    where ds='{0}'  -- 改为 ds
    group by recent_days, category1_id, category1_name, category2_id, category2_name, category3_id, category3_name
    union all
    select
        '{0}' dt,
        recent_days,
        category1_id,
        category1_name,
        category2_id,
        category2_name,
        category3_id,
        category3_name,
        sum(order_count) order_count,
        count(distinct if(order_count > 0, user_id, null)) order_user_count
    from
    (
        select
            recent_days,
            category1_id,
            category1_name,
            category2_id,
            category2_name,
            category3_id,
            category3_name,
            case recent_days
                when 7 then order_count_7d
                when 30 then order_count_30d
            end order_count,
            user_id  -- 确保包含 user_id 字段
        from dws_trade_user_sku_order_nd lateral view explode(array(7, 30)) tmp as recent_days
        where ds='{0}'  -- 改为 ds
    ) t1
    group by recent_days, category1_id, category1_name, category2_id, category2_name, category3_id, category3_name
    """.format(dt)
    df = spark.sql(sql)
    insert_to_hive(df, "ads_order_stats_by_cate", dt)


def etl_ads_sku_cart_num_top3_by_cate(spark, dt):
    sql = """
    select
        '{0}' dt,
        category1_id,
        category1_name,
        category2_id,
        category2_name,
        category3_id,
        category3_name,
        sku_id,
        sku_name,
        cart_num,
        rk
    from
    (
        select
            sku_id,
            sku_name,
            category1_id,
            category1_name,
            category2_id,
            category2_name,
            category3_id,
            category3_name,
            cart_num,
            rank() over (partition by category1_id, category2_id, category3_id order by cart_num desc) rk
        from
        (
            select
                sku_id,
                sum(sku_num) cart_num
            from dwd_trade_cart_full
            where ds='{0}'
            group by sku_id
        ) cart
        left join
        (
            select
                id,
                sku_name,
                category1_id,
                category1_name,
                category2_id,
                category2_name,
                category3_id,
                category3_name
            from dim_sku
            where ds='{0}'
        ) sku
        on cart.sku_id = sku.id
    ) t1
    where rk <= 3
    """.format(dt)
    df = spark.sql(sql)
    insert_to_hive(df, "ads_sku_cart_num_top3_by_cate", dt)


def etl_ads_sku_favor_count_top3_by_tm(spark, dt):
    sql = """
    select
        '{0}' dt,
        tm_id,
        tm_name,
        sku_id,
        sku_name,
        favor_add_count_1d,
        rk
    from
    (
        select
            tm_id,
            tm_name,
            sku_id,
            sku_name,
            favor_add_count_1d,
            rank() over (partition by tm_id order by favor_add_count_1d desc) rk
        from dws_interaction_sku_favor_add_1d
        where ds='{0}'
    ) t1
    where rk <= 3
    """.format(dt)
    df = spark.sql(sql)
    insert_to_hive(df, "ads_sku_favor_count_top3_by_tm", dt)


def etl_ads_order_to_pay_interval_avg(spark, dt):
    sql = """
    select
        '{0}' dt,
        cast(avg(to_unix_timestamp(payment_time) - to_unix_timestamp(order_time)) as bigint) order_to_pay_interval_avg
    from dwd_trade_trade_flow_acc
    where ds='{0}' and payment_date_id='{0}'
    """.format(dt)
    df = spark.sql(sql)
    insert_to_hive(df, "ads_order_to_pay_interval_avg", dt)


def etl_ads_coupon_stats(spark, dt):
    sql = """
    select
        '{0}' as dt,
        coupon_id,
        coupon_name,
        cast(sum(used_count_1d) as bigint),
        cast(count(*) as bigint)
    from gmall.dws_tool_user_coupon_coupon_used_1d
    where ds = '{0}'
    group by coupon_id, coupon_name
    """.format(dt)

    df = spark.sql(sql)
    insert_to_hive(df, "ads_coupon_stats", dt)


def main(dt):
    spark = get_spark_session()

    etl_ads_traffic_stats_by_channel(spark, dt)
    etl_ads_page_path(spark, dt)
    etl_ads_user_change(spark, dt)
    etl_ads_user_stats(spark, dt)
    etl_ads_user_action(spark, dt)
    etl_ads_order_continuously_user_count(spark, dt)
    etl_ads_repeat_purchase_by_tm(spark, dt)
    etl_ads_order_stats_by_tm(spark, dt)
    etl_ads_order_stats_by_cate(spark, dt)
    etl_ads_sku_cart_num_top3_by_cate(spark, dt)
    etl_ads_sku_favor_count_top3_by_tm(spark, dt)
    etl_ads_order_to_pay_interval_avg(spark, dt)
    etl_ads_coupon_stats(spark, dt)

    print("[SUCCESS] All ADS ETL completed for dt={}".format(dt))


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: spark-submit ads_etl.py <partition_date>")
        sys.exit(1)
    main(sys.argv[1])
