set hive.exec.mode.local.auto=true;
create database gmall;
use gmall;

CREATE TABLE IF NOT EXISTS ods_activity_info (
                                                 id INT,
                                                 activity_name STRING,
                                                 activity_type STRING,
                                                 activity_desc STRING,
                                                 start_time DATE,
                                                 end_time DATE,
                                                 create_time DATE,
                                                 operate_time DATE
)PARTITIONED BY (ds STRING)
    LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/gmall/ods/ods_activity_info'
    TBLPROPERTIES (
                      'orc.compress' = 'SNAPPY',
                      'external.table.purge' = 'true'
                  );

CREATE TABLE IF NOT EXISTS ods_activity_rule (
                                                 id INT,
                                                 activity_id INT,
                                                 activity_type STRING,
                                                 condition_amount DECIMAL(16,2),
    condition_num INT,
    benefit_amount DECIMAL(16,2),
    benefit_discount DECIMAL(10,2),
    benefit_level INT,
    create_time DATE,
    operate_time DATE
    ) PARTITIONED BY (ds STRING)
    LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/gmall/ods/ods_activity_rule'
    TBLPROPERTIES (
                      'orc.compress' = 'SNAPPY',
                      'external.table.purge' = 'true'
                  );

CREATE TABLE IF NOT EXISTS ods_activity_sku (
                                                id INT,
                                                activity_id INT,
                                                sku_id INT,
                                                create_time DATE,
                                                operate_time DATE
)PARTITIONED BY (ds STRING)
    LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/gmall/ods/ods_activity_sku'
    TBLPROPERTIES (
                      'orc.compress' = 'SNAPPY',
                      'external.table.purge' = 'true'
                  );

CREATE TABLE IF NOT EXISTS ods_base_attr_info (
                                                  id INT,
                                                  attr_name STRING,
                                                  category_id INT,
                                                  category_level INT,
                                                  create_time DATE,
                                                  operate_time DATE
)PARTITIONED BY (ds STRING)
    LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/gmall/ods/ods_base_attr_info'
    TBLPROPERTIES (
                      'orc.compress' = 'SNAPPY',
                      'external.table.purge' = 'true'
                  );

CREATE TABLE IF NOT EXISTS ods_base_attr_value (
                                                   id INT,
                                                   value_name STRING,
                                                   attr_id INT,
                                                   create_time DATE,
                                                   operate_time DATE
)PARTITIONED BY (ds STRING)
    LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/gmall/ods/ods_base_attr_value'
    TBLPROPERTIES (
                      'orc.compress' = 'SNAPPY',
                      'external.table.purge' = 'true'
                  );

CREATE TABLE IF NOT EXISTS ods_base_category1 (
                                                  id INT,
                                                  name STRING,
                                                  create_time DATE,
                                                  operate_time DATE
)PARTITIONED BY (ds STRING)
    LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/gmall/ods/ods_base_category1'
    TBLPROPERTIES (
                      'orc.compress' = 'SNAPPY',
                      'external.table.purge' = 'true'
                  );

CREATE TABLE IF NOT EXISTS ods_base_category2 (
                                                  id INT,
                                                  name STRING,
                                                  category1_id INT,
                                                  create_time DATE,
                                                  operate_time DATE
)PARTITIONED BY (ds STRING)
    LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/gmall/ods/ods_base_category2'
    TBLPROPERTIES (
                      'orc.compress' = 'SNAPPY',
                      'external.table.purge' = 'true'
                  );

CREATE TABLE IF NOT EXISTS ods_base_category3 (
                                                  id INT,
                                                  name STRING,
                                                  category2_id INT,
                                                  create_time DATE,
                                                  operate_time DATE
)PARTITIONED BY (ds STRING)
    LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/gmall/ods/ods_base_category3'
    TBLPROPERTIES (
                      'orc.compress' = 'SNAPPY',
                      'external.table.purge' = 'true'
                  );

CREATE TABLE IF NOT EXISTS ods_base_dic (
                                            dic_code STRING,
                                            dic_name STRING,
                                            parent_code STRING,
                                            create_time DATE,
                                            operate_time DATE
)PARTITIONED BY (ds STRING)
    LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/gmall/ods/ods_base_dic'
    TBLPROPERTIES (
                      'orc.compress' = 'SNAPPY',
                      'external.table.purge' = 'true'
                  );

CREATE TABLE IF NOT EXISTS ods_base_frontend_param (
                                                       id INT,
                                                       code STRING,
                                                       delete_id INT,
                                                       create_time DATE,
                                                       operate_time DATE
)PARTITIONED BY (ds STRING)
    LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/gmall/ods/ods_base_frontend_param'
    TBLPROPERTIES (
                      'orc.compress' = 'SNAPPY',
                      'external.table.purge' = 'true'
                  );

CREATE TABLE IF NOT EXISTS ods_base_province (
                                                 id INT,
                                                 name STRING,
                                                 region_id STRING,
                                                 area_code STRING,
                                                 iso_code STRING,
                                                 iso_3166_2 STRING,
                                                 create_time DATE,
                                                 operate_time DATE
)PARTITIONED BY (ds STRING)
    LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/gmall/ods/ods_base_province'
    TBLPROPERTIES (
                      'orc.compress' = 'SNAPPY',
                      'external.table.purge' = 'true'
                  );

CREATE TABLE IF NOT EXISTS ods_base_region (
                                               id STRING,
                                               region_name STRING,
                                               create_time DATE,
                                               operate_time DATE
)PARTITIONED BY (ds STRING)
    LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/gmall/ods/ods_base_region'
    TBLPROPERTIES (
                      'orc.compress' = 'SNAPPY',
                      'external.table.purge' = 'true'
                  );

CREATE TABLE IF NOT EXISTS ods_base_sale_attr (
                                                  id INT,
                                                  name STRING,
                                                  create_time DATE,
                                                  operate_time DATE
)PARTITIONED BY (ds STRING)
    LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/gmall/ods/ods_base_sale_attr'
    TBLPROPERTIES (
                      'orc.compress' = 'SNAPPY',
                      'external.table.purge' = 'true'
                  );

CREATE TABLE IF NOT EXISTS ods_base_trademark (
                                                  id INT,
                                                  tm_name STRING,
                                                  logo_url STRING,
                                                  create_time DATE,
                                                  operate_time DATE
)PARTITIONED BY (ds STRING)
    LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/gmall/ods/ods_base_trademark'
    TBLPROPERTIES (
                      'orc.compress' = 'SNAPPY',
                      'external.table.purge' = 'true'
                  );

CREATE TABLE IF NOT EXISTS ods_cart_info (
                                             id INT,
                                             user_id STRING,
                                             sku_id INT,
                                             cart_price DECIMAL(10,2),
    sku_num INT,
    img_url STRING,
    sku_name STRING,
    is_checked INT,
    create_time DATE,
    operate_time DATE,
    is_ordered INT,
    order_time DATE
    )PARTITIONED BY (ds STRING)
    LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/gmall/ods/ods_cart_info'
    TBLPROPERTIES (
                      'orc.compress' = 'SNAPPY',
                      'external.table.purge' = 'true'
                  );

CREATE TABLE IF NOT EXISTS ods_cms_banner (
                                              id INT,
                                              title STRING,
                                              image_url STRING,
                                              link_url STRING,
                                              sort INT,
                                              create_time DATE,
                                              operate_time DATE
)PARTITIONED BY (ds STRING)
    LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/gmall/ods/ods_cms_banner'
    TBLPROPERTIES (
                      'orc.compress' = 'SNAPPY',
                      'external.table.purge' = 'true'
                  );

CREATE TABLE IF NOT EXISTS ods_comment_info (
                                                id INT,
                                                user_id INT,
                                                nick_name STRING,
                                                head_img STRING,
                                                sku_id INT,
                                                spu_id INT,
                                                order_id INT,
                                                appraise STRING,
                                                comment_txt STRING,
                                                create_time DATE,
                                                operate_time DATE
)PARTITIONED BY (ds STRING)
    LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/gmall/ods/ods_comment_info'
    TBLPROPERTIES (
                      'orc.compress' = 'SNAPPY',
                      'external.table.purge' = 'true'
                  );

CREATE TABLE IF NOT EXISTS ods_coupon_info (
                                               id INT,
                                               coupon_name STRING,
                                               coupon_type STRING,
                                               condition_amount DECIMAL(10,2),
    condition_num INT,
    activity_id INT,
    benefit_amount DECIMAL(16,2),
    benefit_discount DECIMAL(10,2),
    create_time DATE,
    range_type STRING,
    limit_num INT,
    taken_count INT,
    start_time DATE,
    end_time DATE,
    operate_time DATE,
    expire_time DATE,
    range_desc STRING
    )PARTITIONED BY (ds STRING)
    LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/gmall/ods/ods_coupon_info'
    TBLPROPERTIES (
                      'orc.compress' = 'SNAPPY',
                      'external.table.purge' = 'true'
                  );

CREATE TABLE IF NOT EXISTS ods_coupon_range (
                                                id INT,
                                                coupon_id INT,
                                                range_type STRING,
                                                range_id INT,
                                                create_time DATE,
                                                operate_time DATE
)PARTITIONED BY (ds STRING)
    LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/gmall/ods/ods_coupon_range'
    TBLPROPERTIES (
                      'orc.compress' = 'SNAPPY',
                      'external.table.purge' = 'true'
                  );

CREATE TABLE IF NOT EXISTS ods_coupon_use (
                                              id INT,
                                              coupon_id INT,
                                              user_id INT,
                                              order_id INT,
                                              coupon_status STRING,
                                              get_time DATE,
                                              using_time DATE,
                                              used_time DATE,
                                              expire_time DATE,
                                              create_time DATE,
                                              operate_time DATE
)PARTITIONED BY (ds STRING)
    LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/gmall/ods/ods_coupon_use'
    TBLPROPERTIES (
                      'orc.compress' = 'SNAPPY',
                      'external.table.purge' = 'true'
                  );

CREATE TABLE IF NOT EXISTS ods_favor_info (
                                              id INT,
                                              user_id INT,
                                              sku_id INT,
                                              spu_id INT,
                                              is_cancel STRING,
                                              create_time DATE,
                                              operate_time DATE
)PARTITIONED BY (ds STRING)
    LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/gmall/ods/ods_favor_info'
    TBLPROPERTIES (
                      'orc.compress' = 'SNAPPY',
                      'external.table.purge' = 'true'
                  );

CREATE TABLE IF NOT EXISTS ods_financial_sku_cost (
                                                      id STRING,
                                                      sku_id INT,
                                                      sku_name STRING,
                                                      busi_date STRING,
                                                      is_lastest STRING,
                                                      sku_cost DECIMAL(16,2),
    create_time DATE
    )PARTITIONED BY (ds STRING)
    LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/gmall/ods/ods_financial_sku_cost'
    TBLPROPERTIES (
                      'orc.compress' = 'SNAPPY',
                      'external.table.purge' = 'true'
                  );

CREATE TABLE IF NOT EXISTS ods_order_detail (
                                                id INT,
                                                order_id INT,
                                                sku_id INT,
                                                sku_name STRING,
                                                img_url STRING,
                                                order_price DECIMAL(10,2),
    sku_num INT,
    create_time DATE,
    split_total_amount DECIMAL(16,2),
    split_activity_amount DECIMAL(16,2),
    split_coupon_amount DECIMAL(16,2),
    operate_time DATE
    )PARTITIONED BY (ds STRING)
    LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/gmall/ods/ods_order_detail'
    TBLPROPERTIES (
                      'orc.compress' = 'SNAPPY',
                      'external.table.purge' = 'true'
                  );

CREATE TABLE IF NOT EXISTS ods_order_detail_activity (
                                                         id INT,
                                                         order_id INT,
                                                         order_detail_id INT,
                                                         activity_id INT,
                                                         activity_rule_id INT,
                                                         sku_id INT,
                                                         create_time DATE,
                                                         operate_time DATE
)PARTITIONED BY (ds STRING)
    LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/gmall/ods/ods_order_detail_activity'
    TBLPROPERTIES (
                      'orc.compress' = 'SNAPPY',
                      'external.table.purge' = 'true'
                  );

CREATE TABLE IF NOT EXISTS ods_order_detail_coupon (
                                                       id INT,
                                                       order_id INT,
                                                       order_detail_id INT,
                                                       coupon_id INT,
                                                       coupon_use_id INT,
                                                       sku_id INT,
                                                       create_time DATE,
                                                       operate_time DATE
)PARTITIONED BY (ds STRING)
    LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/gmall/ods/ods_order_detail_coupon'
    TBLPROPERTIES (
                      'orc.compress' = 'SNAPPY',
                      'external.table.purge' = 'true'
                  );

CREATE TABLE IF NOT EXISTS ods_order_info (
                                              id INT,
                                              consignee STRING,
                                              consignee_tel STRING,
                                              total_amount DECIMAL(10,2),
    order_status STRING,
    user_id INT,
    payment_way STRING,
    delivery_address STRING,
    order_comment STRING,
    out_trade_no STRING,
    trade_body STRING,
    create_time DATE,
    operate_time DATE,
    expire_time DATE,
    process_status STRING,
    tracking_no STRING,
    parent_order_id INT,
    img_url STRING,
    province_id INT,
    activity_reduce_amount DECIMAL(16,2),
    coupon_reduce_amount DECIMAL(16,2),
    original_total_amount DECIMAL(16,2),
    feight_fee DECIMAL(16,2),
    feight_fee_reduce DECIMAL(16,2),
    refundable_time DATE
    )PARTITIONED BY (ds STRING)
    LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/gmall/ods/ods_order_info'
    TBLPROPERTIES (
                      'orc.compress' = 'SNAPPY',
                      'external.table.purge' = 'true'
                  );

CREATE TABLE IF NOT EXISTS ods_order_refund_info (
                                                     id INT,
                                                     user_id INT,
                                                     order_id INT,
                                                     sku_id INT,
                                                     refund_type STRING,
                                                     refund_num INT,
                                                     refund_amount DECIMAL(16,2),
    refund_reason_type STRING,
    refund_reason_txt STRING,
    refund_status STRING,
    create_time DATE,
    operate_time DATE
    )PARTITIONED BY (ds STRING)
    LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/gmall/ods/ods_order_refund_info'
    TBLPROPERTIES (
                      'orc.compress' = 'SNAPPY',
                      'external.table.purge' = 'true'
                  );

CREATE TABLE IF NOT EXISTS ods_order_status_log (
                                                    id INT,
                                                    order_id INT,
                                                    order_status STRING,
                                                    create_time DATE,
                                                    operate_time DATE
)PARTITIONED BY (ds STRING)
    LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/gmall/ods/ods_order_status_log'
    TBLPROPERTIES (
                      'orc.compress' = 'SNAPPY',
                      'external.table.purge' = 'true'
                  );

CREATE TABLE IF NOT EXISTS ods_payment_info (
                                                id INT,
                                                out_trade_no STRING,
                                                order_id INT,
                                                user_id INT,
                                                payment_type STRING,
                                                trade_no STRING,
                                                total_amount DECIMAL(10,2),
    subject STRING,
    payment_status STRING,
    create_time DATE,
    callback_time DATE,
    callback_content STRING,
    operate_time DATE
    )PARTITIONED BY (ds STRING)
    LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/gmall/ods/ods_payment_info'
    TBLPROPERTIES (
                      'orc.compress' = 'SNAPPY',
                      'external.table.purge' = 'true'
                  );

CREATE TABLE IF NOT EXISTS ods_promotion_pos (
                                                 id INT,
                                                 pos_location STRING,
                                                 pos_type STRING,
                                                 promotion_type STRING,
                                                 create_time DATE,
                                                 operate_time DATE
)PARTITIONED BY (ds STRING)
    LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/gmall/ods/ods_promotion_pos'
    TBLPROPERTIES (
                      'orc.compress' = 'SNAPPY',
                      'external.table.purge' = 'true'
                  );


CREATE TABLE IF NOT EXISTS ods_promotion_refer (
                                                   id INT,
                                                   refer_name STRING,
                                                   create_time DATE,
                                                   operate_time DATE
)PARTITIONED BY (ds STRING)
    LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/gmall/ods/ods_promotion_refer'
    TBLPROPERTIES (
                      'orc.compress' = 'SNAPPY',
                      'external.table.purge' = 'true'
                  );


CREATE TABLE IF NOT EXISTS ods_refund_payment (
                                                  id INT,
                                                  out_trade_no STRING,
                                                  order_id INT,
                                                  sku_id INT,
                                                  payment_type STRING,
                                                  trade_no STRING,
                                                  total_amount DECIMAL(10,2),
    subject STRING,
    refund_status STRING,
    create_time DATE,
    callback_time DATE,
    callback_content STRING,
    operate_time DATE
    )PARTITIONED BY (ds STRING)
    LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/gmall/ods/ods_refund_payment'
    TBLPROPERTIES (
                      'orc.compress' = 'SNAPPY',
                      'external.table.purge' = 'true'
                  );

CREATE TABLE IF NOT EXISTS ods_seckill_goods (
                                                 id INT,
                                                 spu_id INT,
                                                 sku_id INT,
                                                 sku_name STRING,
                                                 sku_default_img STRING,
                                                 price DECIMAL(10,2),
    cost_price DECIMAL(10,2),
    create_time DATE,
    check_time DATE,
    status STRING,
    start_time DATE,
    end_time DATE,
    num INT,
    stock_count INT,
    sku_desc STRING
    )PARTITIONED BY (ds STRING)
    LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/gmall/ods/ods_seckill_goods'
    TBLPROPERTIES (
                      'orc.compress' = 'SNAPPY',
                      'external.table.purge' = 'true'
                  );

CREATE TABLE IF NOT EXISTS ods_sku_attr_value (
                                                  id INT,
                                                  attr_id INT,
                                                  value_id INT,
                                                  sku_id INT,
                                                  attr_name STRING,
                                                  value_name STRING,
                                                  create_time DATE,
                                                  operate_time DATE
)PARTITIONED BY (ds STRING)
    LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/gmall/ods/ods_sku_attr_value'
    TBLPROPERTIES (
                      'orc.compress' = 'SNAPPY',
                      'external.table.purge' = 'true'
                  );

CREATE TABLE IF NOT EXISTS ods_sku_image (
                                             id INT,
                                             sku_id INT,
                                             img_name STRING,
                                             img_url STRING,
                                             spu_img_id INT,
                                             is_default STRING,
                                             create_time DATE,
                                             operate_time DATE
)PARTITIONED BY (ds STRING)
    LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/gmall/ods/ods_sku_image'
    TBLPROPERTIES (
                      'orc.compress' = 'SNAPPY',
                      'external.table.purge' = 'true'
                  );

CREATE TABLE IF NOT EXISTS ods_sku_info (
                                            id INT,
                                            spu_id INT,
                                            price DECIMAL(10,0),
    sku_name STRING,
    sku_desc STRING,
    weight DECIMAL(10,2),
    tm_id INT,
    category3_id INT,
    sku_default_img STRING,
    is_sale TINYINT,
    create_time DATE,
    operate_time DATE
    )PARTITIONED BY (ds STRING)
    LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/gmall/ods/ods_sku_info'
    TBLPROPERTIES (
                      'orc.compress' = 'SNAPPY',
                      'external.table.purge' = 'true'
                  );

CREATE TABLE IF NOT EXISTS ods_sku_sale_attr_value (
                                                       id INT,
                                                       sku_id INT,
                                                       spu_id INT,
                                                       sale_attr_value_id INT,
                                                       sale_attr_id INT,
                                                       sale_attr_name STRING,
                                                       sale_attr_value_name STRING,
                                                       create_time DATE,
                                                       operate_time DATE
)PARTITIONED BY (ds STRING)
    LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/gmall/ods/ods_sku_sale_attr_value'
    TBLPROPERTIES (
                      'orc.compress' = 'SNAPPY',
                      'external.table.purge' = 'true'
                  );

CREATE TABLE IF NOT EXISTS ods_spu_image (
                                             id INT,
                                             spu_id INT,
                                             img_name STRING,
                                             img_url STRING,
                                             create_time DATE,
                                             operate_time DATE
)PARTITIONED BY (ds STRING)
    LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/gmall/ods/ods_spu_image'
    TBLPROPERTIES (
                      'orc.compress' = 'SNAPPY',
                      'external.table.purge' = 'true'
                  );

CREATE TABLE IF NOT EXISTS ods_spu_info (
                                            id INT,
                                            spu_name STRING,
                                            description STRING,
                                            category3_id INT,
                                            tm_id INT,
                                            create_time DATE,
                                            operate_time DATE
)PARTITIONED BY (ds STRING)
    LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/gmall/ods/ods_spu_info'
    TBLPROPERTIES (
                      'orc.compress' = 'SNAPPY',
                      'external.table.purge' = 'true'
                  );

CREATE TABLE IF NOT EXISTS ods_spu_poster (
                                              id INT,
                                              spu_id INT,
                                              img_name STRING,
                                              img_url STRING,
                                              create_time DATE,
                                              operate_time DATE,
                                              is_deleted TINYINT
)PARTITIONED BY (ds STRING)
    LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/gmall/ods/ods_spu_poster'
    TBLPROPERTIES (
                      'orc.compress' = 'SNAPPY',
                      'external.table.purge' = 'true'
                  );

CREATE TABLE IF NOT EXISTS ods_spu_sale_attr (
                                                 id INT,
                                                 spu_id INT,
                                                 base_sale_attr_id INT,
                                                 sale_attr_name STRING,
                                                 create_time DATE,
                                                 operate_time DATE
)PARTITIONED BY (ds STRING)
    LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/gmall/ods/ods_spu_sale_attr'
    TBLPROPERTIES (
                      'orc.compress' = 'SNAPPY',
                      'external.table.purge' = 'true'
                  );

CREATE TABLE IF NOT EXISTS ods_spu_sale_attr_value (
                                                       id INT,
                                                       spu_id INT,
                                                       base_sale_attr_id INT,
                                                       sale_attr_value_name STRING,
                                                       sale_attr_name STRING,
                                                       create_time DATE,
                                                       operate_time DATE
)PARTITIONED BY (ds STRING)
    LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/gmall/ods/ods_spu_sale_attr_value'
    TBLPROPERTIES (
                      'orc.compress' = 'SNAPPY',
                      'external.table.purge' = 'true'
                  );

CREATE TABLE IF NOT EXISTS ods_user_address (
                                                id INT,
                                                user_id INT,
                                                province_id INT,
                                                user_address STRING,
                                                consignee STRING,
                                                phone_num STRING,
                                                is_default STRING,
                                                create_time DATE,
                                                operate_time DATE
)PARTITIONED BY (ds STRING)
    LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/gmall/ods/ods_user_address'
    TBLPROPERTIES (
                      'orc.compress' = 'SNAPPY',
                      'external.table.purge' = 'true'
                  );

CREATE TABLE IF NOT EXISTS ods_user_info (
                                             id INT,
                                             login_name STRING,
                                             nick_name STRING,
                                             passwd STRING,
                                             name STRING,
                                             phone_num STRING,
                                             email STRING,
                                             head_img STRING,
                                             user_level STRING,
                                             birthday DATE,
                                             gender STRING,
                                             create_time DATE,
                                             operate_time DATE,
                                             status STRING
)PARTITIONED BY (ds STRING)
    LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/gmall/ods/ods_user_info'
    TBLPROPERTIES (
                      'orc.compress' = 'SNAPPY',
                      'external.table.purge' = 'true'
                  );

CREATE TABLE IF NOT EXISTS ods_ware_info (
                                             id INT,
                                             name STRING,
                                             address STRING,
                                             areacode STRING
)PARTITIONED BY (ds STRING)
    LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/gmall/ods/ods_ware_info'
    TBLPROPERTIES (
                      'orc.compress' = 'SNAPPY',
                      'external.table.purge' = 'true'
                  );

CREATE TABLE IF NOT EXISTS ods_ware_order_task (
                                                   id INT,
                                                   order_id INT,
                                                   consignee STRING,
                                                   consignee_tel STRING,
                                                   delivery_address STRING,
                                                   order_comment STRING,
                                                   payment_way STRING,
                                                   task_status STRING,
                                                   order_body STRING,
                                                   tracking_no STRING,
                                                   create_time DATE,
                                                   ware_id INT,
                                                   task_comment STRING
)PARTITIONED BY (ds STRING)
    LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/gmall/ods/ods_ware_order_task'
    TBLPROPERTIES (
                      'orc.compress' = 'SNAPPY',
                      'external.table.purge' = 'true'
                  );

CREATE TABLE IF NOT EXISTS ods_ware_order_task_detail (
                                                          id INT,
                                                          sku_id INT,
                                                          sku_name STRING,
                                                          sku_num INT,
                                                          task_id INT,
                                                          refund_status STRING
)PARTITIONED BY (ds STRING)
    LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/gmall/ods/ods_ware_order_task_detail'
    TBLPROPERTIES (
                      'orc.compress' = 'SNAPPY',
                      'external.table.purge' = 'true'
                  );

CREATE TABLE IF NOT EXISTS ods_ware_sku (
                                            id INT,
                                            sku_id INT,
                                            warehouse_id INT,
                                            stock INT,
                                            stock_name STRING,
                                            stock_locked INT
)PARTITIONED BY (ds STRING)
    LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/gmall/ods/ods_ware_sku'
    TBLPROPERTIES (
                      'orc.compress' = 'SNAPPY',
                      'external.table.purge' = 'true'
                  );

CREATE TABLE IF NOT EXISTS ods_z_log (
                                         id INT,
                                         log STRING
)PARTITIONED BY (ds STRING)
    LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/gmall/ods/ods_z_log'
    TBLPROPERTIES (
                      'orc.compress' = 'SNAPPY',
                      'external.table.purge' = 'true'
                  );