set hive.exec.mode.local.auto=true;
use gmall;

DROP TABLE IF EXISTS dwd_trade_cart_add_inc;
CREATE EXTERNAL TABLE dwd_trade_cart_add_inc
(
    `id`                  STRING COMMENT '编号',
    `user_id`            STRING COMMENT '用户ID',
    `sku_id`             STRING COMMENT 'SKU_ID',
    `date_id`            STRING COMMENT '日期ID',
    `create_time`        STRING COMMENT '加购时间',
    `sku_num`            BIGINT COMMENT '加购物车件数'
) COMMENT '交易域加购事务事实表'
    PARTITIONED BY (`ds` STRING)
    STORED AS ORC
    LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/gmall/dwd/dwd_trade_cart_add_inc/'
    TBLPROPERTIES ('orc.compress' = 'snappy');
set hive.exec.dynamic.partition.mode=nonstrict;
insert into table dwd_trade_cart_add_inc partition (ds)
select
    data.id,
    data.user_id,
    data.sku_id,
    date_format(create_time, 'yyyy-MM-dd') date_id,
    data.create_time,
    data.sku_num,
    '20250629' as ds
from ods_cart_info  data
;

DROP TABLE IF EXISTS dwd_trade_order_detail_inc;
CREATE EXTERNAL TABLE dwd_trade_order_detail_inc
(
    `id`                     STRING COMMENT '编号',
    `order_id`              STRING COMMENT '订单ID',
    `user_id`               STRING COMMENT '用户ID',
    `sku_id`                STRING COMMENT '商品ID',
    `province_id`          STRING COMMENT '省份ID',
    `activity_id`          STRING COMMENT '参与活动ID',
    `activity_rule_id`    STRING COMMENT '参与活动规则ID',
    `coupon_id`             STRING COMMENT '使用优惠券ID',
    `date_id`               STRING COMMENT '下单日期ID',
    `create_time`           STRING COMMENT '下单时间',
    `sku_num`                BIGINT COMMENT '商品数量',
    `split_original_amount` DECIMAL(16, 2) COMMENT '原始价格',
    `split_activity_amount` DECIMAL(16, 2) COMMENT '活动优惠分摊',
    `split_coupon_amount`   DECIMAL(16, 2) COMMENT '优惠券优惠分摊',
    `split_total_amount`    DECIMAL(16, 2) COMMENT '最终价格分摊'
) COMMENT '交易域下单事务事实表'
    PARTITIONED BY (`ds` STRING)
    STORED AS ORC
    LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/gmall/dwd/dwd_trade_order_detail_inc/'
    TBLPROPERTIES ('orc.compress' = 'snappy');
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd_trade_order_detail_inc partition (ds)
select
    od.id,
    order_id,
    user_id,
    sku_id,
    province_id,
    activity_id,
    activity_rule_id,
    coupon_id,
    date_format(create_time, 'yyyy-MM-dd') date_id,
    null as create_time,
    sku_num,
    split_original_amount,
    nvl(split_activity_amount,0.0),
    nvl(split_coupon_amount,0.0),
    split_total_amount,
    '20250629' as ds
from
    (
        select
            data.id,
            data.order_id,
            data.sku_id,
            data.create_time,
            data.sku_num,
            data.sku_num * data.order_price split_original_amount,
            data.split_total_amount,
            data.split_activity_amount,
            data.split_coupon_amount
        from ods_order_detail data
        where ds = '20250629'
    ) od
        left join
    (
        select
            data.id,
            data.user_id,
            data.province_id
        from ods_order_info data
        where ds = '20250629'
    ) oi
    on od.order_id = oi.id
        left join
    (
        select
            data.order_detail_id,
            data.activity_id,
            data.activity_rule_id
        from ods_order_detail_activity data
        where ds = '20250629'
    ) act
    on od.id = act.order_detail_id
        left join
    (
        select
            data.order_detail_id,
            data.coupon_id
        from ods_order_detail_coupon data
        where ds = '20250629'
    ) cou
    on od.id = cou.order_detail_id;

DROP TABLE IF EXISTS dwd_trade_pay_detail_suc_inc;
CREATE EXTERNAL TABLE dwd_trade_pay_detail_suc_inc
(
    `id`                      STRING COMMENT '编号',
    `order_id`               STRING COMMENT '订单ID',
    `user_id`                STRING COMMENT '用户ID',
    `sku_id`                 STRING COMMENT 'SKU_ID',
    `province_id`           STRING COMMENT '省份ID',
    `activity_id`           STRING COMMENT '参与活动ID',
    `activity_rule_id`     STRING COMMENT '参与活动规则ID',
    `coupon_id`              STRING COMMENT '使用优惠券ID',
    `payment_type_code`     STRING COMMENT '支付类型编码',
    `payment_type_name`     STRING COMMENT '支付类型名称',
    `date_id`                STRING COMMENT '支付日期ID',
    `callback_time`         STRING COMMENT '支付成功时间',
    `sku_num`                 BIGINT COMMENT '商品数量',
    `split_original_amount` DECIMAL(16, 2) COMMENT '应支付原始金额',
    `split_activity_amount` DECIMAL(16, 2) COMMENT '支付活动优惠分摊',
    `split_coupon_amount`   DECIMAL(16, 2) COMMENT '支付优惠券优惠分摊',
    `split_payment_amount`  DECIMAL(16, 2) COMMENT '支付金额'
) COMMENT '交易域支付成功事务事实表'
    PARTITIONED BY (`ds` STRING)
    STORED AS ORC
    LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/gmall/dwd/dwd_trade_pay_detail_suc_inc/'
    TBLPROPERTIES ('orc.compress' = 'snappy');
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd_trade_pay_detail_suc_inc partition (ds)
select
    od.id,
    od.order_id,
    user_id,
    sku_id,
    province_id,
    activity_id,
    activity_rule_id,
    coupon_id,
    payment_type,
    pay_dic.dic_name,
    date_format(callback_time, 'yyyy-MM-dd') date_id,
    callback_time,
    sku_num,
    split_original_amount,
    nvl(split_activity_amount,0.0),
    nvl(split_coupon_amount,0.0),
    split_total_amount,
    '20250629'
from
    (
        select
            data.id,
            data.order_id,
            data.sku_id,
            data.sku_num,
            data.sku_num * data.order_price split_original_amount,
            data.split_total_amount,
            data.split_activity_amount,
            data.split_coupon_amount
        from ods_order_detail data
        where ds = '20250629'
    ) od
        join
    (
        select
            data.user_id,
            data.order_id,
            data.payment_type,
            data.callback_time
        from ods_payment_info data
        where ds='20250629'
    ) pi
    on od.order_id=pi.order_id
        left join
    (
        select
            data.id,
            data.province_id
        from ods_order_info data
        where ds = '20250629'
    ) oi
    on od.order_id = oi.id
        left join
    (
        select
            data.order_detail_id,
            data.activity_id,
            data.activity_rule_id
        from ods_order_detail_activity data
        where ds = '20250629'
    ) act
    on od.id = act.order_detail_id
        left join
    (
        select
            data.order_detail_id,
            data.coupon_id
        from ods_order_detail_coupon data
        where ds = '20250629'
    ) cou
    on od.id = cou.order_detail_id
        left join
    (
        select
            dic_code,
            dic_name
        from ods_base_dic
        where ds='20250629'
          and parent_code='11'
    ) pay_dic
    on pi.payment_type=pay_dic.dic_code;

insert overwrite table dwd_trade_pay_detail_suc_inc partition (ds='20250629')
select
    od.id,
    od.order_id,
    user_id,
    sku_id,
    province_id,
    activity_id,
    activity_rule_id,
    coupon_id,
    payment_type,
    pay_dic.dic_name,
    date_format(callback_time, 'yyyy-MM-dd') date_id,
    callback_time,
    sku_num,
    split_original_amount,
    nvl(split_activity_amount,0.0),
    nvl(split_coupon_amount,0.0),
    split_total_amount
from
    (
        select
            data.id,
            data.order_id,
            data.sku_id,
            data.sku_num,
            data.sku_num * data.order_price split_original_amount,
            data.split_total_amount,
            data.split_activity_amount,
            data.split_coupon_amount
        from ods_order_detail data
        where (ds = '20250629' or ds = date_add('20250629',-1))
    ) od
        join
    (
        select
            data.user_id,
            data.order_id,
            data.payment_type,
            data.callback_time
        from ods_payment_info data
        where ds='20250629'
    ) pi
    on od.order_id=pi.order_id
        left join
    (
        select
            data.id,
            data.province_id
        from ods_order_info data
        where (ds = '20250629' or ds = date_add('20250629',-1))
    ) oi
    on od.order_id = oi.id
        left join
    (
        select
            data.order_detail_id,
            data.activity_id,
            data.activity_rule_id
        from ods_order_detail_activity data
        where (ds = '20250629' or ds = date_add('20250629',-1))
    ) act
    on od.id = act.order_detail_id
        left join
    (
        select
            data.order_detail_id,
            data.coupon_id
        from ods_order_detail_coupon data
        where (ds = '20250629' or ds = date_add('20250629',-1))
    ) cou
    on od.id = cou.order_detail_id
        left join
    (
        select
            dic_code,
            dic_name
        from ods_base_dic
        where ds='20250629'
          and parent_code='11'
    ) pay_dic
    on pi.payment_type=pay_dic.dic_code;


DROP TABLE IF EXISTS dwd_trade_cart_full;
CREATE EXTERNAL TABLE dwd_trade_cart_full
(
    `id`         STRING COMMENT '编号',
    `user_id`   STRING COMMENT '用户ID',
    `sku_id`    STRING COMMENT 'SKU_ID',
    `sku_name`  STRING COMMENT '商品名称',
    `sku_num`   BIGINT COMMENT '现存商品件数'
) COMMENT '交易域购物车周期快照事实表'
    PARTITIONED BY (`ds` STRING)
    STORED AS ORC
    LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/gmall/dwd/dwd_trade_cart_full/'
    TBLPROPERTIES ('orc.compress' = 'snappy');
insert overwrite table dwd_trade_cart_full partition(ds='20250629')
select
    id,
    user_id,
    sku_id,
    sku_name,
    sku_num
from ods_cart_info
where ds='20250629'
  and is_ordered='0';

insert into table dwd_trade_cart_full partition(ds='20250626')
select
    id,
    user_id,
    sku_id,
    sku_name,
    sku_num
from ods_cart_info
where is_ordered='0';

DROP TABLE IF EXISTS dwd_trade_trade_flow_acc;
CREATE EXTERNAL TABLE dwd_trade_trade_flow_acc
(
    `order_id`               STRING COMMENT '订单ID',
    `user_id`                STRING COMMENT '用户ID',
    `province_id`           STRING COMMENT '省份ID',
    `order_date_id`         STRING COMMENT '下单日期ID',
    `order_time`             STRING COMMENT '下单时间',
    `payment_date_id`        STRING COMMENT '支付日期ID',
    `payment_time`           STRING COMMENT '支付时间',
    `finish_date_id`         STRING COMMENT '确认收货日期ID',
    `finish_time`             STRING COMMENT '确认收货时间',
    `order_original_amount` DECIMAL(16, 2) COMMENT '下单原始价格',
    `order_activity_amount` DECIMAL(16, 2) COMMENT '下单活动优惠分摊',
    `order_coupon_amount`   DECIMAL(16, 2) COMMENT '下单优惠券优惠分摊',
    `order_total_amount`    DECIMAL(16, 2) COMMENT '下单最终价格分摊',
    `payment_amount`         DECIMAL(16, 2) COMMENT '支付金额'
) COMMENT '交易域交易流程累积快照事实表'
    PARTITIONED BY (`ds` STRING)
    STORED AS ORC
    LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/gmall/dwd/dwd_trade_trade_flow_acc/'
    TBLPROPERTIES ('orc.compress' = 'snappy');
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd_trade_trade_flow_acc partition(ds)
select
    oi.id,
    user_id,
    province_id,
    date_format(create_time,'yyyy-MM-dd'),
    create_time,
    date_format(callback_time,'yyyy-MM-dd'),
    callback_time,
    date_format(finish_time,'yyyy-MM-dd'),
    finish_time,
    original_total_amount,
    activity_reduce_amount,
    coupon_reduce_amount,
    total_amount,
    nvl(payment_amount,0.0),
    '20250629'
from
    (
        select
            data.id,
            data.user_id,
            data.province_id,
            data.create_time,
            data.original_total_amount,
            data.activity_reduce_amount,
            data.coupon_reduce_amount,
            data.total_amount
        from ods_order_info data
        where ds='20250629'
    )oi
        left join
    (
        select
            data.order_id,
            data.callback_time,
            data.total_amount payment_amount
        from ods_payment_info data
        where ds='20250629'

    )pi
    on oi.id=pi.order_id
        left join
    (
        select
            data.order_id,
            data.operate_time finish_time
        from ods_order_status_log data
        where ds='20250629'
          and data.order_status='1004'
    )log
    on oi.id=log.order_id;

DROP TABLE IF EXISTS dwd_tool_coupon_used_inc;
CREATE EXTERNAL TABLE dwd_tool_coupon_used_inc
(
    `id`           STRING COMMENT '编号',
    `coupon_id`    STRING COMMENT '优惠券ID',
    `user_id`      STRING COMMENT '用户ID',
    `order_id`     STRING COMMENT '订单ID',
    `date_id`      STRING COMMENT '日期ID',
    `payment_time` STRING COMMENT '使用(支付)时间'
) COMMENT '优惠券使用（支付）事务事实表'
    PARTITIONED BY (`ds` STRING)
    STORED AS ORC
    LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/gmall/dwd/dwd_tool_coupon_used_inc/'
    TBLPROPERTIES ("orc.compress" = "snappy");
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd_tool_coupon_used_inc partition(ds)
select
    data.id,
    data.coupon_id,
    data.user_id,
    data.order_id,
    date_format(get_time, 'yyyy-MM-dd') date_id,
    data.used_time,
    date_format(get_time, 'yyyy-MM-dd')
from ods_coupon_use data
where ds='20250629';

insert overwrite table dwd_tool_coupon_used_inc partition(ds='20250629')
select
    data.id,
    data.coupon_id,
    data.user_id,
    data.order_id,
    date_format(get_time, 'yyyy-MM-dd') date_id,
    data.used_time
from ods_coupon_use data
where ds='20250629';

insert into table dwd_tool_coupon_used_inc partition(ds='20250626')
select
    data.id,
    data.coupon_id,
    data.user_id,
    data.order_id,
    date_format(get_time, 'yyyy-MM-dd') date_id,
    data.used_time
from ods_coupon_use data
;

DROP TABLE IF EXISTS dwd_interaction_favor_add_inc;
CREATE EXTERNAL TABLE dwd_interaction_favor_add_inc
(
    `id`          STRING COMMENT '编号',
    `user_id`     STRING COMMENT '用户ID',
    `sku_id`      STRING COMMENT 'SKU_ID',
    `date_id`     STRING COMMENT '日期ID',
    `create_time` STRING COMMENT '收藏时间'
) COMMENT '互动域收藏商品事务事实表'
    PARTITIONED BY (`ds` STRING)
    STORED AS ORC
    LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/gmall/dwd/dwd_interaction_favor_add_inc/'
    TBLPROPERTIES ("orc.compress" = "snappy");
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd_interaction_favor_add_inc partition(ds)
select
    data.id,
    data.user_id,
    data.sku_id,
    date_format(create_time, 'yyyy-MM-dd') date_id,
    data.create_time,
    '20250629'
from ods_favor_info data
where ds='20250629';

insert overwrite table dwd_interaction_favor_add_inc partition(ds='20250629')
select
    data.id,
    data.user_id,
    data.sku_id,
    date_format(data.create_time,'yyyy-MM-dd') date_id,
    data.create_time
from ods_favor_info data
where ds='20250629';

DROP TABLE IF EXISTS dwd_traffic_page_view_inc;
CREATE EXTERNAL TABLE dwd_traffic_page_view_inc
(
    `province_id`    STRING COMMENT '省份ID',
    `brand`           STRING COMMENT '手机品牌',
    `channel`         STRING COMMENT '渠道',
    `is_new`          STRING COMMENT '是否首次启动',
    `model`           STRING COMMENT '手机型号',
    `mid_id`          STRING COMMENT '设备ID',
    `operate_system` STRING COMMENT '操作系统',
    `user_id`         STRING COMMENT '会员ID',
    `version_code`   STRING COMMENT 'APP版本号',
    `page_item`       STRING COMMENT '目标ID',
    `page_item_type` STRING COMMENT '目标类型',
    `last_page_id`    STRING COMMENT '上页ID',
    `page_id`          STRING COMMENT '页面ID ',
    `from_pos_id`     STRING COMMENT '点击坑位ID',
    `from_pos_seq`    STRING COMMENT '点击坑位位置',
    `refer_id`         STRING COMMENT '营销渠道ID',
    `date_id`          STRING COMMENT '日期ID',
    `view_time`       STRING COMMENT '跳入时间',
    `session_id`      STRING COMMENT '所属会话ID',
    `during_time`     BIGINT COMMENT '持续时间毫秒'
) COMMENT '流量域页面浏览事务事实表'
    PARTITIONED BY (`ds` STRING)
    STORED AS ORC
    LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/gmall/dwd/dwd_traffic_page_view_inc'
    TBLPROPERTIES ('orc.compress' = 'snappy');
insert into table dwd_traffic_page_view_inc partition(ds='20250629')

SELECT
    get_json_object(log, '$.common.ar') AS province_id,
    get_json_object(log, '$.common.ba') AS brand,
    get_json_object(log, '$.common.ch') AS channel,
    get_json_object(log, '$.common.is_new') AS is_new,
    get_json_object(log, '$.common.md') AS model,
    get_json_object(log, '$.common.mid') AS mid_id,
    get_json_object(log, '$.common.os') AS operate_system,
    get_json_object(log, '$.common.uid') AS user_id,
    get_json_object(log, '$.common.vc') AS version_code,
    get_json_object(log, '$.page.item') AS page_item,
    get_json_object(log, '$.page.item_type') AS page_item_type,
    get_json_object(log, '$.page.last_page_id') AS last_page_id,
    get_json_object(log, '$.page.page_id') AS page_id,
    get_json_object(log, '$.page.from_pos_id') AS from_pos_id,
    get_json_object(log, '$.page.from_pos_seq') AS from_pos_seq,
    get_json_object(log, '$.page.refer_id') AS refer_id,
    date_format(from_utc_timestamp(get_json_object(log, '$.ts'), 'GMT+8'), 'yyyy-MM-dd') AS date_id,
    date_format(from_utc_timestamp(get_json_object(log, '$.ts'), 'GMT+8'), 'yyyy-MM-dd HH:mm:ss') AS view_time,
    get_json_object(log, '$.common.sid') AS session_id,
    get_json_object(log, '$.page.during_time') AS during_time
FROM ods_z_log
WHERE ds = '20250629'
  AND get_json_object(log, '$.page') IS NOT NULL;



DROP TABLE IF EXISTS dwd_user_register_inc;
CREATE EXTERNAL TABLE dwd_user_register_inc
(
    `user_id`          STRING COMMENT '用户ID',
    `date_id`          STRING COMMENT '日期ID',
    `create_time`     STRING COMMENT '注册时间',
    `channel`          STRING COMMENT '应用下载渠道',
    `province_id`     STRING COMMENT '省份ID',
    `version_code`    STRING COMMENT '应用版本',
    `mid_id`           STRING COMMENT '设备ID',
    `brand`            STRING COMMENT '设备品牌',
    `model`            STRING COMMENT '设备型号',
    `operate_system` STRING COMMENT '设备操作系统'
) COMMENT '用户域用户注册事务事实表'
    PARTITIONED BY (`ds` STRING)
    STORED AS ORC
    LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/gmall/dwd/dwd_user_register_inc/'
    TBLPROPERTIES ("orc.compress" = "snappy");

set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite  table dwd_user_register_inc partition (ds)
select
    ui.user_id,
    date_format(create_time,'yyyy-MM-dd') date_id,
    create_time,
    channel,
    province_id,
    version_code,
    mid_id,
    brand,
    model,
    operate_system,
    date_format(create_time,'yyyy-MM-dd')
from
    (
        select
            id user_id,
            create_time
        from ods_user_info
        where ds='20250629'
    )ui
        left join
    (
        select
            get_json_object(log, '$.common.ar') AS province_id,
            get_json_object(log, '$.common.ba') AS brand,
            get_json_object(log, '$.common.ch') AS channel,
            get_json_object(log, '$.common.md') AS model,
            get_json_object(log, '$.common.mid') AS mid_id,
            get_json_object(log, '$.common.os') AS operate_system,
            get_json_object(log, '$.common.uid') AS user_id,
            get_json_object(log, '$.common.vc') AS version_code
        from ods_z_log
        where ds='20250629'
          and get_json_object(log, '$.page.page_id')='register'
          and get_json_object(log, '$.common.uid') IS NOT NULL
    )log
    on ui.user_id = log.user_id;
-- 每日装载

insert overwrite table dwd_user_register_inc partition(ds='20250629')
select
    id,
    date_format(create_time,'yyyy-MM-dd') date_id,
    create_time,
    channel,
    province_id,
    version_code,
    mid_id,
    brand,
    model,
    operate_system
from
    (
        select
            id,
            create_time
        from ods_user_info
        where ds='20250629'
    )ui
        left join
    (
        select
            get_json_object(log, '$.common.ar') AS province_id,
            get_json_object(log, '$.common.ba') AS brand,
            get_json_object(log, '$.common.ch') AS channel,
            get_json_object(log, '$.common.md') AS model,
            get_json_object(log, '$.common.mid') AS mid_id,
            get_json_object(log, '$.common.os') AS operate_system,
            get_json_object(log, '$.common.uid') AS user_id,
            get_json_object(log, '$.common.vc') AS version_code
        from ods_z_log
        where ds='20250629'
          and get_json_object(log, '$.page.page_id')='register'
          and get_json_object(log, '$.common.uid') IS NOT NULL
    )log
    on ui.id=log.user_id;

DROP TABLE IF EXISTS dwd_user_login_inc;
CREATE EXTERNAL TABLE dwd_user_login_inc
(
    `user_id`         STRING COMMENT '用户ID',
    `date_id`         STRING COMMENT '日期ID',
    `login_time`     STRING COMMENT '登录时间',
    `channel`         STRING COMMENT '应用下载渠道',
    `province_id`    STRING COMMENT '省份ID',
    `version_code`   STRING COMMENT '应用版本',
    `mid_id`          STRING COMMENT '设备ID',
    `brand`           STRING COMMENT '设备品牌',
    `model`           STRING COMMENT '设备型号',
    `operate_system` STRING COMMENT '设备操作系统'
) COMMENT '用户域用户登录事务事实表'
    PARTITIONED BY (`ds` STRING)
    STORED AS ORC
    LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/gmall/dwd/dwd_user_login_inc/'
    TBLPROPERTIES ("orc.compress" = "snappy");

-- 设置动态分区模式
set hive.exec.dynamic.partition.mode=nonstrict;

insert overwrite table dwd_user_login_inc partition (ds = '20250629')
select
    user_id,
    from_unixtime(cast(ts/1000 as bigint), 'yyyy-MM-dd') as date_id,
    from_unixtime(cast(ts/1000 as bigint), 'yyyy-MM-dd HH:mm:ss') as login_time,
    channel,
    province_id,
    version_code,
    mid_id,
    brand,
    model,
    operate_system
from (
         select
             user_id,
             channel,
             province_id,
             version_code,
             mid_id,
             brand,
             model,
             operate_system,
             ts
         from (
                  select
                      get_json_object(log, '$.common.uid') as user_id,
                      get_json_object(log, '$.common.ch') as channel,
                      get_json_object(log, '$.common.ar') as province_id,
                      get_json_object(log, '$.common.vc') as version_code,
                      get_json_object(log, '$.common.mid') as mid_id,
                      get_json_object(log, '$.common.ba') as brand,
                      get_json_object(log, '$.common.md') as model,
                      get_json_object(log, '$.common.os') as operate_system,
                      get_json_object(log, '$.ts') as ts,
                      row_number() over (partition by get_json_object(log, '$.common.sid') order by get_json_object(log, '$.ts')) as rn
                  from ods_z_log
                  where ds = '20250629'
                    and get_json_object(log, '$.page') is not null
                    and get_json_object(log, '$.common.uid') is not null
              ) t1
         where rn = 1
     ) t2;