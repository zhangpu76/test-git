set hive.exec.mode.local.auto=True;
use tms;

drop table if exists dwd_trade_order_detail_inc;
create external table dwd_trade_order_detail_inc(
      `id` bigint comment '运单明细ID',
      `order_id` string COMMENT '运单ID',
      `cargo_type` string COMMENT '货物类型ID',
      `cargo_type_name` string COMMENT '货物类型名称',
      `volumn_length` bigint COMMENT '长cm',
      `volumn_width` bigint COMMENT '宽cm',
      `volumn_height` bigint COMMENT '高cm',
      `weight` decimal(16,2) COMMENT '重量 kg',
      `order_time` string COMMENT '下单时间',
      `order_no` string COMMENT '运单号',
      `status` string COMMENT '运单状态',
      `status_name` string COMMENT '运单状态名称',
      `collect_type` string COMMENT '取件类型，1为网点自寄，2为上门取件',
      `collect_type_name` string COMMENT '取件类型名称',
      `user_id` bigint COMMENT '用户ID',
      `receiver_complex_id` bigint COMMENT '收件人小区id',
      `receiver_province_id` string COMMENT '收件人省份id',
      `receiver_city_id` string COMMENT '收件人城市id',
      `receiver_district_id` string COMMENT '收件人区县id',
      `receiver_name` string COMMENT '收件人姓名',
      `sender_complex_id` bigint COMMENT '发件人小区id',
      `sender_province_id` string COMMENT '发件人省份id',
      `sender_city_id` string COMMENT '发件人城市id',
      `sender_district_id` string COMMENT '发件人区县id',
      `sender_name` string COMMENT '发件人姓名',
      `cargo_num` bigint COMMENT '货物个数',
      `amount` decimal(16,2) COMMENT '金额',
      `estimate_arrive_time` string COMMENT '预计到达时间',
      `distance` decimal(16,2) COMMENT '距离，单位：公里',
      `ts` bigint COMMENT '时间戳'
) comment '交易域订单明细事务事实表'
partitioned by (`ds` string comment '统计日期')
stored as orc
location '/warehouse/tms/dwd/dwd_trade_order_detail_inc'
tblproperties('orc.compress' = 'snappy');

set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd_trade_order_detail_inc partition (ds)
select cargo.id,
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
       unix_timestamp(order_time, 'yyyy-MM-dd HH:mm:ss') * 1000  ts,  -- 新增ts字段：将order_time转为毫秒级时间戳
       date_format(order_time, 'yyyy-MM-dd') ds
from (select after.id,
             after.order_id,
             after.cargo_type,
             after.volume_length,
             after.volume_width,
             after.volume_height,
             after.weight,
             concat(substr(after.create_time, 1, 10), ' ', substr(after.create_time, 12, 8)) order_time,
             ds
      from ods_order_cargo as after
      where is_deleted = '0') cargo
         join
     (select after.id,
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
             -- 关键修改：适配string类型的estimate_arrive_time
             case
                 -- 若为空，直接返回null
                 when after.estimate_arrive_time is null or trim(after.estimate_arrive_time) = '' then null
                 -- 若为数字字符串（时间戳），转换为UTC时间
                 when after.estimate_arrive_time rlike '^\\d+$' then date_format(
                         from_utc_timestamp(cast(after.estimate_arrive_time as bigint), 'UTC'),
                         'yyyy-MM-dd HH:mm:ss'
                     )
                 -- 若为直接时间字符串（如"2025-07-09 12:00:00"），直接格式化
                 else date_format(from_utc_timestamp(after.estimate_arrive_time, 'UTC'), 'yyyy-MM-dd HH:mm:ss')
                 end as estimate_arrive_time,
             after.distance
      from ods_order_info as after
      where after.is_deleted = '0') info
     on cargo.order_id = info.id
         left join
     (select id,
             name
      from ods_base_dic
      where is_deleted = '0') dic_for_cargo_type
     on cargo.cargo_type = cast(dic_for_cargo_type.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where  is_deleted = '0') dic_for_status
     on info.status = cast(dic_for_status.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where  is_deleted = '0') dic_for_collect_type
     on info.collect_type = cast(dic_for_cargo_type.id as string);

select * from dwd_trade_order_detail_inc;

drop table if exists dwd_trade_pay_suc_detail_inc;
create external table dwd_trade_pay_suc_detail_inc(
      `id` bigint comment '运单明细ID',
      `order_id` string COMMENT '运单ID',
      `cargo_type` string COMMENT '货物类型ID',
      `cargo_type_name` string COMMENT '货物类型名称',
      `volumn_length` bigint COMMENT '长cm',
      `volumn_width` bigint COMMENT '宽cm',
      `volumn_height` bigint COMMENT '高cm',
      `weight` decimal(16,2) COMMENT '重量 kg',
      `payment_time` string COMMENT '支付时间',
      `order_no` string COMMENT '运单号',
      `status` string COMMENT '运单状态',
      `status_name` string COMMENT '运单状态名称',
      `collect_type` string COMMENT '取件类型，1为网点自寄，2为上门取件',
      `collect_type_name` string COMMENT '取件类型名称',
      `user_id` bigint COMMENT '用户ID',
      `receiver_complex_id` bigint COMMENT '收件人小区id',
      `receiver_province_id` string COMMENT '收件人省份id',
      `receiver_city_id` string COMMENT '收件人城市id',
      `receiver_district_id` string COMMENT '收件人区县id',
      `receiver_name` string COMMENT '收件人姓名',
      `sender_complex_id` bigint COMMENT '发件人小区id',
      `sender_province_id` string COMMENT '发件人省份id',
      `sender_city_id` string COMMENT '发件人城市id',
      `sender_district_id` string COMMENT '发件人区县id',
      `sender_name` string COMMENT '发件人姓名',
      `payment_type` string COMMENT '支付方式',
      `payment_type_name` string COMMENT '支付方式名称',
      `cargo_num` bigint COMMENT '货物个数',
      `amount` decimal(16,2) COMMENT '金额',
      `estimate_arrive_time` string COMMENT '预计到达时间',
      `distance` decimal(16,2) COMMENT '距离，单位：公里',
      `ts` bigint COMMENT '时间戳'
) comment '交易域支付成功事务事实表'
partitioned by (`ds` string comment '统计日期')
stored as orc
location '/warehouse/tms/dwd/dwd_trade_pay_suc_detail_inc'
tblproperties('orc.compress' = 'snappy');

set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd_trade_pay_suc_detail_inc partition (ds)
select cargo.id,
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
       -- 修正estimate_arrive_time处理：适配string类型
       case
           when estimate_arrive_time is null or trim(estimate_arrive_time) = '' then null
           -- 若为字符串时间戳（如"1627833600"），转换为UTC时间
           when estimate_arrive_time rlike '^\\d+$' then date_format(
                   from_utc_timestamp(cast(estimate_arrive_time as bigint), 'UTC'),
                   'yyyy-MM-dd HH:mm:ss'
               )
           -- 若为直接时间字符串（如"2025-07-09 12:00:00"），直接使用
           else estimate_arrive_time
           end as estimate_arrive_time,
       distance,
       unix_timestamp(payment_time, 'yyyy-MM-dd HH:mm:ss') * 1000  ts,
       date_format(payment_time, 'yyyy-MM-dd') ds
from (select after.id,
             after.order_id,
             after.cargo_type,
             after.volume_length,
             after.volume_width,
             after.volume_height,
             after.weight,
             ds
      from ods_order_cargo as after
      where  after.is_deleted = '0') cargo
         join
     (select after.id,
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
             after.estimate_arrive_time,  -- 直接获取string类型字段
             after.distance,
             concat(substr(after.update_time, 1, 10), ' ', substr(after.update_time, 12, 8)) payment_time
      from ods_order_info as after
     ) info
     on cargo.order_id = info.id
         left join
     (select id,
             name
      from ods_base_dic
      where  is_deleted = '0') dic_for_cargo_type
     on cargo.cargo_type = cast(dic_for_cargo_type.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where is_deleted = '0') dic_for_status
     on info.status = cast(dic_for_status.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where  is_deleted = '0') dic_for_collect_type
     on info.collect_type = cast(dic_for_cargo_type.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where  is_deleted = '0') dic_for_payment_type
     on info.payment_type = cast(dic_for_payment_type.id as string);

select * from dwd_trade_pay_suc_detail_inc;

drop table if exists dwd_trade_order_cancel_detail_inc;
create external table dwd_trade_order_cancel_detail_inc(
      `id` bigint comment '运单明细ID',
      `order_id` string COMMENT '运单ID',
      `cargo_type` string COMMENT '货物类型ID',
      `cargo_type_name` string COMMENT '货物类型名称',
      `volumn_length` bigint COMMENT '长cm',
      `volumn_width` bigint COMMENT '宽cm',
      `volumn_height` bigint COMMENT '高cm',
      `weight` decimal(16,2) COMMENT '重量 kg',
      `cancel_time` string COMMENT '取消时间',
      `order_no` string COMMENT '运单号',
      `status` string COMMENT '运单状态',
      `status_name` string COMMENT '运单状态名称',
      `collect_type` string COMMENT '取件类型，1为网点自寄，2为上门取件',
      `collect_type_name` string COMMENT '取件类型名称',
      `user_id` bigint COMMENT '用户ID',
      `receiver_complex_id` bigint COMMENT '收件人小区id',
      `receiver_province_id` string COMMENT '收件人省份id',
      `receiver_city_id` string COMMENT '收件人城市id',
      `receiver_district_id` string COMMENT '收件人区县id',
      `receiver_name` string COMMENT '收件人姓名',
      `sender_complex_id` bigint COMMENT '发件人小区id',
      `sender_province_id` string COMMENT '发件人省份id',
      `sender_city_id` string COMMENT '发件人城市id',
      `sender_district_id` string COMMENT '发件人区县id',
      `sender_name` string COMMENT '发件人姓名',
      `cargo_num` bigint COMMENT '货物个数',
      `amount` decimal(16,2) COMMENT '金额',
      `estimate_arrive_time` string COMMENT '预计到达时间',
      `distance` decimal(16,2) COMMENT '距离，单位：公里',
      `ts` bigint COMMENT '时间戳'
) comment '交易域取消运单事务事实表'
partitioned by (`ds` string comment '统计日期')
stored as orc
location '/warehouse/tms/dwd/dwd_trade_order_cancel_detail_inc'
tblproperties('orc.compress' = 'snappy');

set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd_trade_order_cancel_detail_inc partition (ds)
select
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
    -- 修复 estimate_arrive_time 的处理逻辑
    case
        when info.estimate_arrive_time is null or trim(info.estimate_arrive_time) = '' then null
        -- 如果是数字字符串（时间戳），转换为日期时间
        when info.estimate_arrive_time rlike '^\\d+$' then
            date_format(
                    from_utc_timestamp(cast(info.estimate_arrive_time as bigint), 'UTC'),
                    'yyyy-MM-dd HH:mm:ss'
                )
        -- 如果已经是日期时间字符串，直接使用
        else info.estimate_arrive_time
        end as estimate_arrive_time,
    distance,
    unix_timestamp(cancel_time, 'yyyy-MM-dd HH:mm:ss') * 1000  ts,
    date_format(cancel_time, 'yyyy-MM-dd') ds
from (
         select
             after.id,
             after.order_id,
             after.cargo_type,
             after.volume_length,
             after.volume_width,
             after.volume_height,
             after.weight,
             ds
         from ods_order_cargo as after
         where after.is_deleted = '0'
     ) cargo
         join (
    select
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
        after.estimate_arrive_time,  -- 直接选择原始字符串字段，不进行转换
        after.distance,
        concat(substr(after.update_time, 1, 10), ' ', substr(after.update_time, 12, 8)) cancel_time
    from ods_order_info as after
) info
              on cargo.order_id = info.id
         left join (
    select id, name
    from ods_base_dic
    where is_deleted = '0'
) dic_for_cargo_type
                   on cargo.cargo_type = cast(dic_for_cargo_type.id as string)
         left join (
    select id, name
    from ods_base_dic
    where is_deleted = '0'
) dic_for_status
                   on info.status = cast(dic_for_status.id as string)
         left join (
    select id, name
    from ods_base_dic
    where is_deleted = '0'
) dic_for_collect_type
                   on info.collect_type = cast(dic_for_cargo_type.id as string);

select * from dwd_trade_order_cancel_detail_inc;

drop table if exists dwd_trans_receive_detail_inc;
create external table dwd_trans_receive_detail_inc(
      `id` bigint comment '运单明细ID',
      `order_id` string COMMENT '运单ID',
      `cargo_type` string COMMENT '货物类型ID',
      `cargo_type_name` string COMMENT '货物类型名称',
      `volumn_length` bigint COMMENT '长cm',
      `volumn_width` bigint COMMENT '宽cm',
      `volumn_height` bigint COMMENT '高cm',
      `weight` decimal(16,2) COMMENT '重量 kg',
      `receive_time` string COMMENT '揽收时间',
      `order_no` string COMMENT '运单号',
      `status` string COMMENT '运单状态',
      `status_name` string COMMENT '运单状态名称',
      `collect_type` string COMMENT '取件类型，1为网点自寄，2为上门取件',
      `collect_type_name` string COMMENT '取件类型名称',
      `user_id` bigint COMMENT '用户ID',
      `receiver_complex_id` bigint COMMENT '收件人小区id',
      `receiver_province_id` string COMMENT '收件人省份id',
      `receiver_city_id` string COMMENT '收件人城市id',
      `receiver_district_id` string COMMENT '收件人区县id',
      `receiver_name` string COMMENT '收件人姓名',
      `sender_complex_id` bigint COMMENT '发件人小区id',
      `sender_province_id` string COMMENT '发件人省份id',
      `sender_city_id` string COMMENT '发件人城市id',
      `sender_district_id` string COMMENT '发件人区县id',
      `sender_name` string COMMENT '发件人姓名',
      `payment_type` string COMMENT '支付方式',
      `payment_type_name` string COMMENT '支付方式名称',
      `cargo_num` bigint COMMENT '货物个数',
      `amount` decimal(16,2) COMMENT '金额',
      `estimate_arrive_time` string COMMENT '预计到达时间',
      `distance` decimal(16,2) COMMENT '距离，单位：公里',
      `ts` bigint COMMENT '时间戳'
) comment '物流域揽收事务事实表'
partitioned by (`ds` string comment '统计日期')
stored as orc
location '/warehouse/tms/dwd/dwd_trans_receive_detail_inc'
tblproperties('orc.compress' = 'snappy');

set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd_trans_receive_detail_inc partition (ds)
select cargo.id,
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
       -- 修复 estimate_arrive_time 的处理逻辑
       case
           when info.estimate_arrive_time is null or trim(info.estimate_arrive_time) = '' then null
           -- 如果是数字字符串（时间戳），转换为日期时间
           when info.estimate_arrive_time rlike '^\\d+$' then
               date_format(
                       from_utc_timestamp(cast(info.estimate_arrive_time as bigint), 'UTC'),
                       'yyyy-MM-dd HH:mm:ss'
                   )
           -- 如果已经是日期时间字符串，直接使用
           else info.estimate_arrive_time
           end as estimate_arrive_time,
       distance,
       unix_timestamp(receive_time, 'yyyy-MM-dd HH:mm:ss') * 1000  ts,
       date_format(receive_time, 'yyyy-MM-dd') ds
from (select after.id,
             after.order_id,
             after.cargo_type,
             after.volume_length,
             after.volume_width,
             after.volume_height,
             after.weight,
             ds
      from ods_order_cargo as after
      where  after.is_deleted = '0') cargo
         join
     (select after.id,
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
      from ods_order_info as after
     ) info
     on cargo.order_id = info.id
         left join
     (select id,
             name
      from ods_base_dic
      where is_deleted = '0') dic_for_cargo_type
     on cargo.cargo_type = cast(dic_for_cargo_type.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where  is_deleted = '0') dic_for_status
     on info.status = cast(dic_for_status.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where  is_deleted = '0') dic_for_collect_type
     on info.collect_type = cast(dic_for_cargo_type.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where is_deleted = '0') dic_for_payment_type
     on info.payment_type = cast(dic_for_payment_type.id as string);

select * from dwd_trans_receive_detail_inc;

drop table if exists dwd_trans_dispatch_detail_inc;
create external table dwd_trans_dispatch_detail_inc(
      `id` bigint comment '运单明细ID',
      `order_id` string COMMENT '运单ID',
      `cargo_type` string COMMENT '货物类型ID',
      `cargo_type_name` string COMMENT '货物类型名称',
      `volumn_length` bigint COMMENT '长cm',
      `volumn_width` bigint COMMENT '宽cm',
      `volumn_height` bigint COMMENT '高cm',
      `weight` decimal(16,2) COMMENT '重量 kg',
      `dispatch_time` string COMMENT '发单时间',
      `order_no` string COMMENT '运单号',
      `status` string COMMENT '运单状态',
      `status_name` string COMMENT '运单状态名称',
      `collect_type` string COMMENT '取件类型，1为网点自寄，2为上门取件',
      `collect_type_name` string COMMENT '取件类型名称',
      `user_id` bigint COMMENT '用户ID',
      `receiver_complex_id` bigint COMMENT '收件人小区id',
      `receiver_province_id` string COMMENT '收件人省份id',
      `receiver_city_id` string COMMENT '收件人城市id',
      `receiver_district_id` string COMMENT '收件人区县id',
      `receiver_name` string COMMENT '收件人姓名',
      `sender_complex_id` bigint COMMENT '发件人小区id',
      `sender_province_id` string COMMENT '发件人省份id',
      `sender_city_id` string COMMENT '发件人城市id',
      `sender_district_id` string COMMENT '发件人区县id',
      `sender_name` string COMMENT '发件人姓名',
      `payment_type` string COMMENT '支付方式',
      `payment_type_name` string COMMENT '支付方式名称',
      `cargo_num` bigint COMMENT '货物个数',
      `amount` decimal(16,2) COMMENT '金额',
      `estimate_arrive_time` string COMMENT '预计到达时间',
      `distance` decimal(16,2) COMMENT '距离，单位：公里',
      `ts` bigint COMMENT '时间戳'
) comment '物流域发单事务事实表'
partitioned by (`ds` string comment '统计日期')
stored as orc
location '/warehouse/tms/dwd/dwd_trans_dispatch_detail_inc'
tblproperties('orc.compress' = 'snappy');

set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd_trans_dispatch_detail_inc partition (ds)
select cargo.id,
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
from (select after.id,
             after.order_id,
             after.cargo_type,
             after.volume_length,
             after.volume_width,
             after.volume_height,
             after.weight,
             ds
      from ods_order_cargo as after
      where  after.is_deleted = '0') cargo
         join
     (select after.id,
             after.order_no,
             after.status,
             after.collect_type,
             after.user_id,
             after.receiver_complex_id,
             after.receiver_province_id,
             after.receiver_city_id,
             after.receiver_district_id,
             concat(substr(after.receiver_name, 1, 1), '*')                                  receiver_name,
             after.sender_complex_id,
             after.sender_province_id,
             after.sender_city_id,
             after.sender_district_id,
             concat(substr(after.sender_name, 1, 1), '*')                                    sender_name,
             after.payment_type,
             after.cargo_num,
             after.amount,
             -- 关键修改：适配string类型的estimate_arrive_time
             case
                 -- 若为空，直接返回null
                 when after.estimate_arrive_time is null or trim(after.estimate_arrive_time) = '' then null
                 -- 若为数字字符串（时间戳），转换为UTC时间
                 when after.estimate_arrive_time rlike '^\\d+$' then date_format(
                         from_utc_timestamp(cast(after.estimate_arrive_time as bigint), 'UTC'),
                         'yyyy-MM-dd HH:mm:ss'
                     )
                 -- 若为直接时间字符串（如"2025-07-09 12:00:00"），直接格式化
                 else date_format(from_utc_timestamp(after.estimate_arrive_time, 'UTC'), 'yyyy-MM-dd HH:mm:ss')
                 end as estimate_arrive_time,
             after.distance,
             concat(substr(after.update_time, 1, 10), ' ', substr(after.update_time, 12, 8)) dispatch_time
      from ods_order_info as after
     ) info
     on cargo.order_id = info.id
         left join
     (select id,
             name
      from ods_base_dic
      where  is_deleted = '0') dic_for_cargo_type
     on cargo.cargo_type = cast(dic_for_cargo_type.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where  is_deleted = '0') dic_for_status
     on info.status = cast(dic_for_status.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where  is_deleted = '0') dic_for_collect_type
     on info.collect_type = cast(dic_for_cargo_type.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where  is_deleted = '0') dic_for_payment_type
     on info.payment_type = cast(dic_for_payment_type.id as string);

select * from dwd_trans_dispatch_detail_inc;

drop table if exists dwd_trans_bound_finish_detail_inc;
create external table dwd_trans_bound_finish_detail_inc(
      `id` bigint comment '运单明细ID',
      `order_id` string COMMENT '运单ID',
      `cargo_type` string COMMENT '货物类型ID',
      `cargo_type_name` string COMMENT '货物类型名称',
      `volumn_length` bigint COMMENT '长cm',
      `volumn_width` bigint COMMENT '宽cm',
      `volumn_height` bigint COMMENT '高cm',
      `weight` decimal(16,2) COMMENT '重量 kg',
      `bound_finish_time` string COMMENT '转运完成时间',
      `order_no` string COMMENT '运单号',
      `status` string COMMENT '运单状态',
      `status_name` string COMMENT '运单状态名称',
      `collect_type` string COMMENT '取件类型，1为网点自寄，2为上门取件',
      `collect_type_name` string COMMENT '取件类型名称',
      `user_id` bigint COMMENT '用户ID',
      `receiver_complex_id` bigint COMMENT '收件人小区id',
      `receiver_province_id` string COMMENT '收件人省份id',
      `receiver_city_id` string COMMENT '收件人城市id',
      `receiver_district_id` string COMMENT '收件人区县id',
      `receiver_name` string COMMENT '收件人姓名',
      `sender_complex_id` bigint COMMENT '发件人小区id',
      `sender_province_id` string COMMENT '发件人省份id',
      `sender_city_id` string COMMENT '发件人城市id',
      `sender_district_id` string COMMENT '发件人区县id',
      `sender_name` string COMMENT '发件人姓名',
      `payment_type` string COMMENT '支付方式',
      `payment_type_name` string COMMENT '支付方式名称',
      `cargo_num` bigint COMMENT '货物个数',
      `amount` decimal(16,2) COMMENT '金额',
      `estimate_arrive_time` string COMMENT '预计到达时间',
      `distance` decimal(16,2) COMMENT '距离，单位：公里',
      `ts` bigint COMMENT '时间戳'
) comment '物流域转运完成事务事实表'
partitioned by (`ds` string comment '统计日期')
stored as orc
location '/warehouse/tms/dwd/dwd_trans_bound_finish_detail_inc'
tblproperties('orc.compress' = 'snappy');

set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd_trans_bound_finish_detail_inc partition (ds)
select cargo.id,
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
from (select after.id,
             after.order_id,
             after.cargo_type,
             after.volume_length,
             after.volume_width,
             after.volume_height,
             after.weight,
             ds
      from ods_order_cargo as after
      where  after.is_deleted = '0') cargo
         join
     (select after.id,
             after.order_no,
             after.status,
             after.collect_type,
             after.user_id,
             after.receiver_complex_id,
             after.receiver_province_id,
             after.receiver_city_id,
             after.receiver_district_id,
             concat(substr(after.receiver_name, 1, 1), '*')                                  receiver_name,
             after.sender_complex_id,
             after.sender_province_id,
             after.sender_city_id,
             after.sender_district_id,
             concat(substr(after.sender_name, 1, 1), '*')                                    sender_name,
             after.payment_type,
             after.cargo_num,
             after.amount,
             -- 关键修改：适配string类型的estimate_arrive_time
             case
                 -- 若为空，直接返回null
                 when after.estimate_arrive_time is null or trim(after.estimate_arrive_time) = '' then null
                 -- 若为数字字符串（时间戳），转换为UTC时间
                 when after.estimate_arrive_time rlike '^\\d+$' then date_format(
                         from_utc_timestamp(cast(after.estimate_arrive_time as bigint), 'UTC'),
                         'yyyy-MM-dd HH:mm:ss'
                     )
                 -- 若为直接时间字符串（如"2025-07-09 12:00:00"），直接格式化
                 else date_format(from_utc_timestamp(after.estimate_arrive_time, 'UTC'), 'yyyy-MM-dd HH:mm:ss')
                 end as estimate_arrive_time,
             after.distance,
             concat(substr(after.update_time, 1, 10), ' ', substr(after.update_time, 12, 8)) bound_finish_time
      from ods_order_info  as after
     ) info
     on cargo.order_id = info.id
         left join
     (select id,
             name
      from ods_base_dic
      where is_deleted = '0') dic_for_cargo_type
     on cargo.cargo_type = cast(dic_for_cargo_type.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where  is_deleted = '0') dic_for_status
     on info.status = cast(dic_for_status.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where  is_deleted = '0') dic_for_collect_type
     on info.collect_type = cast(dic_for_cargo_type.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where  is_deleted = '0') dic_for_payment_type
     on info.payment_type = cast(dic_for_payment_type.id as string);

select * from dwd_trans_bound_finish_detail_inc;

drop table if exists dwd_trans_deliver_suc_detail_inc;
create external table dwd_trans_deliver_suc_detail_inc(
      `id` bigint comment '运单明细ID',
      `order_id` string COMMENT '运单ID',
      `cargo_type` string COMMENT '货物类型ID',
      `cargo_type_name` string COMMENT '货物类型名称',
      `volumn_length` bigint COMMENT '长cm',
      `volumn_width` bigint COMMENT '宽cm',
      `volumn_height` bigint COMMENT '高cm',
      `weight` decimal(16,2) COMMENT '重量 kg',
      `deliver_suc_time` string COMMENT '派送成功时间',
      `order_no` string COMMENT '运单号',
      `status` string COMMENT '运单状态',
      `status_name` string COMMENT '运单状态名称',
      `collect_type` string COMMENT '取件类型，1为网点自寄，2为上门取件',
      `collect_type_name` string COMMENT '取件类型名称',
      `user_id` bigint COMMENT '用户ID',
      `receiver_complex_id` bigint COMMENT '收件人小区id',
      `receiver_province_id` string COMMENT '收件人省份id',
      `receiver_city_id` string COMMENT '收件人城市id',
      `receiver_district_id` string COMMENT '收件人区县id',
      `receiver_name` string COMMENT '收件人姓名',
      `sender_complex_id` bigint COMMENT '发件人小区id',
      `sender_province_id` string COMMENT '发件人省份id',
      `sender_city_id` string COMMENT '发件人城市id',
      `sender_district_id` string COMMENT '发件人区县id',
      `sender_name` string COMMENT '发件人姓名',
      `payment_type` string COMMENT '支付方式',
      `payment_type_name` string COMMENT '支付方式名称',
      `cargo_num` bigint COMMENT '货物个数',
      `amount` decimal(16,2) COMMENT '金额',
      `estimate_arrive_time` string COMMENT '预计到达时间',
      `distance` decimal(16,2) COMMENT '距离，单位：公里',
      `ts` bigint COMMENT '时间戳'
) comment '物流域派送成功事务事实表'
partitioned by (`ds` string comment '统计日期')
stored as orc
location '/warehouse/tms/dwd/dwd_trans_deliver_suc_detail_inc'
tblproperties('orc.compress' = 'snappy');

set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd_trans_deliver_suc_detail_inc partition (ds)
select cargo.id,
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
from (select after.id,
             after.order_id,
             after.cargo_type,
             after.volume_length,
             after.volume_width,
             after.volume_height,
             after.weight,
             ds
      from ods_order_cargo as after
      where  after.is_deleted = '0') cargo
         join
     (select after.id,
             after.order_no,
             after.status,
             after.collect_type,
             after.user_id,
             after.receiver_complex_id,
             after.receiver_province_id,
             after.receiver_city_id,
             after.receiver_district_id,
             concat(substr(after.receiver_name, 1, 1), '*')                                  receiver_name,
             after.sender_complex_id,
             after.sender_province_id,
             after.sender_city_id,
             after.sender_district_id,
             concat(substr(after.sender_name, 1, 1), '*')                                    sender_name,
             after.payment_type,
             after.cargo_num,
             after.amount,
             -- 关键修改：适配string类型的estimate_arrive_time
             case
                 -- 若为空，直接返回null
                 when after.estimate_arrive_time is null or trim(after.estimate_arrive_time) = '' then null
                 -- 若为数字字符串（时间戳），转换为UTC时间
                 when after.estimate_arrive_time rlike '^\\d+$' then date_format(
                         from_utc_timestamp(cast(after.estimate_arrive_time as bigint), 'UTC'),
                         'yyyy-MM-dd HH:mm:ss'
                     )
                 -- 若为直接时间字符串（如"2025-07-09 12:00:00"），直接格式化
                 else date_format(from_utc_timestamp(after.estimate_arrive_time, 'UTC'), 'yyyy-MM-dd HH:mm:ss')
                 end as estimate_arrive_time,
             after.distance,
             concat(substr(after.update_time, 1, 10), ' ', substr(after.update_time, 12, 8)) deliver_suc_time
      from ods_order_info as after
     ) info
     on cargo.order_id = info.id
         left join
     (select id,
             name
      from ods_base_dic
      where  is_deleted = '0') dic_for_cargo_type
     on cargo.cargo_type = cast(dic_for_cargo_type.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where  is_deleted = '0') dic_for_status
     on info.status = cast(dic_for_status.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where  is_deleted = '0') dic_for_collect_type
     on info.collect_type = cast(dic_for_cargo_type.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where  is_deleted = '0') dic_for_payment_type
     on info.payment_type = cast(dic_for_payment_type.id as string);

select * from dwd_trans_deliver_suc_detail_inc;

drop table if exists dwd_trans_sign_detail_inc;
create external table dwd_trans_sign_detail_inc(
      `id` bigint comment '运单明细ID',
      `order_id` string COMMENT '运单ID',
      `cargo_type` string COMMENT '货物类型ID',
      `cargo_type_name` string COMMENT '货物类型名称',
      `volumn_length` bigint COMMENT '长cm',
      `volumn_width` bigint COMMENT '宽cm',
      `volumn_height` bigint COMMENT '高cm',
      `weight` decimal(16,2) COMMENT '重量 kg',
      `sign_time` string COMMENT '签收时间',
      `order_no` string COMMENT '运单号',
      `status` string COMMENT '运单状态',
      `status_name` string COMMENT '运单状态名称',
      `collect_type` string COMMENT '取件类型，1为网点自寄，2为上门取件',
      `collect_type_name` string COMMENT '取件类型名称',
      `user_id` bigint COMMENT '用户ID',
      `receiver_complex_id` bigint COMMENT '收件人小区id',
      `receiver_province_id` string COMMENT '收件人省份id',
      `receiver_city_id` string COMMENT '收件人城市id',
      `receiver_district_id` string COMMENT '收件人区县id',
      `receiver_name` string COMMENT '收件人姓名',
      `sender_complex_id` bigint COMMENT '发件人小区id',
      `sender_province_id` string COMMENT '发件人省份id',
      `sender_city_id` string COMMENT '发件人城市id',
      `sender_district_id` string COMMENT '发件人区县id',
      `sender_name` string COMMENT '发件人姓名',
      `payment_type` string COMMENT '支付方式',
      `payment_type_name` string COMMENT '支付方式名称',
      `cargo_num` bigint COMMENT '货物个数',
      `amount` decimal(16,2) COMMENT '金额',
      `estimate_arrive_time` string COMMENT '预计到达时间',
      `distance` decimal(16,2) COMMENT '距离，单位：公里',
      `ts` bigint COMMENT '时间戳'
) comment '物流域签收事务事实表'
partitioned by (`ds` string comment '统计日期')
stored as orc
location '/warehouse/tms/dwd/dwd_trans_sign_detail_inc'
tblproperties('orc.compress' = 'snappy');

set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd_trans_sign_detail_inc partition (ds)
select cargo.id,
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
       -- 修复estimate_arrive_time（适配string类型）
       case
           when info.estimate_arrive_time is null or trim(info.estimate_arrive_time) = '' then null
           when info.estimate_arrive_time rlike '^\\d+$' then
               date_format(from_utc_timestamp(cast(info.estimate_arrive_time as bigint), 'UTC'), 'yyyy-MM-dd HH:mm:ss')
           else info.estimate_arrive_time
           end as estimate_arrive_time,
       distance,
       unix_timestamp(sign_time, 'yyyy-MM-dd HH:mm:ss') * 1000  ts,  -- 补全ts字段
       date_format(sign_time, 'yyyy-MM-dd') ds
from (select after.id,
             after.order_id,
             after.cargo_type,
             after.volume_length,
             after.volume_width,
             after.volume_height,
             after.weight,
             ds
      from ods_order_cargo as after
      where  after.is_deleted = '0') cargo
         join
     (select after.id,
             after.order_no,
             after.status,
             after.collect_type,
             after.user_id,
             after.receiver_complex_id,
             after.receiver_province_id,
             after.receiver_city_id,
             after.receiver_district_id,
             concat(substr(after.receiver_name, 1, 1), '*')                                  receiver_name,
             after.sender_complex_id,
             after.sender_province_id,
             after.sender_city_id,
             after.sender_district_id,
             concat(substr(after.sender_name, 1, 1), '*')                                    sender_name,
             after.payment_type,
             after.cargo_num,
             after.amount,
             after.estimate_arrive_time,  -- 直接获取string类型字段
             after.distance,
             concat(substr(after.update_time, 1, 10), ' ', substr(after.update_time, 12, 8)) sign_time
      from ods_order_info as after
     ) info
     on cargo.order_id = info.id
         left join
     (select id,
             name
      from ods_base_dic
      where  is_deleted = '0') dic_for_cargo_type
     on cargo.cargo_type = cast(dic_for_cargo_type.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where  is_deleted = '0') dic_for_status
     on info.status = cast(dic_for_status.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where is_deleted = '0') dic_for_collect_type
     on info.collect_type = cast(dic_for_cargo_type.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where  is_deleted = '0') dic_for_payment_type
     on info.payment_type = cast(dic_for_payment_type.id as string);

select * from dwd_trans_sign_detail_inc;

drop table if exists dwd_trade_order_process_inc;
create external table dwd_trade_order_process_inc(
      `id` bigint comment '运单明细ID',
      `order_id` string COMMENT '运单ID',
      `cargo_type` string COMMENT '货物类型ID',
      `cargo_type_name` string COMMENT '货物类型名称',
      `volumn_length` bigint COMMENT '长cm',
      `volumn_width` bigint COMMENT '宽cm',
      `volumn_height` bigint COMMENT '高cm',
      `weight` decimal(16,2) COMMENT '重量 kg',
      `order_time` string COMMENT '下单时间',
      `order_no` string COMMENT '运单号',
      `status` string COMMENT '运单状态',
      `status_name` string COMMENT '运单状态名称',
      `collect_type` string COMMENT '取件类型，1为网点自寄，2为上门取件',
      `collect_type_name` string COMMENT '取件类型名称',
      `user_id` bigint COMMENT '用户ID',
      `receiver_complex_id` bigint COMMENT '收件人小区id',
      `receiver_province_id` string COMMENT '收件人省份id',
      `receiver_city_id` string COMMENT '收件人城市id',
      `receiver_district_id` string COMMENT '收件人区县id',
      `receiver_name` string COMMENT '收件人姓名',
      `sender_complex_id` bigint COMMENT '发件人小区id',
      `sender_province_id` string COMMENT '发件人省份id',
      `sender_city_id` string COMMENT '发件人城市id',
      `sender_district_id` string COMMENT '发件人区县id',
      `sender_name` string COMMENT '发件人姓名',
      `payment_type` string COMMENT '支付方式',
      `payment_type_name` string COMMENT '支付方式名称',
      `cargo_num` bigint COMMENT '货物个数',
      `amount` decimal(16,2) COMMENT '金额',
      `estimate_arrive_time` string COMMENT '预计到达时间',
      `distance` decimal(16,2) COMMENT '距离，单位：公里',
      `ts` bigint COMMENT '时间戳',
      `start_date` string COMMENT '开始日期',
      `end_date` string COMMENT '结束日期'
) comment '交易域运单累积快照事实表'
partitioned by (`ds` string comment '统计日期')
stored as orc
location '/warehouse/tms/dwd/dwd_order_process'
tblproperties('orc.compress' = 'snappy');

set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd_trade_order_process_inc partition (ds)
select cargo.id,
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
       ds,
       date_format(order_time, 'yyyy-MM-dd') start_date,
       end_date,
       end_date                              ds
from (select after.id,
             after.order_id,
             after.cargo_type,
             after.volume_length,
             after.volume_width,
             after.volume_height,
             after.weight,
             concat(substr(after.create_time, 1, 10), ' ', substr(after.create_time, 12, 8)) order_time,
             ds
      from ods_order_cargo as after
      where  after.is_deleted = '0') cargo
         join
     (select after.id,
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
             after.payment_type,
             after.cargo_num,
             after.amount,
             -- 关键修改：适配string类型的estimate_arrive_time
             case
                 -- 若为空，直接返回null
                 when after.estimate_arrive_time is null or trim(after.estimate_arrive_time) = '' then null
                 -- 若为数字字符串（时间戳），转换为UTC时间
                 when after.estimate_arrive_time rlike '^\\d+$' then date_format(
                         from_utc_timestamp(cast(after.estimate_arrive_time as bigint), 'UTC'),
                         'yyyy-MM-dd HH:mm:ss'
                     )
                 -- 若为直接时间字符串（如"2025-07-09 12:00:00"），直接格式化
                 else date_format(from_utc_timestamp(after.estimate_arrive_time, 'UTC'), 'yyyy-MM-dd HH:mm:ss')
                 end as estimate_arrive_time,
             after.distance,
             if(after.status = '60080' or
                after.status = '60999',
                concat(substr(after.update_time, 1, 10)),
                '9999-12-31')                               end_date
      from ods_order_info as after
      where  after.is_deleted = '0') info
     on cargo.order_id = info.id
         left join
     (select id,
             name
      from ods_base_dic
      where  is_deleted = '0') dic_for_cargo_type
     on cargo.cargo_type = cast(dic_for_cargo_type.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where  is_deleted = '0') dic_for_status
     on info.status = cast(dic_for_status.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where  is_deleted = '0') dic_for_collect_type
     on info.collect_type = cast(dic_for_cargo_type.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where  is_deleted = '0') dic_for_payment_type
     on info.payment_type = cast(dic_for_payment_type.id as string);

select * from dwd_trade_order_process_inc;

drop table if exists dwd_trans_trans_finish_inc;
create external table dwd_trans_trans_finish_inc(
      `id` bigint comment '运输任务ID',
      `shift_id` bigint COMMENT '车次ID',
      `line_id` bigint COMMENT '路线ID',
      `start_org_id` bigint COMMENT '起始机构ID',
      `start_org_name` string COMMENT '起始机构名称',
      `end_org_id` bigint COMMENT '目的机构ID',
      `end_org_name` string COMMENT '目的机构名称',
      `order_num` bigint COMMENT '运单个数',
      `driver1_emp_id` bigint COMMENT '司机1ID',
      `driver1_name` string COMMENT '司机1名称',
      `driver2_emp_id` bigint COMMENT '司机2ID',
      `driver2_name` string COMMENT '司机2名称',
      `truck_id` bigint COMMENT '卡车ID',
      `truck_no` string COMMENT '卡车号牌',
      `actual_start_time` string COMMENT '实际启动时间',
      `actual_end_time` string COMMENT '实际到达时间',
      `estimate_end_time` string COMMENT '预估到达时间',
      `actual_distance` decimal(16,2) COMMENT '实际行驶距离',
      `finish_dur_sec` bigint COMMENT '运输完成历经时长：秒',
      `ts` bigint COMMENT '时间戳'
) comment '物流域运输事务事实表'
partitioned by (`ds` string comment '统计日期')
stored as orc
location '/warehouse/tms/dwd/dwd_trans_trans_finish_inc'
tblproperties('orc.compress' = 'snappy');

set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd_trans_trans_finish_inc partition (ds)
select
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
    -- 修复actual_start_time：适配字符串类型
    case
        when info.actual_start_time_str is null or trim(info.actual_start_time_str) = '' then null
        -- 若为数字字符串（时间戳）
        when info.actual_start_time_str rlike '^\\d+$' then date_format(
                from_utc_timestamp(cast(info.actual_start_time_str as bigint), 'UTC'),
                'yyyy-MM-dd HH:mm:ss'
            )
        -- 若为直接时间字符串
        else info.actual_start_time_str
        end as actual_start_time,
    -- 修复actual_end_time：同上述逻辑
    case
        when info.actual_end_time_str is null or trim(info.actual_end_time_str) = '' then null
        when info.actual_end_time_str rlike '^\\d+$' then date_format(
                from_utc_timestamp(cast(info.actual_end_time_str as bigint), 'UTC'),
                'yyyy-MM-dd HH:mm:ss'
            )
        else info.actual_end_time_str
        end as actual_end_time,
    dim_tb.estimated_time as estimate_end_time,
    actual_distance,
    -- 修复finish_dur_sec：使用unix_timestamp直接解析字符串时间
    case
        when info.actual_start_time_str is not null and info.actual_end_time_str is not null
            -- 直接计算两个时间字符串的秒数差
            then unix_timestamp(info.actual_end_time_str, 'yyyy-MM-dd HH:mm:ss')
            - unix_timestamp(info.actual_start_time_str, 'yyyy-MM-dd HH:mm:ss')
        else null
        end as finish_dur_sec,
    -- 补充ts字段
    case
        when info.actual_end_time_str rlike '^\\d+$' then cast(info.actual_end_time_str as bigint)
        else unix_timestamp(info.actual_end_time_str, 'yyyy-MM-dd HH:mm:ss') * 1000
        end as ts,
    info.ds  -- 分区字段ds
from (select
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
          after.actual_start_time as actual_start_time_str,  -- 保留原始字符串
          after.actual_end_time as actual_end_time_str,      -- 保留原始字符串
          after.actual_distance,
          -- 计算ds（基于实际到达时间）
          case
              when after.actual_end_time rlike '^\\d+$' then date_format(
                      from_utc_timestamp(cast(after.actual_end_time as bigint), 'UTC'),
                      'yyyy-MM-dd'
                  )
              else date_format(from_utc_timestamp(after.actual_end_time, 'UTC'), 'yyyy-MM-dd')
              end as ds
      from ods_transport_task after
     ) info
         left join (
    select id, estimated_time
    from dim_shift_full
) dim_tb on info.shift_id = dim_tb.id
    limit 200;

select * from dwd_trans_trans_finish_inc;

drop table if exists dwd_bound_inbound_inc;
create external table dwd_bound_inbound_inc(
      `id` bigint COMMENT '中转记录ID',
      `order_id` bigint COMMENT '运单ID',
      `org_id` bigint COMMENT '机构ID',
      `inbound_time` string COMMENT '入库时间',
      `inbound_emp_id` bigint COMMENT '入库人员'
) comment '中转域入库事务事实表'
partitioned by (`ds` string comment '统计日期')
stored as orc
location '/warehouse/tms/dwd/dwd_bound_inbound_inc'
tblproperties('orc.compress' = 'snappy');

set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd_bound_inbound_inc partition (ds= '20250720')
select
    after.id,
    after.order_id,
    after.org_id,
    -- 修复inbound_time转换：适配字符串类型的时间数据
    case
        -- 处理空值
        when after.inbound_time is null or trim(after.inbound_time) = '' then null
        -- 若为数字字符串（时间戳，支持秒或毫秒级）
        when after.inbound_time rlike '^\\d+$' then
            date_format(
                    from_utc_timestamp(
                        -- 10位视为秒级，转为毫秒；否则视为毫秒级
                            case when length(after.inbound_time) = 10
                                     then cast(after.inbound_time as bigint) * 1000
                                 else cast(after.inbound_time as bigint)
                                end,
                            'UTC'
                        ),
                    'yyyy-MM-dd HH:mm:ss'
                )
        -- 若为直接时间字符串（如'2025-07-20 10:00:00'）
        else date_format(from_utc_timestamp(after.inbound_time, 'UTC'), 'yyyy-MM-dd HH:mm:ss')
        end as inbound_time,
    after.inbound_emp_id
from ods_order_org_bound after
    limit 250;

select * from dwd_bound_inbound_inc;

drop table if exists dwd_bound_sort_inc;
create external table dwd_bound_sort_inc(
      `id` bigint COMMENT '中转记录ID',
      `order_id` bigint COMMENT '订单ID',
      `org_id` bigint COMMENT '机构ID',
      `sort_time` string COMMENT '分拣时间',
      `sorter_emp_id` bigint COMMENT '分拣人员'
) comment '中转域分拣事务事实表'
partitioned by (`ds` string comment '统计日期')
stored as orc
location '/warehouse/tms/dwd/dwd_bound_sort_inc'
tblproperties('orc.compress' = 'snappy');

set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd_bound_sort_inc partition (ds="20250720")
select
    after.id,
    after.order_id,
    after.org_id,
    -- 修复sort_time转换：适配字符串类型的时间数据
    case
        -- 处理空值或空字符串
        when after.sort_time is null or trim(after.sort_time) = '' then null
        -- 若为数字字符串（时间戳，支持秒或毫秒级）
        when after.sort_time rlike '^\\d+$' then
            date_format(
                    from_utc_timestamp(
                        -- 10位视为秒级，转为毫秒；否则视为毫秒级
                            case when length(after.sort_time) = 10
                                     then cast(after.sort_time as bigint) * 1000
                                 else cast(after.sort_time as bigint)
                                end,
                            'UTC'
                        ),
                    'yyyy-MM-dd HH:mm:ss'
                )
        -- 若为直接时间字符串（如'2025-07-20 10:00:00'）
        else after.sort_time  -- 直接使用原始字符串（假设已为正确格式）
        end as sort_time,
    after.sorter_emp_id
from ods_order_org_bound as after
where after.sort_time is not null  -- 过滤原始字段非null的记录
    limit 250;

select * from dwd_bound_sort_inc;

drop table if exists dwd_bound_outbound_inc;
create external table dwd_bound_outbound_inc(
      `id` bigint COMMENT '中转记录ID',
      `order_id` bigint COMMENT '订单ID',
      `org_id` bigint COMMENT '机构ID',
      `outbound_time` string COMMENT '出库时间',
      `outbound_emp_id` bigint COMMENT '出库人员'
) comment '中转域出库事务事实表'
partitioned by (`ds` string comment '统计日期')
stored as orc
location '/warehouse/tms/dwd/dwd_bound_outbound_inc'
tblproperties('orc.compress' = 'snappy');

set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd_bound_outbound_inc partition (ds="20250720")
select
    after.id,
    after.order_id,
    after.org_id,
    -- 修复outbound_time转换：适配字符串类型的时间数据
    case
        -- 处理空值或空字符串
        when after.outbound_time is null or trim(after.outbound_time) = '' then null
        -- 若为数字字符串（时间戳，支持秒或毫秒级）
        when after.outbound_time rlike '^\\d+$' then
            date_format(
                    from_utc_timestamp(
                        -- 10位视为秒级，转为毫秒；否则视为毫秒级
                            case when length(after.outbound_time) = 10
                                     then cast(after.outbound_time as bigint) * 1000
                                 else cast(after.outbound_time as bigint)
                                end,
                            'UTC'
                        ),
                    'yyyy-MM-dd HH:mm:ss'
                )
        -- 若为直接时间字符串（如'2025-07-20 10:00:00'）
        else after.outbound_time  -- 直接使用原始字符串（假设已为正确格式）
        end as outbound_time,
    after.outbound_emp_id
from ods_order_org_bound as after
where after.outbound_time is not null;

select * from dwd_bound_outbound_inc;