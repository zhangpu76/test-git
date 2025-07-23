set hive.exec.mode.local.auto=True;
use tms;

drop table if exists dws_trade_org_cargo_type_order_1d;
create external table dws_trade_org_cargo_type_order_1d(
      `org_id` bigint comment '机构ID',
      `org_name` string comment '转运站名称',
      `city_id` bigint comment '城市ID',
      `city_name` string comment '城市名称',
      `cargo_type` string comment '货物类型',
      `cargo_type_name` string comment '货物类型名称',
      `order_count` bigint comment '下单数',
      `order_amount` decimal(16,2) comment '下单金额'
) comment '交易域机构货物类型粒度下单 1 日汇总表'
partitioned by(`ds` string comment '统计日期')
stored as orc
location '/warehouse/tms/dws/dws_trade_org_cargo_type_order_1d'
tblproperties('orc.compress' = 'snappy');

set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dws_trade_org_cargo_type_order_1d partition (ds)
select org_id,
       org_name,
       city_id,
       region.name city_name,
       cargo_type,
       cargo_type_name,
       order_count,
       order_amount,
       ds
from (select org_id,
             org_name,
             sender_city_id  city_id,
             cargo_type,
             cargo_type_name,
             count(order_id) order_count,
             sum(amount)     order_amount,
             ds
      from (select order_id,
                   cargo_type,
                   cargo_type_name,
                   sender_district_id,
                   sender_city_id,
                   max(amount) amount,
                   ds
            from (select order_id,
                         cargo_type,
                         cargo_type_name,
                         sender_district_id,
                         sender_city_id,
                         amount,
                         ds
                  from dwd_trade_order_detail_inc) detail
            group by order_id,
                     cargo_type,
                     cargo_type_name,
                     sender_district_id,
                     sender_city_id,
                     ds) distinct_detail
               left join
           (select id org_id,
                   org_name,
                   region_id
            from dim_organ_full
            where ds = '20250720') org
           on distinct_detail.sender_district_id = org.region_id
      group by org_id,
               org_name,
               cargo_type,
               cargo_type_name,
               sender_city_id,
               ds) agg
         left join (
    select id,
           name
    from dim_region_full
    where ds = '20250720'
) region on city_id = region.id;

select * from dws_trade_org_cargo_type_order_1d;

drop table if exists dws_trans_dispatch_1d;
create external table dws_trans_dispatch_1d(
      `order_count` bigint comment '发单总数',
      `order_amount` decimal(16,2) comment '发单总金额'
) comment '物流域发单 1 日汇总表'
partitioned by (`ds` string comment '统计日期')
stored as orc
location '/warehouse/tms/dws/dws_trans_dispatch_1d/'
tblproperties('orc.compress'='snappy');

set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dws_trans_dispatch_1d partition (ds)
select count(order_id)      order_count,
       sum(distinct_amount) order_amount,
       ds
from (select order_id,
             ds,
             max(amount) distinct_amount
      from dwd_trans_dispatch_detail_inc
      group by order_id,
               ds) distinct_info
group by ds;

select * from dws_trans_dispatch_1d;

drop table if exists dws_trans_org_deliver_suc_1d;
create external table dws_trans_org_deliver_suc_1d(
      `org_id` bigint comment '转运站ID',
      `org_name` string comment '转运站名称',
      `city_id` bigint comment '城市ID',
      `city_name` string comment '城市名称',
      `province_id` bigint comment '省份ID',
      `province_name` string comment '省份名称',
      `order_count` bigint comment '派送成功次数（订单数）'
) comment '物流域转运站粒度派送成功 1 日汇总表'
partitioned by (`ds` string comment '统计日期')
stored as orc
location '/warehouse/tms/dws/dws_trans_org_deliver_suc_1d/'
tblproperties('orc.compress'='snappy');

set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dws_trans_org_deliver_suc_1d partition (ds)
select org_id,
       org_name,
       city_id,
       city.name       city_name,
       province_id,
       province.name   province_name,
       count(order_id) order_count,
       ds
from (select order_id,
             receiver_district_id,
             ds
      from dwd_trans_deliver_suc_detail_inc
      group by order_id, receiver_district_id, ds) detail
         left join
     (select id        org_id,
             org_name,
             region_id district_id
      from dim_organ_full
      where ds = '20250720') organ
     on detail.receiver_district_id = organ.district_id
         left join
     (select id,
             parent_id city_id
      from dim_region_full
      where ds = '20250720') district
     on district_id = district.id
         left join
     (select id,
             name,
             parent_id province_id
      from dim_region_full
      where ds = '20250720') city
     on city_id = city.id
         left join
     (select id,
             name
      from dim_region_full
      where ds = '20250720') province
     on province_id = province.id
group by org_id,
         org_name,
         city_id,
         city.name,
         province_id,
         province.name,
         ds;

select * from dws_trans_org_deliver_suc_1d;

drop table if exists dws_trans_org_receive_1d;
create external table dws_trans_org_receive_1d(
      `org_id` bigint comment '转运站ID',
      `org_name` string comment '转运站名称',
      `city_id` bigint comment '城市ID',
      `city_name` string comment '城市名称',
      `province_id` bigint comment '省份ID',
      `province_name` string comment '省份名称',
      `order_count` bigint comment '揽收次数',
      `order_amount` decimal(16, 2) comment '揽收金额'
) comment '物流域转运站粒度揽收 1 日汇总表'
partitioned by (`ds` string comment '统计日期')
stored as orc
location '/warehouse/tms/dws/dws_trans_org_receive_1d/'
tblproperties ('orc.compress'='snappy');

set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dws_trans_org_receive_1d partition (ds)
select org_id,
       org_name,
       city_id,
       city_name,
       province_id,
       province_name,
       count(order_id)      order_count,
       sum(distinct_amount) order_amount,
       ds
from (select order_id,
             org_id,
             org_name,
             city_id,
             city_name,
             province_id,
             province_name,
             max(amount) distinct_amount,
             ds
      from (select order_id,
                   amount,
                   sender_district_id,
                   ds
            from dwd_trans_receive_detail_inc) detail
               left join
           (select id org_id,
                   org_name,
                   region_id
            from dim_organ_full
            where ds = '20250720') organ
           on detail.sender_district_id = organ.region_id
               left join
           (select id,
                   parent_id
            from dim_region_full
            where ds = '20250720') district
           on region_id = district.id
               left join
           (select id   city_id,
                   name city_name,
                   parent_id
            from dim_region_full
            where ds = '20250720') city
           on district.parent_id = city_id
               left join
           (select id   province_id,
                   name province_name,
                   parent_id
            from dim_region_full
            where ds = '20250720') province
           on city.parent_id = province_id
      group by order_id,
               org_id,
               org_name,
               city_id,
               city_name,
               province_id,
               province_name,
               ds) distinct_tb
group by org_id,
         org_name,
         city_id,
         city_name,
         province_id,
         province_name,
         ds;

select * from dws_trans_org_receive_1d;

drop table if exists dws_trans_org_sort_1d;
create external table dws_trans_org_sort_1d(
      `org_id` bigint comment '机构ID',
      `org_name` string comment '机构名称',
      `city_id` bigint comment '城市ID',
      `city_name` string comment '城市名称',
      `province_id` bigint comment '省份ID',
      `province_name` string comment '省份名称',
      `sort_count` bigint comment '分拣次数'
) comment '物流域机构粒度分拣 1 日汇总表'
partitioned by (`ds` string comment '统计日期')
stored as orc
location '/warehouse/tms/dws/dws_trans_org_sort_1d/'
tblproperties('orc.compress'='snappy');

set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dws_trans_org_sort_1d partition (ds)
select org_id,
       org_name,
       if(org_level = 1, city_for_level1.id, province_for_level1.id)         city_id,
       if(org_level = 1, city_for_level1.name, province_for_level1.name)     city_name,
       if(org_level = 1, province_for_level1.id, province_for_level2.id)     province_id,
       if(org_level = 1, province_for_level1.name, province_for_level2.name) province_name,
       sort_count,
       ds
from (select org_id,
             count(*) sort_count,
             ds
      from dwd_bound_sort_inc
      group by org_id, ds) agg
         left join
     (select id,
             org_name,
             org_level,
             region_id
      from dim_organ_full
      where ds = '20250720') org
     on org_id = org.id
         left join
     (select id,
             name,
             parent_id
      from dim_region_full
      where ds = '20250720') city_for_level1
     on region_id = city_for_level1.id
         left join
     (select id,
             name,
             parent_id
      from dim_region_full
      where ds = '20250720') province_for_level1
     on city_for_level1.parent_id = province_for_level1.id
         left join
     (select id,
             name,
             parent_id
      from dim_region_full
      where ds = '20250720') province_for_level2
     on province_for_level1.parent_id = province_for_level2.id;

select * from dws_trans_org_sort_1d;

drop table if exists dws_trans_org_truck_model_type_trans_finish_1d;
create external table dws_trans_org_truck_model_type_trans_finish_1d(
      `org_id` bigint comment '机构ID',
      `org_name` string comment '机构名称',
      `truck_model_type` string comment '卡车类别编码',
      `truck_model_type_name` string comment '卡车类别名称',
      `trans_finish_count` bigint comment '运输完成次数',
      `trans_finish_distance` decimal(16,2) comment '运输完成里程',
      `trans_finish_dur_sec` bigint comment '运输完成时长，单位：秒'
) comment '物流域机构卡车类别粒度运输最近 1 日汇总表'
partitioned by (`ds` string comment '统计日期')
stored as orc
location '/warehouse/tms/dws/dws_trans_org_truck_model_type_trans_finish_1d/'
tblproperties('orc.compress'='snappy');

set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dws_trans_org_truck_model_type_trans_finish_1d partition (ds)
select org_id,
       org_name,
       truck_model_type,
       truck_model_type_name,
       count(trans_finish.id) truck_finish_count,
       sum(actual_distance)   trans_finish_distance,
       sum(finish_dur_sec)    trans_finish_dur_sec,
       ds
from (select id,
             start_org_id   org_id,
             start_org_name org_name,
             truck_id,
             actual_distance,
             finish_dur_sec,
             ds
      from dwd_trans_trans_finish_inc) trans_finish
         left join
     (select id,
             truck_model_type,
             truck_model_type_name
      from dim_truck_full
      where ds = '20250720') truck_info
     on trans_finish.truck_id = truck_info.id
group by org_id,
         org_name,
         truck_model_type,
         truck_model_type_name,
         ds;

select * from dws_trans_org_truck_model_type_trans_finish_1d;

drop table if exists dws_trade_org_cargo_type_order_nd;
create external table dws_trade_org_cargo_type_order_nd(
      `org_id` bigint comment '机构ID',
      `org_name` string comment '转运站名称',
      `city_id` bigint comment '城市ID',
      `city_name` string comment '城市名称',
      `cargo_type` string comment '货物类型',
      `cargo_type_name` string comment '货物类型名称',
      `recent_days` tinyint comment '最近天数',
      `order_count` bigint comment '下单数',
      `order_amount` decimal(16,2) comment '下单金额'
) comment '交易域机构货物类型粒度下单 n 日汇总表'
partitioned by(`ds` string comment '统计日期')
stored as orc
location '/warehouse/tms/dws/dws_trade_org_cargo_type_order_nd'
tblproperties('orc.compress' = 'snappy');

insert overwrite table dws_trade_org_cargo_type_order_nd partition (ds = '20250720')
select
    org_id,
    org_name,
    city_id,
    city_name,
    cargo_type,
    cargo_type_name,
    recent_days,
    sum(order_count) as order_count,
    sum(order_amount) as order_amount
from dws_trade_org_cargo_type_order_1d
         lateral view explode(array(7, 30)) tmp as recent_days
-- 关键：将计算的起始日期转换为 yyyyMMdd 格式，与源表 ds 匹配
where ds >= regexp_replace(
    date_add('2025-07-20', -recent_days + 1),  -- 先按 yyyy-MM-dd 计算
    '-', ''  -- 去除分隔符，转为 yyyyMMdd
    )
  and ds <= '20250720'  -- 不超过统计日期
group by
    org_id, org_name, city_id, city_name, cargo_type, cargo_type_name, recent_days;

select * from dws_trade_org_cargo_type_order_nd;

drop table if exists dws_trans_bound_finish_td;
create external table dws_trans_bound_finish_td(
      `order_count` bigint comment '发单数',
      `order_amount` decimal(16,2) comment '发单金额'
) comment '物流域转运完成历史至今汇总表'
partitioned by (`ds` string comment '统计日期')
stored as orc
location 'warehouse/tms/dws/dws_trans_bound_finish_td'
tblproperties('orc.compress'='snappy');

insert overwrite table dws_trans_bound_finish_td partition (ds = '20250720')
select count(order_id)   order_count,
       sum(order_amount) order_amount
from (select order_id,
             max(amount) order_amount
      from dwd_trans_bound_finish_detail_inc
      group by order_id) distinct_info;

select * from dws_trans_bound_finish_td;

drop table if exists dws_trans_dispatch_nd;
create external table dws_trans_dispatch_nd(
      `recent_days` tinyint comment '最近天数',
      `order_count` bigint comment '发单总数',
      `order_amount` decimal(16,2) comment '发单总金额'
) comment '物流域发单 1 日汇总表'
partitioned by (`ds` string comment '统计日期')
stored as orc
location '/warehouse/tms/dws/dws_trans_dispatch_nd/'
tblproperties('orc.compress'='snappy');

insert overwrite table dws_trans_dispatch_nd partition (ds = '20250720')
select
    recent_days,
    sum(order_count) as order_count,
    sum(order_amount) as order_amount
from dws_trans_dispatch_1d
         lateral view explode(array(7, 30)) tmp as recent_days
where ds >= regexp_replace(
    date_add('2025-07-20', -recent_days + 1),
    '-', ''
    )
  and ds <= '20250720'
group by recent_days;

select * from dws_trans_dispatch_nd;

drop table if exists dws_trans_dispatch_td;
create external table dws_trans_dispatch_td(
      `order_count` bigint comment '发单数',
      `order_amount` decimal(16,2) comment '发单金额'
) comment '物流域发单历史至今汇总表'
partitioned by (`ds` string comment '统计日期')
stored as orc
location '/warehouse/tms/dws/dws_trans_dispatch_td'
tblproperties('orc.compress'='snappy');

insert overwrite table dws_trans_dispatch_td partition (ds = '20250720')
select sum(order_count)  order_count,
       sum(order_amount) order_amount
from dws_trans_dispatch_1d;

select * from dws_trans_dispatch_td;

drop table if exists dws_trans_org_deliver_suc_nd;
create external table dws_trans_org_deliver_suc_nd(
      `org_id` bigint comment '转运站ID',
      `org_name` string comment '转运站名称',
      `city_id` bigint comment '城市ID',
      `city_name` string comment '城市名称',
      `province_id` bigint comment '省份ID',
      `province_name` string comment '省份名称',
      `recent_days` tinyint comment '最近天数',
      `order_count` bigint comment '派送成功次数（订单数）'
) comment '物流域转运站粒度派送成功 n 日汇总表'
partitioned by (`ds` string comment '统计日期')
stored as orc
location '/warehouse/tms/dws/dws_trans_org_deliver_suc_nd/'
tblproperties('orc.compress'='snappy');

insert overwrite table dws_trans_org_deliver_suc_nd partition (ds = '20250720')
select
    org_id,
    org_name,
    city_id,
    city_name,
    province_id,
    province_name,
    recent_days,
    sum(order_count) as order_count
from dws_trans_org_deliver_suc_1d
         lateral view explode(array(7, 30)) tmp as recent_days
where ds >= regexp_replace(
    date_add('2025-07-20', -recent_days + 1),
    '-', ''
    )
  and ds <= '20250720'
group by
    org_id,
    org_name,
    city_id,
    city_name,
    province_id,
    province_name,
    recent_days;

select * from dws_trans_org_deliver_suc_nd;

drop table if exists dws_trans_org_receive_nd;
create external table dws_trans_org_receive_nd(
      `org_id` bigint comment '转运站ID',
      `org_name` string comment '转运站名称',
      `city_id` bigint comment '城市ID',
      `city_name` string comment '城市名称',
      `province_id` bigint comment '省份ID',
      `province_name` string comment '省份名称',
      `recent_days` tinyint comment '最近天数',
      `order_count` bigint comment '揽收次数',
      `order_amount` decimal(16, 2) comment '揽收金额'
) comment '物流域转运站粒度揽收 n 日汇总表'
partitioned by (`ds` string comment '统计日期')
stored as orc
location '/warehouse/tms/dws/dws_trans_org_receive_nd/'
tblproperties ('orc.compress'='snappy');

insert overwrite table dws_trans_org_receive_nd partition (ds = '20250720')
-- 7天汇总
select
    org_id, org_name, city_id, city_name, province_id, province_name,
    7 as recent_days,
    sum(order_count) as order_count,
    sum(order_amount) as order_amount
from dws_trans_org_receive_1d
where ds between '20250714' and '20250720'  -- 直接用yyyyMMdd格式
group by org_id, org_name, city_id, city_name, province_id, province_name

union all

-- 30天汇总
select
    org_id, org_name, city_id, city_name, province_id, province_name,
    30 as recent_days,
    sum(order_count) as order_count,
    sum(order_amount) as order_amount
from dws_trans_org_receive_1d
where ds between '20250621' and '20250720'  -- 直接用yyyyMMdd格式
group by org_id, org_name, city_id, city_name, province_id, province_name;

select * from dws_trans_org_receive_nd;

drop table if exists dws_trans_org_sort_nd;
create external table dws_trans_org_sort_nd(
      `org_id` bigint comment '机构ID',
      `org_name` string comment '机构名称',
      `city_id` bigint comment '城市ID',
      `city_name` string comment '城市名称',
      `province_id` bigint comment '省份ID',
      `province_name` string comment '省份名称',
      `recent_days` tinyint comment '最近天数',
      `sort_count` bigint comment '分拣次数'
) comment '物流域机构粒度分拣 n 日汇总表'
partitioned by (`ds` string comment '统计日期')
stored as orc
location '/warehouse/tms/dws/dws_trans_org_sort_nd/'
tblproperties('orc.compress'='snappy');

set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dws_trans_org_sort_nd partition (ds = '20250720')
select
    org_id,
    org_name,
    city_id,
    city_name,
    province_id,
    province_name,
    recent_days,
    sum(sort_count) as sort_count
from dws_trans_org_sort_1d
         lateral view explode(array(7, 30)) tmp as recent_days
where
-- 修正日期格式并转换为相同格式进行比较
    ds >= regexp_replace(
    date_add('2025-07-20', -recent_days + 1),  -- 先计算正确的日期
    '-', ''  -- 移除分隔符，转为 yyyyMMdd 格式与源表匹配
    )
group by
    org_id,
    org_name,
    city_id,
    city_name,
    province_id,
    province_name,
    recent_days;

select * from dws_trans_org_sort_nd;

drop table if exists dws_trans_shift_trans_finish_nd;
create external table dws_trans_shift_trans_finish_nd(
      `shift_id` bigint comment '班次ID',
      `city_id` bigint comment '城市ID',
      `city_name` string comment '城市名称',
      `org_id` bigint comment '机构ID',
      `org_name` string comment '机构名称',
      `line_id` bigint comment '线路ID',
      `line_name` string comment '线路名称',
      `driver1_emp_id` bigint comment '第一司机员工ID',
      `driver1_name` string comment '第一司机姓名',
      `driver2_emp_id` bigint comment '第二司机员工ID',
      `driver2_name` string comment '第二司机姓名',
      `truck_model_type` string comment '卡车类别编码',
      `truck_model_type_name` string comment '卡车类别名称',
      `recent_days` tinyint comment '最近天数',
      `trans_finish_count` bigint comment '转运完成次数',
      `trans_finish_distance` decimal(16,2) comment '转运完成里程',
      `trans_finish_dur_sec` bigint comment '转运完成时长，单位：秒',
      `trans_finish_order_count` bigint comment '转运完成运单数',
      `trans_finish_delay_count` bigint comment '逾期次数'
) comment '物流域班次粒度转运完成最近 n 日汇总表'
partitioned by (`ds` string comment '统计日期')
stored as orc
location '/warehouse/tms/dws/dws_trans_shift_trans_finish_nd/'
tblproperties('orc.compress'='snappy');

insert overwrite table dws_trans_shift_trans_finish_nd partition (ds = '20250720')
select
    aggregated.shift_id,
    if(first.org_level = 1, first.region_id, city.id) as city_id,
    if(first.org_level = 1, first.region_name, city.name) as city_name,
    aggregated.org_id,
    aggregated.org_name,
    aggregated.line_id,
    for_line_name.line_name,
    aggregated.driver1_emp_id,
    aggregated.driver1_name,
    aggregated.driver2_emp_id,
    aggregated.driver2_name,
    truck_info.truck_model_type,
    truck_info.truck_model_type_name,
    30 as recent_days,  -- 明确统计最近30天
    sum(aggregated.trans_finish_count) as trans_finish_count,
    sum(aggregated.trans_finish_distance) as trans_finish_distance,
    sum(aggregated.trans_finish_dur_sec) as trans_finish_dur_sec,
    sum(aggregated.trans_finish_order_count) as trans_finish_order_count,
    sum(aggregated.trans_finish_delay_count) as trans_finish_delay_count
from (
         select
             shift_id,
             line_id,
             truck_id,
             start_org_id as org_id,
             start_org_name as org_name,
             driver1_emp_id,
             driver1_name,
             driver2_emp_id,
             driver2_name,
             count(id) as trans_finish_count,
             sum(actual_distance) as trans_finish_distance,
             sum(finish_dur_sec) as trans_finish_dur_sec,
             sum(order_num) as trans_finish_order_count,
             sum(if(actual_end_time > estimate_end_time, 1, 0)) as trans_finish_delay_count
         from dwd_trans_trans_finish_inc
         where ds between date_sub('2023-01-06', 29) and '2023-01-06'  -- 计算最近30天
         group by
             shift_id, line_id, truck_id, start_org_id, start_org_name,
             driver1_emp_id, driver1_name, driver2_emp_id, driver2_name
     ) aggregated
-- 使用与目标表相同的日期分区
         left join dim_organ_full first
on aggregated.org_id = first.id and first.ds = '20250720'
    left join dim_region_full parent
    on first.region_id = parent.id and parent.ds = '20250720'
    left join dim_region_full city
    on parent.parent_id = city.id and city.ds = '20250720'
    left join dim_shift_full for_line_name
    on aggregated.shift_id = for_line_name.id and for_line_name.ds = '20250720'
    left join dim_truck_full truck_info
    on aggregated.truck_id = truck_info.id and truck_info.ds = '20250720'
group by
    aggregated.shift_id,
    if(first.org_level = 1, first.region_id, city.id),
    if(first.org_level = 1, first.region_name, city.name),
    aggregated.org_id,
    aggregated.org_name,
    aggregated.line_id,
    for_line_name.line_name,
    aggregated.driver1_emp_id,
    aggregated.driver1_name,
    aggregated.driver2_emp_id,
    aggregated.driver2_name,
    truck_info.truck_model_type,
    truck_info.truck_model_type_name;

select * from dws_trans_shift_trans_finish_nd;
