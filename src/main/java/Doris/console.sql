create table if not exists bigdata_car_analysis_data_ws.ods_mapping_kf_traffic_car_info_dtl(
      id bigint,
      msid varchar(255),
      datatype bigint,
      data varchar(255),
      upexgmsgregister json,  -- 存储第一条数据的upexgmsgregister（第二条无，为NULL）
      vehicleno varchar(255),
      datalen bigint,
      vehiclecolor bigint,
      vec1 bigint,  -- 第二条数据有，第一条无（为NULL）
      vec2 bigint,  -- 第二条数据有，第一条无（为NULL）
      vec3 bigint,  -- 第二条数据有，第一条无（为NULL）
      encrypy bigint,  -- 第二条数据有，第一条无（为NULL）
      altitude bigint,  -- 第二条数据有，第一条无（为NULL）
      alarm json,  -- 第二条数据有，第一条无（为NULL）
      state json,  -- 第二条数据有，第一条无（为NULL）
      lon double,  -- 第二条数据有，第一条无（为NULL）
      lat double,  -- 第二条数据有，第一条无（为NULL）
      msgId bigint,  -- 第二条数据有，第一条无（为NULL）
      direction bigint,  -- 第二条数据有，第一条无（为NULL）
      dateTime varchar(255),  -- 第二条数据有，第一条无（为NULL）
      ds date,  -- 第二条数据有，第一条无（为NULL）
      ts bigint  -- 第二条数据有，第一条无（为NULL）
)
engine=OLAP
DUPLICATE KEY(`id`)  -- 以id作为去重键
PARTITION BY RANGE (ds)()  -- 按日期ds分区（动态创建）
DISTRIBUTED BY HASH(`vehicleno`) BUCKETS 16  -- 按车牌哈希分桶
PROPERTIES (
    "dynamic_partition.enable" = "true",  -- 启用动态分区
    "dynamic_partition.time_unit" = "DAY",  -- 按天分区
    "dynamic_partition.start" = "-180",  -- 保留180天历史分区
    "dynamic_partition.end" = "30",  -- 预创建30天未来分区
    "dynamic_partition.prefix" = "p",  -- 分区名前缀（如p20250730）
    "replication_num" = "1"  -- 副本数（单节点测试用1，生产环境建议3）
);