package com.liuliang;

import com.stream.common.base.BaseSQLApp;
import com.stream.common.constant.Constant;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * 页面行为-用户-地域-渠道宽表（支撑《大数据-用户画像-03-流量主题流量看板V1.2-20250115.pdf》核心指标）
 * 覆盖指标：店铺访客数（新老拆分）、页面流量排行TOP10、店铺浏览量（分端）、城市分布排行
 */
public class DwdPageUserRegionChannel extends BaseSQLApp {
    // 目标DWD层Kafka主题（依据文档指标需求定义）
    private static final String TARGET_KAFKA_TOPIC = "dwd_page_user_region_channel";

    public static void main(String[] args) {
        // 启动参数：端口号10014、并行度4、目标主题（适配文档流量看板数据消费）
        new DwdPageUserRegionChannel().start(10014, 4, TARGET_KAFKA_TOPIC);
    }

    /**
     * 核心处理入口（public方法，封装全流程逻辑）
     * 流程：配置环境→读取数据源→关联维度表→写入目标Kafka
     */
    @Override
    public void handle(StreamTableEnvironment tableEnv) {
        // 1. 初始化执行环境与检查点（适配文档对数据准确性的要求）
        initEnvConfig(tableEnv);

        // 2. 读取数据源：Kafka页面日志 + HBase维度表（public方法暴露核心数据源读取）
        readOdsPageLogFromKafka(tableEnv);
        readDimUserFromHBase(tableEnv);
        readDimCityFromHBase(tableEnv);
        readDimChannelFromHBase(tableEnv);

        // 3. 过滤有效页面日志（public方法，依据文档指标过滤无效数据）
        Table validPageLog = filterValidPageLog(tableEnv);

        // 4. 关联维度表生成宽表（public方法，支撑文档多维度指标计算）
        Table dwdWideTable = joinDimensionTables(tableEnv, validPageLog);

        // 5. 写入目标Kafka主题（public方法，供文档流量看板消费）
        writeToTargetKafka(tableEnv, dwdWideTable);
    }

    /**
     * 初始化执行环境与检查点配置（public方法，保障数据一致性）
     * 依据文档指标对“数据准确性”的要求，配置精确一次语义与状态管理
     */
    public void initEnvConfig(StreamTableEnvironment tableEnv) {
        // 基础参数：并行度、时区（匹配文档日志时间范围）
        tableEnv.getConfig().setParallelism(4);
        tableEnv.getConfig().setLocalTimeZone(java.time.ZoneId.of("Asia/Shanghai"));
        tableEnv.getConfig().setIdleStateRetention(Duration.ofDays(1));

        // 检查点配置（精确一次语义，避免文档指标重复计算）
        tableEnv.getConfig().getConfiguration().setString(
                "execution.checkpointing.interval", "30000"
        );
        tableEnv.getConfig().getConfiguration().setString(
                "execution.checkpointing.mode", "EXACTLY_ONCE"
        );
        tableEnv.getConfig().getConfiguration().setString(
                "state.backend", "rocksdb"
        );
        tableEnv.getConfig().getConfiguration().setString(
                "state.checkpoints.dir", "hdfs://hadoop-cluster/flink/checkpoints/dwd_page_user_region_channel"
        );
    }

    /**
     * 从Kafka读取ODS层页面日志（public方法，文档“页面流量排行”指标的核心行为数据源）
     * 日志主题：traffic_page_log（与实际Kafka主题一致）
     */
    public void readOdsPageLogFromKafka(StreamTableEnvironment tableEnv) {
        String createKafkaTableSql = "CREATE TABLE ods_page_log (\n" +
                "    -- 公共模块：支撑文档“访客数、渠道、地域”指标\n" +
                "    common STRUCT<\n" +
                "        uid STRING,        -- 用户ID（关联user_base，支撑人群特征）\n" +
                "        mid STRING,        -- 设备ID（未登录用户，补充访客数统计）\n" +
                "        ch STRING,         -- 渠道编码（关联channel_dict，支撑流量来源排行）\n" +
                "        is_new STRING,     -- 是否新用户（1=新/0=老，文档“新老访客”统计）\n" +
                "        platform STRING,   -- 访问端（PC/无线，文档“分端浏览量”统计）\n" +
                "        ar STRING          -- 地区码（关联city_dict，文档“城市分布”排行）\n" +
                "    >,\n" +
                "    -- 页面模块：支撑文档“页面流量排行、停留时长”指标\n" +
                "    page STRUCT<\n" +
                "        page_id STRING,    -- 页面ID（如home/cart，文档“页面排行TOP10”核心字段）\n" +
                "        page_title STRING, -- 页面标题（如“购物车”，文档页面识别需求）\n" +
                "        during_time BIGINT,-- 页面停留时长（毫秒，文档行为深度分析）\n" +
                "        view_count INT     -- 浏览次数（文档“店铺浏览量”统计）\n" +
                "    >,\n" +
                "    ts BIGINT,           -- 日志时间戳（文档“实时/日/周”指标时间维度）\n" +
                "    pt AS PROCTIME(),    -- 处理时间（用于HBase维度表Lookup Join）\n" +
                "    -- 水印：容忍3秒乱序，适配日志传输延迟\n" +
                "    WATERMARK FOR ts AS ts - INTERVAL '3' SECOND\n" +
                ") WITH (\n" +
                "    'connector' = 'kafka',\n" +
                "    'topic' = 'traffic_page_log',\n" +
                "    'properties.bootstrap.servers' = '" + Constant.KAFKA_BOOTSTRAP_SERVERS + "',\n" +
                "    'properties.group.id' = 'dwd_page_log_group',\n" +
                "    'format' = 'json',\n" +
                "    'json.fail-on-missing-field' = 'false', -- 容忍缺失字段（如部分日志无page_title）\n" +
                "    'json.ignore-parse-errors' = 'true'    -- 忽略解析错误，避免任务中断\n" +
                ")";
        tableEnv.executeSql(createKafkaTableSql);
    }

    /**
     * 从HBase读取用户维度表（public方法，文档“人群特征-性别/年龄分布”指标支撑）
     * HBase表：dw_dim:user_base（命名空间+表名，与实际一致）
     */
    public void readDimUserFromHBase(StreamTableEnvironment tableEnv) {
        String createHBaseTableSql = "CREATE TABLE dim_user_base_hbase (\n" +
                "    rowkey STRING PRIMARY KEY, -- 行键=user_base.uid\n" +
                "    info ROW<\n" +
                "        gender TINYINT,        -- 性别（0=未知/1=男/2=女，文档“性别分布”）\n" +
                "        age TINYINT,           -- 年龄（文档“年龄分布”指标）\n" +
                "        user_status TINYINT    -- 用户状态（1=正常，过滤禁用用户）\n" +
                "    >\n" +
                ") WITH (\n" +
                "    'connector' = 'hbase-1.4',\n" +
                "    'table-name' = 'dw_dim:user_base',\n" +
                "    'zookeeper.quorum' = '" + Constant.ZOOKEEPER_QUORUM + "',\n" +
                "    'lookup.cache.max-rows' = '10000', -- 缓存1万条用户数据\n" +
                "    'lookup.cache.ttl' = '60000'       -- 缓存1分钟，适配用户信息更新\n" +
                ")";
        tableEnv.executeSql(createHBaseTableSql);
    }

    /**
     * 从HBase读取城市字典表（public方法，文档“城市分布排行”指标支撑）
     * HBase表：dw_dim:city_dict（命名空间+表名，与实际一致）
     */
    public void readDimCityFromHBase(StreamTableEnvironment tableEnv) {
        String createHBaseTableSql = "CREATE TABLE dim_city_dict_hbase (\n" +
                "    rowkey STRING PRIMARY KEY, -- 行键=city_dict.area_code（匹配日志common.ar）\n" +
                "    info ROW<\n" +
                "        city_name STRING,      -- 城市名称（如“北京市”，文档“城市分布”）\n" +
                "        province_name STRING   -- 省份名称（如“北京市”，文档省级分析）\n" +
                "    >\n" +
                ") WITH (\n" +
                "    'connector' = 'hbase-1.4',\n" +
                "    'table-name' = 'dw_dim:city_dict',\n" +
                "    'zookeeper.quorum' = '" + Constant.ZOOKEEPER_QUORUM + "',\n" +
                "    'lookup.cache.max-rows' = '500', -- 城市数量有限，缓存500条\n" +
                "    'lookup.cache.ttl' = '86400000'  -- 缓存1天，行政区划更新极少\n" +
                ")";
        tableEnv.executeSql(createHBaseTableSql);
    }

    /**
     * 从HBase读取渠道字典表（public方法，文档“流量来源排行TOP10”指标支撑）
     * HBase表：dw_dim:channel_dict（命名空间+表名，与实际一致）
     */
    public void readDimChannelFromHBase(StreamTableEnvironment tableEnv) {
        String createHBaseTableSql = "CREATE TABLE dim_channel_dict_hbase (\n" +
                "    rowkey STRING PRIMARY KEY, -- 行键=channel_dict.channel_code（匹配日志common.ch）\n" +
                "    info ROW<\n" +
                "        channel_name STRING,    -- 渠道名称（如“豌豆荚”，文档“流量来源排行”）\n" +
                "        channel_type TINYINT    -- 渠道类型（1=应用商店，文档渠道分类分析）\n" +
                "    >\n" +
                ") WITH (\n" +
                "    'connector' = 'hbase-1.4',\n" +
                "    'table-name' = 'dw_dim:channel_dict',\n" +
                "    'zookeeper.quorum' = '" + Constant.ZOOKEEPER_QUORUM + "',\n" +
                "    'lookup.cache.max-rows' = '100', -- 渠道数量有限，缓存100条\n" +
                "    'lookup.cache.ttl' = '3600000'  -- 缓存1小时，渠道信息更新少\n" +
                ")";
        tableEnv.executeSql(createHBaseTableSql);
    }

    /**
     * 过滤有效页面日志（public方法，保障文档指标数据质量）
     * 过滤规则：排除异常时间、无效页面ID、无效浏览次数
     */
    public Table filterValidPageLog(StreamTableEnvironment tableEnv) {
        String filterSql = "SELECT\n" +
                "    common.uid,\n" +
                "    common.mid,\n" +
                "    common.ch AS channel_code,\n" +
                "    common.is_new,\n" +
                "    common.platform,\n" +
                "    common.ar AS area_code,\n" +
                "    page.page_id,\n" +
                "    page.page_title,\n" +
                "    page.during_time,\n" +
                "    page.view_count,\n" +
                "    ts,\n" +
                "    pt\n" +
                "FROM ods_page_log\n" +
                "WHERE\n" +
                "    -- 仅保留2025年1月1日之后的日志（匹配文档时间范围）\n" +
                "    ts > 1735689600000\n" +
                "    -- 页面ID非空（排除异常日志，文档“页面排行”需有效页面）\n" +
                "    AND page.page_id IS NOT NULL\n" +
                "    -- 浏览次数>0（排除无效行为，文档“浏览量”统计需有效）\n" +
                "    AND page.view_count > 0";
        return tableEnv.sqlQuery(filterSql);
    }

    /**
     * 关联维度表生成DWD宽表（public方法，文档多维度指标的核心数据来源）
     * 关联逻辑：页面日志 ←左连→ 用户表 ←左连→ 城市表 ←内连→ 渠道表
     */
    public Table joinDimensionTables(StreamTableEnvironment tableEnv, Table validPageLog) {
        // 注册有效日志为临时视图
        tableEnv.createTemporaryView("valid_page_log", validPageLog);

        // 关联维度表：补充用户、城市、渠道属性
        String joinSql = "SELECT\n" +
                "    -- 唯一标识：避免文档指标重复计算（用户/设备ID+时间戳+页面ID）\n" +
                "    CONCAT(COALESCE(vpl.uid, vpl.mid), '_', vpl.ts, '_', vpl.page_id) AS dwd_log_id,\n" +
                "    -- 时间维度：文档“实时/日/周”指标聚合字段\n" +
                "    vpl.ts,\n" +
                "    DATE_FORMAT(FROM_UNIXTIME(vpl.ts / 1000), 'yyyy-MM-dd') AS stat_date,\n" +
                "    DATE_FORMAT(FROM_UNIXTIME(vpl.ts / 1000), 'HH') AS stat_hour,\n" +
                "    -- 页面行为：文档“页面流量排行、浏览量”指标字段\n" +
                "    vpl.page_id,\n" +
                "    vpl.page_title,\n" +
                "    vpl.during_time,\n" +
                "    vpl.view_count,\n" +
                "    vpl.platform,\n" +
                "    -- 用户维度：文档“人群特征-性别/年龄”指标字段\n" +
                "    vpl.uid,\n" +
                "    vpl.mid,\n" +
                "    vpl.is_new,\n" +
                "    COALESCE(ub.info.gender, 0) AS gender, -- 默认0=未知性别\n" +
                "    COALESCE(ub.info.age, 0) AS age,       -- 默认0=未知年龄\n" +
                "    -- 地域维度：文档“城市分布排行”指标字段\n" +
                "    vpl.area_code,\n" +
                "    COALESCE(ct.info.city_name, '未知城市') AS city_name,\n" +
                "    COALESCE(ct.info.province_name, '未知省份') AS province_name,\n" +
                "    -- 渠道维度：文档“流量来源排行”指标字段\n" +
                "    vpl.channel_code,\n" +
                "    COALESCE(ch.info.channel_name, '未知渠道') AS channel_name,\n" +
                "    COALESCE(ch.info.channel_type, 0) AS channel_type\n" +
                "FROM valid_page_log vpl\n" +
                "-- 左连用户表：保留未登录用户（mid标识），支撑文档“全量访客数”统计\n" +
                "LEFT JOIN dim_user_base_hbase FOR SYSTEM_TIME AS OF vpl.pt ub\n" +
                "    ON vpl.uid = ub.rowkey\n" +
                "    AND ub.info.user_status = 1 -- 过滤禁用用户，保障文档指标有效性\n" +
                "-- 左连城市表：补充地域信息，支撑文档“城市分布”排行\n" +
                "LEFT JOIN dim_city_dict_hbase FOR SYSTEM_TIME AS OF vpl.pt ct\n" +
                "    ON vpl.area_code = ct.rowkey\n" +
                "-- 内连渠道表：过滤无有效渠道的日志，文档“流量来源”需有效渠道\n" +
                "JOIN dim_channel_dict_hbase FOR SYSTEM_TIME AS OF vpl.pt ch\n" +
                "    ON vpl.channel_code = ch.rowkey";
        return tableEnv.sqlQuery(joinSql);
    }
    /**
     * 将关联后的DWD宽表写入目标Kafka主题（public方法，文档流量看板的核心数据消费入口）
     * 目标主题：dwd_page_user_region_channel（与配置一致）
     * 文档依据：支撑“店铺访客数、页面流量排行、城市分布排行”等指标的下游消费
     */
    public void writeToTargetKafka(StreamTableEnvironment tableEnv, Table dwdWideTable) {
        // 1. 注册关联后的宽表为临时视图，便于后续写入操作
        tableEnv.createTemporaryView("dwd_page_user_region_channel_view", dwdWideTable);

        // 2. 定义DWD层Kafka输出表结构（字段与宽表完全对齐，适配文档指标需求）
        String createKafkaSinkTableSql = "CREATE TABLE dwd_page_user_region_channel (\n" +
                "    -- 唯一标识：避免文档指标重复计算（如店铺访客数去重）\n" +
                "    dwd_log_id STRING PRIMARY KEY NOT ENFORCED,\n" +
                "    -- 时间维度：支撑文档“实时/日/周/月”指标聚合（如页面流量排行按日统计）\n" +
                "    ts BIGINT,\n" +
                "    stat_date STRING,\n" +
                "    stat_hour STRING,\n" +
                "    -- 页面行为维度：支撑文档“页面流量排行TOP10”指标（如按page_id统计访客数）\n" +
                "    page_id STRING,\n" +
                "    page_title STRING,\n" +
                "    during_time BIGINT,\n" +
                "    view_count INT,\n" +
                "    platform STRING,\n" +
                "    -- 用户维度：支撑文档“店铺访客数（新老拆分）、人群特征”指标（如按is_new拆分新老访客）\n" +
                "    uid STRING,\n" +
                "    mid STRING,\n" +
                "    is_new STRING,\n" +
                "    gender TINYINT,\n" +
                "    age TINYINT,\n" +
                "    -- 地域维度：支撑文档“城市分布排行”指标（如按city_name统计访客占比）\n" +
                "    area_code STRING,\n" +
                "    city_name STRING,\n" +
                "    province_name STRING,\n" +
                "    -- 渠道维度：支撑文档“流量来源排行TOP10”指标（如按channel_name统计访客数）\n" +
                "    channel_code STRING,\n" +
                "    channel_name STRING,\n" +
                "    channel_type TINYINT\n" +
                ") WITH (\n" +
                "    'connector' = 'upsert-kafka', -- 采用Upsert模式，确保数据唯一性（适配文档指标准确性）\n" +
                "    'topic' = '" + TARGET_KAFKA_TOPIC + "', -- 目标Kafka主题（与定义一致）\n" +
                "    'properties.bootstrap.servers' = '" + Constant.KAFKA_BOOTSTRAP_SERVERS + "', -- Kafka集群地址（从常量获取，确保配置统一）\n" +
                "    'properties.group.id' = 'dwd_page_user_region_channel_sink', -- 消费组ID（用于下游流量看板消费）\n" +
                "    'key.format' = 'json', -- 主键格式（JSON，适配Kafka数据解析）\n" +
                "    'value.format' = 'json', -- 值格式（JSON，便于下游解析字段）\n" +
                "    'key.json.ignore-parse-errors' = 'true', -- 忽略主键解析错误，避免任务中断\n" +
                "    'value.json.ignore-parse-errors' = 'true' -- 忽略值解析错误，保障数据输出连续性\n" +
                ")";
        // 执行创建Kafka输出表SQL
        tableEnv.executeSql(createKafkaSinkTableSql);

        // 3. 将宽表数据写入Kafka主题（支撑文档流量看板实时消费）
        String insertSql = "INSERT INTO dwd_page_user_region_channel\n" +
                "SELECT \n" +
                "    dwd_log_id,\n" +
                "    ts,\n" +
                "    stat_date,\n" +
                "    stat_hour,\n" +
                "    page_id,\n" +
                "    page_title,\n" +
                "    during_time,\n" +
                "    view_count,\n" +
                "    platform,\n" +
                "    uid,\n" +
                "    mid,\n" +
                "    is_new,\n" +
                "    gender,\n" +
                "    age,\n" +
                "    area_code,\n" +
                "    city_name,\n" +
                "    province_name,\n" +
                "    channel_code,\n" +
                "    channel_name,\n" +
                "    channel_type\n" +
                "FROM dwd_page_user_region_channel_view";
        // 执行插入操作，将宽表数据写入Kafka
        tableEnv.executeSql(insertSql);
    }
}