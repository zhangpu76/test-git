package com.liuliang;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class MySQLToKafkaLogSync {
    // 配置参数（省略，与之前一致）
    private static final String MYSQL_HOST = "cdh01";
    private static final int MYSQL_PORT = 3306;
    private static final String MYSQL_USER = "root";
    private static final String MYSQL_PASSWORD = "123456";
    private static final String MYSQL_DATABASE = "traffic_board_db";
    private static final String MYSQL_LOG_TABLE = "ods_traffic_behavior_raw_log";
    private static final String KAFKA_BOOTSTRAP_SERVERS = "cdh01:9092,cdh02:9092,cdh03:9092";
    private static final String KAFKA_TARGET_TOPIC = "traffic_board_log";
    private static final String FLINK_CHECKPOINT_DIR = "hdfs://cdh01:8020/flink/checkpoints/mysql-log-sync";

    public static void main(String[] args) throws Exception {
        // 1. 初始化Flink执行环境（省略，与之前一致）
        Configuration flinkConfig = new Configuration();
        flinkConfig.setString("state.backend", "filesystem");
        flinkConfig.setString("state.checkpoints.dir", FLINK_CHECKPOINT_DIR);
        flinkConfig.setBoolean("classloader.check-leaked-classloader", false);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(flinkConfig);

        // 2. 配置容错策略（关键修复：修改ExternalizedCheckpointCleanup的包路径）
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                3,
                TimeUnit.SECONDS.toMillis(5)
        ));
        env.enableCheckpointing(30000);
        env.getCheckpointConfig().setCheckpointTimeout(600000);
        // 修复：使用新包路径的ExternalizedCheckpointCleanup
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        );

        // 3. 后续逻辑（MySQL CDC数据源、数据处理、Kafka Sink等，与之前一致）
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname(MYSQL_HOST)
                .port(MYSQL_PORT)
                .username(MYSQL_USER)
                .password(MYSQL_PASSWORD)
                .databaseList(MYSQL_DATABASE)
                .tableList(MYSQL_DATABASE + "." + MYSQL_LOG_TABLE)
                .startupOptions(StartupOptions.initial())
                .connectionPoolSize(5)
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();

        DataStream<String> cdcRawStream = env.fromSource(
                mySqlSource,
                WatermarkStrategy.noWatermarks(),
                "MySQL-Log-CDC-Source"
        );
        cdcRawStream.print("【1. 原始CDC数据】: ");

        // 数据过滤、转换、Kafka写入等逻辑（省略，与之前一致）
        DataStream<String> kafkaLogStream = cdcRawStream
                .filter(new FilterFunction<String>() {
                    @Override
                    public boolean filter(String cdcJson) {
                        // 过滤逻辑不变
                        try {
                            JSONObject json = new JSONObject(cdcJson);
                            String op = json.optString("op", "");
                            JSONObject after = json.optJSONObject("after");
                            boolean keep = ("r".equals(op) || "c".equals(op) || "u".equals(op)) && after != null;
                            if (keep) {
                                System.out.printf("【过滤通过】op=%s, 数据预览=%s%n", op, cdcJson.substring(0, 100) + "...");
                            } else {
                                System.out.printf("【过滤丢弃】op=%s, 数据预览=%s%n", op, cdcJson.substring(0, 100) + "...");
                            }
                            return keep;
                        } catch (JSONException e) {
                            System.err.printf("【过滤异常】JSON解析失败，数据=%s%n", cdcJson);
                            return false;
                        }
                    }
                }).map(new MapFunction<String, String>() {
                    @Override
                    public String map(String cdcJson) {
                        // 转换逻辑不变
                        try {
                            JSONObject json = new JSONObject(cdcJson);
                            JSONObject after = json.getJSONObject("after");
                            System.out.printf("【转换完成】原始数据预览=%s → 目标数据预览=%s%n",
                                    cdcJson.substring(0, 80) + "...",
                                    after.toString().substring(0, 80) + "...");
                            return after.toString();
                        } catch (JSONException e) {
                            JSONObject errorLog = new JSONObject();
                            errorLog.put("log_type", "exception");
                            errorLog.put("error_type", "after_parse_fail");
                            errorLog.put("error_msg", e.getMessage());
                            errorLog.put("original_cdc_data", cdcJson.substring(0, 200) + "...");
                            errorLog.put("sync_time", System.currentTimeMillis());
                            System.err.printf("【转换异常】生成异常日志=%s%n", errorLog);
                            return errorLog.toString();
                        }
                    }
                });

        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers(KAFKA_BOOTSTRAP_SERVERS)
                .setRecordSerializer(KafkaRecordSerializationSchema.<String>builder()
                        .setTopic(KAFKA_TARGET_TOPIC)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setDeliveryGuarantee(org.apache.flink.connector.base.DeliveryGuarantee.AT_LEAST_ONCE)
                .setKafkaProducerConfig(buildKafkaProducerProps())
                .build();

        kafkaLogStream.sinkTo(kafkaSink).name("MySQL-Log-To-Kafka-Sink");
        env.execute("MySQL Log Table Full+Incremental Sync To Kafka");
    }

    private static Properties buildKafkaProducerProps() {
        Properties props = new Properties();
        props.setProperty("linger.ms", "500");
        props.setProperty("batch.size", "16384");
        props.setProperty("buffer.memory", "33554432");
        props.setProperty("retries", "3");
        return props;
    }
}