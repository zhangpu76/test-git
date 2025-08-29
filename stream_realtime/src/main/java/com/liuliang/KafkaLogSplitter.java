package com.liuliang;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.json.JSONException;
import org.json.JSONObject;

import java.time.Duration;
import java.util.Properties;

/**
 * 功能：从 Kafka 消费MySQL表（ods_traffic_behavior_raw_log）的记录
 * 解析嵌套在`log`字段中的业务日志，分流为page/click/err/session_start/session_end
 */
public class KafkaLogSplitter {
    // -------------------------- 配置参数（根据实际环境修改）--------------------------
    private static final String KAFKA_BOOTSTRAP_SERVERS = "cdh01:9092,cdh02:9092,cdh03:9092";
    private static final String KAFKA_INPUT_TOPIC = "traffic_board_log"; // 输入：同步MySQL的主题
    // 输出主题
    private static final String OUTPUT_TOPIC_PAGE = "traffic_page_log";
    private static final String OUTPUT_TOPIC_CLICK = "traffic_click_log";
    private static final String OUTPUT_TOPIC_ERR = "traffic_err_log";
    private static final String OUTPUT_TOPIC_SESSION_START = "traffic_session_start";
    private static final String OUTPUT_TOPIC_SESSION_END = "traffic_session_end";
    private static final String OUTPUT_TOPIC_UNKNOWN = "traffic_unknown_log";
    // Flink检查点
    private static final String FLINK_CHECKPOINT_DIR = "hdfs://cdh01:8020/flink/checkpoints/kafka-log-split";

    // 侧输出流标签
    private static final OutputTag<String> TAG_CLICK = new OutputTag<String>("click-log") {};
    private static final OutputTag<String> TAG_ERR = new OutputTag<String>("err-log") {};
    private static final OutputTag<String> TAG_SESSION_START = new OutputTag<String>("session-start") {};
    private static final OutputTag<String> TAG_SESSION_END = new OutputTag<String>("session-end") {};
    private static final OutputTag<String> TAG_UNKNOWN = new OutputTag<String>("unknown-log") {};

    public static void main(String[] args) throws Exception {
        // 1. 初始化Flink环境
        Configuration flinkConfig = new Configuration();
        flinkConfig.setString("state.backend", "filesystem");
        flinkConfig.setString("state.checkpoints.dir", FLINK_CHECKPOINT_DIR);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(flinkConfig);

        // 2. 容错配置
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5000));
        env.enableCheckpointing(30000);
        env.getCheckpointConfig().setCheckpointTimeout(600000);

        // 3. Kafka Source（消费MySQL同步的主题）
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(KAFKA_BOOTSTRAP_SERVERS)
                .setTopics(KAFKA_INPUT_TOPIC)
                .setGroupId("kafka-log-splitter-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // 4. 读取原始数据（MySQL表记录）
        DataStream<String> rawMysqlStream = env.fromSource(
                kafkaSource,
                WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((mysqlRecord, ts) -> {
                            try {
                                // 从MySQL记录的create_time提取时间戳（外层字段）
                                JSONObject mysqlJson = new JSONObject(mysqlRecord);
                                return mysqlJson.optLong("create_time", System.currentTimeMillis());
                            } catch (JSONException e) {
                                System.err.println("【时间戳提取失败】MySQL记录: " + mysqlRecord.substring(0, 100));
                                return System.currentTimeMillis();
                            }
                        }),
                "MySQL-Log-Source"
        );
        rawMysqlStream.print("【1. 原始MySQL记录】: ");

        // 5. 核心：解析嵌套日志并分流（关键修复点）
        SingleOutputStreamOperator<String> pageLogStream = rawMysqlStream.process(
                new ProcessFunction<String, String>() {
                    @Override
                    public void processElement(String mysqlRecord, Context ctx, Collector<String> out) {
                        try {
                            // 步骤1：解析外层MySQL记录（获取id、log、create_time）
                            JSONObject mysqlJson = new JSONObject(mysqlRecord);
                            String mysqlLogField = mysqlJson.optString("log", "").trim(); // 提取嵌套的业务日志字段
                            String mysqlId = mysqlJson.optString("id", "unknown_id"); // MySQL记录ID（用于追溯）

                            // 检查嵌套日志是否为空
                            if (mysqlLogField.isEmpty()) {
                                String msg = "MySQL记录的log字段为空，id=" + mysqlId;
                                ctx.output(TAG_UNKNOWN, buildUnknownLog(mysqlRecord, "empty_log_field", msg));
                                return;
                            }

                            // 步骤2：解析嵌套的业务日志（mysqlLogField是JSON字符串）
                            JSONObject businessLog = new JSONObject(mysqlLogField);
                            // 步骤3：根据业务日志的"存在字段"判断类型（核心逻辑）
                            String logType = getBusinessLogType(businessLog);
                            System.out.printf("【分流核心】MySQL_id=%s, 业务类型=%s%n", mysqlId, logType);

                            // 步骤4：日志增强（合并MySQL元数据+业务日志）
                            JSONObject enrichedLog = enrichBusinessLog(mysqlJson, businessLog, logType);

                            // 步骤5：按类型分流
                            switch (logType) {
                                case "page":
                                    out.collect(enrichedLog.toString()); // 主输出流：page
                                    break;
                                case "click":
                                    ctx.output(TAG_CLICK, enrichedLog.toString()); // 侧输出：click
                                    break;
                                case "err":
                                    ctx.output(TAG_ERR, enrichedLog.toString()); // 侧输出：err（匹配你的样例数据）
                                    break;
                                case "session_start":
                                    ctx.output(TAG_SESSION_START, enrichedLog.toString()); // 侧输出：session_start
                                    break;
                                case "session_end":
                                    ctx.output(TAG_SESSION_END, enrichedLog.toString()); // 侧输出：session_end
                                    break;
                                default:
                                    // 未知类型：记录详细信息便于排查
                                    String unknownMsg = "无法识别业务类型，业务日志包含字段：" + businessLog.keySet();
                                    ctx.output(TAG_UNKNOWN, buildUnknownLog(mysqlRecord, "unknown_business_type", unknownMsg));
                            }

                        } catch (JSONException e) {
                            // 解析异常（外层MySQL记录或内层业务日志格式错误）
                            String errorType = e.getMessage().contains("log") ? "business_log_parse_error" : "mysql_record_parse_error";
                            ctx.output(TAG_UNKNOWN, buildUnknownLog(mysqlRecord, errorType, e.getMessage()));
                            System.err.printf("【解析异常】记录: %s, 错误: %s%n", mysqlRecord.substring(0, 100), e.getMessage());
                        }
                    }

                    /**
                     * 核心方法：根据业务日志的"存在字段"判断类型
                     * - err类型：包含"err"字段（匹配你的样例数据）
                     * - page类型：包含"page"字段
                     * - click类型：包含"click"字段
                     * - session_start：包含"session_start"字段
                     * - session_end：包含"session_end"字段
                     */
                    private String getBusinessLogType(JSONObject businessLog) {
                        if (businessLog.has("err")) {
                            return "err";
                        } else if (businessLog.has("page")) {
                            return "page";
                        } else if (businessLog.has("click")) {
                            return "click";
                        } else if (businessLog.has("session_start")) {
                            return "session_start";
                        } else if (businessLog.has("session_end")) {
                            return "session_end";
                        } else {
                            return "unknown";
                        }
                    }

                    /**
                     * 日志增强：合并MySQL元数据（id/create_time）和业务日志，添加分流信息
                     */
                    private JSONObject enrichBusinessLog(JSONObject mysqlJson, JSONObject businessLog, String logType) {
                        JSONObject enriched = new JSONObject();
                        try {
                            // 1. 添加MySQL元数据（便于追溯原始记录）
                            enriched.put("mysql_id", mysqlJson.optString("id", "unknown_id"));
                            enriched.put("mysql_create_time", mysqlJson.optLong("create_time", System.currentTimeMillis()));
                            // 2. 添加业务日志核心内容
                            enriched.put("business_log", businessLog);
                            // 3. 添加分流元数据
                            enriched.put("split_time", System.currentTimeMillis());
                            enriched.put("split_type", logType);
                            enriched.put("source_table", "ods_traffic_behavior_raw_log");
                        } catch (JSONException e) {
                            System.err.println("【日志增强失败】" + e.getMessage());
                            return businessLog; // 增强失败返回原始业务日志
                        }
                        return enriched;
                    }

                    /**
                     * 构建未知类型日志（包含完整上下文，便于排查）
                     */
                    private String buildUnknownLog(String mysqlRecord, String errorType, String errorMsg) {
                        JSONObject unknownLog = new JSONObject();
                        try {
                            unknownLog.put("error_type", errorType);
                            unknownLog.put("error_msg", errorMsg);
                            unknownLog.put("raw_mysql_record", mysqlRecord.substring(0, Math.min(1000, mysqlRecord.length())) + "...");
                            unknownLog.put("split_time", System.currentTimeMillis());
                            unknownLog.put("source_table", "ods_traffic_behavior_raw_log");
                        } catch (JSONException e) {
                            return "{\"error\":\"构建未知日志失败: " + e.getMessage() + "\", \"raw_record\":\"" + mysqlRecord + "\"}";
                        }
                        return unknownLog.toString();
                    }
                }
        ).name("Business-Log-Splitter");

        // 6. 提取各侧输出流
        DataStream<String> clickStream = pageLogStream.getSideOutput(TAG_CLICK);
        DataStream<String> errStream = pageLogStream.getSideOutput(TAG_ERR);
        DataStream<String> sessionStartStream = pageLogStream.getSideOutput(TAG_SESSION_START);
        DataStream<String> sessionEndStream = pageLogStream.getSideOutput(TAG_SESSION_END);
        DataStream<String> unknownStream = pageLogStream.getSideOutput(TAG_UNKNOWN);

        // 7. 打印调试（重点观察err流是否有数据）
        pageLogStream.print("【2. 分流-page】: ");
        clickStream.print("【2. 分流-click】: ");
        errStream.print("【2. 分流-err】: "); // 你的样例数据会出现在这里
        sessionStartStream.print("【2. 分流-session_start】: ");
        sessionEndStream.print("【2. 分流-session_end】: ");
        unknownStream.print("【2. 分流-未知】: ");

        // 8. 写入Kafka目标主题
        pageLogStream.sinkTo(buildKafkaSink(OUTPUT_TOPIC_PAGE)).name("Page-To-Kafka");
        clickStream.sinkTo(buildKafkaSink(OUTPUT_TOPIC_CLICK)).name("Click-To-Kafka");
        errStream.sinkTo(buildKafkaSink(OUTPUT_TOPIC_ERR)).name("Err-To-Kafka"); // 样例会写入这个主题
        sessionStartStream.sinkTo(buildKafkaSink(OUTPUT_TOPIC_SESSION_START)).name("SessionStart-To-Kafka");
        sessionEndStream.sinkTo(buildKafkaSink(OUTPUT_TOPIC_SESSION_END)).name("SessionEnd-To-Kafka");
        unknownStream.sinkTo(buildKafkaSink(OUTPUT_TOPIC_UNKNOWN)).name("Unknown-To-Kafka");

        // 9. 执行任务
        env.execute("MySQL Business Log Splitter (ods_traffic_behavior_raw_log)");
    }

    /**
     * 通用Kafka Sink构建（统一配置）
     */
    private static KafkaSink<String> buildKafkaSink(String targetTopic) {
        return KafkaSink.<String>builder()
                .setBootstrapServers(KAFKA_BOOTSTRAP_SERVERS)
                .setRecordSerializer(KafkaRecordSerializationSchema.<String>builder()
                        .setTopic(targetTopic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setDeliveryGuarantee(org.apache.flink.connector.base.DeliveryGuarantee.AT_LEAST_ONCE)
                .setKafkaProducerConfig(new Properties() {{
                    put("linger.ms", "500");
                    put("batch.size", "16384");
                    put("retries", "3"); // 重试避免网络波动
                    put("acks", "1"); // 至少1个broker确认接收
                }})
                .build();
    }
}