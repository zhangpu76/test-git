package com.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.stream.common.utils.KafkaUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Set;

public class OdsLogBaseStreamApp {

    public static void main(String[] args) throws Exception {
        // 1. 初始化执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(5000); // 每5秒触发一次Checkpoint
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 2. 从Kafka读取数据（使用修复后的KafkaSource）
        KafkaSource<String> source = KafkaUtils.getKafkaSource(
                "realtime_log",    // 输入topic
                "ods_mysql_tab",   // 消费组ID
                "cdh01:9092"       // Kafka地址
        );

        // 3. 解析JSON数据
        SingleOutputStreamOperator<JSONObject> jsonStream = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka_Source")
                .map(value -> {
                    System.out.println("准备解析数据: " + value);  // 打印待解析的数据
                    return value;
                })
                .map(new MapFunction<String, JSONObject>() {
                    private int count = 0;  // 计数器
                    @Override
                    public JSONObject map(String value) throws Exception {
                        count++;
                        System.out.println("正在解析第 " + count + " 条数据");  // 跟踪解析进度
                        try {
                            return JSON.parseObject(value);
                        } catch (Exception e) {
                            System.err.println("第 " + count + " 条数据解析失败: " + value);
                            return null;
                        }
                    }
                })
                .filter(json -> json != null)  // 过滤解析失败的数据
                .name("Parse_JSON");

        jsonStream.map(json -> {
            Set<String> keys = json.keySet();
            System.out.println("JSON字段列表: " + keys); // 打印所有字段，确认是否有start/page等
            return json;
        }).name("Debug_Print_Json_Fields");
        // 4.1 启动日志（增加打印）
        SingleOutputStreamOperator<String> startLogStream = jsonStream
                .filter(json -> {
                    boolean hasStart = json.containsKey("start");
                    if (hasStart) {
                        System.out.println("匹配启动日志: " + json.toJSONString()); // 打印匹配的数据
                    }
                    return hasStart;
                })
                .map(JSON::toJSONString)
                .name("Filter_And_Convert_Start_Log");

// 4.2 页面日志（增加打印）
        SingleOutputStreamOperator<String> pageLogStream = jsonStream
                .filter(json -> {
                    boolean hasPage = json.containsKey("page");
                    if (hasPage) {
                        System.out.println("匹配页面日志: " + json.toJSONString());
                    }
                    return hasPage;
                })
                .map(JSON::toJSONString)
                .name("Filter_And_Convert_Page_Log");

// 4.3 展示日志（同理增加打印）
        SingleOutputStreamOperator<String> displayLogStream = jsonStream
                .filter(json -> {
                    boolean hasDisplays = json.containsKey("displays");
                    if (hasDisplays) {
                        System.out.println("匹配展示日志: " + json.toJSONString());
                    }
                    return hasDisplays;
                })
                .map(JSON::toJSONString)
                .name("Filter_And_Convert_Display_Log");

// 4.4 动作日志（同理）
        SingleOutputStreamOperator<String> actionLogStream = jsonStream
                .filter(json -> {
                    boolean hasActions = json.containsKey("actions");
                    if (hasActions) {
                        System.out.println("匹配动作日志: " + json.toJSONString());
                    }
                    return hasActions;
                })
                .map(JSON::toJSONString)
                .name("Filter_And_Convert_Action_Log");

// 4.5 错误日志（同理）
        SingleOutputStreamOperator<String> errorLogStream = jsonStream
                .filter(json -> {
                    boolean hasErr = json.containsKey("err");
                    if (hasErr) {
                        System.out.println("匹配错误日志: " + json.toJSONString());
                    }
                    return hasErr;
                })
                .map(JSON::toJSONString)
                .name("Filter_And_Convert_Error_Log");

        startLogStream.print("写入启动日志: ");
        pageLogStream.print("写入页面日志: ");
        displayLogStream.print("写入展示日志: ");
        actionLogStream.print("写入动作日志: ");
        errorLogStream.print("写入错误日志: ");
        // 5. 写入Kafka ODS层对应的topic
        String bootstrapServers = "cdh01:9092";
        startLogStream.sinkTo(KafkaUtils.buildKafkaSink(bootstrapServers, "ods_start_log"))
                .name("Sink_To_Start_Log_Topic");

        pageLogStream.sinkTo(KafkaUtils.buildKafkaSink(bootstrapServers, "ods_page_log"))
                .name("Sink_To_Page_Log_Topic");

        displayLogStream.sinkTo(KafkaUtils.buildKafkaSink(bootstrapServers, "ods_display_log"))
                .name("Sink_To_Display_Log_Topic");

        actionLogStream.sinkTo(KafkaUtils.buildKafkaSink(bootstrapServers, "ods_action_log"))
                .name("Sink_To_Action_Log_Topic");

        errorLogStream.sinkTo(KafkaUtils.buildKafkaSink(bootstrapServers, "ods_error_log"))
                .name("Sink_To_Error_Log_Topic");

        // 6. 执行作业
        env.execute("ODS_Log_Base_Processing_Job");
    }
}