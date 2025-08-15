package com.bawei.wc;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.io.IOException;

public class dim {
    public static void main(String[] args) throws Exception {
//1.1指定流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 设置并行度
        env.setParallelism(4);
        //TODO 2. 检查点相关的设置
        //2.1开启检查点
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        //2.2设置检查点超时时间
        env.getCheckpointConfig().setCheckpointTimeout(60000L);
        //2.3设置job取消时是否保留检查点
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //2.4设置两个检查点之间最小时间间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);
        //2.5设置重启策略
        //env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 3000L));
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(30),Time.seconds(3)));
        //2.6设置状态后端以及检查点存储路径
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://cdh01:8020/ck");
        //2.7设置操作hadoop的用户
        System.setProperty("HADooP_USER_NAME","atguigu");
        //TODO 3.从kafka的topic_db主题中读取业务数据
        //3.1声明消费的主题以及消费者组
        String topic="topic_db";
        String groupId="dim_app_group";
        //3.2创建消费者对象
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("cdh01:9092,cdh02:9092,cdh03:9092")
                .setTopics(topic)
                .setGroupId(groupId)
        //在生产环境中，一般为了保证消费的精准一次性，需要手动维护偏移量，KafkaSource->KafkaSourceReader->存储偏移量变量
        //.setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
        //从最末尾位点开始消费
                .setStartingOffsets(OffsetsInitializer.latest())
                //注意：如果使用Flink提供的SimpleStringSchema对String类型的消息进行反序列化，如果消息为空，会报错
                //.setValueOnlyDeserializer(new SimpleStringSchema())
                .setValueOnlyDeserializer(
                        new DeserializationSchema<String>() {
                            @Override
                            public String deserialize(byte[] bytes) throws IOException {
                                if(bytes != null){
                                    return new String(bytes);
                                }
                                return null;
                            }

                            @Override
                            public boolean isEndOfStream(String s) {
                                return false;
                            }

                            @Override
                            public TypeInformation<String> getProducedType() {
                                return TypeInformation.of(String.class);
                            }
                        }
                )
                .build();
        //3.3消费数据封装为流
        DataStreamSource<String> kafkaStrDS = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source");
        kafkaStrDS.print("kafkaStrDS -> ");
        //TODO 4.对业务流中数据类型进行转换并进行简单的ETL jsonStr->jsonObj
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.process(
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String jsonStr, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        String db = jsonObj.getString("database");
                        String type = jsonObj.getString("type");
                        String data = jsonObj.getString("data");
                        if ("realtime_v1".equals(db)
                                && ("insert".equals(type)
                                || "update".equals(type)
                                || "delete".equals(type)
                                || "bootstrap-insert".equals(type))
                                && data != null
                                && data.length() > 2
                        ) {
                            out.collect(jsonObj);
                        }
                    }
                }
        );
        jsonObjDS.print("jsonObjDS -> ");
        //TODO 5. 使用FlinkCDC读取配置表中的配置信息
        //5.1创建MySQLSource对象
//        Properties props = new Properties();
//        props.setProperty("useSsL", "false");
//        props.setProperty("allowPublicKeyRetrieval", "true");
//        MySqlSource.<String>builder()
//                .hostname("cdh01")
//                .port(3306)
//                .databaseList("realtime_v1")
//                .tableList("realtime_v1.activity_info")
//                .username("root")
//                .password("123456")
//                .deserializer(new JsonDebeziumDeserializationSchema())
//                .startupOptions(StartupOptions.initial())
//                .jdbcProperties(props)
//                .build();
        //5.2读取数据封装为流
        //TODO 6.对配置流中的数据类型进行转换 jsonStr->实体类对象
        //TODO 7.根据配置表中的配置信息到HBase中执行建表或者删除表操作
        //TODO 8.将配置流中的配置信息进行广播---broadcast
        //TODO 9.将主流业务数据和广播流配置信息进行关联---connect
        //TODO 10.处理关联后的数据（判断是否为维度）
        //processElement：处理主流业务数据   根据维度表名到广播状态中读取配置信息，判断是否为维度
        //processBroadcastElement:处理广播流配置信息   将配置数据放到广播状态中 k：维度表名 v：一个配置对象
        //TODO 11.将维度数据同步到HBase表中
        env.execute();
    }
}
