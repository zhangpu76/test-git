package com;


import com.stream.common.utils.LogUtils;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Test01_FlinkCDC {
    public static void main(String[] args) throws Exception {
        LogUtils.disableAllLogging();
        //TODo 1. 基本环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //TODO 2.设置并行度
        env.setParallelism(1);
        // enable checkpoint
        env.enableCheckpointing(3000);
        //TOD03.使用FlinkCDC读取MySQL表中数据
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("cdh01")
                .port(3306)
                .databaseList("realtime_v2") // set captured database
                .tableList("realtime_v2.t_user") // set captured table
                .username("root")
                .password("123456")
                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSoN String
                .build();

env
        .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(),"MySQL Source")
        .print(); // use parallelism 1 for sink to keep message ordering
        env.execute("Print MySQL Snapshot + Binlog");
    }
}
