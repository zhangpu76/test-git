package com.stream.common.base;

import com.stream.common.constant.Constant;
import com.stream.common.utils.SQLUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public abstract class BaseSQLApp {
    public void start(int port,int parallelism,String ck) {
        // TODO 1.基本环境准备
        //1.1指定流处理环境
        Configuration conf = new Configuration();
        conf.set(RestOptions.PORT,port);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2设置并行度
        env.setParallelism(parallelism);
        //1.3指定表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
//        //TODO 2.检查点相关的设置
//        //2.1开启检查点
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
//        //2.2设置检查点超时时间
//        env.getCheckpointConfig().setCheckpointTimeout(6000L);
//        //2.3设置状态取消后，检查点是否保留
//        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//        //2.4设置两个检查点之间最小时间间隔
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);
//        //2.5设置重启策略T;
//        env.setRestartStrategy(RestartStrategies.failureRateRestart( 3, Time.days(30),Time.seconds(3)));
//        //2.6设置状态后端
//        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage("hdfs://cdh01:8020/ck");
//        //2.7设置操作hadoop的用户
//        System.setProperty("HADooP_USER_NAME","root");

        //TODO 3.业务处理逻辑
        handle(tableEnv);
    }
    public abstract void handle(StreamTableEnvironment tableEnv);

    // 从topic_db主题中读取数据，创建动态表
    protected void readOdsDb(StreamTableEnvironment tableEnv,String groupId) {
        tableEnv.executeSql("" +
                "CREATE TABLE topic_db(\n" +
                "  `database` STRING,\n" +
                "  `table` STRING,\n" +
                "  `type` STRING,\n" +
                "  `ts` bigint,\n" +
                "  `data` MAP<STRING, STRING>,\n" +
                "  `old` MAP<STRING, STRING>,\n" +
                "  proc_time as proctime()\n" +
                ") " + SQLUtil.getKafkaDDL(Constant.TOPIC_DB,groupId));
//        tableEnv.executeSql("select * from topic_db").print();
    }

    // 从Hbase的字典表中读取数据，创建动态表
    public void readBaseDic(StreamTableEnvironment tableEnv) {
        tableEnv.executeSql("CREATE TABLE base_dic (\n" +
                " dic_code STRING,\n" +
                " info ROW<dic_name STRING>,\n" +
                " PRIMARY KEY (dic_code) NOT ENFORCED\n" +
                ")" + SQLUtil.getHBaseDDL("dim_base_dic"));
//        tableEnv.executeSql("select * from base_dic").print();
    }
}
