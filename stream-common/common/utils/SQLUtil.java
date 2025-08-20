package com.stream.common.utils;

import com.stream.common.constant.Constant;

public class SQLUtil {
    public static String getKafkaDDL(String topic, String groupId) {
        return "WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = '"+ topic +"',\n" +
                "  'properties.bootstrap.servers' = 'cdh01:9092',\n" +
                "  'properties.group.id' = '"+ groupId +"',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'json'\n" +
                ")";
    }

    public static String getHBaseDDL(String tableName) {
        return "WITH(\n" +
                " 'connector' = 'hbase-2.2',\n" +
                " 'table-name' = '"+tableName +"',\n" +
                " 'zookeeper.quorum' = 'cdh01:2181,cdh02:2181,cdh03:2181',\n" +
                " 'lookup.async' = 'true',\n" +
                " 'lookup.cache' = 'PARTIAL',\n" +
                " 'lookup.partial-cache.max-rows' = '500',\n" +
                " 'lookup.partial-cache.expire-after-write' = '1 hour',\n" +
                " 'lookup.partial-cache.expire-after-access' = '1 hour'\n" +
                ")";
    }

    public static String getUpsertKafkaDDL(String topic) {
        return "WITH (\n" +
                " 'connector' = 'upsert-kafka',\n" +
                " 'topic' = '"+ topic +"',\n" +
                " 'properties.bootstrap.servers' = '"+ Constant.KAFKA_BROKERS +"',\n" +
                " 'key.format' = 'json',\n" +
                " 'value.format' = 'json'\n" +
                ")";
    }
}
