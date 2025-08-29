package com.liuliang;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.json.JSONObject;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

public class MySqlToHBaseCDC {
    // 配置参数 - 请根据实际环境修改
    private static final String MYSQL_HOST = "cdh01";
    private static final int MYSQL_PORT = 3306;
    private static final String MYSQL_USER = "root";
    private static final String MYSQL_PASSWORD = "123456";
    private static final String MYSQL_DATABASE = "traffic_board_db";
    private static final String ZOOKEEPER_QUORUM = "cdh01:2181,cdh02:2181,cdh03:2181";
    // HBase表名前缀（统一在这里定义）
    private static final String HBASE_TABLE_PREFIX = "ods_";

    public static void main(String[] args) throws Exception {
        // 1. 初始化Flink执行环境
        Configuration flinkConfig = new Configuration();
        flinkConfig.setBoolean("classloader.check-leaked-classloader", false);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(flinkConfig);

        // 配置重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                3, // 最多重启3次
                TimeUnit.SECONDS.toMillis(5) // 每次重启间隔5秒
        ));

        // 启用检查点（5分钟一次）
        env.enableCheckpointing(300000);

        // 2. 创建MySQL CDC数据源
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname(MYSQL_HOST)
                .port(MYSQL_PORT)
                .username(MYSQL_USER)
                .password(MYSQL_PASSWORD)
                .databaseList(MYSQL_DATABASE)
                .tableList(
                        MYSQL_DATABASE + ".user_base",
                        MYSQL_DATABASE + ".sku_info",
                        MYSQL_DATABASE + ".city_dict",
                        MYSQL_DATABASE + ".member_level",
                        MYSQL_DATABASE + ".channel_dict",
                        MYSQL_DATABASE + ".category_dict",
                        MYSQL_DATABASE + ".brand_dict",
                        MYSQL_DATABASE + ".order_info"
                )
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();

        // 3. 读取CDC数据并打印原始数据（用于调试）
        DataStream<String> cdcStream = env.fromSource(
                mySqlSource,
                WatermarkStrategy.noWatermarks(),
                "MySQL CDC Source"
        );
        cdcStream.print("【CDC原始数据】");

        // 4. 处理各表数据并写入HBase
        cdcStream.filter(tableFilter("user_base"))
                .map(new UserDataMapper())
                .addSink(new HBaseSinkFunction("user_base")).setParallelism(1);

        cdcStream.filter(tableFilter("sku_info"))
                .map(new SkuDataMapper())
                .addSink(new HBaseSinkFunction("sku_info")).setParallelism(1);

        cdcStream.filter(tableFilter("city_dict"))
                .map(new CityDataMapper())
                .addSink(new HBaseSinkFunction("city_dict")).setParallelism(1);

        cdcStream.filter(tableFilter("member_level"))
                .map(new MemberLevelMapper())
                .addSink(new HBaseSinkFunction("member_level")).setParallelism(1);

        cdcStream.filter(tableFilter("channel_dict"))
                .map(new ChannelDictMapper())
                .addSink(new HBaseSinkFunction("channel_dict")).setParallelism(1);

        cdcStream.filter(tableFilter("category_dict"))
                .map(new CategoryDictMapper())
                .addSink(new HBaseSinkFunction("category_dict")).setParallelism(1);

        cdcStream.filter(tableFilter("brand_dict"))
                .map(new BrandDictMapper())
                .addSink(new HBaseSinkFunction("brand_dict")).setParallelism(1);

        cdcStream.filter(tableFilter("order_info"))
                .map(new OrderInfoMapper())
                .addSink(new HBaseSinkFunction("order_info")).setParallelism(1);

        // 执行任务
        env.execute("MySQL CDC to HBase Sync");
    }

    /**
     * 通用表筛选器：筛选指定表的非删除数据
     */
    private static FilterFunction<String> tableFilter(String targetTable) {
        return json -> {
            try {
                JSONObject data = new JSONObject(json);
                // 从source对象中获取表名（Debezium的标准结构）
                JSONObject source = data.getJSONObject("source");
                String table = source.optString("table", "");
                String op = data.optString("op", "");

                // 打印过滤信息用于调试
                boolean keep = targetTable.equals(table) && !"d".equals(op);
                System.out.printf("【过滤】表名=%s, 目标表=%s, 操作类型=%s, 是否保留=%s%n",
                        table, targetTable, op, keep);
                return keep;
            } catch (Exception e) {
                System.err.println("过滤数据异常: " + e.getMessage() + ", 数据: " + json);
                return false;
            }
        };
    }

    // 用户表映射器 (user_base)
    public static class UserDataMapper implements MapFunction<String, Put> {
        private final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        @Override
        public Put map(String json) {
            try {
                JSONObject data = new JSONObject(json);
                JSONObject after = data.optJSONObject("after");
                if (after == null) {
                    System.err.println("user_base数据after为null: " + json);
                    return null;
                }

                String rowkey = after.optString("uid", "");
                if (rowkey.isEmpty()) {
                    System.err.println("user_base rowkey为空: " + after);
                    return null;
                }

                Put put = new Put(Bytes.toBytes(rowkey));
                // 映射所有字段，确保与MySQL表结构一致
                addColumn(put, "uid", after.optString("uid", ""));
                addColumn(put, "user_name", after.optString("user_name", ""));
                addColumn(put, "gender", after.optInt("gender", 0));
                addColumn(put, "age", after.optInt("age", 0));
                addColumn(put, "register_time", formatTime(after.optLong("register_time", 0)));
                addColumn(put, "city_id", after.optString("city_id", ""));
                addColumn(put, "member_level_id", after.optInt("member_level_id", 0));
                addColumn(put, "user_status", after.optInt("user_status", 0));
                addColumn(put, "create_time", formatTime(after.optLong("create_time", 0)));
                addColumn(put, "update_time", formatTime(after.optLong("update_time", 0)));

                System.out.println("生成user_base Put: " + rowkey);
                return put;
            } catch (Exception e) {
                System.err.println("处理user_base错误: " + e.getMessage() + ", 数据: " + json);
                return null;
            }
        }

        private String formatTime(long ts) {
            return ts <= 0 ? "" : sdf.format(new Date(ts));
        }

        private void addColumn(Put put, String q, String v) {
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(q), Bytes.toBytes(v));
        }

        private void addColumn(Put put, String q, int v) {
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(q), Bytes.toBytes(v));
        }
    }

    // 商品表映射器 (sku_info)
    public static class SkuDataMapper implements MapFunction<String, Put> {
        private final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        @Override
        public Put map(String json) {
            try {
                JSONObject data = new JSONObject(json);
                JSONObject after = data.optJSONObject("after");
                if (after == null) {
                    System.err.println("sku_info数据after为null: " + json);
                    return null;
                }

                String rowkey = after.optString("sku_id", "");
                if (rowkey.isEmpty()) {
                    System.err.println("sku_info rowkey为空: " + after);
                    return null;
                }

                Put put = new Put(Bytes.toBytes(rowkey));
                // 映射所有字段
                addColumn(put, "sku_id", rowkey);
                addColumn(put, "sku_name", after.optString("sku_name", ""));
                addColumn(put, "category3_id", after.optString("category3_id", ""));
                addColumn(put, "brand_id", after.optString("brand_id", ""));
                addColumn(put, "price", after.optDouble("price", 0.00));
                addColumn(put, "sku_status", after.optInt("sku_status", 0));
                addColumn(put, "create_time", formatTime(after.optLong("create_time", 0)));
                addColumn(put, "update_time", formatTime(after.optLong("update_time", 0)));

                System.out.println("生成sku_info Put: " + rowkey);
                return put;
            } catch (Exception e) {
                System.err.println("处理sku_info错误: " + e.getMessage() + ", 数据: " + json);
                return null;
            }
        }

        private String formatTime(long ts) {
            return ts <= 0 ? "" : sdf.format(new Date(ts));
        }

        private void addColumn(Put put, String q, String v) {
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(q), Bytes.toBytes(v));
        }

        private void addColumn(Put put, String q, double v) {
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(q), Bytes.toBytes(String.valueOf(v)));
        }

        private void addColumn(Put put, String q, int v) {
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(q), Bytes.toBytes(v));
        }
    }

    // 城市字典表映射器 (city_dict)
    public static class CityDataMapper implements MapFunction<String, Put> {
        private final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        @Override
        public Put map(String json) {
            try {
                JSONObject data = new JSONObject(json);
                JSONObject after = data.optJSONObject("after");
                if (after == null) {
                    System.err.println("city_dict数据after为null: " + json);
                    return null;
                }

                String rowkey = after.optString("city_id", "");
                if (rowkey.isEmpty()) {
                    System.err.println("city_dict rowkey为空: " + after);
                    return null;
                }

                Put put = new Put(Bytes.toBytes(rowkey));
                // 映射所有字段
                addColumn(put, "city_id", rowkey);
                addColumn(put, "city_name", after.optString("city_name", ""));
                addColumn(put, "area_code", after.optString("area_code", ""));
                addColumn(put, "province_name", after.optString("province_name", ""));
                addColumn(put, "create_time", formatTime(after.optLong("create_time", 0)));
                addColumn(put, "update_time", formatTime(after.optLong("update_time", 0)));

                System.out.println("生成city_dict Put: " + rowkey);
                return put;
            } catch (Exception e) {
                System.err.println("处理city_dict错误: " + e.getMessage() + ", 数据: " + json);
                return null;
            }
        }

        private String formatTime(long ts) {
            return ts <= 0 ? "" : sdf.format(new Date(ts));
        }

        private void addColumn(Put put, String q, String v) {
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(q), Bytes.toBytes(v));
        }
    }

    // 会员等级表映射器 (member_level)
    public static class MemberLevelMapper implements MapFunction<String, Put> {
        private final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        @Override
        public Put map(String json) {
            try {
                JSONObject data = new JSONObject(json);
                JSONObject after = data.optJSONObject("after");
                if (after == null) {
                    System.err.println("member_level数据after为null: " + json);
                    return null;
                }

                String rowkey = after.optString("level_id", "");
                if (rowkey.isEmpty()) {
                    System.err.println("member_level rowkey为空: " + after);
                    return null;
                }

                Put put = new Put(Bytes.toBytes(rowkey));
                // 映射所有字段
                addColumn(put, "level_id", rowkey);
                addColumn(put, "level_name", after.optString("level_name", ""));
                addColumn(put, "level_threshold", after.optDouble("level_threshold", 0.00));
                addColumn(put, "level_benefit", after.optString("level_benefit", ""));
                addColumn(put, "create_time", formatTime(after.optLong("create_time", 0)));
                addColumn(put, "update_time", formatTime(after.optLong("update_time", 0)));

                System.out.println("生成member_level Put: " + rowkey);
                return put;
            } catch (Exception e) {
                System.err.println("处理member_level错误: " + e.getMessage() + ", 数据: " + json);
                return null;
            }
        }

        private String formatTime(long ts) {
            return ts <= 0 ? "" : sdf.format(new Date(ts));
        }

        private void addColumn(Put put, String q, String v) {
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(q), Bytes.toBytes(v));
        }

        private void addColumn(Put put, String q, double v) {
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(q), Bytes.toBytes(String.valueOf(v)));
        }
    }

    // 渠道字典表映射器 (channel_dict)
    public static class ChannelDictMapper implements MapFunction<String, Put> {
        private final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        @Override
        public Put map(String json) {
            try {
                JSONObject data = new JSONObject(json);
                JSONObject after = data.optJSONObject("after");
                if (after == null) {
                    System.err.println("channel_dict数据after为null: " + json);
                    return null;
                }

                String rowkey = after.optString("channel_code", "");
                if (rowkey.isEmpty()) {
                    System.err.println("channel_dict rowkey为空: " + after);
                    return null;
                }

                Put put = new Put(Bytes.toBytes(rowkey));
                // 映射所有字段
                addColumn(put, "channel_code", rowkey);
                addColumn(put, "channel_name", after.optString("channel_name", ""));
                addColumn(put, "channel_type", after.optInt("channel_type", 0));
                addColumn(put, "channel_status", after.optInt("channel_status", 0));
                addColumn(put, "create_time", formatTime(after.optLong("create_time", 0)));
                addColumn(put, "update_time", formatTime(after.optLong("update_time", 0)));

                System.out.println("生成channel_dict Put: " + rowkey);
                return put;
            } catch (Exception e) {
                System.err.println("处理channel_dict错误: " + e.getMessage() + ", 数据: " + json);
                return null;
            }
        }

        private String formatTime(long ts) {
            return ts <= 0 ? "" : sdf.format(new Date(ts));
        }

        private void addColumn(Put put, String q, String v) {
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(q), Bytes.toBytes(v));
        }

        private void addColumn(Put put, String q, int v) {
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(q), Bytes.toBytes(v));
        }
    }

    // 品类字典表映射器 (category_dict)
    public static class CategoryDictMapper implements MapFunction<String, Put> {
        private final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        @Override
        public Put map(String json) {
            try {
                JSONObject data = new JSONObject(json);
                JSONObject after = data.optJSONObject("after");
                if (after == null) {
                    System.err.println("category_dict数据after为null: " + json);
                    return null;
                }

                String rowkey = after.optString("category_id", "");
                if (rowkey.isEmpty()) {
                    System.err.println("category_dict rowkey为空: " + after);
                    return null;
                }

                Put put = new Put(Bytes.toBytes(rowkey));
                // 映射所有字段
                addColumn(put, "category_id", rowkey);
                addColumn(put, "category_name", after.optString("category_name", ""));
                addColumn(put, "parent_id", after.optString("parent_id", ""));
                addColumn(put, "category_level", after.optInt("category_level", 0));
                addColumn(put, "create_time", formatTime(after.optLong("create_time", 0)));
                addColumn(put, "update_time", formatTime(after.optLong("update_time", 0)));

                System.out.println("生成category_dict Put: " + rowkey);
                return put;
            } catch (Exception e) {
                System.err.println("处理category_dict错误: " + e.getMessage() + ", 数据: " + json);
                return null;
            }
        }

        private String formatTime(long ts) {
            return ts <= 0 ? "" : sdf.format(new Date(ts));
        }

        private void addColumn(Put put, String q, String v) {
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(q), Bytes.toBytes(v));
        }

        private void addColumn(Put put, String q, int v) {
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(q), Bytes.toBytes(v));
        }
    }

    // 品牌字典表映射器 (brand_dict)
    public static class BrandDictMapper implements MapFunction<String, Put> {
        private final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        @Override
        public Put map(String json) {
            try {
                JSONObject data = new JSONObject(json);
                JSONObject after = data.optJSONObject("after");
                if (after == null) {
                    System.err.println("brand_dict数据after为null: " + json);
                    return null;
                }

                String rowkey = after.optString("brand_id", "");
                if (rowkey.isEmpty()) {
                    System.err.println("brand_dict rowkey为空: " + after);
                    return null;
                }

                Put put = new Put(Bytes.toBytes(rowkey));
                // 映射所有字段
                addColumn(put, "brand_id", rowkey);
                addColumn(put, "brand_name", after.optString("brand_name", ""));
                addColumn(put, "brand_logo", after.optString("brand_logo", ""));
                addColumn(put, "brand_desc", after.optString("brand_desc", ""));
                addColumn(put, "status", after.optInt("status", 0));
                addColumn(put, "create_time", formatTime(after.optLong("create_time", 0)));
                addColumn(put, "update_time", formatTime(after.optLong("update_time", 0)));

                System.out.println("生成brand_dict Put: " + rowkey);
                return put;
            } catch (Exception e) {
                System.err.println("处理brand_dict错误: " + e.getMessage() + ", 数据: " + json);
                return null;
            }
        }

        private String formatTime(long ts) {
            return ts <= 0 ? "" : sdf.format(new Date(ts));
        }

        private void addColumn(Put put, String q, String v) {
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(q), Bytes.toBytes(v));
        }

        private void addColumn(Put put, String q, int v) {
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(q), Bytes.toBytes(v));
        }
    }

    // 订单表映射器 (order_info)
    public static class OrderInfoMapper implements MapFunction<String, Put> {
        private final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        @Override
        public Put map(String json) {
            try {
                JSONObject data = new JSONObject(json);
                JSONObject after = data.optJSONObject("after");
                if (after == null) {
                    System.err.println("order_info数据after为null: " + json);
                    return null;
                }

                String rowkey = after.optString("order_id", "");
                if (rowkey.isEmpty()) {
                    System.err.println("order_info rowkey为空: " + after);
                    return null;
                }

                Put put = new Put(Bytes.toBytes(rowkey));
                // 映射所有字段
                addColumn(put, "order_id", rowkey);
                addColumn(put, "uid", after.optString("uid", ""));
                addColumn(put, "sku_id", after.optString("sku_id", ""));
                addColumn(put, "order_amount", after.optDouble("order_amount", 0.00));
                addColumn(put, "pay_status", after.optInt("pay_status", 0));
                addColumn(put, "create_time", formatTime(after.optLong("create_time", 0)));
                addColumn(put, "pay_time", formatTime(after.optLong("pay_time", 0)));
                addColumn(put, "order_status", after.optInt("order_status", 0));
                addColumn(put, "channel_code", after.optString("channel_code", ""));
                addColumn(put, "update_time", formatTime(after.optLong("update_time", 0)));

                System.out.println("生成order_info Put: " + rowkey);
                return put;
            } catch (Exception e) {
                System.err.println("处理order_info错误: " + e.getMessage() + ", 数据: " + json);
                return null;
            }
        }

        private String formatTime(long ts) {
            return ts <= 0 ? "" : sdf.format(new Date(ts));
        }

        private void addColumn(Put put, String q, String v) {
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(q), Bytes.toBytes(v));
        }

        private void addColumn(Put put, String q, double v) {
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(q), Bytes.toBytes(String.valueOf(v)));
        }

        private void addColumn(Put put, String q, int v) {
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(q), Bytes.toBytes(v));
        }
    }

    /**
     * HBase Sink函数：负责表创建和数据写入
     */
    public static class HBaseSinkFunction extends RichSinkFunction<Put> {
        private final String baseTableName;
        private Connection connection;
        private BufferedMutator mutator;
        private String fullTableName;

        public HBaseSinkFunction(String baseTableName) {
            this.baseTableName = baseTableName;
            this.fullTableName = HBASE_TABLE_PREFIX + baseTableName;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            // 初始化HBase配置
            org.apache.hadoop.conf.Configuration hbaseConf = HBaseConfiguration.create();
            hbaseConf.set("hbase.zookeeper.quorum", ZOOKEEPER_QUORUM);

            // 创建HBase连接
            connection = ConnectionFactory.createConnection(hbaseConf);

            // 确保表存在（不存在则创建）
            ensureTableExists();

            // 初始化批量写入器
            BufferedMutatorParams params = new BufferedMutatorParams(TableName.valueOf(fullTableName));
            params.writeBufferSize(1024 * 1024 * 5); // 5MB缓冲区
            mutator = connection.getBufferedMutator(params);

            System.out.println("【HBase Sink】" + fullTableName + " 初始化完成");
        }

        @Override
        public void invoke(Put value, Context context) throws Exception {
            if (value != null) {
                String rowkey = Bytes.toString(value.getRow());
                mutator.mutate(value);
                // 每100条数据手动刷盘一次（平衡性能和可靠性）
                if (mutator.getWriteBufferSize() >= 1024 * 1024 * 4) { // 接近4MB时刷盘
                    mutator.flush();
                    System.out.println("【HBase Sink】" + fullTableName + " 缓冲区刷盘，rowkey: " + rowkey);
                }
                System.out.println("【HBase Sink】写入数据，表: " + fullTableName + ", rowkey: " + rowkey);
            }
        }

        @Override
        public void close() throws Exception {
            // 关闭资源前强制刷盘，确保数据不丢失
            if (mutator != null) {
                mutator.flush();
                mutator.close();
                System.out.println("【HBase Sink】" + fullTableName + " 关闭时刷盘完成");
            }
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
            super.close();
        }

        /**
         * 确保HBase表存在，不存在则创建（含info列族）
         */
        private void ensureTableExists() throws IOException {
            try (Admin admin = connection.getAdmin()) {
                TableName tableName = TableName.valueOf(fullTableName);

                if (admin.tableExists(tableName)) {
                    System.out.println("【HBase表检查】" + fullTableName + " 已存在");
                    return;
                }

                // 创建表描述器
                TableDescriptorBuilder tableDesc = TableDescriptorBuilder.newBuilder(tableName);

                // 创建info列族（设置数据保留1年）
                ColumnFamilyDescriptor cfDesc = ColumnFamilyDescriptorBuilder
                        .newBuilder(Bytes.toBytes("info"))
                        .setTimeToLive(365 * 24 * 3600) // 31536000秒 = 1年
                        .setBlockCacheEnabled(true)
                        .build();

                tableDesc.setColumnFamily(cfDesc);

                // 创建表
                admin.createTable(tableDesc.build());
                System.out.println("【HBase表创建】" + fullTableName + " 创建成功");
            }
        }
    }
}