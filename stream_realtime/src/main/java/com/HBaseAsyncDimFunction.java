package com;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;

/**
 * 异步 I/O 函数：查询 HBase 维度表 dim_
 * 支持 user_id, sku_id, province_id 等字段 enrich
 */
public class HBaseAsyncDimFunction implements AsyncFunction<JSONObject, JSONObject> {

    private transient Connection connection;

    @Override
    public void asyncInvoke(JSONObject input, ResultFuture<JSONObject> resultFuture) throws Exception {
        if (connection == null || connection.isClosed()) {
            connection = ConnectionFactory.createConnection();
        }

        JSONObject common = input.getJSONObject("common");
        String uid = common.getString("uid");
        String skuId = input.getJSONObject("page") != null ? input.getJSONObject("page").getString("item") : null;
        String provinceId = common.getString("ar");

        CompletableFuture
                .supplyAsync(() -> {
                    try {
                        JSONObject dimData = new JSONObject();

                        // user 维度
                        if (uid != null) {
                            JSONObject userInfo = getDimFromHBase("dim_user_info", uid);
                            if (userInfo != null) dimData.put("user_info", userInfo);
                        }

                        // sku 维度
                        if (skuId != null) {
                            JSONObject skuInfo = getDimFromHBase("dim_sku_info", skuId);
                            if (skuInfo != null) dimData.put("sku_info", skuInfo);
                        }

                        // 省份维度
                        if (provinceId != null) {
                            JSONObject provinceInfo = getDimFromHBase("dim_province_info", provinceId);
                            if (provinceInfo != null) dimData.put("province_info", provinceInfo);
                        }

                        input.put("dim", dimData);
                        return input;

                    } catch (Exception e) {
                        e.printStackTrace();
                        return input;
                    }
                })
                .thenAccept(result -> resultFuture.complete(Collections.singleton(result)));
    }

    private JSONObject getDimFromHBase(String tableName, String rowKey) throws Exception {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        Result result = table.get(get);

        if (result.isEmpty()) {
            return null;
        }

        JSONObject json = new JSONObject();
        result.getNoVersionMap().forEach((cf, qualifierMap) -> {
            qualifierMap.forEach((qualifier, value) -> {
                String column = Bytes.toString(qualifier);
                String val = Bytes.toString(value);
                json.put(column, val);
            });
        });
        return json;
    }

    @Override
    public void timeout(JSONObject input, ResultFuture<JSONObject> resultFuture) throws Exception {
        // 超时直接返回原始数据
        resultFuture.complete(Collections.singleton(input));
    }
}
