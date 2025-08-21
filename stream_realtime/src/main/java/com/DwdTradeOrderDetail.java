package com;

import com.stream.common.base.BaseSQLApp;
import com.stream.common.constant.Constant;
import com.stream.common.utils.SQLUtil;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 下单事实表
 */
public class DwdTradeOrderDetail extends BaseSQLApp {
    public static void main(String[] args) {
        new DwdTradeOrderDetail().start(
                10014,
                4,
                Constant.TOPIC_DWD_TRADE_ORDER_DETAIL
        );
    }

    @Override
    public void handle(StreamTableEnvironment tableEnv) {
        //TODO 设置状态的保留时间[传输的延迟+业务上的滞后关系]
//        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(10));
        //ToDo 从kafka的topic_db主题中读取数据创建动态表
        readOdsDb(tableEnv, Constant.TOPIC_DWD_TRADE_ORDER_DETAIL);
        //TODO 过滤出订单明细数据
        Table orderDetail = tableEnv.sqlQuery("select " +
                        " `data`['id'] id," +
                        " `data`['order_id'] order_id," +
                        " `data`['sku_id'] sku_id," +
                        " `data`['sku_name'] sku_name," +
                        " `data`['create_time'] create_time," +
                        " `data`['sku_num'] sku_num," +
                        " cast(cast(`data`['sku_num'] as decimal(16,2)) * " +
                        " cast(`data`['order_price'] as decimal(16,2)) as String) split_original_amount," + // 分摊原始总金额
                        " `data`['split_total_amount'] split_total_amount," +  // 分摊总金额
                        " `data`['split_activity_amount'] split_activity_amount," + // 分摊活动金额
                        " `data`['split_coupon_amount'] split_coupon_amount," + // 分摊的优惠券金额
                        " ts " +
                        "from topic_db " +
                        "where `table`='order_detail' " +
                        "and `type`='insert' ");
        tableEnv.createTemporaryView("order_detaill", orderDetail);

//        orderDetail.execute().print();

        //TODO 过滤出订单数据
        Table orderInfo = tableEnv.sqlQuery(
                "select " +
                        "data['id'] id," +
                        "data['user_id'] user_id," +
                        "data['province_id'] province_id " +
                        "from topic_db " +
                        "where `table`='order_info' " +
                        "and `type`='insert' ");
        tableEnv.createTemporaryView("order_infol", orderInfo);

//        orderInfo.execute().print();

        //TODo 过滤出明细活动数据
        Table orderDetailActivity = tableEnv.sqlQuery(
                "select " +
                        "data['order_detail_id'] order_detail_id, " +
                        "data['activity_id'] activity_id, " +
                        "data['activity_rule_id'] activity_rule_id " +
                        "from topic_db " +
                        "where `table`='order_detail_activity' " +
                        "and `type`='insert' ");
        tableEnv.createTemporaryView("order_detail_activityl", orderDetailActivity);

//        orderDetailActivity.execute().print();

        //TODO 过滤出明细优惠券数据
        Table orderDetailCoupon = tableEnv.sqlQuery(
                "select " +
                        "data['order_detail_id'] order_detail_id, " +
                        "data['coupon_id'] coupon_id " +
                        "from topic_db " +
                        "where `table`='order_detail_coupon' " +
                        "and `type`='insert' ");
        tableEnv.createTemporaryView("order_detail_couponl", orderDetailCoupon);

//        orderDetailCoupon.execute().print();

        //TODO 关联上述4张表
        Table result = tableEnv.sqlQuery(
                "select " +
                        "od.id," +
                        "od.order_id," +
                        "oi.user_id," +
                        "od.sku_id," +
                        "od.sku_name," +
                        "oi.province_id," +
                        "act.activity_id," +
                        "act.activity_rule_id," +
                        "cou.coupon_id," +
                        "date_format(od.create_time, 'yyyy-MM-dd') date_id," +  // 年月日
                        "od.create_time," +
                        "od.sku_num," +
                        "od.split_original_amount," +
                        "od.split_activity_amount," +
                        "od.split_coupon_amount," +
                        "od.split_total_amount," +
                        "od.ts " +
                        "from order_detaill od " +
                        "join order_infol oi on od.order_id=oi.id " +
                        "left join order_detail_activityl act " +
                        "on od.id=act.order_detail_id " +
                        "left join order_detail_couponl cou " +
                        "on od.id=cou.order_detail_id " );


//        result.execute().print();
        //TODO 将关联的结果写到Kafka主题
        //创建动态表和要写入的主题进行映射
        tableEnv.executeSql(
                "create table "+ Constant.TOPIC_DWD_TRADE_ORDER_DETAIL +"(" +
                        "id STRING," +
                        "order_id STRING," +
                        "user_id STRING," +
                        "sku_id STRING," +
                        "sku_name STRING," +
                        "province_id STRING," +
                        "activity_id STRING," +
                        "activity_rule_id STRING," +
                        "coupon_id STRING," +
                        "date_id STRING," +
                        "create_time STRING," +
                        "sku_num STRING," +
                        "split_original_amount STRING," +
                        "split_activity_amount STRING," +
                        "split_coupon_amount STRING," +
                        "split_total_amount STRING," +
                        "ts bigint," +
                        "PRIMARY KEY (id) NOT ENFORCED " +
                        ")" + SQLUtil.getUpsertKafkaDDL(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL));
        //写入
        result.executeInsert(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL);
    }
}
