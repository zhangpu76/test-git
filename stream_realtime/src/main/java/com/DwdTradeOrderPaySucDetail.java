package com;

import com.stream.common.base.BaseSQLApp;
import com.stream.common.constant.Constant;
import com.stream.common.utils.SQLUtil;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 支付成功事实表
 */
public class DwdTradeOrderPaySucDetail extends BaseSQLApp {
    public static void main(String[] args) {
        new DwdTradeOrderPaySucDetail().start(
                10016,
                4,
                Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS
        );
    }
    @Override
    public void handle(StreamTableEnvironment tableEnv) {
        //TODO 从下单事实表读取数据创建动态表
        tableEnv.executeSql(
                "create table dwd_trade_order_detail(" +
                        "id string," +
                        "order_id string," +
                        "user_id string," +
                        "sku_id string," +
                        "sku_name string," +
                        "province_id string," +
                        "activity_id string," +
                        "activity_rule_id string," +
                        "coupon_id string," +
                        "date_id string," +
                        "create_time string," +
                        "sku_num string," +
                        "split_original_amount string," +
                        "split_activity_amount string," +
                        "split_coupon_amount string," +
                        "split_total_amount string," +
                        "ts bigint," +
                        "et as to_timestamp_ltz(ts, 3), " +
                        "watermark for et as et - interval '3' second " +
                        ")" + SQLUtil.getKafkaDDL(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL,Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS));

        //TODO 从topic_db主题中读取数据创建动态表
        readOdsDb(tableEnv,Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS);

        //TODO 过滤出支付成功数据
        Table paymentInfo = tableEnv.sqlQuery("SELECT \n" +
                "  `data`['user_id'] AS user_id,\n" +
                "  `data`['order_id'] AS order_id,\n" +
                "  `data`['payment_type'] AS payment_type,\n" +
                "  `data`['callback_time'] AS callback_time,\n" +
                "  `pt`,\n" +
                "  `ts`,\n" +
                "  `et`\n" +
                "FROM topic_db\n" +
                "WHERE `database` = 'realtime_v1'\n" +
                "  AND `table` = 'payment_info'\n" +
                "  AND `type` = 'update'\n" +
                "  AND `old`['payment_status'] IS NOT NULL\n" +
                "  AND `data`['payment_status'] = '1602';");
        tableEnv.createTemporaryView("payment_info", paymentInfo);

//        paymentInfo.execute().print();
        //TODO 从HBase中读取字典数据创建动态表
        readBaseDic(tableEnv);

        //TODO 支付和字典进行关联---lookupjoin
        //ToDO 关联的结果和下单数据进行关联---IntervalJoin
        Table result = tableEnv.sqlQuery(
                "select " +
                        "od.id order_detail_id," +
                        "od.order_id," +
                        "od.user_id," +
                        "od.sku_id," +
                        "od.sku_name," +
                        "od.province_id," +
                        "od.activity_id," +
                        "od.activity_rule_id," +
                        "od.coupon_id," +
                        "pi.payment_type payment_type_code ," +
                        "dic.dic_name payment_type_name," +
                        "pi.callback_time," +
                        "od.sku_num," +
                        "od.split_original_amount," +
                        "od.split_activity_amount," +
                        "od.split_coupon_amount," +
                        "od.split_total_amount split_payment_amount," +
                        "pi.ts " +
                        "from payment_info pi " +
                        "join dwd_trade_order_detail od " +
                        "on pi.order_id=od.order_id " +
                        "and od.et >= pi.et - interval '5' minute " +
                        "and od.et <= pi.et + interval '5' minute " +
                        "join base_dic for system_time as of pi.pt as dic " +
                        "on pi.payment_type=dic.dic_code ");

//        result.execute().print();

        //TODO 将关联的结果写到kafka主题中
        tableEnv.executeSql("create table "+ Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS +"(" +
                "order_detail_id string," +
                "order_id string," +
                "user_id string," +
                "sku_id string," +
                "sku_name string," +
                "province_id string," +
                "activity_id string," +
                "activity_rule_id string," +
                "coupon_id string," +
                "payment_type_code string," +
                "payment_type_name string," +
                "callback_time string," +
                "sku_num string," +
                "split_original_amount string," +
                "split_activity_amount string," +
                "split_coupon_amount string," +
                "split_payment_amount string," +
                "ts bigint ," +
                "PRIMARY KEY (order_detail_id) NOT ENFORCED " +
                ")" + SQLUtil.getUpsertKafkaDDL(Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS));

        result.executeInsert(Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS);
    }
}
