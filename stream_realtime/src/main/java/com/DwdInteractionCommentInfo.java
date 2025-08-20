package com;

import com.stream.common.base.BaseSQLApp;
import com.stream.common.constant.Constant;
import com.stream.common.utils.SQLUtil;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 评论事实表
 */
public class DwdInteractionCommentInfo extends BaseSQLApp {
    public static void main(String[] args) {
        new DwdInteractionCommentInfo().start(10012,4,Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO);
    }

    @Override
    public void handle(StreamTableEnvironment tableEnv) {
        //TODO 从kafka的topic_db主题中读取数据创建动态表 ---kafka连接器
        readOdsDb(tableEnv,Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO);

        //TODO 过滤出评论数据    --where table='comment_info' type='insert'
        Table commentInfo = tableEnv.sqlQuery("select\n" +
                "            `data`['id'] id,\n" +
                "            `data`['user_id'] user_id,\n" +
                "            `data`['sku_id'] sku_id,\n" +
                "            `data`['appraise'] appraise,\n" +
                "            `data`['comment_txt'] comment_txt,\n" +
                "        ts,\n" +
                "        proc_time\n" +
                "        from topic_db where `table`='comment_info' and `type`='insert'");

//        commentInfo.execute().print();
        //将表对象注册到表执行环境中
        tableEnv.createTemporaryView("comment_info",commentInfo);

        //TODO 从HBaSe中读取字典数据创建动态表   ----hbase连接器
        readBaseDic(tableEnv);

        //TODO 将评论表和字典表进行关联  ---- lookup Join
        Table joinedTable = tableEnv.sqlQuery("SELECT\n" +
                "                c.id,\n" +
                "                c.user_id,\n" +
                "                c.sku_id,\n" +
                "                c.appraise,\n" +
                "                dic.info.dic_name AS appraise_name,  -- 从info取名称\n" +
                "                c.comment_txt,\n" +
                "                c.ts\n" +
                "        FROM comment_info AS c\n" +
                "        JOIN base_dic FOR SYSTEM_TIME AS OF c.proc_time AS dic\n" +
                "        ON c.appraise = dic.dic_code");  // 直接用行键关联

        // 执行查询并打印结果
        joinedTable.execute().print();
        //TODO 将关联的结果写到kafka主题中  ----upsertkafka连接器
        //7.1创建动态表和要写入的主题进行映射
        tableEnv.executeSql("CREATE TABLE "+ Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO +" (\n" +
                "                id STRING,\n" +
                "                user_id STRING,\n" +
                "                sku_id STRING,\n" +
                "                appraise STRING,\n" +
                "                appraise_name STRING,\n" +
                "                comment_txt STRING,\n" +
                "                ts bigint,\n" +
                "                PRIMARY KEY (id) NOT ENFORCED\n" +
                "        ) " + SQLUtil.getUpsertKafkaDDL(Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO));
        //7.2写入
        joinedTable.executeInsert(Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO);
    }
}
