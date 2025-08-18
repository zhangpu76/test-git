//package com.bawei.wc;
//
//import org.apache.flink.api.common.typeinfo.Types;
//import org.apache.flink.api.java.ExecutionEnvironment;
//import org.apache.flink.api.java.operators.AggregateOperator;
//import org.apache.flink.api.java.operators.DataSource;
//import org.apache.flink.api.java.operators.FlatMapOperator;
//import org.apache.flink.api.java.operators.UnsortedGrouping;
//import org.apache.flink.api.java.tuple.Tuple2;
//import org.apache.flink.util.Collector;
//
//
//public class BatchWordCount {
//    public static void main(String[] args) throws Exception{
//        // 1. 创建执行环境
//        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//        // 2. 读取文件
//        DataSource<String> lineDataSource = env.readTextFile("input/wc.txt");
//        // 3. 转换数据(二元组)
//        FlatMapOperator<String, Tuple2<String, Long>> wordAndOneTuple = lineDataSource.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
//            // 对每行数据进行分词
//            String[] words = line.split(" ");
//            // 遍历所有分词，输出二元组
//            for (String word : words) {
//                out.collect(Tuple2.of(word, 1L));
//            }
//        }).returns(Types.TUPLE(Types.STRING, Types.LONG));
//
//        // 4. 按word分组聚合
//        UnsortedGrouping<Tuple2<String, Long>> wordAndOneGroup = wordAndOneTuple.groupBy(0);
//
//        AggregateOperator<Tuple2<String, Long>> sum = wordAndOneGroup.sum(1);
//
//        sum.print();
//    }
//}
