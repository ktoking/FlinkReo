package com.atguigu.apitest.transform;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class TransformTest1_Base {
    public static void main(String[] args) throws Exception{
        //创建执行环境
        StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); //保证一个线程solt读有序

        //从文件读取数据
        DataStream<String> dataStream=env.readTextFile("G:\\idea_repo\\FlinkTutorial\\src\\main\\resources\\sensor.txt");

        //1.map 把String转换为长度输出
        DataStream<Integer> mapStream = dataStream.map(new MapFunction<String, Integer>() {
            public Integer map(String s) throws Exception {
                return s.length();
            }
        });
        //由于是函数式接口就可以简化lamda表达式
//        SingleOutputStreamOperator<Integer> map = dataStream.map((e) -> {
//            return e.length();
//        });


        //2.flatmap 按照给定格式分段(逗号)
        DataStream<String> flatMapStream=dataStream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                String[] split = s.split(",");
                for (String s1 : split) {
                    collector.collect(s1);
                }
            }
        });
        //lamda表达式写法
//        dataStream.flatMap((e,collector)->{
//            String[] split = e.split(",");
//            for (String s1 : split) {
//                collector.collect(s1);
//            }
//        });

        //3.filter ,筛选seneor_1开头的id对应的数据
        DataStream<String> filterStream=dataStream.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
//                return s.split(",")[0].equals("sensor_1");
                return s.startsWith("sensor_1"); //判断是否以xx为前缀
            }
        });
//        dataStream.filter((e)->{return e.startsWith("sensor_1");});一行lamda

        mapStream.print("map");
        flatMapStream.print("flatMap");
        filterStream.print("filter");

        env.execute();
    }
}
