package com.atguigu.wc;

import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StreamWordCount {
    public static void main(String[] args) throws Exception {
        //创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(8); //设置分布式多线程数
        //从文件中读取路径
//        String path="G:\\idea_repo\\FlinkTutorial\\src\\main\\resources\\hello.txt";
//        DataStream<String> inputDataStream = env.readTextFile(path);

        //从程序启动参数中提取配置项
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        int port = parameterTool.getInt("port");
        //从socket文本流读取数据
        DataStream<String> inputDataStream = env.socketTextStream(host,port);


        //基于数据流转化计算
        DataStream<Tuple2<String, Integer>> resultStream = inputDataStream.flatMap(new WordCount.MyFlatMapper())
                .keyBy(0)
                .sum(1);
        resultStream.print();

        //执行任务 (有状态存储,每次进来一个流都会打印一次)
        env.execute();
    }
}
