package com.atguigu.apitest.source;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SourceTest2_File {
    public static void main(String[] args) throws Exception{
        //创建执行环境
        StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();

        //从文件读取数据
        DataStream<String> dataStream=env.readTextFile("G:\\idea_repo\\FlinkTutorial\\src\\main\\resources\\sensor.txt");

        //打印输出
        dataStream.print();

        //执行
        env.execute();
    }
}
