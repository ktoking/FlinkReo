package com.atguigu.apitest.transform;

import com.atguigu.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformTest5_RichFunction {
    public static void main(String[] args) throws Exception{
        //创建执行环境
        StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4); //保证一个线程solt读有序

        //从文件读取数据
        DataStream<String> inputStream=env.readTextFile("G:\\idea_repo\\FlinkTutorial\\src\\main\\resources\\sensor.txt");

        //lambda表达式简化
        DataStream<SensorReading> dataStream = inputStream.map((e) -> {
            final String[] split = e.split(",");
            return new SensorReading(split[0], new Long(split[1]), new Double(split[2]));
        });

        DataStream<Tuple2<String,Integer>> mapStream = dataStream.map(new Mymapper());


        env.execute();
    }
    //自定义map实现类
    public static class Mymapper0 implements MapFunction<SensorReading, Tuple2<String,Integer>> {

        @Override
        public Tuple2<String, Integer> map(SensorReading sensorReading) throws Exception {
            return null;
        }
    }

    //实现自定义富函数类
    public static class Mymapper extends RichMapFunction<SensorReading, Tuple2<String,Integer>> {

        @Override
        public Tuple2<String, Integer> map(SensorReading sensorReading) throws Exception {
//            getRuntimeContext().getState();

            return new Tuple2<>(sensorReading.getId(),getRuntimeContext().getIndexOfThisSubtask());
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            // 初始化工作, 一般是定义状态,或者建立数据库连接
            System.out.println("open");
        }

        @Override
        public void close() throws Exception {
            //关闭连接,清空状态的收尾操作
            System.out.println("close");
        }
    }

}
