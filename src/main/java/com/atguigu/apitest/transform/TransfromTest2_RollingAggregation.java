package com.atguigu.apitest.transform;

import com.atguigu.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransfromTest2_RollingAggregation {
    public static void main(String[] args) throws Exception{
        //创建执行环境
        StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); //保证一个线程solt读有序

        //从文件读取数据
        DataStream<String> inputStream=env.readTextFile("G:\\idea_repo\\FlinkTutorial\\src\\main\\resources\\sensor.txt");

        //转换成SensorReading 类型
//        DataStream<SensorReading> dataStream=inputStream.map(new MapFunction<String, SensorReading>() {
//            @Override
//            public SensorReading map(String s) throws Exception {
//                 String[] split = s.split(",");
//                 return new SensorReading(split[0],new Long(split[1]),new Double(split[2]));
//            }
//        });
        //lambda表达式简化
        DataStream<SensorReading> dataStream = inputStream.map((e) -> {
            final String[] split = e.split(",");
            return new SensorReading(split[0], new Long(split[1]), new Double(split[2]));
        });

        //分组
        KeyedStream<SensorReading, Tuple> keyedStream = dataStream.keyBy("id");
//        KeyedStream<SensorReading, String> sensorReadingStringKeyedStream = dataStream.keyBy(SensorReading::getId);

        //滚动聚合,取当前最大的温度值(只更新最大数据temperature的值,其他值不变) 若只想要更新后最大值用 maxby
        DataStream<SensorReading> resultStram = keyedStream.maxBy("temperature");

        resultStram.print();

        env.execute();
    }
}
