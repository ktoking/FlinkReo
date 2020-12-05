package com.atguigu.apitest.transform;

import com.atguigu.beans.SensorReading;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformTest3_Reduce {
    public static void main(String[] args) throws Exception{
        //创建执行环境
        StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); //保证一个线程solt读有序

        //从文件读取数据
        DataStream<String> inputStream=env.readTextFile("G:\\idea_repo\\FlinkTutorial\\src\\main\\resources\\sensor.txt");

        //lambda表达式简化
        DataStream<SensorReading> dataStream = inputStream.map((e) -> {
            final String[] split = e.split(",");
            return new SensorReading(split[0], new Long(split[1]), new Double(split[2]));
        });


        //分组
        KeyedStream<SensorReading, Tuple> keyedStream = dataStream.keyBy("id");


        //reduce聚合, 取最大的温度值,以及当前最新的时间戳
        DataStream<SensorReading> reduce = keyedStream.reduce(new ReduceFunction<SensorReading>() {
            @Override
            public SensorReading reduce(SensorReading cur, SensorReading newData) throws Exception {
                return new SensorReading(cur.getId(), newData.getTimeStamp(), Math.max(cur.getTemperature(), newData.getTemperature()));
            }
        });
//        keyedStream.reduce((c,n)->{
//            return new SensorReading(c.getId(), n.getTimeStamp(), Math.max(c.getTemperature(), n.getTemperature()));
//        });

        reduce.print();

        env.execute();
    }
}
