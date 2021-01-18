package com.atguigu.apitest.state;

import com.atguigu.beans.SensorReading;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StateTest2_KeyedState {
    public static void main(String[] args) throws Exception{
        //创建执行环境
        StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); //保证一个线程solt读有序

        //从文件读取数据
        DataStream<String> inputStream=env.readTextFile("D:\\sorf\\ideaReo\\FlinkReo\\src\\main\\resources\\sensor.txt");

        //lambda表达式简化
        DataStream<SensorReading> dataStream = inputStream.map((e) -> {
            final String[] split = e.split(",");
            return new SensorReading(split[0], new Long(split[1]), new Double(split[2]));
        });

        dataStream
                .keyBy("id")
                .map(new MyKeyCountMapper(){});
    }

    //自定义RichMapFuntion
    public static  class MyKeyCountMapper extends RichMapFunction<SensorReading,Integer>{

        @Override
        public Integer map(SensorReading sensorReading) throws Exception {
            return null;
        }
    }
}
