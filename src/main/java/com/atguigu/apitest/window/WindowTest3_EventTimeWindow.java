package com.atguigu.apitest.window;

import com.atguigu.beans.SensorReading;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

public class WindowTest3_EventTimeWindow {

    public static void main(String[] args) throws Exception {
        //创建执行环境
        StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); //保证一个线程solt读有序
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //从文件读取数据
        DataStream<String> inputStream=env.readTextFile("D:\\sorf\\ideaReo\\FlinkReo\\src\\main\\resources\\sensor.txt");

        //lambda表达式简化
        DataStream<SensorReading> dataStream = inputStream.map((e) -> {
            final String[] split = e.split(",");
            return new SensorReading(split[0], new Long(split[1]), new Double(split[2]));
        })
                //升序数据设置事件时间和watermark
//                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<SensorReading>() {
//                    @Override
//                    public long extractAscendingTimestamp(SensorReading sensorReading) {
//                        return sensorReading.getTimeStamp()*1000L;
//                    }
//                })
                // 乱序数据设计时间戳和watermark
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.seconds(2)) { //定义最大乱序程度
            @Override
            public long extractTimestamp(SensorReading sensorReading) { //提取时间戳
                return sensorReading.getTimeStamp()*1000L;
            }
        });


        //统计15秒内温度最小值，基于事件的时间开创聚合
        DataStream<SensorReading> minTemperature = dataStream.keyBy("id").timeWindow(Time.seconds(5)).minBy("temperature");
        minTemperature.print();

        env.execute();
    }
}
