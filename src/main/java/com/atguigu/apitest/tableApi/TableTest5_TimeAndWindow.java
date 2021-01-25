package com.atguigu.apitest.tableApi;

import com.atguigu.beans.SensorReading;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class TableTest5_TimeAndWindow {
    public static void main(String[] args) throws Exception {

        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //设置为事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 2. 导入文件数据
        DataStreamSource<String> inputStream = env.readTextFile("D:\\sorf\\ideaReo\\FlinkReo\\src\\main\\resources\\sensor.txt");

        // 3. 转化为POJO
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] split = line.split(",");
            return new SensorReading(split[0], new Long(split[1]), new Double(split[2]));
        })
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.seconds(2)) {
                    @Override
                    public long extractTimestamp(SensorReading sensorReading) {
                        return sensorReading.getTimeStamp()*1000L;
                    }
                });

        // 4. 将流转化为表（指定处理时间xx.proctime）
//        Table dataTable = tableEnv.fromDataStream(dataStream, "id , timeStamp as ts , temperature as temp , pt.proctime");
//        Table dataTable = tableEnv.fromDataStream(dataStream, "id , timeStamp.rowtime as ts , temperature as temp");
        Table dataTable = tableEnv.fromDataStream(dataStream, "id , timeStamp as ts , temperature as temp , rt.rowtime");

        // 5. 窗口操作
        // group Window
        // table API
        Table resultTable = dataTable.window(Tumble.over("10.seconds").on("rt").as("tw"))
                .groupBy("id,tw")
                .select("id,id.count,temp.avg,tw.end");


        //打印表结构
        dataTable.printSchema();
        tableEnv.toAppendStream(dataTable,Row.class).print();

    }
}
