package com.atguigu.hotItems_analysis.hotitems;

import com.atguigu.hotItems_analysis.beans.UserBehavior;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Slide;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class HotItemsWithSql {
    public static void main(String[] args) throws Exception{

        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 2. 读取数据，创建数据流
        DataStream<String> inputStream = env.readTextFile("D:\\sorf\\ideaReo\\FlinkReo\\HotItemAnalysis\\src\\main\\resources\\UserBehavior.csv");

        // 3. 转化为POJO 分配时间戳与waterMark
        DataStream<UserBehavior> dataStream = inputStream
                .map(line -> {
                    String[] arr = line.split(",");
                    return new UserBehavior(new Long(arr[0]), new Long(arr[1]), new Integer(arr[2]), arr[3], new Long(arr[4]));
                })
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
                    @Override
                    public long extractAscendingTimestamp(UserBehavior userBehavior) {
                        return userBehavior.getTimestamp() * 1000L;
                    }
                });

        // 4. 创建表执行环境,用blink版本
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv= StreamTableEnvironment.create(env, settings);

        // 5. 将流转换成表
        Table dataTable = tableEnv.fromDataStream(dataStream, "itemId, behavior, timestamp.rowtime as ts");

        // 6. 分组开窗
        //table api
        Table windowAggTable = dataTable
                .filter("behavior = 'pv' ")
                .window(Slide.over("1.hours").every("5.minutes").on("ts").as("w"))
                .groupBy("itemId,w")
                .select("itemId , w.end as windowEnd, itemId.count as cnt");

        // 7. 利用开窗函数，对count值进行排序并获取Row number，得到Top N
        // SQL
        DataStream<Row> aggStream = tableEnv.toAppendStream(windowAggTable, Row.class);
        tableEnv.createTemporaryView("agg",aggStream,"itemId,windowEnd,cnt");


        Table resultTable = tableEnv.sqlQuery("select * from" +
                " (select *, ROW_NUMBER() over (partition by windowEnd order by cnt desc) as row_num" +
                " from agg ) " +
                " where row_num <= 5 ");

//        tableEnv.toRetractStream(resultTable,Row.class).print();


        // 纯SQL实现
        tableEnv.createTemporaryView("data_table", dataStream, "itemId, behavior, timestamp.rowtime as ts");
        Table resultSqlTable = tableEnv.sqlQuery("select * from " +
                "  ( select *, ROW_NUMBER() over (partition by windowEnd order by cnt desc) as row_num " +
                "  from ( " +
                "    select itemId, count(itemId) as cnt, HOP_END(ts, interval '5' minute, interval '1' hour) as windowEnd " +
                "    from data_table " +
                "    where behavior = 'pv' " +
                "    group by itemId, HOP(ts, interval '5' minute, interval '1' hour)" +
                "    )" +
                "  ) " +
                " where row_num <= 5 ");

        tableEnv.toRetractStream(resultSqlTable,Row.class).print();


        env.execute("hot items with sql job");

    }
}
