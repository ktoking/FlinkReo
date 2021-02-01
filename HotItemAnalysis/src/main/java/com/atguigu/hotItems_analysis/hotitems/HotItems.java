package com.atguigu.hotItems_analysis.hotitems;

import com.atguigu.hotItems_analysis.beans.ItemViewCount;
import com.atguigu.hotItems_analysis.beans.UserBehavior;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;


public class HotItems {

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

        // 4. 分组开窗聚合， 得到每个窗口内各个商品的count值
        DataStream<UserBehavior> windowaggStream = dataStream
                .filter( e -> {  // 过滤pv行为
                    return e.getBehavior().equals("pv");
                })
                .keyBy("itemId"); // 按照商品id分组



    }
}
