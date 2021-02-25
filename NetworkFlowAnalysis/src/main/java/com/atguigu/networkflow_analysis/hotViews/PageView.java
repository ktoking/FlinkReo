package com.atguigu.networkflow_analysis.hotViews;

import com.atguigu.networkflow_analysis.beans.PageViewCount;
import com.atguigu.networkflow_analysis.beans.UserBehavior;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Random;


public class PageView {
    public static void main(String[] args) throws Exception{

        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 2. 读取数据，创建数据流
        DataStream<String> inputStream = env.readTextFile("D:\\sorf\\ideaReo\\FlinkReo\\NetworkFlowAnalysis\\src\\main\\resources\\UserBehavior.csv");

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

//        // 4. 分组开窗聚合， 得到每个窗口内各个商品的count值
//        SingleOutputStreamOperator<Tuple2<String, Long>> pvResultStream = dataStream
//                .filter(e -> {  // 过滤pv行为
//                    return e.getBehavior().equals("pv");
//                })
//                .map(new MapFunction<UserBehavior, Tuple2<String, Long>>() {
//                    @Override
//                    public Tuple2<String, Long> map(UserBehavior userBehavior) throws Exception {
//                        return new Tuple2<>("pv", 1L);
//                    }
//                })
//                .keyBy(0) // 按照商品id分组
//                .timeWindow(Time.hours(1))  //开一小时滚动窗口
//                .sum(1);

        // 并行任务改进，设计随机key,解决数据倾斜问题
        SingleOutputStreamOperator<PageViewCount> pv = dataStream
                .filter(e -> {  // 过滤pv行为
                    return e.getBehavior().equals("pv");
                })
                .map(new MapFunction<UserBehavior, Tuple2<Integer, Long>>() {
                    @Override
                    public Tuple2<Integer, Long> map(UserBehavior userBehavior) throws Exception {
                        Random random = new Random();
                        return new Tuple2<>(random.nextInt(10), 1l);
                    }
                })
                .keyBy(e -> e.f0)
                .timeWindow(Time.hours(1))
                .aggregate(new PvCountAgg(), new PvCountResult());

        // 将各分区数据汇总起来
        SingleOutputStreamOperator<PageViewCount> pvResultStream = pv
                .keyBy(PageViewCount::getWindowEnd)
                .process(new TotalPvCount());
//                .sum("count")


        pvResultStream.print();
        env.execute("PV COUNT JOB");

    }

    // 实现自定义增量聚合函数
    public static class PvCountAgg implements AggregateFunction<Tuple2<Integer,Long>,Long,Long>{

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Tuple2<Integer, Long> integerLongTuple2, Long aLong) {
            return aLong+1;
        }

        @Override
        public Long getResult(Long aLong) {
            return aLong;
        }

        @Override
        public Long merge(Long aLong, Long acc1) {
            return aLong+acc1;
        }
    }

    // 实现自定义窗口函数
    public static class PvCountResult implements WindowFunction<Long, PageViewCount,Integer, TimeWindow>{

        @Override
        public void apply(Integer integer, TimeWindow timeWindow, Iterable<Long> iterable, Collector<PageViewCount> collector) throws Exception {
            collector.collect(new PageViewCount(integer.toString(), timeWindow.getEnd(), iterable.iterator().next()));
        }
    }

    // 实现自定义窗口类，把相同窗口分组统计的count值叠加
    public static class TotalPvCount extends KeyedProcessFunction<Long,PageViewCount,PageViewCount>{

        // 定义状态，保存当前的总count值
        ValueState<Long> totalCountState;

        @Override
        public void open(Configuration parameters) throws Exception {
            totalCountState=getRuntimeContext().getState(new ValueStateDescriptor<Long>("total-count",Long.class,0L));
        }

        @Override
        public void processElement(PageViewCount pageViewCount, Context context, Collector<PageViewCount> collector) throws Exception {
            totalCountState.update(totalCountState.value()+pageViewCount.getCount());
            context.timerService().registerEventTimeTimer(pageViewCount.getWindowEnd()+1);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<PageViewCount> out) throws Exception {

            // 定时器触发，所有分组count值都到齐，直接输出当前的总count数量
            Long value = totalCountState.value();
            out.collect(new PageViewCount("pv",ctx.getCurrentKey(),value));

            // 清空状态
            totalCountState.clear();
        }
    }
}
