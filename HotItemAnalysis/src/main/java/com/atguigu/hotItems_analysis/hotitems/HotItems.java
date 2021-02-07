package com.atguigu.hotItems_analysis.hotitems;

import com.atguigu.hotItems_analysis.beans.ItemViewCount;
import com.atguigu.hotItems_analysis.beans.UserBehavior;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;


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
        DataStream<ItemViewCount> windowaggStream = dataStream
                .filter( e -> {  // 过滤pv行为
                    return e.getBehavior().equals("pv");
                })
                .keyBy("itemId") // 按照商品id分组
                .timeWindow(Time.hours(1),Time.minutes(5))  //开滑动窗口
                .aggregate(new ItemCountAgg(),new WindowItemCountResult());


        // 5. 收集同一窗口的所有商品count数据,排序输出topN
        DataStream<String> windowEnd = windowaggStream
                .keyBy("windowEnd") //按照窗口分组
                .process(new TopNHotItems(5));// 用自定义处理函数排序取前N

        windowEnd.print();

        env.execute("hot items analysis");

    }

    // 实现增量聚合函数
    public static class ItemCountAgg implements AggregateFunction<UserBehavior,Long,Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UserBehavior userBehavior, Long aLong) {
            return aLong+1;
        }

        @Override
        public Long getResult(Long aLong) {
            return aLong;
        }

        @Override
        public Long merge(Long aLong, Long acc1) {
            return acc1+acc1;
        }
    }

    // 自定义全窗口函数
    public static class WindowItemCountResult implements WindowFunction<Long,ItemViewCount, Tuple, TimeWindow> {

        @Override
        public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Long> iterable, Collector<ItemViewCount> collector) throws Exception {
            Long itemId = tuple.getField(0);
            long end = timeWindow.getEnd();
            Long count = iterable.iterator().next();
            collector.collect(new ItemViewCount(itemId,end,count));
        }
    }

    public static class TopNHotItems extends KeyedProcessFunction<Tuple,ItemViewCount,String> {

        // 定义属性,topN的大小
        private Integer topN;

        public TopNHotItems(Integer topN) {
            this.topN = topN;
        }

        // 定义列表状态,保存当前窗口内所有输出的ItemViewCount
        ListState<ItemViewCount> listState;

        @Override
        public void open(Configuration parameters) throws Exception {
            listState=getRuntimeContext().getListState(new ListStateDescriptor<ItemViewCount>("item-view-count-list",ItemViewCount.class));
        }

        @Override
        public void processElement(ItemViewCount itemViewCount, Context context, Collector<String> collector) throws Exception {
            //每来一条数据就存入list中,并注册定时器
            listState.add(itemViewCount);
            context.timerService().registerEventTimeTimer(itemViewCount.getWindowEnd()+1);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            // 定时器触发,当前已收集到所有数据,排序输出
            ArrayList<ItemViewCount> itemViewCounts = Lists.newArrayList(listState.get().iterator());

            itemViewCounts.sort((n1,n2)->{
                return n2.getCount().intValue()-n1.getCount().intValue();
            });

            //将排名信息格式化成String,方便打印输出
            StringBuilder sb=new StringBuilder();
            sb.append("===================================");
            sb.append("窗口结束时间:").append(new Timestamp(timestamp-1)).append("\n");

            // 遍历列表,取出topN
            for (int i = 0; i < Math.min(topN,itemViewCounts.size()); i++) {
                ItemViewCount currentItemViewCount = itemViewCounts.get(i);
                sb.append("NO ").append(i+1).append(":")
                        .append("商品ID = ").append(currentItemViewCount.getItemId())
                        .append("热门度 = ").append(currentItemViewCount.getCount())
                        .append("\n");
            }

            sb.append("============================\n\n");

            out.collect(sb.toString());

        }
    }
}
