package com.atguigu.networkflow_analysis.hotViews;

import com.atguigu.networkflow_analysis.beans.ApacheLogEvent;
import com.atguigu.networkflow_analysis.beans.PageViewCount;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.awt.*;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;

public class HotPages {

    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        // 1. 读取文件,转化为pojo类型
        DataStream<String> inputStream = env.readTextFile("D:\\sorf\\ideaReo\\FlinkReo\\NetworkFlowAnalysis\\src\\main\\resources\\apache.log");

        DataStream<ApacheLogEvent> dataStream= inputStream.map( e -> {
            String[] fields=e.split(" ");
            SimpleDateFormat simpleDateFormat=new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss");
            long time = simpleDateFormat.parse(fields[3]).getTime();
            return new ApacheLogEvent(fields[0],fields[1],time,fields[5],fields[6]);
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<ApacheLogEvent>(Time.minutes(1)) {
            @Override
            public long extractTimestamp(ApacheLogEvent apacheLogEvent) {
                return apacheLogEvent.getTimestamp();
            }
        });

        // 2. 分组开窗聚合
        SingleOutputStreamOperator<PageViewCount> windowAggStream = dataStream.filter(e -> {
            return e.getMethod().equals("GET");
        })     // 过滤GET请求
                .keyBy(ApacheLogEvent::getUrl)      // 按照URL分组
                .timeWindow(Time.minutes(10), Time.seconds(5))
                .aggregate(new PageCountAgg(), new PageCountResult());

        // 3. 收集同一窗口count数据，排序输出
        DataStream<String> resultStream = windowAggStream.keyBy(PageViewCount::getWindowEnd)
                .process(new TopNHotPages(3));

        resultStream.print();

        env.execute("hot page job");
    }

    // 自定义的聚合函数
    public static class PageCountAgg implements AggregateFunction<ApacheLogEvent,Long,Long>{

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(ApacheLogEvent apacheLogEvent, Long aLong) {
            return aLong+1;
        }

        @Override
        public Long getResult(Long aLong) {
            return aLong;
        }

        @Override
        public Long merge(Long aLong, Long acc1) {
            return acc1+aLong;
        }
    }

    // 实现自定义的窗口函数
    public static class PageCountResult implements WindowFunction<Long,PageViewCount,String, TimeWindow>{

        @Override
        public void apply(String url, TimeWindow timeWindow, Iterable<Long> iterable, Collector<PageViewCount> collector) throws Exception {
            collector.collect(new PageViewCount(url,timeWindow.getEnd(),iterable.iterator().next()));
        }
    }

    // 实现自定义的处理函数
    public static class TopNHotPages extends KeyedProcessFunction<Long,PageViewCount,String>{
        private Integer topSize;

        public TopNHotPages(Integer topSize) {
            this.topSize = topSize;
        }

        // 定义状态，保存当前所有PageViewCount到list中
        ListState<PageViewCount> listState;

        @Override
        public void open(Configuration parameters) throws Exception {
            listState=getRuntimeContext().getListState(new ListStateDescriptor<PageViewCount>("page-count-list",PageViewCount.class));
        }

        @Override
        public void processElement(PageViewCount pageViewCount, Context context, Collector<String> collector) throws Exception {
            listState.add(pageViewCount);
            context.timerService().registerEventTimeTimer(pageViewCount.getWindowEnd()+1);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            ArrayList<PageViewCount> pageViewCounts = Lists.newArrayList(listState.get().iterator());
            pageViewCounts.sort((o1, o2) -> {
                if(o1.getCount()>o2.getCount()) return -1;
                else if(o1.getCount()<o2.getCount()) return 1;
                else return 0;
            });

            //将排名信息格式化成String,方便打印输出
            StringBuilder sb=new StringBuilder();
            sb.append("===================================");
            sb.append("窗口结束时间:").append(new Timestamp(timestamp-1)).append("\n");

            // 格式化成String
            // 遍历列表,取出topN
            for (int i = 0; i < Math.min(topSize,pageViewCounts.size()); i++) {
                PageViewCount currentItemViewCount = pageViewCounts.get(i);
                sb.append("NO ").append(i+1).append(":")
                        .append("页面URL = ").append(currentItemViewCount.getUrl())
                        .append("浏览量 = ").append(currentItemViewCount.getCount())
                        .append("\n");
            }

            sb.append("============================\n\n");

            out.collect(sb.toString());
        }

    }

}
