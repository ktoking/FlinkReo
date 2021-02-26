package com.atguigu.networkflow_analysis.hotViews;

import com.atguigu.networkflow_analysis.beans.PageViewCount;
import com.atguigu.networkflow_analysis.beans.UserBehavior;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;

import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

public class UvWithBloomFilter {
    public static void main(String[] args) throws Exception{
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
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

        SingleOutputStreamOperator<PageViewCount> uvStream=dataStream
                .filter(e->"uv".equals(e.getBehavior()))
                .timeWindowAll(Time.hours(1))
                .trigger(new MyTrigger())
                .process(new UvCountResultWithBloomFliter());

        uvStream.print();

        env.execute("uv count with bloom fliter");
    }

    public static class MyTrigger extends Trigger<UserBehavior, TimeWindow>{

        @Override
        public TriggerResult onElement(UserBehavior userBehavior, long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
            // 每一条数据来到，直接触发窗口计算，并且直接清空窗口
            return TriggerResult.FIRE_AND_PURGE;
        }

        @Override
        public TriggerResult onProcessingTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onEventTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public void clear(TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {

        }
    }

    // 自定义一个布隆过滤器
    public static class MyBloomFliter {
        // 定义位图的大小,一般需要定义2的整次幂
        private Integer cap;

        public MyBloomFliter(Integer cap) {
            this.cap = cap;
        }

        // 实现一个hash函数
        public Long hashCode(String value,Integer seed){
            Long result = 0L;
            for (int i = 0; i < value.length(); i++) {
                result = result*seed+value.charAt(i);
            }
            return result&(cap-1);
        }
    }

    public static class UvCountResultWithBloomFliter extends ProcessAllWindowFunction<UserBehavior, PageViewCount,TimeWindow> {

        // 定义jedis连接和布隆过滤器
        Jedis jedis;
        MyBloomFliter myBloomFliter;

        @Override
        public void open(Configuration parameters) throws Exception {
            jedis=new Jedis("39.96.86.4",6379);
            myBloomFliter=new MyBloomFliter(1<<29);  // 要处理一亿个数据，用64Mb位图
        }

        @Override
        public void close() throws Exception {
            jedis.close();
        }


        @Override
        public void process(Context context, Iterable<UserBehavior> iterable, Collector<PageViewCount> collector) throws Exception {
            // 将位图和窗口的count全部存在redis
            Long windowEnd = context.window().getEnd();
            String bitmapKey=windowEnd.toString();
            // 把count值存成一张hash表
            String countHashName="uv_count";
            String countKey=windowEnd.toString();

            // 1. 取当前的userId
            Long userId=iterable.iterator().next().getUserId();

            // 2. 计算位图中的offset
            Long offset = myBloomFliter.hashCode(userId.toString(), 61);

            // 3. 用redis的getbit命令，判断对应位置的值
            Boolean isExist=jedis.getbit(bitmapKey,offset);

            if(!isExist){
                // 如果不存在，对应位图置为1
                jedis.setbit(bitmapKey,offset,true);

                // 更新redis保存的count值
                Long uvCount =0L;
                String uvCountString = jedis.hget(countHashName, countKey);
                // 如果不存在记录值，那就更新为0，要是有存在值，那我就把这个值=1
                if(uvCountString!=null&&!"".equals(uvCountString))  uvCount=Long.valueOf(uvCountString);

                jedis.hset(countHashName,countKey,String.valueOf(uvCount+1));

                collector.collect(new PageViewCount("uv",windowEnd,uvCount+1));
            }
        }
    }
}
