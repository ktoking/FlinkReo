package com.atguigu.apitest.window;

import com.atguigu.beans.SensorReading;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.runtime.aggregate.AggregateAggFunction;
import org.apache.flink.util.Collector;
import scala.Tuple3;

public class WindowTest1_TimeWndow {

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


        //窗口测试
        //  1.增量聚合,每来一个数+1
        DataStream<Integer> resultStream=dataStream.keyBy("id")
//                .countWindow(10,2);
//                .window(EventTimeSessionWindows.withGap(Time.seconds(15)));
                .timeWindow(Time.seconds(15))
//                .window(TumblingProcessingTimeWindows.of(Time.seconds(15)));
        .aggregate(new AggregateFunction<SensorReading,Integer,Integer>(){

            @Override
            public Integer createAccumulator() {
                return 0;
            }

            @Override
            public Integer add(SensorReading sensorReading, Integer integer) {
                return integer+1;
            }

            @Override
            public Integer getResult(Integer integer) {
                return integer;
            }

            @Override
            public Integer merge(Integer integer, Integer acc1) {
                return integer+acc1;
            }
        });

        resultStream.print();



        //  2.全窗口函数
        DataStream<Tuple3<String,Long,Integer>> resultStream2=dataStream.keyBy("id")
                .timeWindow(Time.seconds(15))
                .apply(new WindowFunction<SensorReading, Tuple3<String,Long,Integer>, Tuple, TimeWindow>() {
                    @Override
                    public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<SensorReading> input, Collector<Tuple3<String,Long,Integer>> out) throws Exception {
                        String Id = tuple.getField(0);
                        Long windowEnd=timeWindow.getEnd();
                        Integer size = IteratorUtils.toList(input.iterator()).size();
                        out.collect(new Tuple3<>(Id,windowEnd,size));
                    }
                });

        resultStream2.print();

        env.execute();

    }
}
