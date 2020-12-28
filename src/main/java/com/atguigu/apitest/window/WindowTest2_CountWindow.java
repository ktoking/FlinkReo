package com.atguigu.apitest.window;

import com.atguigu.beans.SensorReading;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.runtime.aggregate.AggregateAggFunction;


public class WindowTest2_CountWindow {
    public static void main(String[] args) throws Exception {
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


        //开计数窗口测试
        DataStream<Double> avgStream=dataStream.keyBy("id")
                .countWindow(10,2)
                .aggregate(new AggregateFunction<SensorReading, Tuple2<Double,Integer>,Double>(){

                    @Override
                    public Tuple2<Double, Integer> createAccumulator() {
                        return new Tuple2<>(0.0,0);
                    }

                    @Override
                    public Tuple2<Double, Integer> add(SensorReading sensorReading, Tuple2<Double, Integer> accumulator) {

                        return new Tuple2(accumulator.f0+sensorReading.getTemperature(),accumulator.f1+1);
                    }

                    @Override
                    public Double getResult(Tuple2<Double, Integer> accumulator) {
                        return accumulator.f0/accumulator.f1;
                    }

                    @Override
                    public Tuple2<Double, Integer> merge(Tuple2<Double, Integer> a, Tuple2<Double, Integer> b) {
                        return new Tuple2<>(a.f0+b.f0,a.f1+b.f1);
                    }
                });

        avgStream.print();

        env.execute();

    }
}
