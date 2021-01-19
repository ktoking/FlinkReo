package com.atguigu.apitest.state;

import com.atguigu.beans.SensorReading;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import scala.Tuple3;

public class StateTest3_KeyedStateAapplicationCase {

    public static void main(String[] args) throws Exception{
        //创建执行环境
        StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); //保证一个线程solt读有序

        //从文件读取数据
        DataStream<String> inputStream=env.readTextFile("D:\\sorf\\ideaReo\\FlinkReo\\src\\main\\resources\\sensor.txt");

        //lambda表达式简化
        DataStream<SensorReading> dataStream = inputStream.map((e) -> {
            final String[] split = e.split(",");
            return new SensorReading(split[0], new Long(split[1]), new Double(split[2]));
        });

        SingleOutputStreamOperator<Tuple3<String,Double,Double>> resuleStream = dataStream
                .keyBy("id")
                .flatMap(new TempChangeWarning(10.0) {

                });

        resuleStream.print();
        env.execute();
    }

    //实现自定义函数类
    public static class TempChangeWarning extends RichFlatMapFunction<SensorReading, Tuple3<String,Double,Double>>{

        //私有属性，当前温度的阈值
        private Double threshold;

        public TempChangeWarning(Double threshold) {
            this.threshold = threshold;
        }

        //定义状态，保存上一次的温度值
        private ValueState<Double> lastTempState;

        @Override
        public void open(Configuration parameters) throws Exception {
            lastTempState=getRuntimeContext().getState(new ValueStateDescriptor<Double>("last-temp",Double.class));
        }

        @Override
        public void flatMap(SensorReading sensorReading, Collector<Tuple3<String, Double, Double>> out) throws Exception {
            //获取状态
            Double value = lastTempState.value();

            //不为null,就计算两次差值是否大于阈值
            if(value!=null){
                double diff = Math.abs(sensorReading.getTemperature() - value);
                if(diff>=threshold){
                    out.collect(new Tuple3<>(sensorReading.getId(),value,sensorReading.getTemperature()));
                }
            }

            //如果为null，保存这一次的值
            lastTempState.update(sensorReading.getTemperature());

        }

        @Override
        public void close() throws Exception {
            lastTempState.clear();
        }
    }

}
