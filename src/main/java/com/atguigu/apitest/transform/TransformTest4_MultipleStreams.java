package com.atguigu.apitest.transform;

import com.atguigu.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

import java.util.Collections;
import java.util.List;

public class TransformTest4_MultipleStreams {
    public static void main(String[] args) throws Exception{
        //创建执行环境
        StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); //保证一个线程solt读有序

        //从文件读取数据
        DataStream<String> inputStream=env.readTextFile("G:\\idea_repo\\FlinkTutorial\\src\\main\\resources\\sensor.txt");

        //封装对象
        DataStream<SensorReading> dataStream = inputStream.map((e) -> {
            final String[] split = e.split(",");
            return new SensorReading(split[0], new Long(split[1]), new Double(split[2]));
        });


        //1,分流操作 按照温度值30度为界
         SplitStream<SensorReading> splitStream = dataStream.split(new OutputSelector<SensorReading>() {
            @Override
            public Iterable<String> select(SensorReading sensorReading) {
                return (sensorReading.getTemperature() > 30) ? Collections.singletonList("high") : Collections.singletonList("low");
            }
        });

         DataStream<SensorReading> high = splitStream.select("high");
         DataStream<SensorReading> low = splitStream.select("low");
         DataStream<SensorReading> all = splitStream.select("high","low");

         high.print("high");
         low.print("low");
         all.print("all");

         //2.合并流,将高温流转换成二元组类型,与低温流连接合并之后,输出状态信息
        SingleOutputStreamOperator<Tuple2<String, Double>> warnStream = high.map(new MapFunction<SensorReading, Tuple2<String, Double>>() {
            @Override
            public Tuple2<String, Double> map(SensorReading sensorReading) throws Exception {
                return new Tuple2(sensorReading.getId(), sensorReading.getTemperature());
            }
        });

        //warn合并low stream
        ConnectedStreams<Tuple2<String, Double>, SensorReading> connectedStreams = warnStream.connect(low);

        //指定第三个参数为合并后的流的类型
         DataStream<Object> resultData = connectedStreams.map(new CoMapFunction<Tuple2<String, Double>, SensorReading,Object>() {

             @Override
             public Object map1(Tuple2<String, Double> value) throws Exception {
                 return new Tuple3<>(value.f0,value.f1,"high temp warning");
             }

             @Override
             public Object map2(SensorReading value) throws Exception {
                 return new Tuple2<>(value.getId(),"normal");
             }
         });

        resultData.print();


        // 3. union联合多条流 数据类型必须相同
        DataStream<SensorReading> union = high.union(low, all);


        env.execute();


    }
}
