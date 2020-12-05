package com.atguigu.apitest.source;

import com.atguigu.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;


public class SourceTest4_UDF {

    public static void main(String[] args) throws Exception{
        //创建执行环境
        StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从文件读取数据
        DataStream<SensorReading> dataStream = env.addSource( new MysensorSource());

        //打印输出
        dataStream.print();

        env.execute();
    }

    //实现自定义SourceFuntion
    public static class MysensorSource implements SourceFunction<SensorReading> {
        //定义一个标志位,用来控制数据的产生
        private static boolean running=true;

        public void run(SourceContext<SensorReading> sourceContext) throws Exception {
            //定义一个随机数发生器
            Random random=new Random();

            //设置10个传感器的初始温度
            final HashMap<String, Double> sensorTemoMap = new HashMap<String, Double>();
            for (int i = 0; i < 10; i++) {
                sensorTemoMap.put("sensor_"+(i+1),+60+random.nextGaussian()*20);
            }
            while (running){
                for (Map.Entry<String, Double> sensorMap : sensorTemoMap.entrySet()) {
                    //在当前温度基础上随机波动
                    Double newTemp=sensorMap.getValue();
                    newTemp+=random.nextGaussian();
                    sensorTemoMap.put(sensorMap.getKey(),newTemp);
                    sourceContext.collect(new SensorReading(sensorMap.getKey(),System.currentTimeMillis(),newTemp));
                }
                //控制输出频率
                Thread.sleep(1000L);
            }
        }

        public void cancel() {
            running=false;
        }
    }
}
