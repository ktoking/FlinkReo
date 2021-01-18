package com.atguigu.apitest.state;

import com.atguigu.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Collections;
import java.util.List;

public class StateTest1_OperatorState {
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

        //定义一个有状态的map操作，统计当前分区的数据个数
        SingleOutputStreamOperator<Integer> resultStream = dataStream.map(new MyCountMapper() {
        });

        env.execute();
    }

    //自定义MapFuntion
    public static class MyCountMapper implements MapFunction<SensorReading,Integer>, ListCheckpointed<Integer> {
        int count=0; //本地变量，保存算子状态

        @Override
        public Integer map(SensorReading sensorReading) throws Exception {
            count++;
            return count;
        }

        //快照保存count的值
        @Override
        public List<Integer> snapshotState(long checkPointId, long tempstamp) throws Exception {
            return Collections.singletonList(count);
        }

        //快照恢复值，便利list去拿到
        @Override
        public void restoreState(List<Integer> state) throws Exception {
            for (Integer integer : state) {
                count+=integer;
            }
        }
    }
}
