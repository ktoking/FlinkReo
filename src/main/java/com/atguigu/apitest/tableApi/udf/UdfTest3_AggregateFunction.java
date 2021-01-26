package com.atguigu.apitest.tableApi.udf;

import com.atguigu.beans.SensorReading;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.expressions.In;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.types.Row;


public class UdfTest3_AggregateFunction {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> inputStream = env.readTextFile("D:\\sorf\\ideaReo\\FlinkReo\\src\\main\\resources\\sensor.txt");

        //转化为POJO
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] split = line.split(",");
            return new SensorReading(split[0], new Long(split[1]), new Double(split[2]));
        });

        //创建表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //基于数据流创建一张表
        Table sensorTable = tableEnv.fromDataStream(dataStream,"id,timeStamp as ts,temperature as temp");

        //自定义聚合函数 统计平均温度值
        AvgTemp avgTemp=new AvgTemp();

        //在环境中注册UDF
        tableEnv.registerFunction("avgTemp",avgTemp);
        Table resultTable = sensorTable
                .groupBy("id")
                .aggregate("avgTemp(temp) as avgTemp")
                .select("id, avgTemp");

        //SQL写法
        tableEnv.createTemporaryView("sensor",sensorTable);
        Table resultTableSql = tableEnv.sqlQuery("select id, avgTemp(temp) from sensor group by id");

        //打印输出
        tableEnv.toRetractStream(resultTable, Row.class).print("resultTable");
        tableEnv.toRetractStream(resultTableSql,Row.class).print("resultTableSql");

        env.execute();

    }

    //实现自定义的
    public static class AvgTemp extends AggregateFunction<Double, Tuple2<Double, Integer>> {

        @Override
        public Double getValue(Tuple2<Double, Integer> accumulator) {
            return accumulator.f0/accumulator.f1;
        }

        @Override
        public Tuple2<Double, Integer> createAccumulator() {
            return new Tuple2<>(0.0,0);
        }

        //必须实现一个accumulator方法,来数据后更新状态
        public void accumulate(Tuple2<Double,Integer> accumulator,Double temp){
            accumulator.f0 += temp;
            accumulator.f1+=1;
        }
    }
}
