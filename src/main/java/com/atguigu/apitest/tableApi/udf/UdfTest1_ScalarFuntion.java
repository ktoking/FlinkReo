package com.atguigu.apitest.tableApi.udf;

import com.atguigu.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;

public class UdfTest1_ScalarFuntion {

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

        //自定义标量函数,实现求ID的hash值
        HashCode hashCode=new HashCode(23);
        //在环境中注册UDF
        tableEnv.registerFunction("hashCode",hashCode);
        Table resultTable = sensorTable.select("id,ts,hashCode(id)");

        //SQL写法
        tableEnv.createTemporaryView("sensor",sensorTable);
        Table resultTableSql = tableEnv.sqlQuery("select id, ts,hashCode(id) from sensor");

        //打印输出
        tableEnv.toAppendStream(resultTable, Row.class).print("resultTable");
        tableEnv.toAppendStream(resultTableSql,Row.class).print("resultTableSql");

        env.execute();

    }

    //实现自己定义的 ScalarFunction
    public static class HashCode extends ScalarFunction{

        private int facot=5;

        public HashCode(int facot) {
            this.facot = facot;
        }

        public int eval(String str){
            return str.hashCode()*facot;
        }
    }
}
