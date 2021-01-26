package com.atguigu.apitest.tableApi.udf;

import com.atguigu.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import scala.Tuple2;

public class UdfTest2_TableFuntion {
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

        //自定义表函数,实现将ID拆分 并输出(word,length)
        Split split=new Split("_");

        //在环境中注册UDF
        tableEnv.registerFunction("split",split);
        Table resultTable = sensorTable
                .joinLateral("split(id) as (word,length)")
                .select("id, ts, word, length");

        //SQL写法
        tableEnv.createTemporaryView("sensor",sensorTable);
        Table resultTableSql = tableEnv.sqlQuery("select id, ts, word, length" +
                " from sensor, lateral table(split(id)) as splitid(word,length)");

        //打印输出
        tableEnv.toAppendStream(resultTable, Row.class).print("resultTable");
        tableEnv.toAppendStream(resultTableSql,Row.class).print("resultTableSql");

        env.execute();

    }

    //实现自定义的TableFuntion
    public static class Split extends TableFunction<Tuple2<String,Integer>>{
        //定义分隔符
        private String separator=",";

        public Split(String separator) {
            this.separator = separator;
        }

        //必须实现一个eval方法,没有返回值
        public void eval(String str){
            for(String s:str.split(separator)){
                collect(new Tuple2<>(s,s.length()));
            }
        }
    }

}
