package com.atguigu.apitest.sink;

import com.atguigu.beans.SensorReading;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class SinkTest4_Jdbc {
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

        dataStream.addSink(new MyJdbcSink());


        env.execute();
    }

    public static class MyJdbcSink extends RichSinkFunction<SensorReading> {
        //声明连接与预编译语句
        Connection connection=null;
        PreparedStatement insertStmt=null;
        PreparedStatement updateStmt=null;
        @Override
        public void open(Configuration parameters) throws Exception {
            connection= DriverManager.getConnection("jdbc:mysql://localhost:3306/wang","root","kaikai");
            insertStmt=connection.prepareStatement("insert into sensor_temp (id,temp) values(?,?)");
            updateStmt=connection.prepareStatement("update sensor_temp set temp = ? where id = ?");
        }


        @Override
        public void invoke(SensorReading value, Context context) throws Exception {
            //直接执行更新语句,如果没有更新那么就插入
            updateStmt.setDouble(1,value.getTemperature());
            updateStmt.setString(2,value.getId());
            updateStmt.execute();
            if(updateStmt.getUpdateCount()==0){
                insertStmt.setString(1,value.getId());
                insertStmt.setDouble(2,value.getTemperature());
                insertStmt.execute();
            }
        }

        @Override
        public void close() throws Exception {
            insertStmt.close();
            updateStmt.close();
            connection.close();
        }
    }
}
