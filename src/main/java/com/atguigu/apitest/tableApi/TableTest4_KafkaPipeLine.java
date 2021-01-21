package com.atguigu.apitest.tableApi;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;

public class TableTest4_KafkaPipeLine {
    //winIp
    public static final String IP="localhost";

    public static void main(String[] args) throws Exception {


        // 1.创建运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 2. 连接kafka读取数据
        tableEnv.connect(new Kafka()
                .version("0.11")
                .topic("sensor")
                .property("zookeeper.connect",IP+":2181")
                .property("bootstrap.servers",IP+":9092")
        )
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("timestamp",DataTypes.BIGINT())
                        .field("temp",DataTypes.DOUBLE())
                )
                .createTemporaryTable("inputTable");


        // 3. 查询转换
        Table sensorTable = tableEnv.from("inputTable");


        //简单转化
        Table resultTable = sensorTable.select("id,temp")
                .filter("id='sensor_6'");

        //聚合统计
        Table aggTable = sensorTable.groupBy("id")
                .select("id,id.count as count,temp.avg as avgTemp");

        // 3.2 Sql
        tableEnv.sqlQuery("select id,temp from inputTable where id = 'sensor_6' ");
        Table avgTempTable = tableEnv.sqlQuery("select id , count(id) as cnt, avg(temp) as avgTemp from inputTable group by id");

        // 4. 建立kafka连接,输出到不同的topic
        tableEnv.connect(new Kafka()
                .version("0.11")
                .topic("sinktest")
                .property("zookeeper.connect","39.96.86.4:2181")
                .property("bootstrap.servers","39.96.86.4:9092")
        )
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
//                        .field("timestamp",DataTypes.BIGINT())
                        .field("temp",DataTypes.DOUBLE())
                )
                .createTemporaryTable("outputTable");

        resultTable.insertInto("outputTable");

        env.execute();
    }
}
