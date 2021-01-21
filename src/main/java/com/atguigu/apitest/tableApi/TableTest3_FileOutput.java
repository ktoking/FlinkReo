package com.atguigu.apitest.tableApi;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;

public class TableTest3_FileOutput {
    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //创建执行环境定义
        String filePath="D:\\sorf\\ideaReo\\FlinkReo\\src\\main\\resources\\sensor.txt";
        tableEnv.connect(new FileSystem().path(filePath))
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("timestamp",DataTypes.BIGINT())
                        .field("temp",DataTypes.DOUBLE())
                )
                .createTemporaryTable("inputTable");

        Table inputTable = tableEnv.from("inputTable");

        // 3 查询转换
        // 3.1 Table Api

        //简单转化
        Table resultTable = inputTable.select("id,temp")
                .filter("id='sensor_6'");

        //聚合统计
        Table aggTable = inputTable.groupBy("id")
                .select("id,id.count as count,temp.avg as avgTemp");

        // 3.2 Sql
        tableEnv.sqlQuery("select id,temp from inputTable where id = 'sensor_6' ");
        Table avgTempTable = tableEnv.sqlQuery("select id , count(id) as cnt, avg(temp) as avgTemp from inputTable group by id");


        // 4. 输出到文件
        // 4.1 连接外部文件输出表
        //创建执行环境定义
        String outputPath="D:\\sorf\\ideaReo\\FlinkReo\\src\\main\\resources\\out.txt";
        tableEnv.connect(new FileSystem().path(outputPath))
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("temperature",DataTypes.DOUBLE())
                )
                .createTemporaryTable("outputTable");

        resultTable.insertInto("outputTable");

//        Table outputTable = tableEnv.from("outputTable");



        env.execute();
    }
}
