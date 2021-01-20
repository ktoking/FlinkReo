package com.atguigu.apitest.tableApi;


import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

public class TableTest2_CommonApi {
    public static void main(String[] args) throws Exception{


        StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


        //基于老版本的planner的流处理
        EnvironmentSettings oldStreamSetting = EnvironmentSettings.newInstance()
                .useOldPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(env,oldStreamSetting);


        //基于老版本的planner批处理
        ExecutionEnvironment batchEnv = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment oldBatchTableEnv = BatchTableEnvironment.create(batchEnv);


        //基于blink的流处理
        EnvironmentSettings blinkStreamSetting = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment blinkStreamTableEnv = StreamTableEnvironment.create(env,blinkStreamSetting);

        //基于blink的批处理
        EnvironmentSettings blinkBatchSetting = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inBatchMode()
                .build();
        TableEnvironment blinkBatch =TableEnvironment.create(blinkBatchSetting);


        // 2 表的创建：链接外部系统，读取数据
        // 2.1 读取文件
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
//        inputTable.printSchema();
//        tableEnv.toAppendStream(inputTable, Row.class).print();

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


        //打印输出 toRetractStream表示一条数据的撤回，一条数据的写入
        tableEnv.toAppendStream(resultTable,Row.class).print("result");
        tableEnv.toRetractStream(aggTable,Row.class).print("agg");
        tableEnv.toRetractStream(avgTempTable,Row.class).print("avgTemp");

        env.execute();


    }
}
