package com.atguigu.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

//批处理word count
public class WordCount {
    public static void main(String[] args) throws Exception {
        //创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //从文件中读取路径
        String path="G:\\idea_repo\\FlinkTutorial\\src\\main\\resources\\hello.txt";
        DataSource<String> inputDataSource = env.readTextFile(path);

        //对数据集进行处理,按照空格进行展开,按照(word, count)进行统计
        DataSet<Tuple2<String, Integer>> resultset = inputDataSource.flatMap(new MyFlatMapper())
                .groupBy(0)    //按照第0个位置分组
                .sum(1);//将第二个位置上的数据求和

        resultset.print();

    }

    public  static  class MyFlatMapper implements FlatMapFunction<String, Tuple2<String,Integer>>{
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            //按空格分词
            String[] word = value.split(" ");
            //遍历所有word,包成二元组输出
            for (String s : word) {
                out.collect(new Tuple2<String, Integer>(s,1));
            }
        }
    }
}
