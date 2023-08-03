package com.learn.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

public class sqlDemo {
    public static void main(String[] args) throws Exception {
        //TODO 1.创建表环境
        //1.1 写法一
//        EnvironmentSettings settings = EnvironmentSettings.newInstance()
//                .inStreamingMode()
//                .build();
//        TableEnvironment tableEnvironment = TableEnvironment.create(settings);


        //1.2 写法二
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(env);

        //TODO 2.创建表
//        executeSql():语句可以是DDL/DML/DQL/SHOW/DESCRIBE/EXPLAIN/USE
        streamTableEnvironment.executeSql("CREATE TABLE source ( \n" +
                "    id INT, \n" +
                "    ts BIGINT, \n" +
                "    vc INT\n" +
                ") WITH ( \n" +
                "    'connector' = 'datagen', \n" +
                "    'rows-per-second'='1', \n" +
                "    'fields.id.kind'='random', \n" +
                "    'fields.id.min'='1', \n" +
                "    'fields.id.max'='10', \n" +
                "    'fields.ts.kind'='sequence', \n" +
                "    'fields.ts.start'='1', \n" +
                "    'fields.ts.end'='1000000', \n" +
                "    'fields.vc.kind'='random', \n" +
                "    'fields.vc.min'='1', \n" +
                "    'fields.vc.max'='100'\n" +
                ");\n");

        streamTableEnvironment.executeSql("CREATE TABLE sink (\n" +
                "    id INT, \n" +
                "    sumVC INT \n" +
                ") WITH (\n" +
                "'connector' = 'print'\n" +
                ");\n");

        //TODO 3.执行查询
        //3.1 使用sql进行查询
        /**
         * sqlQuery("SELECT * FROM " + tableName + " WHERE a > 12");
         * 注意，返回的Table是一个API对象，并且只包含管道描述。它实际上对应于SQL术语中的视图。调用Table.execute()来触发执行，或者直接使用executeSql(String)。
         * 参数:
         * query—要求值的SQL查询。
         * 返回:
         * Table对象描述用于进一步转换的管道。
         */
        Table table = streamTableEnvironment.sqlQuery("select id,sum(vc) as sumVc from source where id > 5 group by id");

        //将table对象，注册成表名
        streamTableEnvironment.createTemporaryView("tmp",table);
        streamTableEnvironment.sqlQuery("select * from tmp where id > 7;");

        //3.2 用table api 来查询
//        Table source = streamTableEnvironment.from("source");
//        Table result = source
//                .where($("id").isGreater(5))
//                .groupBy($("id"))
//                .aggregate($("vc").sum().as("sumVC"))
//                .select($("id"), $("sumVC"));

        //TODO 4.输出表
        //4.1 sql用法
        streamTableEnvironment.executeSql("insert into sink select * from tmp");
        //4.2 table api用法
//        result.executeInsert("sink");

    }
}
