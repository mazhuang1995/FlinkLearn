package com.learn.sql;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

public class TableFunctionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        DataStreamSource<String> strDS = env.fromElements(
                "hello word",
                "hello word hi",
                "hello java"
        );

        Table table = tableEnvironment.fromDataStream(strDS, $("words"));
        tableEnvironment.createTemporaryView("str",table);

        //TODO 2.注册函数
        tableEnvironment.createTemporaryFunction("SplitFunction", SplitFunction.class);

        //TODO 3.调用自定义函数
        tableEnvironment
                // 3.1 交叉连接
//                .sqlQuery("select words,word,length from str,lateral table(SplitFunction(words))")
                // 3.2 带 on true 条件的左连接
//                .sqlQuery("select words,word,length from str left join lateral table(SplitFunction(words)) on true")
                // 3.3 重命名侧向输出表中的字段
                .sqlQuery("select words,newWord,newLength from str left join lateral table(SplitFunction(words)) as T(newWord,newLength) on true")
                .execute()
                .print();


    }

    // TODO 1.继承 TableFunction<返回的类型>
    @FunctionHint(output = @DataTypeHint("ROW<word STRING,length INT>"))
    public static class SplitFunction extends TableFunction<Row>{
        //返回是void，用collect方法输出
        public void eval(String str){
            for (String word:str.split(" ")
                 ) {
                collect(Row.of(word,word.length()));
            }
        }
    }
}
