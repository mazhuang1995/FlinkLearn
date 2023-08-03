package com.learn.sql;

import com.learn.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.InputGroup;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

public class ScalarFunctionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        DataStreamSource<WaterSensor> sensorDS = env.fromElements(new WaterSensor("s1", 1L, 2), new WaterSensor("s1", 1L, 7), new WaterSensor("s2", 1L, 2), new WaterSensor("s3", 1L, 1), new WaterSensor("s3", 1L, 4));

        Table sensorTable = tableEnv.fromDataStream(sensorDS);
        tableEnv.createTemporaryView("sensor", sensorTable);

        //TODO 2.注册函数
        tableEnv.createTemporaryFunction("HashFunction", HashFunction.class);

        //TODO 3.调用自定义函数
//        3.1 sql用法
//        tableEnv
//                .sqlQuery("select id,HashFunction(id) as hashId from sensor")
//                .execute()
//                .print();

        //3.2 table api 用法
        sensorTable.select(call("HashFunction", $("id"))).execute().print();


    }

    //TODO 1.定义自定义函数的实现类
    public static class HashFunction extends ScalarFunction {
        //接收任意类型的输入，返回int类型输出
//        DataTypeHint(inputGroup = InputGroup.ANY)对输入参数的类型做了标注，表示 eval 的 参数可以是任意类型
        public int eval(@DataTypeHint(inputGroup = InputGroup.ANY) Object o) {
            return o.hashCode();
        }
    }
}
