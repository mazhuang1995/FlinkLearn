package com.learn.sql;

import com.learn.bean.WaterSensor;
import com.sun.xml.internal.bind.v2.TODO;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class TableStreamDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        DataStreamSource<WaterSensor> sensorDS = env.fromElements(
                new WaterSensor("s1", 1L, 1),
                new WaterSensor("s1", 2L, 2),
                new WaterSensor("s2", 2L, 2),
                new WaterSensor("s3", 3L, 3),
                new WaterSensor("s3", 4L, 4)
        );

        //TODO 1.流转换成表
        //tableEnvironment.fromChangelogStream()
        Table sensorTable = tableEnvironment.fromDataStream(sensorDS);
        tableEnvironment.createTemporaryView("sensor", sensorTable);

        Table filterTable = tableEnvironment.sqlQuery("select id,ts,vc from sensor where ts > 2");
        Table sumTable = tableEnvironment.sqlQuery("select id,sum(vc) from sensor group by id");

        //TODO 2.追加流
        //2.1 追加流
        tableEnvironment.toDataStream(filterTable,WaterSensor.class).print("filter");
        //2.2 changelog流：结果需要更新
        tableEnvironment.toChangelogStream(sumTable).print("sum");

        //只要代码中调用了DataStreamAPI ,就需要execute ,否则不需要
        env.execute();

    }
}
