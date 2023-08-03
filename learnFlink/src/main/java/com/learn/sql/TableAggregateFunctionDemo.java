package com.learn.sql;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.util.Collector;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

public class TableAggregateFunctionDemo {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        //  姓名，分数，权重
        DataStreamSource<Integer> numDS = env.fromElements(3, 6, 12, 5, 8, 9, 4);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        Table numTable = tableEnv.fromDataStream(numDS, $("num"));

        // TODO 2.注册函数
        tableEnv.createTemporaryFunction("Top2", Top2.class);

        // TODO 3.调用 自定义函数: 只能用 Table API
        /**
         * 目前SQL中没有直接使用表聚合函数的方式，所以需要使用Table API的方式来调用。
         * 这里使用了flatAggregate()方法，它就是专门用来调用表聚合函数的接口。
         * 统计num值最大的两个；并将聚合结果的两个字段重命名为value和rank，之后就可以使用select()将它们提取出来了。
         */
        numTable
                .flatAggregate(call("Top2", $("num")).as("value", "rank"))
                .select($("value"), $("rank"))
                .execute().print();

    }

    // TODO 1.继承TableAggregateFunction

    /**
     * AggregateFunction有两个泛型参数<T, ACC>，T表示聚合输出的结果类型，ACC则表示聚合的中间状态类型。
     */
    public static class Top2 extends TableAggregateFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> {

        @Override
        public Tuple2<Integer, Integer> createAccumulator() {
            return Tuple2.of(0, 0);
        }

        /**
         * 每来一条数据调用一次，比较大小，更新最大的前两个数到 acc中
         *Exception in thread "main" org.apache.flink.table.api.ValidationException:
         * Function class 'com.learn.sql.TableAggregateFunctionDemo$Top2' does not implement a method named 'accumulate'.
         * @param acc 累加器
         * @param num 过来的数据
         */
        public void accumulate(Tuple2<Integer, Integer> acc, Integer num) {
            if (num > acc.f0) {
                //新来的变成第一，原来的第一变成第二
                acc.f1 = acc.f0;
                acc.f0 = num;
            } else if (num > acc.f0) {
                //新来的变成第二，原来的第二不要了
                acc.f1 = num;
            }
        }

        /**
         * 输出结果： （数值，排名） 两条最大的之
         *
         * @param acc 累加器
         * @param out 采集器<返回类型>
         */
        public void emitValue(Tuple2<Integer, Integer> acc, Collector<Tuple2<Integer, Integer>> out) {
            if (acc.f0 != 0) {
                out.collect(Tuple2.of(acc.f0, 1));
            }
            if (acc.f1 != 0) {
                out.collect(Tuple2.of(acc.f1, 2));
            }

        }


    }

}
