package com.learn.window;

import com.learn.bean.ClickSource;
import com.learn.bean.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.HashSet;

public class WindowAggregateExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long l) {
                                return event.timestamp;
                            }
                        })
                );

        //所有数据设备相同的key，发送到通哟个分区统计pv和uv，再相除
        stream.keyBy(data -> true)
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(2)))
                .aggregate(new AvgPv())
                .print();

        env.execute();
    }

    public static class AvgPv implements AggregateFunction<Event, Tuple2<HashSet<String>, Long>, Double> {

        //创建累加器
//        创建一个累加器，这就是为聚合创建了一个初始状态，每个聚
// 合任务只会调用一次。
        @Override
        public Tuple2<HashSet<String>, Long> createAccumulator() {
            return Tuple2.of(new HashSet<String>(), 0L);
        }

        //        属于本窗口的数据来一条累加一起，并返回累加器
        @Override
        public Tuple2<HashSet<String>, Long> add(Event value, Tuple2<HashSet<String>, Long> acc) {
            acc.f0.add(value.user);
            return Tuple2.of(acc.f0, acc.f1 + 1L);
        }

        @Override
        public Double getResult(Tuple2<HashSet<String>, Long> acc) {
            //窗口闭合时，增量聚合结束，将计算结果发送到下游
            return (double) acc.f1 / acc.f0.size();
        }

        //不涉及会话窗口，不做任何操作
        @Override
        public Tuple2<HashSet<String>, Long> merge(Tuple2<HashSet<String>, Long> hashSetLongTuple2, Tuple2<HashSet<String>, Long> acc1) {
            return null;
        }
    }
}
