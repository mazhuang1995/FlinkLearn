package com.learn.window;

import com.learn.bean.UrlViewCount;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import com.learn.bean.Event;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;


/**
 * 处理迟到数据
 */
public class ProcessLateDataExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 读取socket文本流
        SingleOutputStreamOperator<Event> stream =
                env.socketTextStream("localhost", 7777)
                        .map(new MapFunction<String, Event>() {
                            @Override
                            public Event map(String value) throws Exception {
                                String[] fields = value.split(" ");
                                return new Event(fields[0].trim(), fields[1].trim(), Long.valueOf(fields[2].trim()));
                            }
                        })
                        // 方式一：设置watermark延迟时间，2秒钟
                        .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                    @Override
                                    public long extractTimestamp(Event element, long recordTimestamp) {
                                        return element.timestamp;
                                    }
                                }));

        //定义侧输出流标签
        OutputTag<Event> outputTag = new OutputTag<Event>("late") {
        };

        SingleOutputStreamOperator<UrlViewCount> result = stream
                .keyBy(data -> data.url)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                //方式二：允许窗口处理迟到的数据，设置1分钟的等待时间
                .allowedLateness(Time.minutes(1))
                //方式三：将最后的迟到数据输出到侧输出流
                .sideOutputLateData(outputTag)
                .aggregate(new UrlCountAgg(), new UrlCountResult());

        result.print("result");
        result.getSideOutput(outputTag).print("late");

        env.execute();

    }

    public static class UrlCountResult extends ProcessWindowFunction<Long, UrlViewCount, String, TimeWindow> {
        @Override
        public void process(String url, Context context, Iterable<Long> elements, Collector<UrlViewCount> out) throws Exception {
            //结合窗口信息，包装输出内容
            long start = context.window().getStart();
            long end = context.window().getEnd();
            out.collect(new UrlViewCount(url, elements.iterator().next(), start, end));

        }
    }

    public static class UrlCountAgg implements AggregateFunction<Event, Long, Long> {


        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Event event, Long acc) {
            return acc + 1;
        }

        @Override
        public Long getResult(Long acc) {
            return acc;
        }

        @Override
        public Long merge(Long aLong, Long acc1) {
            return null;
        }
    }
}
