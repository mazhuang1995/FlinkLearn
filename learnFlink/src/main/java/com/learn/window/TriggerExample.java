package com.learn.window;


import com.learn.bean.ClickSource;
import com.learn.bean.Event;
import com.learn.bean.UrlViewCount;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.stream.Stream;

/**
 * 计算了每个 url 在 10 秒滚动窗
 * 口的 pv 指标，然后设置了触发器，每隔 1 秒钟触发一次窗口的计算。
 */
public class TriggerExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new ClickSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                    @Override
                                    public long extractTimestamp(Event event, long l) {
                                        return event.timestamp;
                                    }
                                })
                )
                .keyBy(r -> r.url)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .trigger(new Mytriger())
                .process(new WindowResult())
                .print();

        env.execute();
    }

    public static class WindowResult extends ProcessWindowFunction<Event, UrlViewCount, String, TimeWindow> {
        @Override
        public void process(String url, Context context, Iterable<Event> elements, Collector<UrlViewCount> out) throws Exception {
            out.collect(
                    new UrlViewCount(url, elements.spliterator().getExactSizeIfKnown(), context.window().getStart(), context.window().getEnd())
            );
        }
    }

    /**
     * 这三个方法返回类型都是 TriggerResult，这是一个枚举类型（enum），
     * 其中定义了对窗口进行操作的四种类型。
     * ⚫ CONTINUE（继续）：什么都不做
     * ⚫ FIRE（触发）：触发计算，输出结果
     * ⚫ PURGE（清除）：清空窗口中的所有数据，销毁窗口
     * ⚫ FIRE_AND_PURGE（触发并清除）：触发计算输出结果，并清除窗口
     */
    public static class Mytriger extends Trigger<Event, TimeWindow> {

        //窗口中每到来一个元素，都会调用这个方法。
        @Override
        public TriggerResult onElement(Event element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
            ValueState<Boolean> isFirstEvent = ctx.getPartitionedState(
                    new ValueStateDescriptor<Boolean>("first-event", Types.BOOLEAN)
            );
            //每秒钟注册一个定时器
            if (isFirstEvent.value() == null) {
                for (Long i = window.getStart(); i < window.getEnd(); i = i + 1000L) {
                    ctx.registerEventTimeTimer(i);
                }
                isFirstEvent.update(true);
            }
            return TriggerResult.CONTINUE;
        }

        //当注册的处理时间定时器触发时，将调用这个方法。
        @Override
        public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }

        //当注册的事件时间定时器触发时，将调用这个方法。
        @Override
        public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.FIRE;
        }

        //        当窗口关闭销毁时，调用这个方法。一般用来清除自定义的状态。
        @Override
        public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
            ValueState<Boolean> isFirstEvent = ctx.getPartitionedState(
                    new ValueStateDescriptor<Boolean>("first-event", Types.BOOLEAN)
            );

            isFirstEvent.clear();
        }
    }
}
