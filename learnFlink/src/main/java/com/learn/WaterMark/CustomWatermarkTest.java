package com.learn.WaterMark;


import com.learn.bean.ClickSource;
import com.learn.bean.Event;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.stream.Stream;

/**
 * 自定义周期性生成水位线：
     *  我们在 onPeriodicEmit()里调用 output.emitWatermark()，就可以发出水位线了；这个方法
     *  由系统框架周期性地调用，默认 200ms 一次。所以水位线的时间戳是依赖当前已有数据的最
     *  大时间戳的（这里的实现与内置生成器类似，也是减去延迟时间再减 1），但具体什么时候生
     *  成与数据无关。
 */
public class CustomWatermarkTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //设置水位线周期发送时间
//        env.getConfig().setAutoWatermarkInterval(200L);


        env
                .addSource(new ClickSource())
                .assignTimestampsAndWatermarks(new CustomWatermarkStrategy())
                .print();

        env.execute();
    }


    public static class CustomWatermarkStrategy implements WatermarkStrategy<Event> {
        @Override
        public TimestampAssigner<Event> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
            return new SerializableTimestampAssigner<Event>() {
                @Override
                public long extractTimestamp(Event element, long recordTimestamp) {
                    //告诉程序数据源里的时间戳是那个字段
                    return element.timestamp;
                }
            };
        }

        @Override
        public WatermarkGenerator<Event> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
            return new WatermarkGenerator<Event>() {
                //延迟时间
                private Long delayTime = 5 * 1000L;
                //观察到的最大时间戳
                private Long maxTs = Long.MIN_VALUE + delayTime + 1L;


                //每来一条数据就调用一次
                @Override
                public void onEvent(Event event, long eventTimestamp, WatermarkOutput output) {
                    maxTs = Math.max(event.timestamp, maxTs);
                }

                //发射水位线，默认200ms调用一次
                @Override
                public void onPeriodicEmit(WatermarkOutput output) {
                    //发送水位线，默认200ms调用一次
                    output.emitWatermark(new Watermark(maxTs - delayTime - 1L));
                }
            };
        }

    }


}
