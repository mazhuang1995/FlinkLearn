package com.learn.WaterMark;

import com.learn.bean.ClickSource;
import com.learn.bean.Event;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 自定义断点式生成水位线：
     * 断点式生成器会不停地检测 onEvent()中的事件，当发现带有水位线信息的特殊事件时，
     *  就立即发出水位线。一般来说，断点式生成器不会通过 onPeriodicEmit()发出水位线。
 */
public class CustomWatermarknPeriodicEmitTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //设置水位线周期发送时间
//        env.getConfig().setAutoWatermarkInterval(200L);


        env
                .addSource(new ClickSource())
                .assignTimestampsAndWatermarks(new CustomWatermarkTest.CustomWatermarkStrategy())
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
                //每来一条数据就调用一次
                @Override
                public void onEvent(Event event, long eventTimestamp, WatermarkOutput output) {
                    if(event.user.equals("Mary")){
                        output.emitWatermark(new Watermark(event.timestamp -1L));
                    }
                }

                //断点式：onPeriodicEmit()不需要做任何事情，因为在onEvent（）方法中已经发送水位线了
                //发射水位线，默认200ms调用一次
                @Override
                public void onPeriodicEmit(WatermarkOutput output) {
                }
            };
        }

    }


}
