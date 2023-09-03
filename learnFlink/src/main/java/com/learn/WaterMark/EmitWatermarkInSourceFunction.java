package com.learn.WaterMark;

import com.learn.bean.ClickSource;
import com.learn.bean.Event;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.util.Calendar;
import java.util.Random;

/**
 * 在自定义的数据源中发送水位线
 */
public class EmitWatermarkInSourceFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new CliskSourceWithWatermark())
                .print();

        env.execute();
    }

    public static class CliskSourceWithWatermark implements SourceFunction<Event> {
        private boolean running = true;

        @Override
        public void run(SourceContext<Event> ctx) throws Exception {
            Random random = new Random();
            String[] userArray = {"Mary", "bob", "Alice"};
            String[] urlArray = {"./home", "./cart", "./prod?id=1"};
            while (running) {
                //毫秒时间戳
                long currTs = Calendar.getInstance().getTimeInMillis();
                String username = userArray[random.nextInt(userArray.length)];
                String url = urlArray[random.nextInt(urlArray.length)];

                //正常发送数据的方式
//                ctx.collect(new Event());

                Event event = new Event(username, url, currTs);
                //(1)collectWithTimestamp
                ctx.collectWithTimestamp(event, event.timestamp);
                //(2)发送水位线
                ctx.emitWatermark(new Watermark(event.timestamp - 1L));
                Thread.sleep(1000L);
            }

        }

        @Override
        public void cancel() {
            running = false;
        }
    }

}
