package com.learn.window;

import com.learn.bean.WaterSensor;
import com.learn.functions.WaterSensorMapFunction;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

//全窗口函数-处理窗口函数
public class WindowProcess {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<WaterSensor> sensorDS = env.socketTextStream("127.0.0.1", 7777).map(new WaterSensorMapFunction());

        KeyedStream<WaterSensor, String> sensorKB = sensorDS.keyBy(sensor -> sensor.getId());

        SingleOutputStreamOperator<String> process = sensorKB.window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                /**
                 * public abstract class ProcessWindowFunction<IN, OUT, KEY, W extends Window> extends AbstractRichFunction
                 */.process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                    /**
                     * 全窗口函数计算逻辑：窗口触发时候才会调用一次，统一计算窗口的所有数据
                     * @param s 分组的key
                     * @param context 上下文
                     * @param iterable 存到数据
                     * @param out 采集器
                     * @throws Exception
                     */
                    @Override
                    public void process(String s, Context context, Iterable<WaterSensor> iterable, Collector<String> out) throws Exception {
                        long l = context.currentProcessingTime();
                        long startTs = context.window().getStart();
                        long endTs = context.window().getEnd();

                        String windowStart = DateFormatUtils.format(startTs, "yyyy-MM-dd HH:mm:ss.SSS");
                        String windowEnd = DateFormatUtils.format(endTs, "yyyy-MM-dd HH:mm:ss.SSS");

                        long count = iterable.spliterator().estimateSize();
                        out.collect("key=" + s + "的窗口[" + windowStart + "," + windowEnd + ")包含" + count + "条数据===>" + iterable.toString());


                    }
                });

        process.print();

        env.execute();


    }

}
