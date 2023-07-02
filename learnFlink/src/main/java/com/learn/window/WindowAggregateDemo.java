package com.learn.window;

import com.learn.bean.WaterSensor;
import com.learn.functions.WaterSensorMapFunction;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class WindowAggregateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> sensorDS = env.socketTextStream("127.0.0.1", 7777).map(new WaterSensorMapFunction());

        KeyedStream<WaterSensor, String> waterSensorStringKeyedStream = sensorDS.keyBy(sensor -> sensor.getId());

        //1.窗口分配器
        WindowedStream<WaterSensor, String, TimeWindow> windowDS = waterSensorStringKeyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));
        //2.窗口函数：增量聚合 Aggregate
        /**
         * 1、属于本窗口的第一条数据来，创建创建窗口，创建累加器
         * 2、增量聚合：来一条计算一条，调用一次add方法
         * 3、窗口输出时调用一次getresult方法
         * 4、输入、中间累加器、输出类型可以不同，非常灵活
         */
        SingleOutputStreamOperator<String> aggregate = windowDS.aggregate(
                /**
                 * 第一个类型：输入数据的类型
                 * 第二个类型：累加器的类型，存储的中间计算结果的类型
                 * 第三个类型：输出的类型
                 */
                new AggregateFunction<WaterSensor, Integer, String>() {
                    /**
                     * 创建累加器，初始化累加器
                     * @return
                     */
                    @Override
                    public Integer createAccumulator() {
                        System.out.println("创建累加器");
                        return 0;
                    }

                    /**
                     * 聚合逻辑
                     * @param waterSensor
                     * @param integer
                     * @return
                     */
                    @Override
                    public Integer add(WaterSensor waterSensor, Integer integer) {
                        System.out.println("调用add方法，value=" + integer);
                        return integer + waterSensor.getVc();
                    }

                    /**
                     * 获取最终结果，窗口出发时输出
                     * @param integer
                     * @return
                     */
                    @Override
                    public String getResult(Integer integer) {
                        System.out.println("调用getResult方法");
                        return integer.toString();
                    }

                    @Override
                    public Integer merge(Integer integer, Integer acc1) {
                        //只有会话窗口才会用到
                        System.out.println("调用merge方法");
                        return null;
                    }
                });

        aggregate.print();

        env.execute();

    }
}
