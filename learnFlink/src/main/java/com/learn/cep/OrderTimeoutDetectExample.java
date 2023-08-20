package com.learn.cep;

import com.learn.bean.OrderEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.functions.TimedOutPartialMatchHandler;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

public class OrderTimeoutDetectExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        KeyedStream<OrderEvent, String> stream = env
                .fromElements(
                        new OrderEvent("user_1", "order_1", "create", 1000L),
                        new OrderEvent("user_2", "order_2", "create", 2000L),
                        new OrderEvent("user_2", "order_2", "pay", 16 * 60 * 1000L),
                        new OrderEvent("user_1", "order_1", "modify", 10 * 1000L),
                        new OrderEvent("user_1", "order_1", "pay", 60 * 1000L),
                        new OrderEvent("user_2", "order_3", "create", 10 * 60 * 1000L),
                        new OrderEvent("user_2", "order_3", "pay", 20 * 60 * 1000L)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<OrderEvent>forMonotonousTimestamps()
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<OrderEvent>() {
                                            @Override
                                            public long extractTimestamp(OrderEvent event, long l) {
                                                return event.timestamp;
                                            }
                                        }
                                )
                )
                .keyBy(order -> order.orderId);// 按照订单ID分组

        //1.定义Pattern
        Pattern<OrderEvent, OrderEvent> pattern = Pattern
                .<OrderEvent>begin("create") //首先是下单事件
                .where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent orderEvent) throws Exception {
                        return orderEvent.eventType.equals("create");
                    }
                })
                .followedBy("pay")  //之后是支付事件；中间可以修改订单，宽松近邻
                .where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent orderEvent) throws Exception {
                        return orderEvent.eventType.equals("pay");
                    }
                })
                .within(Time.minutes(15));  //限制在15分钟以内

        //2.将Pattern应用到流上，检测匹配的复杂事件，得到一个PatternStream
        PatternStream<OrderEvent> patternStream = CEP.pattern(stream, pattern);

        //3.将匹配到的，和超时部分匹配的复杂事件提取出来，然后包装成提示信息输出
        SingleOutputStreamOperator<String> payedOrderStream = patternStream.process(new OrderPayPatternProcessFunction());

        //4.定义一个侧输出流标签，用于标识超时测输出流
        OutputTag<String> timeOutTag = new OutputTag<String>("timeout") {
        };

        //5.将正常匹配和超时部分匹配的处理结果流打印输出
        payedOrderStream.print("payed");
        payedOrderStream.getSideOutput(timeOutTag).print("timeout");

        env.execute();
    }

    //实现自定义的PatternProcessFunction，需要实现TimedOutPartialMatchHandler接口
    private static class OrderPayPatternProcessFunction extends PatternProcessFunction<OrderEvent, String> implements TimedOutPartialMatchHandler<OrderEvent> {
        //处理正常的匹配事件
        @Override
        public void processMatch(Map<String, List<OrderEvent>> map, Context context, Collector<String> out) throws Exception {
            OrderEvent payEvent = map.get("pay").get(0);
            out.collect("订单：" + payEvent.orderId + " 已支付！");
        }

        //处理超时未支付事件
        @Override
        public void processTimedOutMatch(Map<String, List<OrderEvent>> map, Context ctx) throws Exception {
            OrderEvent createEvent = map.get("create").get(0);
            ctx.output(new OutputTag<String>("timeout") {
                       },
                    "订单：" + createEvent.orderId + " 超时未支付，用户为：" + createEvent.userId);

        }
    }
}