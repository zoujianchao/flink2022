package com.zjc.flink.wm;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;

/**
 * @author : zoujianchao
 * @version : 1.0
 * @date : 2022/2/24
 * @description : 输入数据的格式：时间字段,单词,次数
 */
public class EventTimeWMApp {
    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        test01(env);
//
//        env.execute("EventTimeWMApp");
    
        IntelligentMarketing intelligentMarketing = new IntelligentMarketing();
        intelligentMarketing.setTimePeriod(null);
        if (TimePeriod.DAY == intelligentMarketing.getTimePeriod()) {
            System.out.println("pppp");
        }else {
            System.out.println("wwww");
        }
    
        Date date = new Date();
        LocalDateTime dateTime = date.toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime().plusDays(30);
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        String format = dateTimeFormatter.format(dateTime);
        System.out.println(format);
    
    }
    
    public static void test01(StreamExecutionEnvironment env) {
        SingleOutputStreamOperator<String> lines = env.socketTextStream("localhost", 9587)
                .assignTimestampsAndWatermarks(
                        new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(0)) {
                            @Override
                            public long extractTimestamp(String element) {
                                return Long.parseLong(element.split(",")[0]);
                            }
                        }
                );
    
        SingleOutputStreamOperator<Tuple2<String, Integer>> mapStream = lines.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                String[] split = value.split(",");
                return Tuple2.of(split[1].trim(), Integer.parseInt(split[2].trim()));
            }
        });
        
        mapStream.keyBy(x -> x.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .sum(1)
                .print();
        
        
    }
    
    
}
