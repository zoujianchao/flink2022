package com.zjc.flink.window;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author : zoujianchao
 * @version : 1.0
 * @date : 2022/2/23
 * @description :
 */
public class WindowApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        test01(env);
        
        env.execute("WindowApp");
        
    }
    
    public static void test01(StreamExecutionEnvironment env) {
        env.socketTextStream("localhost", 9527)
                .map(new MapFunction<String, Integer>() {
                    @Override
                    public Integer map(String value) throws Exception {
                        return Integer.parseInt(value);
                    }
                }).windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .sum(0).print();
        
    }
    
    //hadoop,1
    //spark,1
    public static void test02(StreamExecutionEnvironment env) {
        env.socketTextStream("localhost", 9527)
                .map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String s) throws Exception {
                        String[] split = s.split(",");
                        return Tuple2.of(split[0], Integer.parseInt(split[1]));
                    }
                }).keyBy(x -> x.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .sum(1)
                .print();
        
    }
}
