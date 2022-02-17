package com.zjc.flink.transformation;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

/**
 * @author : zoujianchao
 * @version : 1.0
 * @date : 2022/2/17
 * @description :
 */
public class TransformationApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        map(env);
        rich(env);
    
        env.execute("TransformationApp");
    }
    
    public static void rich(StreamExecutionEnvironment env) {
        DataStreamSource<String> source = env.readTextFile("data/access.log");
        env.setParallelism(1);
        SingleOutputStreamOperator<Access> mapStream = source.map(new PKMapFunction());
        
        mapStream.print();
    }
    
    public static void map(StreamExecutionEnvironment env) {
//        DataStreamSource<String> source = env.readTextFile("data/access.log");
//        SingleOutputStreamOperator<Access> mapStream = source.map(new MapFunction<String, Access>() {
//            @Override
//            public Access map(String value) throws Exception {
//                String[] splits = value.split(",");
//                Long time = Long.parseLong(splits[0].trim());
//                String domain = splits[1].trim();
//                Double traffic = Double.parseDouble(splits[2].trim());
//                return new Access(time, domain, traffic);
//            }
//        });
//
//        mapStream.print();
    
        ArrayList<Integer> list = new ArrayList<>();
        list.add(1);  // map  * 2 = 2
        list.add(2);  // map  * 2 = 4
        list.add(3);  // map  * 2 = 6
        DataStreamSource<Integer> source = env.fromCollection(list);
    
        source.map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer value) throws Exception {
                return value * 2;
            }
        }).print("t");
    }
    
    
    
    
    
    
    
    
    
    
    
}
