package com.zjc.flink.source;

import com.zjc.flink.transformation.Access;
import com.zjc.flink.transformation.Student;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.NumberSequenceIterator;

import java.util.Properties;

/**
 * @author : zoujianchao
 * @version : 1.0
 * @date : 2022/2/17
 * @description :
 */
public class SourceApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        test01(env);
//        test02(env);
//        test03(env);
        test04(env);
        env.execute("SourceApp");
    }
    
    public static void test01(StreamExecutionEnvironment env) {
        DataStreamSource<String> source = env.socketTextStream("localhost", 9527);
        System.out.println("source: " + source.getParallelism());
        env.setParallelism(4);
        // 接收socket过来的数据，一行一个单词， 把pk的过滤掉
        SingleOutputStreamOperator<String> filterStream = source.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                return !"pk".equals(value);
            }
        });
    
        System.out.println("filter: " + filterStream.getParallelism());
    
        filterStream.print();
    }
    
    public static void test02(StreamExecutionEnvironment env) {
        DataStreamSource<Long> source = env.fromParallelCollection(new NumberSequenceIterator(1, 10), Long.class);
        System.out.println("source: " + source.getParallelism());
    
        SingleOutputStreamOperator<Long> filterStream = source.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long value) throws Exception {
                return value >= 5;
            }
        });
    
        System.out.println("filter: " + filterStream.getParallelism());
        filterStream.print();
    }
    
    public static void test03(StreamExecutionEnvironment env) {
//        DataStreamSource<Access> source = env.addSource(new AccessSource());
        DataStreamSource<Access> source = env.addSource(new AccessSourceV2()).setParallelism(2);
    
        System.out.println(source.getParallelism());
        source.print();
    }
    
    public static void test04(StreamExecutionEnvironment env) {
        DataStreamSource<Student> source = env.addSource(new StudentSource());
        System.out.println(source.getParallelism());
        source.print();
        
    }
    
    public static void test05(StreamExecutionEnvironment env) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "ruozedata001:9092,ruozedata001:9093,ruozedata001:9094");
        properties.setProperty("group.id", "test");
        DataStreamSource<String> source = env.addSource(new FlinkKafkaConsumer<>("flinktopic", new SimpleStringSchema(), properties));
        System.out.println(source.getParallelism());
        source.print();
    }
    
}
