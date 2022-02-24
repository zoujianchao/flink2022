package com.zjc.flink.window;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author : zoujianchao
 * @version : 1.0
 * @date : 2022/2/23
 * @description :
 */
public class PKProcessWindowFunction extends ProcessWindowFunction<Tuple2<String, Integer>, String, String, TimeWindow> {
    @Override
    public void process(String s, Context context, Iterable<Tuple2<String, Integer>> iterable, Collector<String> collector) throws Exception {
        System.out.println("----process invoked...----");
        
        int maxValue = Integer.MIN_VALUE;
        
        for (Tuple2<String, Integer> element : iterable) {
            maxValue = Math.max(element.f1, maxValue);
        }
        collector.collect("当前窗口的最大值是:" + maxValue);
    }
}
