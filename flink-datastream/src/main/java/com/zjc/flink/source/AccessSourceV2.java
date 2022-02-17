package com.zjc.flink.source;

import com.zjc.flink.transformation.Access;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.util.Random;

/**
 * @author : zoujianchao
 * @version : 1.0
 * @date : 2022/2/17
 * @description :
 */
public class AccessSourceV2 implements ParallelSourceFunction<Access> {
    
    boolean running = true;
    
    @Override
    public void run(SourceContext<Access> sourceContext) throws Exception {
        String[] domains = {"imooc.com", "a.com","b.com"};
    
        Random random = new Random();
    
        while (running) {
            for (int i = 0; i < 10 ; i++) {
                Access access = new Access();
                access.setTime(1234567L);
                access.setDomain(domains[random.nextInt(domains.length)]);
                access.setTraffic(random.nextDouble() + 1000);
    
                sourceContext.collect(access);
            }
        
            Thread.sleep(5000);
        }
    }
    
    @Override
    public void cancel() {
        running = false;
    }
}
