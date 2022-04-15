package com.zjc.flink.wm;

/**
 * @author : zoujianchao
 * @version : 1.0
 * @date : 2022/3/10
 * @description :
 */
public class IntelligentMarketing {
    private TimePeriod timePeriod;
    
    public IntelligentMarketing(TimePeriod timePeriod) {
        this.timePeriod = timePeriod;
    }
    
    public IntelligentMarketing() {
    }
    
    public TimePeriod getTimePeriod() {
        return timePeriod;
    }
    
    public void setTimePeriod(TimePeriod timePeriod) {
        this.timePeriod = timePeriod;
    }
    
    @Override
    public String toString() {
        return "IntelligentMarketing{" +
                "timePeriod=" + timePeriod +
                '}';
    }
}
