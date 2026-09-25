package com.anmandu.connector.aggregators;

public class AvgAggregator {

    private Double totalSum = 0d;
    private long totalCount = 0;

    public synchronized Double semiCompute(Double newValue){
        this.totalSum += newValue;
        this.totalCount++;
        return this.totalSum / this.totalCount;
    }

    public synchronized Double getAverage(){
        return this.totalCount == 0 ? 0 : this.totalSum / this.totalCount;
    }

    public synchronized long getCounter(){
        return this.totalCount;
    }
}
