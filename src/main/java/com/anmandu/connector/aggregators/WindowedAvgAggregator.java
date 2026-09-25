package com.anmandu.connector.aggregators;

public class WindowedAvgAggregator {
    private double totalSum = 0;
    private long windowCounter = 0;

    public void aggregate(Double newValue){
        this.totalSum += newValue;
        this.windowCounter++;
    }

    public double getAverage(){
        return this.windowCounter == 0 ? 0 : this.totalSum / this.windowCounter;
    }

    public long getWindowCounter(){
        return this.windowCounter;
    }

    public void reset(){
        this.totalSum = 0;
        this.windowCounter = 0;
    }
}
