package com.anmandu.connector.aggregators;

import java.time.Duration;
import java.time.Instant;

public class TimedAvgAggregator {
    private double totalSum = 0;
    private long windowCounter = 0;
    private Instant start = null;

    public boolean processItem (Instant timestamp, double newValue, int duracionSegundos){
        if(start == null){
            start = timestamp;
        }

        long secondsPassed = Duration.between(start, timestamp).getSeconds();

        if(secondsPassed >= duracionSegundos){
            return true;
        }

        this.totalSum += newValue;
        this.windowCounter++;
        return false;
    }

    public double getAverage(){
        return this.windowCounter == 0 ? 0 : this.totalSum / this.windowCounter;
    }

    public long getWindowCounter(){
        return this.windowCounter;
    }

    public Instant getStart(){
        return this.start;
    }

    public void openNewWindow(Instant newStart, double newStartValue){
        this.totalSum = newStartValue;
        this.windowCounter = 1;
        this.start = newStart;
    }

    public void restart(){
        this.totalSum = 0;
        this.windowCounter = 0;
        this.start = null;
    }

}
