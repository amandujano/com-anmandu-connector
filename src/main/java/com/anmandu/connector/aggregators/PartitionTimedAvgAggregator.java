package com.anmandu.connector.aggregators;

import com.anmandu.connector.dto.Input;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Instant;
import java.util.HashMap;

public class PartitionTimedAvgAggregator {
    private static Logger logger = LogManager.getLogger(PartitionTimedAvgAggregator.class);
    private HashMap<String, TimedAvgAggregator> partitions = new HashMap<>();

    public  void processItem(Input input){
        Double measure = input.getMeasurement();
        String timestamp = input.getTimestamp();
        String sensor = input.getName();
        Integer windowDuration = 4;

        TimedAvgAggregator timedWindowedAvg = null;
        if(!partitions.containsKey(sensor)){
            partitions.put(sensor, new TimedAvgAggregator());
        }

        timedWindowedAvg = partitions.get(sensor);

        Instant stamp = Instant.parse(timestamp+".000z");
        boolean zeroWindow = timedWindowedAvg.processItem(stamp, measure, windowDuration);

        if(zeroWindow){
            logger.info(" [partition: {}][Closed Window] start: {} | processed items: {} | avg: {}", input.getName(),
                    timedWindowedAvg.getStart(), timedWindowedAvg.getWindowCounter(), timedWindowedAvg.getAverage());
            timedWindowedAvg.openNewWindow(stamp, measure);
        }
    }
}
