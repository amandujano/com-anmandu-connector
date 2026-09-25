package com.anmandu.connector;

import com.anmandu.connector.aggregators.AvgAggregator;
import com.anmandu.connector.aggregators.PartitionTimedAvgAggregator;
import com.anmandu.connector.aggregators.TimedAvgAggregator;
import com.anmandu.connector.aggregators.WindowedAvgAggregator;
import com.anmandu.connector.dto.Input;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.builder.RouteBuilder;
import org.springframework.stereotype.Component;

import java.time.Instant;

@Component
public class MySpringBootRouter extends RouteBuilder {
    @Override
    public void configure() {

        // Consumer: Picks up messages from the queue asynchronously
        from("seda:myInternalQueue?size=10")
            .doTry()
                //.to("direct:AvgProcessor")
                //.to("direct:WindowedAvgProcessor")
                //.to("direct:TimeWindowedAvgProcessor")
                .to("direct:PartitionTimeWindowedAvgProcessor")
            .doCatch(Exception.class)
                .log("Failed to process: ${body} | Error: ${exception.message}")
            .end();

        // Aggregator zone
        from("direct:AvgProcessor")
            .routeId("avg-processor")
            .process(new Processor() {
                @Override
                public void process(Exchange exchange) throws Exception {
                    try {
                        Input body = exchange.getIn().getBody(Input.class);
                        Double measure = body.getMeasurement();

                        AvgAggregator avg = exchange.getContext()
                            .getRegistry()
                            .lookupByNameAndType("avgAggregator", AvgAggregator.class);

                        double partialAvg = avg.semiCompute(measure);
                        log.info("measure {} - total items: {} - partialAvg: {}", measure, avg.getCounter(), partialAvg);
                    }
                    catch (Exception e) {
                        System.out.println(e);
                    }
                }
            });

        from("direct:WindowedAvgProcessor")
            .routeId("windowed-avg-processor")
            .process(new Processor() {
                @Override
                public void process(Exchange exchange) throws Exception {
                    try {
                        Input body = exchange.getIn().getBody(Input.class);
                        Double measure = body.getMeasurement();
                        String timestamp = body.getTimestamp();

                        WindowedAvgAggregator windowedAvg = exchange.getContext()
                            .getRegistry()
                            .lookupByNameAndType("windowedAvgAggregator", WindowedAvgAggregator.class);

                        windowedAvg.aggregate(measure);

                        if(windowedAvg.getWindowCounter() == 10){
                            Double avg = windowedAvg.getAverage();
                            log.info("[window 10s completed] last timestamp: {} avg: {}", timestamp, avg);
                            windowedAvg.reset();
                        }
                    }
                    catch (Exception e) {
                        System.out.println(e);
                    }
                }
            });

        from("direct:TimeWindowedAvgProcessor")
            .routeId("time-windowed-avg-processor")
            .process(new Processor() {
                @Override
                public void process(Exchange exchange) throws Exception {
                    try {
                        Input body = exchange.getIn().getBody(Input.class);
                        Double measure = body.getMeasurement();
                        String timestamp = body.getTimestamp();
                        Integer windowDuration = 4;

                        TimedAvgAggregator timedWindowedAvg = exchange.getContext()
                                .getRegistry()
                                .lookupByNameAndType("timedAvgAggregator", TimedAvgAggregator.class);

                        Instant stamp = Instant.parse(timestamp+".000z");
                        boolean zeroWindow = timedWindowedAvg.processItem(stamp, measure, windowDuration);

                        if(zeroWindow){
                            log.info(" [Closed Window] start: {} | processed items: {} | avg: {}",
                                    timedWindowedAvg.getStart(), timedWindowedAvg.getWindowCounter(), timedWindowedAvg.getAverage());
                            timedWindowedAvg.openNewWindow(stamp, measure);
                        }

                        Boolean isLastItem = exchange.getProperty(Exchange.SPLIT_COMPLETE, Boolean.class);
                        if (Boolean.TRUE.equals(isLastItem)) {
                            log.info(" [Final window: closed] start: {} | processed items: {} | avg: {}",
                                    timedWindowedAvg.getStart(), timedWindowedAvg.getWindowCounter(), timedWindowedAvg.getAverage());
                        }
                    }
                    catch (Exception e) {
                        System.out.println(e);
                    }
                }
            });

        from("direct:PartitionTimeWindowedAvgProcessor")
            .routeId("parititon-time-windowed-avg-processor")
            .process(new Processor() {
                @Override
                public void process(Exchange exchange) throws Exception {
                    try {
                        Input body = exchange.getIn().getBody(Input.class);
                        PartitionTimedAvgAggregator partitionTimedAvgAggregator = exchange.getContext()
                                .getRegistry()
                                .lookupByNameAndType("partitionTimedAvgAggregator", PartitionTimedAvgAggregator.class);

                        //Boolean isLastItem = exchange.getProperty(Exchange.SPLIT_COMPLETE, Boolean.class);
                        partitionTimedAvgAggregator.processItem(body);
                    }
                    catch (Exception e) {
                        System.out.println(e);
                    }
                }
            });
    }
}
