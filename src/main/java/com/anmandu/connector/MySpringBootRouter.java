package com.anmandu.connector;

import com.anmandu.connector.aggregators.AvgAggregator;
import com.anmandu.connector.aggregators.TimedAvgAggregator;
import com.anmandu.connector.aggregators.WindowedAvgAggregator;
import com.anmandu.connector.dto.Input;
import org.apache.camel.BindToRegistry;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.model.dataformat.JsonLibrary;
import org.springframework.stereotype.Component;

import java.time.Instant;

/**
 * A simple Camel route that triggers from a timer and calls a bean and prints to system out.
 * <p/>
 * Use <tt>@Component</tt> to make Camel auto detect this route when starting.
 */
@Component
public class MySpringBootRouter extends RouteBuilder {

    @BindToRegistry("avgAggregator")
    private AvgAggregator avg = new AvgAggregator();

    @BindToRegistry("windowedAvgAggregator")
    private WindowedAvgAggregator windowAvg = new WindowedAvgAggregator();

    @BindToRegistry("timedAvgAggregator")
    private TimedAvgAggregator timedWindowAvg = new TimedAvgAggregator();

    @Override
    public void configure() {
        from("timer:hello?period={{timer.period}}").routeId("init")
            .transform().method("myBean", "saySomething")
            .to("seda:myInternalQueue?blockWhenFull=true")
            .log("---");

        from("file://data?fileName=input_data.json&delete=false&noop=true")
            .unmarshal().json(JsonLibrary.Jackson, Input.class)
            .split(body()).parallelProcessing()

                .setHeader("message-id", simple("${header.batch-id}-${random(1,10000)}"))
                .to("seda:myInternalQueue?blockWhenFull=true")
            .end()
            .log("finish");

        // Consumer: Picks up messages from the queue asynchronously
        from("seda:myInternalQueue?size=10")
            .doTry()
                //.to("direct:AvgProcessor")
                .to("direct:WindowedAvgProcessor")
                //.to("direct:TimeWindowedAvgProcessor")
            .doCatch(Exception.class)
                .log("Failed to process: ${body} | Error: ${exception.message}")
            .end();

        from("direct:AvgProcessor")
            .routeId("avg-processor")
            .process(new Processor() {
                @Override
                public void process(Exchange exchange) throws Exception {
                    try {
                        Input body = exchange.getIn().getBody(Input.class);
                        Double measure = body.getMeasurement();

                        // Recuperar el acumulador único desde el Registry de Camel
                        AvgAggregator avg = exchange.getContext()
                            .getRegistry()
                            .lookupByNameAndType("avgAggregator", AvgAggregator.class);

                        double partialAvg = avg.semiCompute(measure);
                        exchange.getIn().setHeader("partialAvg", partialAvg);
                        log.info("consume {} - partialAvg: {}", body.getId(), partialAvg);
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

                        // Recuperar el acumulador único desde el Registry de Camel
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

                        // Recuperar el acumulador único desde el Registry de Camel
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
    }
}
