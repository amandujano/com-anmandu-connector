package com.anmandu.connector;

import com.anmandu.connector.aggregators.AvgAggregator;
import com.anmandu.connector.aggregators.TimedAvgAggregator;
import com.anmandu.connector.aggregators.WindowedAvgAggregator;
import org.apache.camel.BindToRegistry;
import org.apache.camel.builder.RouteBuilder;
import org.springframework.stereotype.Component;

@Component
public class ExtraRegistry extends RouteBuilder {
    @BindToRegistry("avgAggregator")
    private AvgAggregator avg = new AvgAggregator();

    @BindToRegistry("windowedAvgAggregator")
    private WindowedAvgAggregator windowAvg = new WindowedAvgAggregator();

    @BindToRegistry("timedAvgAggregator")
    private TimedAvgAggregator timedWindowAvg = new TimedAvgAggregator();

    @Override
    public void configure() throws Exception {

    }
}
