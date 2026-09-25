package com.anmandu.connector;

import com.anmandu.connector.dto.Input;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.model.dataformat.JsonLibrary;
import org.springframework.stereotype.Component;

@Component
public class PublisherRoutes extends RouteBuilder {
    @Override
    public void configure() throws Exception {
        from("timer:sensorA-{{timer.sensorA}}?period={{timer.sensorA}}").routeId("sensor-A")
                .setHeader("sensor",simple("SENSOR-A") )
                .transform().method("mockInput", "getInput")
                .to("seda:myInternalQueue?blockWhenFull=true");

        from("timer:sensorB-{{timer.sensorB}}?period={{timer.sensorB}}").routeId("sensor-B").precondition("{{multiple.publishers}} == true")
                .setHeader("sensor",simple("SENSOR-B") )
                .transform().method("mockInput", "getInput")
                .to("seda:myInternalQueue?blockWhenFull=true");

        from("timer:sensorC-{{timer.sensorC}}?period={{timer.sensorC}}").routeId("sensor-C").precondition("{{multiple.publishers}} == true")
                .setHeader("sensor",simple("SENSOR-C") )
                .transform().method("mockInput", "getInput")
                .to("seda:myInternalQueue?blockWhenFull=true");

        from("timer:sensorD-{{timer.sensorD}}?period={{timer.sensorD}}").routeId("sensor-D").precondition("{{multiple.publishers}} == true")
                .setHeader("sensor",simple("SENSOR-D") )
                .transform().method("mockInput", "getInput")
                .to("seda:myInternalQueue?blockWhenFull=true");

        from("file://data?fileName=input_data.json&delete=false&noop=true").autoStartup(false)
                .unmarshal().json(JsonLibrary.Jackson, Input.class)
                .split(body())
                    .to("seda:myInternalQueue?blockWhenFull=true")
                .end()
                    .log("finish");

        from("file://data?fileName=input_data_sensor.json&delete=false&noop=true").autoStartup(false)
                .unmarshal().json(JsonLibrary.Jackson, Input.class)
                .split(body()) //.parallelProcessing()
                    .to("seda:myInternalQueue?blockWhenFull=true")
                .end()
                    .log("finish");
    }
}
