package com.anmandu.connector;

import com.anmandu.connector.dto.Input;
import org.apache.camel.Header;
import org.springframework.stereotype.Component;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

@Component("mockInput")
public class MockInput {

    public Input getInput(@Header("sensor") String sensor) {

        TimeZone tz = TimeZone.getTimeZone("UTC");
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss"); // Quoted "Z" to indicate UTC, no timezone offset
        df.setTimeZone(tz);

        Input msg = new Input();
        int ok = (int)(Math.random() * ((1000 - 1) + 1)) + 1;
        msg.setId(ok);
        msg.setMeasurement((Math.random() * ((5000 - 1000) + 1)) + 1000);
        msg.setName(sensor);
        msg.setTimestamp(df.format(new Date()));
        return msg;
    }

}
