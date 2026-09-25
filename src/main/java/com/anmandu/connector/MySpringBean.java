package com.anmandu.connector;

import com.anmandu.connector.dto.Input;
import com.anmandu.connector.dto.Mock;
import com.anmandu.connector.services.MessagesService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

/**
 * A bean that returns a message when you call the {@link #saySomething()} method.
 * <p/>
 * Uses <tt>@Component("myBean")</tt> to register this bean with the name <tt>myBean</tt>
 * that we use in the Camel route to lookup this bean.
 */
@Component("myBean")
public class MySpringBean {

    @Value("${greeting}")
    private String say;

    @Autowired
    MessagesService messagesService;

    public Input saySomething() {

        TimeZone tz = TimeZone.getTimeZone("UTC");
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss"); // Quoted "Z" to indicate UTC, no timezone offset
        df.setTimeZone(tz);

        Input msg = new Input();
        int ok = (int)(Math.random() * ((1000 - 1) + 1)) + 1;
        msg.setId(ok);
        msg.setMeasurement((Math.random() * ((5000 - 1000) + 1)) + 1000);
        msg.setName("ok-" + ok);
        msg.setTimestamp(df.format(new Date()));
        return msg;
    }

}
