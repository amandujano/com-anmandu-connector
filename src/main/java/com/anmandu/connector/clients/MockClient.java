package com.anmandu.connector.clients;

import com.anmandu.connector.dto.Mock;
import org.springframework.stereotype.Service;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@Service
public class MockClient {
    private List<Mock> msgs = new ArrayList<>();

    public MockClient(){
        msgs.add(this.createMock(1));
        msgs.add(this.createMock(2));
        msgs.add(this.createMock(3));
        msgs.add(this.createMock(4));
        msgs.add(this.createMock(5));
    }

    public Mock get(Integer id) {
        return msgs.stream().filter(item -> Objects.equals(item.getId(), id)).toList().get(0);
    }

    public List<Mock> getAll() {
        return msgs;
    }

    private Mock createMock(Integer id){
        long offset = Timestamp.valueOf("2012-01-01 00:00:00").getTime();
        long end = Timestamp.valueOf("2013-01-01 00:00:00").getTime();
        long diff = end - offset + 1;
        Timestamp rand = new Timestamp(offset + (long)(Math.random() * diff));

        Mock mock = new Mock();
        mock.setId(id);
        mock.setMeasurement(Math.random());
        mock.setTimestamp(rand.toString());
        return mock;
    }
}
