package com.anmandu.connector.services;

import com.anmandu.connector.clients.MockClient;
import com.anmandu.connector.dto.Mock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class MessagesService implements IService {

    @Autowired
    private MockClient mockClient;

    @Override
    public Mock get() {
        return this.mockClient.get(1);
    }
}
