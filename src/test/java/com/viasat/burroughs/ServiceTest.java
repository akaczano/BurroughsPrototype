package com.viasat.burroughs;

import com.viasat.burroughs.service.StatementService;

public abstract class ServiceTest {
    protected StatementService service;

    public ServiceTest() {
        service = new StatementService("http://localhost:8088");
    }
}
