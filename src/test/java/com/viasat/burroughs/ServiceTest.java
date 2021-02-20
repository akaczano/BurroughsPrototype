package com.viasat.burroughs;

import com.viasat.burroughs.service.StatementService;

public abstract class ServiceTest {
    protected StatementService service;

    public ServiceTest() {
        String hostname = "http://localhost:8088";
        if (System.getenv().containsKey("KSQL_HOST")) {
            hostname = System.getenv("KSQL_HOST");
        }
        service = new StatementService(hostname);
    }
}
