package com.viasat.burroughs.service.model;

public class HealthDetails {
    private HealthStatus metastore;
    private HealthStatus kafka;

    public HealthStatus getMetastore() {
        return metastore;
    }

    public void setMetastore(HealthStatus metastore) {
        this.metastore = metastore;
    }

    public HealthStatus getKafka() {
        return kafka;
    }

    public void setKafka(HealthStatus kafka) {
        this.kafka = kafka;
    }
}
