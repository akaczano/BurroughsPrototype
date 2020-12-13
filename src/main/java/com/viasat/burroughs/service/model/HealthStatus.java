package com.viasat.burroughs.service.model;

public class HealthStatus {
    private boolean isHealthy;
    private HealthDetails details;

    public boolean isHealthy() {
        return isHealthy;
    }

    public void setHealthy(boolean healthy) {
        isHealthy = healthy;
    }

    public HealthDetails getDetails() {
        return details;
    }

    public void setDetails(HealthDetails details) {
        this.details = details;
    }
}
